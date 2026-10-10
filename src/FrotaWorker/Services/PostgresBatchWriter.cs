using FrotaWorker.Models;
using FrotaWorker.Options;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace FrotaWorker.Services;

/// <summary>
/// Cold path: bufferiza em memória e faz batch insert (a cada N registros ou T ms).
/// Inserts são idempotentes via PK (id_veiculo, ts) + ON CONFLICT DO NOTHING.
/// </summary>
public sealed class PostgresBatchWriter(
    NpgsqlDataSource dataSource,
    IOptions<PostgresOptions> options,
    ILogger<PostgresBatchWriter> logger) : BackgroundService
{
    private readonly PostgresOptions _opt = options.Value;
    private readonly object _bufferLock = new();
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private List<TelemetriaMensagem> _buffer = new();
    private readonly TaskCompletionSource _schemaReady = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task SchemaReady => _schemaReady.Task;

    private const string Ddl = @"
CREATE TABLE IF NOT EXISTS telemetria (
    id_veiculo     TEXT             NOT NULL,
    ts             TIMESTAMPTZ      NOT NULL,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    velocidade     DOUBLE PRECISION NOT NULL,
    status_motor   TEXT,
    correlation_id TEXT,
    PRIMARY KEY (id_veiculo, ts)
);";

    public async Task AddAsync(TelemetriaMensagem msg)
    {
        bool full;
        int count;
        lock (_bufferLock)
        {
            _buffer.Add(msg);
            count = _buffer.Count;
            full = count >= _opt.BatchSize;
        }

        if (!full) return;
        try
        {
            await FlushAsync();
        }
        catch (Exception ex) when (count < _opt.BatchSize * 10)
        {
            // Buffer preservado; será reenviado no próximo flush (timer/commit).
            logger.LogWarning(ex, "Falha no flush do batch; {Count} registros mantidos em buffer", count);
        }
    }

    public async Task FlushAsync(CancellationToken ct = default)
    {
        await _flushLock.WaitAsync(ct);
        try
        {
            List<TelemetriaMensagem> batch;
            lock (_bufferLock)
            {
                if (_buffer.Count == 0) return;
                batch = _buffer;
                _buffer = new List<TelemetriaMensagem>();
            }

            try
            {
                await InsertAsync(batch, ct);
                logger.LogDebug("Batch insert de {Count} registros", batch.Count);
            }
            catch
            {
                lock (_bufferLock) _buffer.InsertRange(0, batch);
                throw;
            }
        }
        finally
        {
            _flushLock.Release();
        }
    }

    private async Task InsertAsync(List<TelemetriaMensagem> batch, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(@"
INSERT INTO telemetria (id_veiculo, ts, latitude, longitude, velocidade, status_motor, correlation_id)
SELECT * FROM unnest(@ids, @ts, @lat, @lon, @vel, @status, @corr)
ON CONFLICT DO NOTHING");
        cmd.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
            { Value = batch.Select(b => b.IdVeiculo).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter("ts", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz)
            { Value = batch.Select(b => b.Timestamp.UtcDateTime).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter("lat", NpgsqlDbType.Array | NpgsqlDbType.Double)
            { Value = batch.Select(b => b.Latitude).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter("lon", NpgsqlDbType.Array | NpgsqlDbType.Double)
            { Value = batch.Select(b => b.Longitude).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter("vel", NpgsqlDbType.Array | NpgsqlDbType.Double)
            { Value = batch.Select(b => b.Velocidade).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter("status", NpgsqlDbType.Array | NpgsqlDbType.Text)
            { Value = batch.Select(b => b.StatusMotor).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter("corr", NpgsqlDbType.Array | NpgsqlDbType.Text)
            { Value = batch.Select(b => b.CorrelationId).ToArray() });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task EnsureSchemaAsync(CancellationToken ct)
    {
        while (true)
        {
            try
            {
                await using (var cmd = dataSource.CreateCommand(Ddl))
                    await cmd.ExecuteNonQueryAsync(ct);

                try
                {
                    await using var hyper = dataSource.CreateCommand(
                        "SELECT create_hypertable('telemetria', 'ts', if_not_exists => true)");
                    await hyper.ExecuteNonQueryAsync(ct);
                    logger.LogInformation("Hypertable TimescaleDB garantida para 'telemetria'");
                }
                catch (PostgresException)
                {
                    logger.LogInformation("TimescaleDB indisponível; usando tabela PostgreSQL comum");
                }

                _schemaReady.TrySetResult();
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Aguardando PostgreSQL para criar schema...");
                await Task.Delay(TimeSpan.FromSeconds(3), ct);
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureSchemaAsync(stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_opt.FlushIntervalMs));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try { await FlushAsync(stoppingToken); }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(ex, "Falha no flush periódico do PostgreSQL");
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        try { await FlushAsync(cancellationToken); }
        catch (Exception ex) { logger.LogError(ex, "Flush final falhou"); }
    }
}
