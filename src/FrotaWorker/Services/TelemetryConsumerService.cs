using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using FrotaWorker.Models;
using FrotaWorker.Options;
using Microsoft.Extensions.Options;
using Npgsql;
using Polly.Timeout;
using Serilog.Context;
using StackExchange.Redis;

namespace FrotaWorker.Services;

public sealed class ConsumerState
{
    private long _lastPollTicks = DateTime.UtcNow.Ticks;
    public DateTime LastPollUtc
    {
        get => new(Interlocked.Read(ref _lastPollTicks), DateTimeKind.Utc);
        set => Interlocked.Exchange(ref _lastPollTicks, value.Ticks);
    }
    public volatile bool Assigned;
}

/// <summary>
/// Consumer Group com commit manual: o offset só é confirmado depois do processamento
/// (e do flush do batch para o PostgreSQL), garantindo at-least-once + processamento idempotente.
/// </summary>
public sealed class TelemetryConsumerService(
    IOptions<KafkaOptions> options,
    TelemetryProcessor processor,
    PostgresBatchWriter writer,
    DlqPublisher dlq,
    ConsumerState state,
    ILogger<TelemetryConsumerService> logger) : BackgroundService
{
    private static readonly string[] CorrelationHeaders = ["X-Correlation-ID", "correlation-id", "correlation_id"];
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly KafkaOptions _opt = options.Value;
    private readonly Dictionary<TopicPartition, Offset> _pending = new();
    private int _pendingCount;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => RunAsync(stoppingToken), stoppingToken);

    private async Task RunAsync(CancellationToken ct)
    {
        await writer.SchemaReady.WaitAsync(ct);

        var config = new ConsumerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            GroupId = _opt.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            SessionTimeoutMs = 15_000,
            MaxPollIntervalMs = 300_000
        };

        using var consumer = new ConsumerBuilder<string?, byte[]?>(config)
            .SetPartitionsAssignedHandler((_, parts) =>
            {
                state.Assigned = parts.Count > 0;
                logger.LogInformation("Partições atribuídas: {Parts}", string.Join(",", parts.Select(p => p.Partition.Value)));
            })
            .SetPartitionsRevokedHandler((c, parts) =>
            {
                logger.LogInformation("Partições revogadas: {Parts}", string.Join(",", parts.Select(p => p.Partition.Value)));
                // Confirma o que já foi processado antes de perder as partições.
                CommitPending(c, parts.Select(p => p.TopicPartition).ToHashSet());
            })
            .SetPartitionsLostHandler((_, parts) =>
            {
                logger.LogWarning("Partições perdidas: {Parts}", string.Join(",", parts.Select(p => p.Partition.Value)));
                foreach (var p in parts) _pending.Remove(p.TopicPartition);
            })
            .SetErrorHandler((_, e) => logger.LogError("Kafka error: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_opt.Topic);
        logger.LogInformation("Consumindo {Topic} no grupo {Group}", _opt.Topic, _opt.GroupId);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string?, byte[]?>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Erro ao consumir");
                    continue;
                }
                catch (OperationCanceledException) { break; }

                state.LastPollUtc = DateTime.UtcNow;

                if (result is null || result.IsPartitionEOF)
                {
                    if (_pendingCount > 0) CommitPending(consumer, null);
                    continue;
                }

                await HandleWithRetriesAsync(result, ct);

                _pending[result.TopicPartition] = result.Offset + 1;
                if (++_pendingCount >= _opt.CommitEveryMessages) CommitPending(consumer, null);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            CommitPending(consumer, null);
            consumer.Close();
        }
    }

    private async Task HandleWithRetriesAsync(ConsumeResult<string?, byte[]?> result, CancellationToken ct)
    {
        var correlationId = ReadCorrelationId(result.Message.Headers);
        using var _ = LogContext.PushProperty("CorrelationId", correlationId);
        using var __ = LogContext.PushProperty("IdVeiculo", result.Message.Key);

        var attempt = 0;
        var transientDelay = TimeSpan.FromSeconds(1);

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            attempt++;
            try
            {
                var msg = Parse(result, correlationId);
                await processor.ProcessAsync(msg, ct);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (InvalidTelemetryException ex)
            {
                // Irrecuperável: payload inválido que passou pela API.
                await dlq.PublishAsync(result, ex, ct);
                return;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                // Dependência indisponível: não confirma o offset, tenta de novo (backoff até 10s).
                logger.LogWarning(ex, "Falha transitória (tentativa {Attempt}); reprocessando em {Delay}s",
                    attempt, transientDelay.TotalSeconds);
                await Task.Delay(transientDelay, ct);
                transientDelay = TimeSpan.FromSeconds(Math.Min(10, transientDelay.TotalSeconds * 2));
                state.LastPollUtc = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                if (attempt >= _opt.MaxProcessingAttempts)
                {
                    await dlq.PublishAsync(result, ex, ct);
                    return;
                }
                logger.LogWarning(ex, "Falha no processamento (tentativa {Attempt}/{Max})",
                    attempt, _opt.MaxProcessingAttempts);
            }
        }
    }

    private static TelemetriaMensagem Parse(ConsumeResult<string?, byte[]?> result, string? correlationId)
    {
        if (result.Message.Value is null || result.Message.Value.Length == 0)
            throw new InvalidTelemetryException("payload vazio");
        try
        {
            var msg = JsonSerializer.Deserialize<TelemetriaMensagem>(result.Message.Value, Json)
                      ?? throw new InvalidTelemetryException("payload nulo");
            return msg with { CorrelationId = correlationId };
        }
        catch (JsonException ex)
        {
            throw new InvalidTelemetryException($"JSON inválido: {ex.Message}", ex);
        }
    }

    private static string? ReadCorrelationId(Headers? headers)
    {
        if (headers is null) return null;
        foreach (var name in CorrelationHeaders)
            if (headers.TryGetLastBytes(name, out var bytes))
                return Encoding.UTF8.GetString(bytes);
        return null;
    }

    private static bool IsTransient(Exception ex) => ex switch
    {
        HttpRequestException h => h.StatusCode is null || (int)h.StatusCode >= 500 || (int)h.StatusCode == 429,
        TimeoutRejectedException or TaskCanceledException or TimeoutException => true,
        RedisException or NpgsqlException or KafkaException => true,
        _ => false
    };

    /// <summary>Faz flush do batch (cold path) e só então confirma os offsets.</summary>
    private void CommitPending(IConsumer<string?, byte[]?> consumer, HashSet<TopicPartition>? only)
    {
        if (_pending.Count == 0) return;
        try
        {
            writer.FlushAsync().GetAwaiter().GetResult();

            var offsets = _pending
                .Where(kv => only is null || only.Contains(kv.Key))
                .Select(kv => new TopicPartitionOffset(kv.Key, kv.Value))
                .ToList();
            if (offsets.Count == 0) return;

            consumer.Commit(offsets);
            foreach (var o in offsets) _pending.Remove(o.TopicPartition);
            _pendingCount = _pending.Count == 0 ? 0 : _pendingCount;
            logger.LogDebug("Offsets confirmados: {Offsets}", string.Join(",", offsets.Select(o => $"{o.Partition.Value}:{o.Offset.Value}")));
        }
        catch (Exception ex)
        {
            // Offset não confirmado: mensagens poderão ser reentregues (processamento é idempotente).
            logger.LogError(ex, "Falha ao confirmar offsets; será tentado novamente");
        }
    }
}
