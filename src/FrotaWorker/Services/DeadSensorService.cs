using FrotaWorker.Models;
using FrotaWorker.Options;
using Microsoft.Extensions.Options;

namespace FrotaWorker.Services;

/// <summary>
/// Bônus: job periódico que detecta "sensor morto" (sem eventos na janela) consultando
/// a última atualização no Redis. A deduplicação evita alertas repetidos entre réplicas.
/// </summary>
public sealed class DeadSensorService(
    RedisStateStore state,
    AlertDispatcher alerts,
    IOptions<RulesOptions> options,
    ILogger<DeadSensorService> logger) : BackgroundService
{
    private readonly RulesOptions _rules = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_rules.DeadSensorCheckIntervalSeconds));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try { await CheckAsync(stoppingToken); }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(ex, "Falha na verificação de sensores mortos");
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task CheckAsync(CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var thresholdMs = _rules.DeadSensorThresholdSeconds * 1000L;

        await foreach (var v in state.ScanVeiculosAsync().WithCancellation(ct))
        {
            var silencio = now - v.AtualizadoEmMs;
            if (silencio <= thresholdMs) continue;
            if (string.Equals(v.StatusMotor, "DESLIGADO", StringComparison.OrdinalIgnoreCase)) continue;

            try
            {
                await alerts.DispatchAsync(new AlertaPayload
                {
                    IdVeiculo = v.IdVeiculo,
                    TipoInfracao = TiposInfracao.SensorMorto,
                    Descricao = $"Sem telemetria há {silencio / 1000}s (limite {_rules.DeadSensorThresholdSeconds}s)",
                    Latitude = v.Latitude,
                    Longitude = v.Longitude,
                    Velocidade = v.Velocidade,
                    Timestamp = DateTimeOffset.UtcNow,
                    CorrelationId = Guid.NewGuid().ToString()
                }, ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Não foi possível enviar alerta de sensor morto para {Veiculo}", v.IdVeiculo);
            }
        }
    }
}
