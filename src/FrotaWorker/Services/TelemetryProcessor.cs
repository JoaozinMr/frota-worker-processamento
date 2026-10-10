using FrotaWorker.Models;

namespace FrotaWorker.Services;

/// <summary>Pipeline por mensagem: hot path (Redis), cold path (buffer), regras e alertas.</summary>
public sealed class TelemetryProcessor(
    RedisStateStore state,
    PostgresBatchWriter writer,
    RuleEvaluator rules,
    AlertDispatcher alerts)
{
    public async Task ProcessAsync(TelemetriaMensagem msg, CancellationToken ct)
    {
        msg.Validate();

        await state.UpdatePositionAsync(msg);
        await writer.AddAsync(msg);

        foreach (var infracao in rules.Evaluate(msg))
        {
            await alerts.DispatchAsync(new AlertaPayload
            {
                IdVeiculo = msg.IdVeiculo,
                TipoInfracao = infracao.Tipo,
                Descricao = infracao.Descricao,
                Latitude = msg.Latitude,
                Longitude = msg.Longitude,
                Velocidade = msg.Velocidade,
                Timestamp = msg.Timestamp,
                CorrelationId = msg.CorrelationId
            }, ct);
        }
    }
}
