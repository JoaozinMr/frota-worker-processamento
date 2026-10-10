using System.Net.Http.Json;
using FrotaWorker.Models;
using FrotaWorker.Options;
using Microsoft.Extensions.Options;

namespace FrotaWorker.Services;

/// <summary>Cliente HTTP síncrono da API de Alertas (Polly configurado no DI).</summary>
public sealed class AlertasClient(HttpClient http, IOptions<AlertasOptions> options)
{
    public const string CorrelationHeader = "X-Correlation-ID";

    public async Task EnviarAsync(AlertaPayload payload, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, options.Value.Path)
        {
            Content = JsonContent.Create(payload)
        };
        if (!string.IsNullOrEmpty(payload.CorrelationId))
            request.Headers.Add(CorrelationHeader, payload.CorrelationId);

        using var response = await http.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();
    }
}

/// <summary>
/// Idempotência: só envia o alerta se não houve o mesmo (veículo, tipo) na janela de deduplicação.
/// A chave é gravada somente após o POST bem-sucedido, para que falhas sejam reprocessadas.
/// </summary>
public sealed class AlertDispatcher(
    RedisStateStore state, AlertasClient client, ILogger<AlertDispatcher> logger)
{
    public async Task<bool> DispatchAsync(AlertaPayload payload, CancellationToken ct)
    {
        if (await state.AlertaJaEnviadoAsync(payload.IdVeiculo, payload.TipoInfracao))
        {
            logger.LogDebug("Alerta {Tipo} para {Veiculo} deduplicado", payload.TipoInfracao, payload.IdVeiculo);
            return false;
        }

        await client.EnviarAsync(payload, ct);
        await state.MarcarAlertaEnviadoAsync(payload.IdVeiculo, payload.TipoInfracao);
        logger.LogWarning("Alerta {Tipo} enviado para {Veiculo}: {Descricao}",
            payload.TipoInfracao, payload.IdVeiculo, payload.Descricao);
        return true;
    }
}
