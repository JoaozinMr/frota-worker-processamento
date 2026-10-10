using System.Globalization;
using FrotaWorker.Models;
using FrotaWorker.Options;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace FrotaWorker.Services;

public sealed record VeiculoEstado(
    string IdVeiculo, double Latitude, double Longitude, double Velocidade,
    string? StatusMotor, long TimestampMs, long AtualizadoEmMs);

/// <summary>Hot path: último estado por veículo e chaves de deduplicação de alertas.</summary>
public sealed class RedisStateStore(IConnectionMultiplexer redis, IOptions<RulesOptions> rules)
{
    // Sobrescreve a posição apenas se o evento for mais novo (tolera reentrega/fora de ordem);
    // 'atualizado_em_ms' (liveness) é sempre renovado.
    private const string UpdateScript = @"
redis.call('HSET', KEYS[1], 'atualizado_em_ms', ARGV[7])
local cur = redis.call('HGET', KEYS[1], 'timestamp_ms')
if cur and tonumber(cur) >= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'timestamp_ms', ARGV[1], 'latitude', ARGV[2], 'longitude', ARGV[3],
  'velocidade', ARGV[4], 'status_motor', ARGV[5], 'id_veiculo', ARGV[6])
return 1";

    private readonly TimeSpan _dedupTtl = TimeSpan.FromMinutes(rules.Value.DeduplicationMinutes);
    private IDatabase Db => redis.GetDatabase();

    public static string PosicaoKey(string id) => $"veiculo:{id}:posicao";
    public static string AlertaKey(string id, string tipo) => $"alerta:{id}:{tipo}";

    public async Task<bool> UpdatePositionAsync(TelemetriaMensagem msg)
    {
        var inv = CultureInfo.InvariantCulture;
        var result = await Db.ScriptEvaluateAsync(UpdateScript,
            [PosicaoKey(msg.IdVeiculo)],
            [
                msg.Timestamp.ToUnixTimeMilliseconds(),
                msg.Latitude.ToString("R", inv),
                msg.Longitude.ToString("R", inv),
                msg.Velocidade.ToString("R", inv),
                msg.StatusMotor ?? "",
                msg.IdVeiculo,
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            ]);
        return (int)result == 1;
    }

    public Task<bool> AlertaJaEnviadoAsync(string id, string tipo) =>
        Db.KeyExistsAsync(AlertaKey(id, tipo));

    public Task MarcarAlertaEnviadoAsync(string id, string tipo) =>
        Db.StringSetAsync(AlertaKey(id, tipo), DateTimeOffset.UtcNow.ToString("O"), _dedupTtl);

    public async IAsyncEnumerable<VeiculoEstado> ScanVeiculosAsync()
    {
        foreach (var endpoint in redis.GetEndPoints())
        {
            var server = redis.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica) continue;

            await foreach (var key in server.KeysAsync(pattern: "veiculo:*:posicao", pageSize: 500))
            {
                var h = await Db.HashGetAllAsync(key);
                if (h.Length == 0) continue;
                var map = h.ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
                if (!map.TryGetValue("id_veiculo", out var id) ||
                    !map.TryGetValue("atualizado_em_ms", out var upd)) continue;

                var inv = CultureInfo.InvariantCulture;
                yield return new VeiculoEstado(
                    id,
                    map.TryGetValue("latitude", out var la) ? double.Parse(la, inv) : 0,
                    map.TryGetValue("longitude", out var lo) ? double.Parse(lo, inv) : 0,
                    map.TryGetValue("velocidade", out var v) ? double.Parse(v, inv) : 0,
                    map.GetValueOrDefault("status_motor"),
                    map.TryGetValue("timestamp_ms", out var ts) ? long.Parse(ts) : 0,
                    long.Parse(upd));
            }
        }
    }
}
