using FrotaWorker.Models;
using FrotaWorker.Options;
using Microsoft.Extensions.Options;

namespace FrotaWorker.Services;

/// <summary>Regras de negócio espaciais: limite de velocidade e geofence (Haversine).</summary>
public sealed class RuleEvaluator(IOptions<RulesOptions> options)
{
    private const double EarthRadiusMeters = 6_371_000;
    private readonly RulesOptions _rules = options.Value;

    public IReadOnlyList<Infracao> Evaluate(TelemetriaMensagem msg)
    {
        var infracoes = new List<Infracao>(2);

        if (msg.Velocidade > _rules.SpeedLimitKmh)
        {
            infracoes.Add(new Infracao(TiposInfracao.ExcessoVelocidade,
                $"Velocidade {msg.Velocidade:F1} km/h excede o limite de {_rules.SpeedLimitKmh:F0} km/h"));
        }

        var distancia = HaversineMeters(msg.Latitude, msg.Longitude,
            _rules.GeofenceCenterLatitude, _rules.GeofenceCenterLongitude);
        if (distancia > _rules.GeofenceRadiusMeters)
        {
            infracoes.Add(new Infracao(TiposInfracao.ForaGeofence,
                $"Veículo a {distancia / 1000:F2} km do centro, fora do raio de {_rules.GeofenceRadiusMeters / 1000:F2} km"));
        }

        return infracoes;
    }

    public static double HaversineMeters(double lat1, double lon1, double lat2, double lon2)
    {
        static double Rad(double deg) => deg * Math.PI / 180;
        var dLat = Rad(lat2 - lat1);
        var dLon = Rad(lon2 - lon1);
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(Rad(lat1)) * Math.Cos(Rad(lat2)) * Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        return 2 * EarthRadiusMeters * Math.Asin(Math.Min(1, Math.Sqrt(a)));
    }
}
