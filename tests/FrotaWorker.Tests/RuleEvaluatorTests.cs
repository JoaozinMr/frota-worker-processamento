using FrotaWorker.Models;
using FrotaWorker.Options;
using FrotaWorker.Services;
using Microsoft.Extensions.Options;
using Xunit;

namespace FrotaWorker.Tests;

public class RuleEvaluatorTests
{
    private static RuleEvaluator Create() => new(Microsoft.Extensions.Options.Options.Create(new RulesOptions
    {
        SpeedLimitKmh = 80,
        GeofenceCenterLatitude = -23.5505,
        GeofenceCenterLongitude = -46.6333,
        GeofenceRadiusMeters = 10_000
    }));

    private static TelemetriaMensagem Msg(double lat, double lon, double vel) => new()
    {
        IdVeiculo = "V1", Latitude = lat, Longitude = lon, Velocidade = vel, Timestamp = DateTimeOffset.UtcNow
    };

    [Fact]
    public void DentroDosLimites_NaoGeraInfracao() =>
        Assert.Empty(Create().Evaluate(Msg(-23.5505, -46.6333, 60)));

    [Fact]
    public void AcimaDoLimite_GeraExcessoVelocidade()
    {
        var r = Create().Evaluate(Msg(-23.5505, -46.6333, 95));
        Assert.Single(r, i => i.Tipo == TiposInfracao.ExcessoVelocidade);
    }

    [Fact]
    public void ForaDoRaio_GeraForaGeofence()
    {
        var r = Create().Evaluate(Msg(-22.9068, -43.1729, 50)); // Rio de Janeiro
        Assert.Single(r, i => i.Tipo == TiposInfracao.ForaGeofence);
    }

    [Fact]
    public void Haversine_SaoPauloRio_Aproximadamente360Km()
    {
        var d = RuleEvaluator.HaversineMeters(-23.5505, -46.6333, -22.9068, -43.1729);
        Assert.InRange(d / 1000, 350, 370);
    }

    [Theory]
    [InlineData("", 0, 0, 10)]
    [InlineData("V1", 91, 0, 10)]
    [InlineData("V1", 0, 181, 10)]
    [InlineData("V1", 0, 0, -1)]
    public void Validate_RejeitaPayloadInvalido(string id, double lat, double lon, double vel)
    {
        var m = Msg(lat, lon, vel) with { IdVeiculo = id };
        Assert.Throws<InvalidTelemetryException>(m.Validate);
    }
}
