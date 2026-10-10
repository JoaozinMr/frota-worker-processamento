using System.Text.Json.Serialization;

namespace FrotaWorker.Models;

public static class TiposInfracao
{
    public const string ExcessoVelocidade = "EXCESSO_VELOCIDADE";
    public const string ForaGeofence = "FORA_GEOFENCE";
    public const string SensorMorto = "SENSOR_MORTO";
}

public sealed class InvalidTelemetryException(string message, Exception? inner = null)
    : Exception(message, inner);

public sealed record TelemetriaMensagem
{
    [JsonPropertyName("id_veiculo")] public string IdVeiculo { get; init; } = "";
    [JsonPropertyName("latitude")] public double Latitude { get; init; }
    [JsonPropertyName("longitude")] public double Longitude { get; init; }
    [JsonPropertyName("velocidade")] public double Velocidade { get; init; }
    [JsonPropertyName("status_motor")] public string? StatusMotor { get; init; }
    [JsonPropertyName("timestamp")] public DateTimeOffset Timestamp { get; init; }

    /// <summary>Preenchido pelo consumer a partir dos headers Kafka.</summary>
    [JsonIgnore] public string? CorrelationId { get; init; }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(IdVeiculo))
            throw new InvalidTelemetryException("id_veiculo ausente");
        if (!double.IsFinite(Latitude) || Latitude is < -90 or > 90)
            throw new InvalidTelemetryException($"latitude inválida: {Latitude}");
        if (!double.IsFinite(Longitude) || Longitude is < -180 or > 180)
            throw new InvalidTelemetryException($"longitude inválida: {Longitude}");
        if (!double.IsFinite(Velocidade) || Velocidade < 0)
            throw new InvalidTelemetryException($"velocidade inválida: {Velocidade}");
        if (Timestamp == default)
            throw new InvalidTelemetryException("timestamp ausente");
    }
}

public sealed record Infracao(string Tipo, string Descricao);

public sealed record AlertaPayload
{
    [JsonPropertyName("id_veiculo")] public string IdVeiculo { get; init; } = "";
    [JsonPropertyName("tipo_infracao")] public string TipoInfracao { get; init; } = "";
    [JsonPropertyName("descricao")] public string Descricao { get; init; } = "";
    [JsonPropertyName("latitude")] public double? Latitude { get; init; }
    [JsonPropertyName("longitude")] public double? Longitude { get; init; }
    [JsonPropertyName("velocidade")] public double? Velocidade { get; init; }
    [JsonPropertyName("timestamp")] public DateTimeOffset Timestamp { get; init; }
    [JsonPropertyName("correlation_id")] public string? CorrelationId { get; init; }
}
