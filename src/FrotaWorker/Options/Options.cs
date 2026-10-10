namespace FrotaWorker.Options;

public sealed class KafkaOptions
{
    public const string Section = "Kafka";
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "frota-worker-processamento";
    public string Topic { get; set; } = "frota.telemetria";
    public string DlqTopic { get; set; } = "frota.telemetria.dlq";
    /// <summary>Tentativas para falhas não-transitórias antes de desviar para a DLQ.</summary>
    public int MaxProcessingAttempts { get; set; } = 3;
    /// <summary>Commit manual de offsets a cada N mensagens (ou quando a fila esvazia).</summary>
    public int CommitEveryMessages { get; set; } = 100;
}

public sealed class RulesOptions
{
    public const string Section = "Rules";
    public double SpeedLimitKmh { get; set; } = 80;
    public double GeofenceCenterLatitude { get; set; } = -23.5505;
    public double GeofenceCenterLongitude { get; set; } = -46.6333;
    public double GeofenceRadiusMeters { get; set; } = 50_000;
    public int DeduplicationMinutes { get; set; } = 5;
    public int DeadSensorThresholdSeconds { get; set; } = 60;
    public int DeadSensorCheckIntervalSeconds { get; set; } = 15;
}

public sealed class PostgresOptions
{
    public const string Section = "Postgres";
    public string ConnectionString { get; set; } = "Host=localhost;Port=5432;Database=frota;Username=frota;Password=frota";
    public int BatchSize { get; set; } = 500;
    public int FlushIntervalMs { get; set; } = 2000;
}

public sealed class RedisOptions
{
    public const string Section = "Redis";
    public string ConnectionString { get; set; } = "localhost:6379";
}

public sealed class AlertasOptions
{
    public const string Section = "Alertas";
    public string BaseUrl { get; set; } = "http://localhost:5100";
    public string Path { get; set; } = "/api/alerts";
    public int TimeoutSeconds { get; set; } = 3;
    /// <summary>Retries com backoff exponencial: 1s, 2s, 4s.</summary>
    public int RetryCount { get; set; } = 3;
}
