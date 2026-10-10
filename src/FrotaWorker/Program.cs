using FrotaWorker.Options;
using FrotaWorker.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Npgsql;
using Polly;
using Polly.Extensions.Http;
using Serilog;
using Serilog.Formatting.Compact;
using StackExchange.Redis;

Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .WriteTo.Console(new RenderedCompactJsonFormatter())
    .CreateBootstrapLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);

    builder.Host.UseSerilog((ctx, cfg) => cfg
        .ReadFrom.Configuration(ctx.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithProperty("Service", "frota-worker-processamento")
        .WriteTo.Console(new RenderedCompactJsonFormatter()));

    var cfg = builder.Configuration;
    builder.Services.Configure<KafkaOptions>(cfg.GetSection(KafkaOptions.Section));
    builder.Services.Configure<RulesOptions>(cfg.GetSection(RulesOptions.Section));
    builder.Services.Configure<PostgresOptions>(cfg.GetSection(PostgresOptions.Section));
    builder.Services.Configure<RedisOptions>(cfg.GetSection(RedisOptions.Section));
    builder.Services.Configure<AlertasOptions>(cfg.GetSection(AlertasOptions.Section));

    // Infra: Redis e PostgreSQL
    builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
    {
        var opts = ConfigurationOptions.Parse(sp.GetRequiredService<IOptions<RedisOptions>>().Value.ConnectionString);
        opts.AbortOnConnectFail = false; // reconecta em background se o Redis cair
        return ConnectionMultiplexer.Connect(opts);
    });
    builder.Services.AddSingleton(sp =>
        NpgsqlDataSource.Create(sp.GetRequiredService<IOptions<PostgresOptions>>().Value.ConnectionString));

    // Comunicação síncrona com a API de Alertas: timeout 3s por tentativa + retry exponencial (1s, 2s, 4s)
    var alertas = cfg.GetSection(AlertasOptions.Section).Get<AlertasOptions>() ?? new AlertasOptions();
    builder.Services.AddHttpClient<AlertasClient>(c =>
        {
            c.BaseAddress = new Uri(alertas.BaseUrl);
            c.Timeout = Timeout.InfiniteTimeSpan; // o timeout é controlado pelo Polly
        })
        .AddPolicyHandler((sp, _) =>
        {
            var logger = sp.GetRequiredService<ILogger<AlertasClient>>();
            return HttpPolicyExtensions.HandleTransientHttpError()
                .Or<Polly.Timeout.TimeoutRejectedException>()
                .WaitAndRetryAsync(
                    alertas.RetryCount,
                    attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt - 1)),
                    (outcome, delay, attempt, _) => logger.LogWarning(
                        "Retry {Attempt} para API de Alertas em {Delay}s ({Reason})", attempt, delay.TotalSeconds,
                        outcome.Exception?.Message ?? outcome.Result?.StatusCode.ToString()));
        })
        .AddPolicyHandler(Policy.TimeoutAsync<HttpResponseMessage>(alertas.TimeoutSeconds));

    // Domínio
    builder.Services.AddSingleton<RuleEvaluator>();
    builder.Services.AddSingleton<RedisStateStore>();
    builder.Services.AddSingleton<AlertDispatcher>();
    builder.Services.AddSingleton<TelemetryProcessor>();
    builder.Services.AddSingleton<DlqPublisher>();
    builder.Services.AddSingleton<ConsumerState>();
    builder.Services.AddSingleton<PostgresBatchWriter>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<PostgresBatchWriter>());
    builder.Services.AddHostedService<TelemetryConsumerService>();
    builder.Services.AddHostedService<DeadSensorService>();

    builder.Services.AddHealthChecks().AddCheck<WorkerHealthCheck>("worker");

    var app = builder.Build();
    app.MapHealthChecks("/health");
    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Worker encerrado inesperadamente");
    throw;
}
finally
{
    Log.CloseAndFlush();
}

internal sealed class WorkerHealthCheck(IConnectionMultiplexer redis, ConsumerState state) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        if (!redis.IsConnected)
            return Task.FromResult(HealthCheckResult.Unhealthy("Redis desconectado"));

        // O loop do consumer faz poll ao menos a cada ~1s; silêncio longo indica travamento.
        var idle = DateTime.UtcNow - state.LastPollUtc;
        return Task.FromResult(idle > TimeSpan.FromMinutes(2)
            ? HealthCheckResult.Unhealthy($"Consumer sem poll há {idle.TotalSeconds:F0}s")
            : HealthCheckResult.Healthy(state.Assigned ? "Consumer com partições" : "Consumer aguardando partições"));
    }
}
