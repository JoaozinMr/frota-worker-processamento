using Confluent.Kafka;
using FrotaWorker.Options;
using Microsoft.Extensions.Options;

namespace FrotaWorker.Services;

/// <summary>Publica no tópico DLQ a mensagem original acrescida de metadados do erro.</summary>
public sealed class DlqPublisher : IDisposable
{
    private readonly IProducer<string?, byte[]?> _producer;
    private readonly string _topic;
    private readonly ILogger<DlqPublisher> _logger;

    public DlqPublisher(IOptions<KafkaOptions> options, ILogger<DlqPublisher> logger)
    {
        _topic = options.Value.DlqTopic;
        _logger = logger;
        _producer = new ProducerBuilder<string?, byte[]?>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 15_000
        }).Build();
    }

    public async Task PublishAsync(ConsumeResult<string?, byte[]?> original, Exception error, CancellationToken ct)
    {
        var headers = new Headers();
        if (original.Message.Headers != null)
            foreach (var h in original.Message.Headers) headers.Add(h.Key, h.GetValueBytes());
        headers.Add("dlq-error", System.Text.Encoding.UTF8.GetBytes(error.Message));
        headers.Add("dlq-exception-type", System.Text.Encoding.UTF8.GetBytes(error.GetType().Name));
        headers.Add("dlq-origin", System.Text.Encoding.UTF8.GetBytes(
            $"{original.Topic}[{original.Partition.Value}]@{original.Offset.Value}"));

        await _producer.ProduceAsync(_topic, new Message<string?, byte[]?>
        {
            Key = original.Message.Key,
            Value = original.Message.Value,
            Headers = headers
        }, ct);

        _logger.LogError(error, "Mensagem {Origem} desviada para DLQ {Topic}",
            $"{original.Topic}[{original.Partition.Value}]@{original.Offset.Value}", _topic);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
