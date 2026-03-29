using RabbitMQ.Client;

namespace LedgerFlow.Worker;

public static class RabbitTopology
{
    public const string Exchange = "ledgerflow.events";
    public const string DeadExchange = "ledgerflow.dead";

    public static async Task DeclareAsync(IChannel channel, CancellationToken ct)
    {
        await channel.ExchangeDeclareAsync(Exchange, ExchangeType.Direct, durable: true, cancellationToken: ct);
        await channel.ExchangeDeclareAsync(DeadExchange, ExchangeType.Fanout, durable: true, cancellationToken: ct);
        await channel.QueueDeclareAsync("ledgerflow.dead", durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
        await channel.QueueBindAsync("ledgerflow.dead", DeadExchange, "", cancellationToken: ct);
        foreach (var (queue, key) in new[]
        {
            ("ledgerflow.payments", "PaymentRequested.v1"),
            ("ledgerflow.reconciliations", "ReconciliationRequested.v1"),
            ("ledgerflow.audit", "PaymentPosted.v1")
        })
        {
            await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-dead-letter-exchange"] = DeadExchange }, cancellationToken: ct);
            await channel.QueueBindAsync(queue, Exchange, key, cancellationToken: ct);
        }
    }
}
