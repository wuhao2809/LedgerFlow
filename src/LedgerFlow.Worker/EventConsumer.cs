using System.Diagnostics;
using System.Text;
using System.Text.Json;
using LedgerFlow.Domain;
using LedgerFlow.Infrastructure;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace LedgerFlow.Worker;

public sealed class EventConsumer(IServiceScopeFactory scopes, RabbitConnectionSettings settings,
    ILogger<EventConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var factory = new ConnectionFactory { Uri = new Uri(settings.Uri) };
                await using var connection = await factory.CreateConnectionAsync(stoppingToken);
                await using var channel = await connection.CreateChannelAsync(
                    new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true), stoppingToken);
                await RabbitTopology.DeclareAsync(channel, stoppingToken);
                await channel.BasicQosAsync(0, 4, false, stoppingToken);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (_, args) => await HandleAsync(channel, args, stoppingToken);
                await channel.BasicConsumeAsync("ledgerflow.payments", false, consumer, stoppingToken);
                await channel.BasicConsumeAsync("ledgerflow.reconciliations", false, consumer, stoppingToken);
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "Consumer failed; reconnecting");
                await Task.Delay(2000, stoppingToken);
            }
        }
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        var type = delivery.BasicProperties.Type ?? delivery.RoutingKey;
        var eventIdValid = Guid.TryParse(delivery.BasicProperties.MessageId, out var eventId);
        Guid aggregateId = Guid.Empty;
        bool aggregateIdValid;
        try
        {
            using var payload = JsonDocument.Parse(delivery.Body);
            aggregateIdValid = payload.RootElement.TryGetProperty("aggregateId", out var element)
                && element.TryGetGuid(out aggregateId);
        }
        catch (JsonException) { aggregateIdValid = false; }
        if (!eventIdValid || !aggregateIdValid || type is not ("PaymentRequested.v1" or "ReconciliationRequested.v1"))
        {
            await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, ct);
            return;
        }

        var parent = delivery.BasicProperties.Headers?.TryGetValue("traceparent", out var traceValue) == true
            ? Encoding.UTF8.GetString((byte[])traceValue!) : null;
        ActivityContext.TryParse(parent, null, out var parentContext);
        using var activity = Telemetry.Source.StartActivity(type == "PaymentRequested.v1" ? "payment.post" : "reconciliation.run",
            ActivityKind.Consumer, parentContext);
        activity?.SetTag("event.id", eventId.ToString());
        activity?.SetTag("event.type", type);
        try
        {
            using var scope = scopes.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            if (type == "PaymentRequested.v1")
            {
                await PostPaymentAsync(db, eventId, aggregateId, ct);
                await scope.ServiceProvider.GetRequiredService<PaymentCache>().InvalidateAsync(aggregateId);
            }
            else await ReconciliationProcessor.ProcessAsync(db, eventId, aggregateId, ct);
            await channel.BasicAckAsync(delivery.DeliveryTag, false, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Processing {EventId} failed", eventId);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            var attempt = 0;
            if (delivery.BasicProperties.Headers?.TryGetValue("x-attempt", out var value) == true)
                attempt = Convert.ToInt32(value);
            if (attempt >= 4)
            {
                await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, ct);
                return;
            }
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(2000, 100 * (1 << attempt))), ct);
                var props = new BasicProperties
                {
                    Persistent = true, MessageId = eventId.ToString(), Type = type,
                    Headers = new Dictionary<string, object?> { ["x-attempt"] = attempt + 1 }
                };
                if (parent is not null) props.Headers["traceparent"] = parent;
                await channel.BasicPublishAsync(RabbitTopology.Exchange, type, mandatory: true,
                    props, delivery.Body, ct);
                await channel.BasicAckAsync(delivery.DeliveryTag, false, ct);
            }
            catch
            {
                await channel.BasicNackAsync(delivery.DeliveryTag, false, requeue: true, ct);
            }
        }
    }

    private static async Task PostPaymentAsync(LedgerDbContext db, Guid eventId, Guid paymentId, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        const string consumer = "payment-posting";
        if (await db.ProcessedMessages.AnyAsync(x => x.ConsumerName == consumer && x.EventId == eventId, ct))
            return;
        var payment = await db.Payments.SingleAsync(x => x.Id == paymentId, ct);
        if (payment.Status == PaymentStatus.Pending)
        {
            var transactionId = Guid.NewGuid();
            db.LedgerTransactions.Add(new LedgerTransactionEntity
            {
                Id = transactionId, PaymentId = paymentId, CreatedAt = DateTime.UtcNow
            });
            db.LedgerEntries.AddRange(
                new LedgerEntryEntity { Id = Guid.NewGuid(), TransactionId = transactionId, Account = "ProcessorClearing", Side = EntrySide.Debit, AmountMinor = payment.AmountMinor, Currency = payment.Currency },
                new LedgerEntryEntity { Id = Guid.NewGuid(), TransactionId = transactionId, Account = "MerchantPayable", Side = EntrySide.Credit, AmountMinor = payment.AmountMinor, Currency = payment.Currency });
            payment.Status = PaymentStatus.Posted;
            payment.PostedAt = DateTime.UtcNow;
            db.OutboxMessages.Add(Events.New("PaymentPosted.v1", paymentId));
        }
        db.ProcessedMessages.Add(new ProcessedMessageEntity
        {
            ConsumerName = consumer, EventId = eventId, ProcessedAt = DateTime.UtcNow
        });
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }
}
