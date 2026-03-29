using System.Diagnostics;
using System.Text;
using LedgerFlow.Infrastructure;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;

namespace LedgerFlow.Worker;

public sealed class OutboxDispatcher(IServiceScopeFactory scopes, RabbitConnectionSettings settings,
    ILogger<OutboxDispatcher> logger) : BackgroundService
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
                while (!stoppingToken.IsCancellationRequested)
                {
                    var count = await DispatchBatchAsync(channel, stoppingToken);
                    if (count == 0) await Task.Delay(500, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox dispatcher failed; reconnecting");
                await Task.Delay(2000, stoppingToken);
            }
        }
    }

    private async Task<int> DispatchBatchAsync(IChannel channel, CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
        var now = DateTime.UtcNow;
        await db.OutboxMessages.Where(x => x.Status == "Sending" && x.LeaseUntil < now)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Status, "Pending"), ct);
        var ids = await db.OutboxMessages.AsNoTracking()
            .Where(x => x.Status == "Pending" && x.NextAttemptAt <= now)
            .OrderBy(x => x.NextAttemptAt).Select(x => x.EventId).Take(20).ToListAsync(ct);
        foreach (var id in ids)
        {
            var claimed = await db.OutboxMessages.Where(x => x.EventId == id && x.Status == "Pending")
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Status, "Sending")
                    .SetProperty(x => x.LeaseUntil, DateTime.UtcNow.AddSeconds(30)), ct);
            if (claimed == 0) continue;
            var message = await db.OutboxMessages.SingleAsync(x => x.EventId == id, ct);
            ActivityContext.TryParse(message.TraceParent, null, out var parentContext);
            using var activity = Telemetry.Source.StartActivity("outbox.publish", ActivityKind.Producer, parentContext);
            activity?.SetTag("event.type", message.EventType);
            activity?.SetTag("event.id", message.EventId.ToString());
            try
            {
                var props = new BasicProperties
                {
                    Persistent = true, MessageId = message.EventId.ToString(), Type = message.EventType,
                    CorrelationId = message.EventId.ToString(),
                    Headers = new Dictionary<string, object?>()
                };
                if (activity is not null) props.Headers["traceparent"] = activity.Id!;
                await channel.BasicPublishAsync(RabbitTopology.Exchange, message.EventType, mandatory: true,
                    props, Encoding.UTF8.GetBytes(message.Payload), ct);
                message.Status = "Sent";
                message.SentAt = DateTime.UtcNow;
                message.LeaseUntil = null;
                await db.SaveChangesAsync(ct);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to publish event {EventId}", id);
                message.AttemptCount++;
                message.Status = message.AttemptCount >= 5 ? "Failed" : "Pending";
                message.LastError = ex.Message[..Math.Min(ex.Message.Length, 500)];
                message.NextAttemptAt = DateTime.UtcNow.AddSeconds(Math.Min(30, 1 << message.AttemptCount));
                message.LeaseUntil = null;
                await db.SaveChangesAsync(ct);
                if (!channel.IsOpen) throw;
            }
        }
        return ids.Count;
    }
}
