using LedgerFlow.Domain;
using LedgerFlow.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Worker;

public static class ReconciliationProcessor
{
    public static async Task ProcessAsync(LedgerDbContext db, Guid eventId, Guid batchId, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        const string consumer = "reconciliation";
        if (await db.ProcessedMessages.AnyAsync(x => x.ConsumerName == consumer && x.EventId == eventId, ct))
            return;
        var batch = await db.ReconciliationBatches.SingleAsync(x => x.Id == batchId, ct);
        if (batch.Status == BatchStatus.Completed)
        {
            db.ProcessedMessages.Add(new ProcessedMessageEntity { ConsumerName = consumer, EventId = eventId, ProcessedAt = DateTime.UtcNow });
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
            return;
        }
        var records = await db.SettlementRecords.AsNoTracking().Where(x => x.BatchId == batchId)
            .OrderBy(x => x.RowNumber).ToListAsync(ct);
        var payments = await db.Payments.AsNoTracking()
            .Where(x => x.Provider == batch.Provider && x.SettlementDate == batch.SettlementDate && x.Status == PaymentStatus.Posted)
            .ToDictionaryAsync(x => x.ExternalReference, ct);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var differences = new List<DiscrepancyEntity>();
        foreach (var row in records)
        {
            if (!seen.Add(row.ExternalReference))
            {
                differences.Add(Difference(batchId, $"row:{row.RowNumber}", DiscrepancyType.DuplicateSettlement,
                    row.ExternalReference, $"Duplicate settlement row {row.RowNumber}"));
                batch.DuplicateSettlement++;
                continue;
            }
            if (!payments.TryGetValue(row.ExternalReference, out var payment))
            {
                differences.Add(Difference(batchId, $"row:{row.RowNumber}", DiscrepancyType.MissingInternal,
                    row.ExternalReference, "No posted payment"));
                batch.MissingInternal++;
            }
            else if (payment.AmountMinor != row.AmountMinor)
            {
                differences.Add(Difference(batchId, $"row:{row.RowNumber}", DiscrepancyType.AmountMismatch,
                    row.ExternalReference, $"Payment {payment.AmountMinor}; settlement {row.AmountMinor}"));
                batch.AmountMismatch++;
            }
            else if (payment.Currency != row.Currency)
            {
                differences.Add(Difference(batchId, $"row:{row.RowNumber}", DiscrepancyType.CurrencyMismatch,
                    row.ExternalReference, $"Payment {payment.Currency}; settlement {row.Currency}"));
                batch.CurrencyMismatch++;
            }
            else batch.Matched++;
        }
        foreach (var payment in payments.Values.Where(x => !seen.Contains(x.ExternalReference)))
        {
            differences.Add(Difference(batchId, $"payment:{payment.Id}", DiscrepancyType.MissingSettlement,
                payment.ExternalReference, "No settlement row"));
            batch.MissingSettlement++;
        }
        db.ReconciliationDiscrepancies.AddRange(differences);
        batch.Status = BatchStatus.Completed;
        batch.CompletedAt = DateTime.UtcNow;
        db.ProcessedMessages.Add(new ProcessedMessageEntity { ConsumerName = consumer, EventId = eventId, ProcessedAt = DateTime.UtcNow });
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    private static DiscrepancyEntity Difference(Guid batchId, string key, DiscrepancyType type, string reference, string detail) => new()
    {
        BatchId = batchId, DiscrepancyKey = key, Type = type, ExternalReference = reference, Detail = detail
    };
}
