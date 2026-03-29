using LedgerFlow.Application;
using LedgerFlow.Domain;
using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Infrastructure;

public sealed class ReconciliationService(LedgerDbContext db) : IReconciliationService
{
    public async Task<Guid> CreateAsync(string provider, DateOnly settlementDate, IReadOnlyList<SettlementRow> rows, CancellationToken ct)
    {
        provider = provider.Trim();
        if (provider.Length is < 1 or > 80 || rows.Count is < 1 or > 100_000)
            throw new ArgumentException("Invalid provider or settlement row count.");
        if (rows.Any(x => x.RowNumber <= 0 || string.IsNullOrWhiteSpace(x.ExternalReference) || x.ExternalReference.Length > 120 || x.AmountMinor <= 0 || x.Currency.Length != 3))
            throw new ArgumentException("Invalid settlement row.");

        var id = Guid.NewGuid();
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        db.ReconciliationBatches.Add(new ReconciliationBatchEntity
        {
            Id = id, Provider = provider, SettlementDate = settlementDate,
            Status = BatchStatus.Pending, TotalRows = rows.Count
        });
        foreach (var row in rows)
            db.SettlementRecords.Add(new SettlementRecordEntity
            {
                BatchId = id, RowNumber = row.RowNumber, ExternalReference = row.ExternalReference.Trim(),
                AmountMinor = row.AmountMinor, Currency = row.Currency.Trim().ToUpperInvariant(), Status = row.Status.Trim()
            });
        db.OutboxMessages.Add(Events.New("ReconciliationRequested.v1", id));
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return id;
    }

    public async Task<ReconciliationResponse?> GetAsync(Guid id, CancellationToken ct)
    {
        var batch = await db.ReconciliationBatches.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
        return batch is null ? null : new ReconciliationResponse(batch.Id, batch.Status.ToString(), batch.TotalRows,
            batch.Matched, batch.MissingInternal, batch.MissingSettlement, batch.AmountMismatch,
            batch.CurrencyMismatch, batch.DuplicateSettlement);
    }

    public async Task<IReadOnlyList<DiscrepancyResponse>> GetDiscrepanciesAsync(Guid id, string? type, int page, int pageSize, CancellationToken ct)
    {
        var query = db.ReconciliationDiscrepancies.AsNoTracking().Where(x => x.BatchId == id);
        if (!string.IsNullOrWhiteSpace(type))
        {
            if (!Enum.TryParse<DiscrepancyType>(type, true, out var parsed)) throw new ArgumentException("Invalid discrepancy type.");
            query = query.Where(x => x.Type == parsed);
        }
        return await query.OrderBy(x => x.Id).Skip((page - 1) * pageSize).Take(pageSize)
            .Select(x => new DiscrepancyResponse(x.Type.ToString(), x.ExternalReference, x.Detail)).ToListAsync(ct);
    }
}
