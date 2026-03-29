using LedgerFlow.Domain;
using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Infrastructure;

public sealed class PaymentEntity
{
    public Guid Id { get; set; }
    public string MerchantId { get; set; } = "";
    public string Provider { get; set; } = "";
    public DateOnly SettlementDate { get; set; }
    public string ExternalReference { get; set; } = "";
    public long AmountMinor { get; set; }
    public string Currency { get; set; } = "";
    public PaymentStatus Status { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? PostedAt { get; set; }
    public uint Version { get; set; }
}

public sealed class IdempotencyEntity
{
    public long Id { get; set; }
    public string Scope { get; set; } = "";
    public string Key { get; set; } = "";
    public string RequestHash { get; set; } = "";
    public Guid PaymentId { get; set; }
    public int ResponseCode { get; set; }
    public string ResponseJson { get; set; } = "";
}

public sealed class LedgerTransactionEntity
{
    public Guid Id { get; set; }
    public Guid PaymentId { get; set; }
    public DateTime CreatedAt { get; set; }
}

public sealed class LedgerEntryEntity
{
    public Guid Id { get; set; }
    public Guid TransactionId { get; set; }
    public string Account { get; set; } = "";
    public EntrySide Side { get; set; }
    public long AmountMinor { get; set; }
    public string Currency { get; set; } = "";
}

public sealed class OutboxEntity
{
    public Guid EventId { get; set; }
    public string EventType { get; set; } = "";
    public string Payload { get; set; } = "";
    public string? TraceParent { get; set; }
    public string Status { get; set; } = "Pending";
    public int AttemptCount { get; set; }
    public DateTime NextAttemptAt { get; set; }
    public DateTime? SentAt { get; set; }
    public string? LastError { get; set; }
    public DateTime? LeaseUntil { get; set; }
}

public sealed class ProcessedMessageEntity
{
    public string ConsumerName { get; set; } = "";
    public Guid EventId { get; set; }
    public DateTime ProcessedAt { get; set; }
}

public sealed class ReconciliationBatchEntity
{
    public Guid Id { get; set; }
    public string Provider { get; set; } = "";
    public DateOnly SettlementDate { get; set; }
    public BatchStatus Status { get; set; }
    public int TotalRows { get; set; }
    public int Matched { get; set; }
    public int MissingInternal { get; set; }
    public int MissingSettlement { get; set; }
    public int AmountMismatch { get; set; }
    public int CurrencyMismatch { get; set; }
    public int DuplicateSettlement { get; set; }
    public DateTime? CompletedAt { get; set; }
    public DateTime? LeaseUntil { get; set; }
}

public sealed class SettlementRecordEntity
{
    public long Id { get; set; }
    public Guid BatchId { get; set; }
    public int RowNumber { get; set; }
    public string ExternalReference { get; set; } = "";
    public long AmountMinor { get; set; }
    public string Currency { get; set; } = "";
    public string Status { get; set; } = "";
}

public sealed class DiscrepancyEntity
{
    public long Id { get; set; }
    public Guid BatchId { get; set; }
    public string DiscrepancyKey { get; set; } = "";
    public DiscrepancyType Type { get; set; }
    public string ExternalReference { get; set; } = "";
    public string Detail { get; set; } = "";
}

public sealed class LedgerDbContext(DbContextOptions<LedgerDbContext> options) : DbContext(options)
{
    public DbSet<PaymentEntity> Payments => Set<PaymentEntity>();
    public DbSet<IdempotencyEntity> IdempotencyRequests => Set<IdempotencyEntity>();
    public DbSet<LedgerTransactionEntity> LedgerTransactions => Set<LedgerTransactionEntity>();
    public DbSet<LedgerEntryEntity> LedgerEntries => Set<LedgerEntryEntity>();
    public DbSet<OutboxEntity> OutboxMessages => Set<OutboxEntity>();
    public DbSet<ProcessedMessageEntity> ProcessedMessages => Set<ProcessedMessageEntity>();
    public DbSet<ReconciliationBatchEntity> ReconciliationBatches => Set<ReconciliationBatchEntity>();
    public DbSet<SettlementRecordEntity> SettlementRecords => Set<SettlementRecordEntity>();
    public DbSet<DiscrepancyEntity> ReconciliationDiscrepancies => Set<DiscrepancyEntity>();

    protected override void OnModelCreating(ModelBuilder m)
    {
        m.Entity<PaymentEntity>(e =>
        {
            e.ToTable("payments", t => t.HasCheckConstraint("ck_payments_amount", "\"AmountMinor\" > 0"));
            e.HasKey(x => x.Id);
            e.HasIndex(x => new { x.Provider, x.ExternalReference }).IsUnique();
            e.HasIndex(x => new { x.Provider, x.SettlementDate, x.Status });
            e.Property(x => x.Status).HasConversion<string>();
            e.Property(x => x.Version).IsRowVersion();
            e.Property(x => x.MerchantId).HasMaxLength(120);
            e.Property(x => x.Provider).HasMaxLength(80);
            e.Property(x => x.ExternalReference).HasMaxLength(120);
            e.Property(x => x.Currency).HasMaxLength(3);
        });
        m.Entity<IdempotencyEntity>(e =>
        {
            e.ToTable("idempotency_requests");
            e.HasKey(x => x.Id);
            e.HasIndex(x => new { x.Scope, x.Key }).IsUnique();
            e.Property(x => x.Scope).HasMaxLength(120);
            e.Property(x => x.Key).HasMaxLength(160);
            e.Property(x => x.RequestHash).HasMaxLength(64);
        });
        m.Entity<LedgerTransactionEntity>(e =>
        {
            e.ToTable("ledger_transactions");
            e.HasKey(x => x.Id);
            e.HasIndex(x => x.PaymentId).IsUnique();
            e.HasOne<PaymentEntity>().WithMany().HasForeignKey(x => x.PaymentId);
        });
        m.Entity<LedgerEntryEntity>(e =>
        {
            e.ToTable("ledger_entries", t =>
            {
                t.HasCheckConstraint("ck_ledger_entries_amount", "\"AmountMinor\" > 0");
                t.HasCheckConstraint("ck_ledger_entries_account", "\"Account\" IN ('ProcessorClearing', 'MerchantPayable')");
            });
            e.HasKey(x => x.Id);
            e.HasIndex(x => x.TransactionId);
            e.HasOne<LedgerTransactionEntity>().WithMany().HasForeignKey(x => x.TransactionId);
            e.Property(x => x.Side).HasConversion<string>();
            e.Property(x => x.Account).HasMaxLength(80);
            e.Property(x => x.Currency).HasMaxLength(3);
        });
        m.Entity<OutboxEntity>(e =>
        {
            e.ToTable("outbox_messages");
            e.HasKey(x => x.EventId);
            e.HasIndex(x => new { x.Status, x.NextAttemptAt });
        });
        m.Entity<ProcessedMessageEntity>(e =>
        {
            e.ToTable("processed_messages");
            e.HasKey(x => new { x.ConsumerName, x.EventId });
        });
        m.Entity<ReconciliationBatchEntity>(e =>
        {
            e.ToTable("reconciliation_batches");
            e.HasKey(x => x.Id);
            e.Property(x => x.Status).HasConversion<string>();
        });
        m.Entity<SettlementRecordEntity>(e =>
        {
            e.ToTable("settlement_records");
            e.HasKey(x => x.Id);
            e.HasIndex(x => new { x.BatchId, x.RowNumber }).IsUnique();
            e.HasIndex(x => new { x.BatchId, x.ExternalReference });
            e.HasOne<ReconciliationBatchEntity>().WithMany().HasForeignKey(x => x.BatchId);
        });
        m.Entity<DiscrepancyEntity>(e =>
        {
            e.ToTable("reconciliation_discrepancies");
            e.HasKey(x => x.Id);
            e.HasIndex(x => new { x.BatchId, x.DiscrepancyKey }).IsUnique();
            e.Property(x => x.Type).HasConversion<string>();
            e.HasOne<ReconciliationBatchEntity>().WithMany().HasForeignKey(x => x.BatchId);
        });
    }
}
