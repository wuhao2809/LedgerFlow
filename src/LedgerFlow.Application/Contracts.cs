using LedgerFlow.Domain;

namespace LedgerFlow.Application;

public sealed record CreatePaymentRequest(string MerchantId, string Provider, DateOnly SettlementDate,
    string ExternalReference, long AmountMinor, string Currency);
public sealed record PaymentResponse(Guid PaymentId, string Status, string MerchantId, string Provider,
    DateOnly SettlementDate, string ExternalReference, long AmountMinor, string Currency);
public sealed record LedgerEntryResponse(string Account, string Side, long AmountMinor, string Currency);
public sealed record LedgerResponse(Guid TransactionId, Guid PaymentId, IReadOnlyList<LedgerEntryResponse> Entries);
public sealed record CreatePaymentResult(int StatusCode, PaymentResponse Response);
public sealed record SettlementRow(int RowNumber, string ExternalReference, long AmountMinor, string Currency, string Status);
public sealed record ReconciliationResponse(Guid BatchId, string Status, int TotalRows, int Matched,
    int MissingInternal, int MissingSettlement, int AmountMismatch, int CurrencyMismatch, int DuplicateSettlement);
public sealed record DiscrepancyResponse(string Type, string ExternalReference, string Detail);

public sealed class IdempotencyConflictException() : Exception("Idempotency key was used with a different request.");
public sealed class DuplicateReferenceException() : Exception("External reference already exists for this provider.");

public interface IPaymentService
{
    Task<CreatePaymentResult> CreateAsync(CreatePaymentRequest request, string key, CancellationToken ct);
    Task<PaymentResponse?> GetAsync(Guid id, CancellationToken ct);
    Task<LedgerResponse?> GetLedgerAsync(Guid id, CancellationToken ct);
}

public interface IReconciliationService
{
    Task<Guid> CreateAsync(string provider, DateOnly settlementDate, IReadOnlyList<SettlementRow> rows, CancellationToken ct);
    Task<ReconciliationResponse?> GetAsync(Guid id, CancellationToken ct);
    Task<IReadOnlyList<DiscrepancyResponse>> GetDiscrepanciesAsync(Guid id, string? type, int page, int pageSize, CancellationToken ct);
}
