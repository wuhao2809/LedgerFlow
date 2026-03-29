using System.Security.Cryptography;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using LedgerFlow.Application;
using LedgerFlow.Domain;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StackExchange.Redis;

namespace LedgerFlow.Infrastructure;

public sealed class PaymentService(LedgerDbContext db, PaymentCache cache) : IPaymentService
{
    public async Task<CreatePaymentResult> CreateAsync(CreatePaymentRequest input, string key, CancellationToken ct)
    {
        using var activity = Telemetry.Source.StartActivity("payment.create");
        if (input.MerchantId is null || input.Provider is null || input.ExternalReference is null || input.Currency is null)
            throw new ArgumentException("Required payment fields are missing.");
        var request = input with
        {
            MerchantId = input.MerchantId.Trim(), Provider = input.Provider.Trim(),
            ExternalReference = input.ExternalReference.Trim(), Currency = input.Currency.Trim().ToUpperInvariant()
        };
        PaymentRules.Validate(request.AmountMinor, request.Currency, request.ExternalReference);
        if (request.MerchantId.Length is < 1 or > 120 || request.Provider.Length is < 1 or > 80)
            throw new ArgumentException("Merchant and provider are required.");
        if (string.IsNullOrWhiteSpace(key) || key.Length > 160) throw new ArgumentException("Invalid Idempotency-Key.");
        var scope = request.MerchantId;
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(request))));
        var existing = await db.IdempotencyRequests.AsNoTracking().SingleOrDefaultAsync(x => x.Scope == scope && x.Key == key, ct);
        if (existing is not null) return await ReplayAsync(existing, hash, ct);

        var id = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var response = new PaymentResponse(id, PaymentStatus.Pending.ToString(), request.MerchantId,
            request.Provider, request.SettlementDate, request.ExternalReference, request.AmountMinor, request.Currency);
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        db.IdempotencyRequests.Add(new IdempotencyEntity
        {
            Scope = scope, Key = key, RequestHash = hash, PaymentId = id, ResponseCode = 202,
            ResponseJson = JsonSerializer.Serialize(response)
        });
        db.Payments.Add(new PaymentEntity
        {
            Id = id, MerchantId = request.MerchantId, Provider = request.Provider,
            SettlementDate = request.SettlementDate, ExternalReference = request.ExternalReference,
            AmountMinor = request.AmountMinor, Currency = request.Currency, Status = PaymentStatus.Pending,
            CreatedAt = now
        });
        db.OutboxMessages.Add(Events.New("PaymentRequested.v1", id));
        try
        {
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
        {
            await tx.RollbackAsync(ct);
            db.ChangeTracker.Clear();
            var winner = await db.IdempotencyRequests.AsNoTracking().SingleOrDefaultAsync(x => x.Scope == scope && x.Key == key, ct);
            if (winner is not null) return await ReplayAsync(winner, hash, ct);
            throw new DuplicateReferenceException();
        }
        return new CreatePaymentResult(202, response);
    }

    private async Task<CreatePaymentResult> ReplayAsync(IdempotencyEntity existing, string hash, CancellationToken ct)
    {
        if (existing.RequestHash != hash) throw new IdempotencyConflictException();
        var response = JsonSerializer.Deserialize<PaymentResponse>(existing.ResponseJson)
            ?? throw new InvalidDataException("Stored idempotency response is invalid.");
        return await Task.FromResult(new CreatePaymentResult(existing.ResponseCode, response));
    }

    public async Task<PaymentResponse?> GetAsync(Guid id, CancellationToken ct)
    {
        var cached = await cache.GetAsync(id);
        if (cached is not null) return cached;
        var payment = await db.Payments.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
        if (payment is null) return null;
        var response = ToResponse(payment);
        await cache.SetAsync(response);
        return response;
    }

    public async Task<LedgerResponse?> GetLedgerAsync(Guid id, CancellationToken ct)
    {
        var transaction = await db.LedgerTransactions.AsNoTracking().SingleOrDefaultAsync(x => x.PaymentId == id, ct);
        if (transaction is null) return null;
        var entries = await db.LedgerEntries.AsNoTracking().Where(x => x.TransactionId == transaction.Id)
            .OrderBy(x => x.Side).Select(x => new LedgerEntryResponse(x.Account, x.Side.ToString(), x.AmountMinor, x.Currency))
            .ToListAsync(ct);
        return new LedgerResponse(transaction.Id, id, entries);
    }

    private static PaymentResponse ToResponse(PaymentEntity p) =>
        new(p.Id, p.Status.ToString(), p.MerchantId, p.Provider, p.SettlementDate,
            p.ExternalReference, p.AmountMinor, p.Currency);
}

public sealed class PaymentCache(string connectionString)
{
    private readonly Lazy<Task<ConnectionMultiplexer>> _connection = new(() => ConnectionMultiplexer.ConnectAsync(connectionString));

    public async Task<PaymentResponse?> GetAsync(Guid id)
    {
        try
        {
            var connection = await _connection.Value.WaitAsync(TimeSpan.FromSeconds(1));
            var value = await connection.GetDatabase().StringGetAsync($"payment:{id}").WaitAsync(TimeSpan.FromSeconds(1));
            return value.IsNull ? null : JsonSerializer.Deserialize<PaymentResponse>((string)value!);
        }
        catch (RedisException) { return null; }
        catch (System.Net.Sockets.SocketException) { return null; }
        catch (TimeoutException) { return null; }
    }

    public async Task SetAsync(PaymentResponse response)
    {
        try
        {
            var connection = await _connection.Value.WaitAsync(TimeSpan.FromSeconds(1));
            await connection.GetDatabase().StringSetAsync($"payment:{response.PaymentId}",
                JsonSerializer.Serialize(response), TimeSpan.FromSeconds(15)).WaitAsync(TimeSpan.FromSeconds(1));
        }
        catch (RedisException) { }
        catch (System.Net.Sockets.SocketException) { }
        catch (TimeoutException) { }
    }

    public async Task InvalidateAsync(Guid id)
    {
        try
        {
            var connection = await _connection.Value.WaitAsync(TimeSpan.FromSeconds(1));
            await connection.GetDatabase().KeyDeleteAsync($"payment:{id}").WaitAsync(TimeSpan.FromSeconds(1));
        }
        catch (RedisException) { }
        catch (System.Net.Sockets.SocketException) { }
        catch (TimeoutException) { }
    }
}

public static class Events
{
    public static OutboxEntity New(string type, Guid aggregateId) => new()
    {
        EventId = Guid.NewGuid(), EventType = type,
        Payload = JsonSerializer.Serialize(new { aggregateId }),
        TraceParent = Activity.Current?.Id,
        Status = "Pending", NextAttemptAt = DateTime.UtcNow
    };
}
