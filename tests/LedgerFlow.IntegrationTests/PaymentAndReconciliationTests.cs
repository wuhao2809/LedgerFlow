using LedgerFlow.Application;
using LedgerFlow.Domain;
using LedgerFlow.Infrastructure;
using LedgerFlow.Worker;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Testcontainers.Redis;

namespace LedgerFlow.IntegrationTests;

public sealed class PaymentAndReconciliationTests
{
    [Fact]
    public async Task Duplicate_payments_and_large_reconciliation_remain_consistent()
    {
        await using var postgres = new PostgreSqlBuilder("postgres:17").Build();
        await using var rabbit = new RabbitMqBuilder("rabbitmq:4-management").Build();
        await using var redis = new RedisBuilder("redis:7-alpine").Build();
        await Task.WhenAll(postgres.StartAsync(), rabbit.StartAsync(), redis.StartAsync());
        var databaseConnectionString = new NpgsqlConnectionStringBuilder(postgres.GetConnectionString())
        {
            MaxPoolSize = 30,
            Timeout = 30
        }.ConnectionString;

        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:Postgres"] = databaseConnectionString,
            ["ConnectionStrings:RabbitMq"] = rabbit.GetConnectionString(),
            ["ConnectionStrings:Redis"] = redis.GetConnectionString() + ",abortConnect=false"
        }).Build();
        using var host = Host.CreateDefaultBuilder().ConfigureServices((_, services) =>
        {
            services.AddLedgerFlow(config, "ledgerflow-integration-test");
            services.AddHostedService<OutboxDispatcher>();
            services.AddHostedService<EventConsumer>();
        }).Build();
        using (var scope = host.Services.CreateScope())
            await DatabaseInitializer.InitializeAsync(scope.ServiceProvider.GetRequiredService<LedgerDbContext>());
        await host.StartAsync();

        var request = new CreatePaymentRequest("merchant-test", "simulator", new DateOnly(2026, 9, 27),
            "duplicate-100", 10_000, "USD");
        var results = await Task.WhenAll(Enumerable.Range(0, 101).Select(async _ =>
        {
            using var scope = host.Services.CreateScope();
            return await scope.ServiceProvider.GetRequiredService<IPaymentService>()
                .CreateAsync(request, "same-key", CancellationToken.None);
        }));
        var paymentId = Assert.Single(results.Select(x => x.Response.PaymentId).Distinct());
        await EventuallyAsync(async () =>
        {
            using var scope = host.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            return await db.Payments.AnyAsync(x => x.Id == paymentId && x.Status == PaymentStatus.Posted);
        });
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            Assert.Equal(1, await db.Payments.CountAsync(x => x.Id == paymentId));
            var transaction = await db.LedgerTransactions.SingleAsync(x => x.PaymentId == paymentId);
            var entries = await db.LedgerEntries.Where(x => x.TransactionId == transaction.Id).ToListAsync();
            Assert.Equal(2, entries.Count);
            Assert.True(PaymentRules.IsBalanced(entries.Select(x => (x.Side, x.AmountMinor))));
        }
        using (var scope = host.Services.CreateScope())
        {
            var service = scope.ServiceProvider.GetRequiredService<IPaymentService>();
            var replay = await service.CreateAsync(request, "same-key", CancellationToken.None);
            Assert.Equal("Pending", replay.Response.Status);
            await Assert.ThrowsAsync<IdempotencyConflictException>(() => service.CreateAsync(
                request with { AmountMinor = 9_999 }, "same-key", CancellationToken.None));
        }
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            await using var tx = await db.Database.BeginTransactionAsync();
            var badId = Guid.NewGuid();
            var badPaymentId = Guid.NewGuid();
            db.Payments.Add(new PaymentEntity
            {
                Id = badPaymentId, MerchantId = "merchant-test", Provider = "simulator", SettlementDate = request.SettlementDate,
                ExternalReference = "unbalanced-test", AmountMinor = 100, Currency = "USD", Status = PaymentStatus.Posted,
                CreatedAt = DateTime.UtcNow, PostedAt = DateTime.UtcNow
            });
            db.LedgerTransactions.Add(new LedgerTransactionEntity { Id = badId, PaymentId = badPaymentId, CreatedAt = DateTime.UtcNow });
            db.LedgerEntries.Add(new LedgerEntryEntity
            {
                Id = Guid.NewGuid(), TransactionId = badId, Account = "ProcessorClearing",
                Side = EntrySide.Debit, AmountMinor = 100, Currency = "USD"
            });
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await db.SaveChangesAsync();
                await tx.CommitAsync();
            });
        }
        var concurrencyPaymentId = Guid.NewGuid();
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            db.Payments.Add(new PaymentEntity
            {
                Id = concurrencyPaymentId, MerchantId = "merchant-test", Provider = "simulator",
                SettlementDate = request.SettlementDate, ExternalReference = "optimistic-concurrency-test",
                AmountMinor = 100, Currency = "USD", Status = PaymentStatus.Pending, CreatedAt = DateTime.UtcNow
            });
            await db.SaveChangesAsync();
        }
        using (var firstScope = host.Services.CreateScope())
        using (var secondScope = host.Services.CreateScope())
        {
            var first = firstScope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            var second = secondScope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            var firstPayment = await first.Payments.SingleAsync(x => x.Id == concurrencyPaymentId);
            var secondPayment = await second.Payments.SingleAsync(x => x.Id == concurrencyPaymentId);
            firstPayment.Status = PaymentStatus.Posted;
            secondPayment.Status = PaymentStatus.Posted;
            await first.SaveChangesAsync();
            await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => second.SaveChangesAsync());
        }

        var day = new DateOnly(2026, 9, 28);
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            db.Payments.AddRange(Enumerable.Range(0, 9999).Select(i => new PaymentEntity
            {
                Id = Guid.NewGuid(), MerchantId = "bulk", Provider = "bulk-provider", SettlementDate = day,
                ExternalReference = $"bulk-{i}", AmountMinor = 1000, Currency = "USD",
                Status = PaymentStatus.Posted, CreatedAt = DateTime.UtcNow, PostedAt = DateTime.UtcNow
            }));
            await db.SaveChangesAsync();
        }
        var rows = Enumerable.Range(0, 9998).Select(i => new SettlementRow(i + 1, $"bulk-{i}",
            i == 1 ? 999 : 1000, i == 2 ? "CAD" : "USD", "settled")).ToList();
        rows.Add(new SettlementRow(9999, "external-only", 1000, "USD", "settled"));
        rows.Add(new SettlementRow(10000, "bulk-0", 1000, "USD", "settled"));
        rows.Add(new SettlementRow(10001, "external-only-2", 1000, "USD", "settled"));
        Guid batchId;
        using (var scope = host.Services.CreateScope())
            batchId = await scope.ServiceProvider.GetRequiredService<IReconciliationService>()
                .CreateAsync("bulk-provider", day, rows, CancellationToken.None);
        await EventuallyAsync(async () =>
        {
            using var scope = host.Services.CreateScope();
            var batch = await scope.ServiceProvider.GetRequiredService<IReconciliationService>()
                .GetAsync(batchId, CancellationToken.None);
            return batch?.Status == "Completed";
        }, TimeSpan.FromMinutes(3));
        using (var scope = host.Services.CreateScope())
        {
            var batch = (await scope.ServiceProvider.GetRequiredService<IReconciliationService>()
                .GetAsync(batchId, CancellationToken.None))!;
            Assert.Equal(10001, batch.TotalRows);
            Assert.Equal(9996, batch.Matched);
            Assert.Equal(2, batch.MissingInternal);
            Assert.Equal(1, batch.MissingSettlement);
            Assert.Equal(1, batch.AmountMismatch);
            Assert.Equal(1, batch.CurrencyMismatch);
            Assert.Equal(1, batch.DuplicateSettlement);
        }
        await host.StopAsync();
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            var message = await db.OutboxMessages.SingleAsync(x => x.EventType == "PaymentRequested.v1");
            message.Status = "Pending";
            message.NextAttemptAt = DateTime.UtcNow;
            var reconciliationMessage = await db.OutboxMessages.SingleAsync(x => x.EventType == "ReconciliationRequested.v1");
            reconciliationMessage.Status = "Pending";
            reconciliationMessage.NextAttemptAt = DateTime.UtcNow;
            await db.SaveChangesAsync();
        }
        using var restarted = Host.CreateDefaultBuilder().ConfigureServices((_, services) =>
        {
            services.AddLedgerFlow(config, "ledgerflow-integration-restarted");
            services.AddHostedService<OutboxDispatcher>();
            services.AddHostedService<EventConsumer>();
        }).Build();
        await restarted.StartAsync();
        await EventuallyAsync(async () =>
        {
            using var scope = restarted.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            return await db.OutboxMessages.CountAsync(x =>
                (x.EventType == "PaymentRequested.v1" || x.EventType == "ReconciliationRequested.v1") && x.Status == "Sent") == 2;
        });
        await Task.Delay(1000);
        using (var scope = restarted.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            Assert.Equal(1, await db.LedgerTransactions.CountAsync(x => x.PaymentId == paymentId));
            Assert.Equal(6, await db.ReconciliationDiscrepancies.CountAsync(x => x.BatchId == batchId));
        }
        await rabbit.ExecAsync(["rabbitmqctl", "stop_app"]);
        Guid delayedPaymentId;
        using (var scope = restarted.Services.CreateScope())
        {
            var created = await scope.ServiceProvider.GetRequiredService<IPaymentService>().CreateAsync(
                request with { ExternalReference = "broker-recovery" }, "broker-recovery", CancellationToken.None);
            delayedPaymentId = created.Response.PaymentId;
        }
        await rabbit.ExecAsync(["rabbitmqctl", "start_app"]);
        await EventuallyAsync(async () =>
        {
            using var scope = restarted.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            return await db.Payments.AnyAsync(x => x.Id == delayedPaymentId && x.Status == PaymentStatus.Posted);
        }, TimeSpan.FromMinutes(2));
        await restarted.StopAsync();

        var faultConfig = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:Postgres"] = databaseConnectionString + ";Command Timeout=1",
            ["ConnectionStrings:RabbitMq"] = rabbit.GetConnectionString(),
            ["ConnectionStrings:Redis"] = redis.GetConnectionString() + ",abortConnect=false"
        }).Build();
        using var databaseRecoveryHost = Host.CreateDefaultBuilder().ConfigureServices((_, services) =>
        {
            services.AddLedgerFlow(faultConfig, "ledgerflow-database-recovery-test");
            services.AddHostedService<OutboxDispatcher>();
            services.AddHostedService<EventConsumer>();
        }).Build();
        Guid databaseRecoveryPaymentId;
        using (var scope = databaseRecoveryHost.Services.CreateScope())
        {
            var created = await scope.ServiceProvider.GetRequiredService<IPaymentService>().CreateAsync(
                request with { ExternalReference = "database-recovery" }, "database-recovery", CancellationToken.None);
            databaseRecoveryPaymentId = created.Response.PaymentId;
        }
        await using var lockConnection = new NpgsqlConnection(databaseConnectionString);
        await lockConnection.OpenAsync();
        await using var lockTransaction = await lockConnection.BeginTransactionAsync();
        await using (var lockCommand = new NpgsqlCommand("LOCK TABLE payments IN ACCESS EXCLUSIVE MODE", lockConnection, lockTransaction))
            await lockCommand.ExecuteNonQueryAsync();
        await databaseRecoveryHost.StartAsync();
        await Task.Delay(TimeSpan.FromSeconds(3));
        await lockTransaction.CommitAsync();
        await EventuallyAsync(async () =>
        {
            using var scope = databaseRecoveryHost.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
            return await db.Payments.AnyAsync(x => x.Id == databaseRecoveryPaymentId && x.Status == PaymentStatus.Posted);
        }, TimeSpan.FromMinutes(1));
        await databaseRecoveryHost.StopAsync();
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        var end = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(45));
        while (DateTime.UtcNow < end)
        {
            if (await condition()) return;
            await Task.Delay(250);
        }
        Assert.Fail("Condition was not met before timeout.");
    }
}
