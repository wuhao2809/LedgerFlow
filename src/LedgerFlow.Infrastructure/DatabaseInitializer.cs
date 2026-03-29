using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Infrastructure;

public static class DatabaseInitializer
{
    public static async Task InitializeAsync(LedgerDbContext db, CancellationToken ct = default)
    {
        await db.Database.EnsureCreatedAsync(ct);
        await db.Database.ExecuteSqlRawAsync("ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS \"TraceParent\" text", ct);
        await db.Database.ExecuteSqlRawAsync("ALTER TABLE idempotency_requests ADD COLUMN IF NOT EXISTS \"ResponseJson\" text NOT NULL DEFAULT ''", ct);
        await db.Database.ExecuteSqlRawAsync("""
            CREATE OR REPLACE FUNCTION ledger_check_balance() RETURNS trigger AS $$
            DECLARE tx_id uuid; debit_total numeric; credit_total numeric; entry_count integer;
            BEGIN
              IF TG_TABLE_NAME = 'ledger_transactions' THEN tx_id := NEW."Id";
              ELSIF TG_OP = 'DELETE' THEN tx_id := OLD."TransactionId";
              ELSE tx_id := NEW."TransactionId";
              END IF;
              IF NOT EXISTS (SELECT 1 FROM ledger_transactions WHERE "Id" = tx_id) THEN RETURN NULL; END IF;
              SELECT COUNT(*),
                     COALESCE(SUM("AmountMinor") FILTER (WHERE "Side" = 'Debit'), 0),
                     COALESCE(SUM("AmountMinor") FILTER (WHERE "Side" = 'Credit'), 0)
                INTO entry_count, debit_total, credit_total
                FROM ledger_entries WHERE "TransactionId" = tx_id;
              IF entry_count < 2 OR debit_total <> credit_total THEN
                RAISE EXCEPTION 'ledger transaction % is not balanced', tx_id USING ERRCODE = '23514';
              END IF;
              RETURN NULL;
            END; $$ LANGUAGE plpgsql;
            """, ct);
        await db.Database.ExecuteSqlRawAsync("""
            DO $$ BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ledger_transaction_balance') THEN
                CREATE CONSTRAINT TRIGGER ledger_transaction_balance
                AFTER INSERT OR UPDATE ON ledger_transactions
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_balance();
              END IF;
              IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ledger_entry_balance') THEN
                CREATE CONSTRAINT TRIGGER ledger_entry_balance
                AFTER INSERT OR UPDATE OR DELETE ON ledger_entries
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_balance();
              END IF;
            END $$;
            """, ct);
    }
}
