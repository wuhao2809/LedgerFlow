# LedgerFlow

LedgerFlow is a simulated payment and settlement reconciliation backend built with C#, ASP.NET Core, PostgreSQL, RabbitMQ, Redis, and Docker. It handles simulated funds only.

## Run locally

Docker Desktop is required. From the project root, run:

```bash
docker compose up -d --build
docker compose ps
```

The API is available at `http://localhost:8080`. The RabbitMQ management UI is at `http://localhost:15672` with local default credentials `guest/guest`. The API creates the database schema on first startup; the Worker starts after the API becomes healthy.

## Create a payment

```bash
curl -i -X POST http://localhost:8080/api/payments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-payment-1' \
  -d '{"merchantId":"demo","provider":"simulator","settlementDate":"2026-09-27","externalReference":"demo-payment-1","amountMinor":10000,"currency":"USD"}'
```

The API returns `202 Accepted` and a `paymentId`. After the Worker processes the event, `GET /api/payments/{paymentId}` returns `Posted`, and `GET /api/payments/{paymentId}/ledger` returns equal debit and credit entries. Repeating the same request with the same idempotency key returns the original `202` response; reusing the key with a different request returns `409 Conflict`.

## Reconcile settlement records

The CSV header must be `external_reference,amount_minor,currency,status`. The parser does not support quoted fields or fields containing commas. The provider and settlement date supplied with the upload should match those of the payments being reconciled.

```bash
curl -i -X POST http://localhost:8080/api/reconciliations \
  -F provider=simulator \
  -F settlementDate=2026-09-27 \
  -F file=@samples/settlement.csv
```

Use the returned `batchId` to query `GET /api/reconciliations/{batchId}` and `GET /api/reconciliations/{batchId}/discrepancies?page=1&pageSize=50`. The sample CSV contains `demo-payment-1`; create the matching payment above if you want that row to match.

## Tests and observability

```bash
dotnet test tests/LedgerFlow.UnitTests/LedgerFlow.UnitTests.csproj
dotnet test tests/LedgerFlow.IntegrationTests/LedgerFlow.IntegrationTests.csproj
docker compose logs otel-collector
```

The integration test uses Testcontainers to start PostgreSQL, RabbitMQ, and Redis. It covers 101 concurrent duplicate requests, redelivery after a Worker restart, recovery from a temporary RabbitMQ outage, the database ledger balance constraint, and 10,001 settlement records. OpenTelemetry traces are sent to the Collector over OTLP and printed in its logs by the debug exporter.

### Follow a payment trace

OpenTelemetry records the steps of a request and how long each step takes. LedgerFlow creates spans for the HTTP request, payment creation, outbox publication, and Worker posting. Spans for the same payment flow share a `Trace ID`.

1. Start the services with `docker compose up -d --build`.
2. Create a **new** payment using the example above. If you have run it before, change both the `Idempotency-Key` and `externalReference`; otherwise the API will replay the previous result or return a reference conflict.
3. Wait a few seconds, then run `docker compose logs --since=2m otel-collector`.
4. Find `POST /api/payments`, `payment.create`, `outbox.publish`, and `payment.post`. Compare their `Trace ID` values. A shared ID connects the API and Worker steps of the same flow.

The API and Worker send traces to the Collector configured by `OTEL_EXPORTER_OTLP_ENDPOINT`. The current Collector prints traces to its logs; there is no separate tracing UI.

## Current limitations

- EF Core `EnsureCreated` creates the initial database schema, and additional SQL installs the deferred ledger balance trigger. There are no versioned migrations yet, so upgrading an existing database requires a migration plan.
- A reconciliation job processes up to 100,000 records in one Worker database transaction. The CSV reader supports only simple, unquoted fields.
- Redis caches payment reads briefly. Payment idempotency, accounting, and message deduplication rely on PostgreSQL.
- RabbitMQ delivers messages at least once. Consumer checks and database constraints prevent repeated delivery from posting a second ledger transaction. A failing consumer message is attempted up to five times before it reaches the dead-letter queue.
