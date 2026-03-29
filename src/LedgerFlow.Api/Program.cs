using LedgerFlow.Application;
using LedgerFlow.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddProblemDetails();
builder.Services.AddLedgerFlow(builder.Configuration, "ledgerflow-api");
var app = builder.Build();

app.UseExceptionHandler(handler => handler.Run(async context =>
{
    var error = context.Features.Get<Microsoft.AspNetCore.Diagnostics.IExceptionHandlerFeature>()?.Error;
    var status = error switch
    {
        IdempotencyConflictException or DuplicateReferenceException => 409,
        ArgumentException or InvalidDataException => 400,
        _ => 500
    };
    context.Response.StatusCode = status;
    await Results.Problem(statusCode: status,
        title: status == 500 ? "Internal server error" : error?.Message).ExecuteAsync(context);
}));

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapPost("/api/payments", async (CreatePaymentRequest request, HttpContext http, IPaymentService payments, CancellationToken ct) =>
{
    var key = http.Request.Headers["Idempotency-Key"].ToString();
    var result = await payments.CreateAsync(request, key, ct);
    return Results.Json(result.Response, statusCode: result.StatusCode);
});
app.MapGet("/api/payments/{id:guid}", async (Guid id, IPaymentService payments, CancellationToken ct) =>
{
    var payment = await payments.GetAsync(id, ct);
    return payment is null ? Results.NotFound() : Results.Ok(payment);
});
app.MapGet("/api/payments/{id:guid}/ledger", async (Guid id, IPaymentService payments, CancellationToken ct) =>
{
    var ledger = await payments.GetLedgerAsync(id, ct);
    return ledger is null ? Results.NotFound() : Results.Ok(ledger);
});
app.MapPost("/api/reconciliations", async (HttpRequest request, IReconciliationService reconciliations, CancellationToken ct) =>
{
    if (!request.HasFormContentType) throw new ArgumentException("multipart/form-data is required.");
    var form = await request.ReadFormAsync(ct);
    var provider = form["provider"].ToString();
    if (!DateOnly.TryParse(form["settlementDate"], out var date)) throw new ArgumentException("Invalid settlementDate.");
    var file = form.Files.GetFile("file") ?? throw new ArgumentException("CSV file is required.");
    if (file.Length is < 1 or > 10_000_000) throw new ArgumentException("CSV file must be at most 10 MB.");
    var rows = await CsvSettlementReader.ReadAsync(file, ct);
    var id = await reconciliations.CreateAsync(provider, date, rows, ct);
    return Results.Accepted($"/api/reconciliations/{id}", new { batchId = id });
});
app.MapGet("/api/reconciliations/{id:guid}", async (Guid id, IReconciliationService reconciliations, CancellationToken ct) =>
{
    var batch = await reconciliations.GetAsync(id, ct);
    return batch is null ? Results.NotFound() : Results.Ok(batch);
});
app.MapGet("/api/reconciliations/{id:guid}/discrepancies", async (Guid id, string? type, int? page, int? pageSize,
    IReconciliationService reconciliations, CancellationToken ct) =>
{
    var actualPage = page ?? 1;
    var actualSize = pageSize ?? 50;
    if (actualPage < 1 || actualSize is < 1 or > 500) throw new ArgumentException("Invalid pagination.");
    var rows = await reconciliations.GetDiscrepanciesAsync(id, type, actualPage, actualSize, ct);
    return Results.Ok(rows);
});

using (var scope = app.Services.CreateScope())
    await DatabaseInitializer.InitializeAsync(scope.ServiceProvider.GetRequiredService<LedgerDbContext>());

app.Run();

public partial class Program;
