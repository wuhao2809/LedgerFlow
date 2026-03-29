using LedgerFlow.Infrastructure;
using LedgerFlow.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddLedgerFlow(builder.Configuration, "ledgerflow-worker");
builder.Services.AddHostedService<OutboxDispatcher>();
builder.Services.AddHostedService<EventConsumer>();

var host = builder.Build();
host.Run();
