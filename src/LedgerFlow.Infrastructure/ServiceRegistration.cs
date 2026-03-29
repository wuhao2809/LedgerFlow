using LedgerFlow.Application;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace LedgerFlow.Infrastructure;

public static class ServiceRegistration
{
    public static IServiceCollection AddLedgerFlow(this IServiceCollection services, IConfiguration configuration, string serviceName)
    {
        var postgres = configuration.GetConnectionString("Postgres")
            ?? throw new InvalidOperationException("ConnectionStrings:Postgres is required.");
        var redis = configuration.GetConnectionString("Redis") ?? "localhost:6379,abortConnect=false";
        services.AddDbContext<LedgerDbContext>(options => options.UseNpgsql(postgres));
        services.AddSingleton(new PaymentCache(redis));
        services.AddScoped<IPaymentService, PaymentService>();
        services.AddScoped<IReconciliationService, ReconciliationService>();
        services.AddSingleton(new RabbitConnectionSettings(configuration.GetConnectionString("RabbitMq") ?? "amqp://guest:guest@localhost:5672"));
        services.AddOpenTelemetry().WithTracing(tracing => tracing
            .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName))
            .AddAspNetCoreInstrumentation()
            .AddSource("LedgerFlow")
            .AddOtlpExporter());
        return services;
    }
}

public sealed record RabbitConnectionSettings(string Uri);
