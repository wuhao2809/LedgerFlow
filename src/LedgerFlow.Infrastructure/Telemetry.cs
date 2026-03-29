using System.Diagnostics;

namespace LedgerFlow.Infrastructure;

public static class Telemetry
{
    public static readonly ActivitySource Source = new("LedgerFlow");
}
