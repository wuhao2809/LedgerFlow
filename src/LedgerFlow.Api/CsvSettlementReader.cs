using System.Globalization;
using LedgerFlow.Application;

public static class CsvSettlementReader
{
    public static async Task<IReadOnlyList<SettlementRow>> ReadAsync(IFormFile file, CancellationToken ct)
    {
        using var reader = new StreamReader(file.OpenReadStream());
        var header = await reader.ReadLineAsync(ct);
        if (header?.Trim('\uFEFF') != "external_reference,amount_minor,currency,status")
            throw new InvalidDataException("Invalid CSV header.");
        var rows = new List<SettlementRow>();
        while (await reader.ReadLineAsync(ct) is { } line)
        {
            if (rows.Count >= 100_000) throw new InvalidDataException("Too many settlement rows.");
            var cells = line.Split(',');
            if (cells.Length != 4 || !long.TryParse(cells[1], NumberStyles.None, CultureInfo.InvariantCulture, out var amount))
                throw new InvalidDataException($"Invalid CSV row {rows.Count + 2}.");
            rows.Add(new SettlementRow(rows.Count + 1, cells[0], amount, cells[2], cells[3]));
        }
        return rows;
    }
}
