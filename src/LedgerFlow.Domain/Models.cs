namespace LedgerFlow.Domain;

public enum PaymentStatus { Pending, Posted }
public enum EntrySide { Debit, Credit }
public enum BatchStatus { Pending, Running, Completed, Failed }
public enum DiscrepancyType { MissingInternal, MissingSettlement, AmountMismatch, CurrencyMismatch, DuplicateSettlement }

public static class PaymentRules
{
    public static void Validate(long amountMinor, string currency, string externalReference)
    {
        if (amountMinor <= 0) throw new ArgumentException("Amount must be positive.", nameof(amountMinor));
        if (currency.Length != 3 || !currency.All(char.IsLetter))
            throw new ArgumentException("Currency must have three letters.", nameof(currency));
        if (string.IsNullOrWhiteSpace(externalReference) || externalReference.Length > 120)
            throw new ArgumentException("External reference is required and at most 120 characters.", nameof(externalReference));
    }

    public static bool IsBalanced(IEnumerable<(EntrySide Side, long AmountMinor)> entries)
    {
        long debit = 0, credit = 0;
        var count = 0;
        foreach (var entry in entries)
        {
            if (entry.AmountMinor <= 0) return false;
            checked
            {
                if (entry.Side == EntrySide.Debit) debit += entry.AmountMinor;
                else credit += entry.AmountMinor;
            }
            count++;
        }
        return count >= 2 && debit == credit;
    }
}
