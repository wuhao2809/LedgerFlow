using LedgerFlow.Domain;

namespace LedgerFlow.UnitTests;

public sealed class PaymentRulesTests
{
    [Fact]
    public void Balanced_entries_are_accepted()
    {
        Assert.True(PaymentRules.IsBalanced([
            (EntrySide.Debit, 10_000L), (EntrySide.Credit, 10_000L)
        ]));
    }

    [Fact]
    public void Unbalanced_entries_are_rejected()
    {
        Assert.False(PaymentRules.IsBalanced([
            (EntrySide.Debit, 10_000L), (EntrySide.Credit, 9_999L)
        ]));
    }

    [Theory]
    [InlineData(0, "USD", "ref")]
    [InlineData(100, "US", "ref")]
    [InlineData(100, "USD", "")]
    public void Invalid_payment_values_are_rejected(long amount, string currency, string reference)
    {
        Assert.Throws<ArgumentException>(() => PaymentRules.Validate(amount, currency, reference));
    }
}
