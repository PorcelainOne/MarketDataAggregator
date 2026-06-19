namespace MarketDataAggregator.Domain;

public sealed record RawExchangeMessage(
    string Source,
    string Payload,
    DateTimeOffset ReceivedAtUtc);
