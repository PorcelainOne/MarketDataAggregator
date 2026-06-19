namespace MarketDataAggregator.Domain;

public sealed record MarketTick(
    string Source,
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTimeOffset ExchangeTimestampUtc,
    DateTimeOffset ReceivedAtUtc,
    string RawPayload,
    string DedupHash);
