using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Application.Abstractions;

public interface IExchangeMessageParser
{
    string Source { get; }

    bool TryParse(RawExchangeMessage message, out MarketTick tick, out string? error);
}
