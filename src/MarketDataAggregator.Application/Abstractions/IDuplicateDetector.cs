using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Application.Abstractions;

public interface IDuplicateDetector
{
    bool TryRegister(MarketTick tick);

    int PurgeExpired();
}
