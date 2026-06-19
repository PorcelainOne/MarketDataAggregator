using MarketDataAggregator.Application.Abstractions;

namespace MarketDataAggregator.Infrastructure;

public sealed class SystemClock : IClock
{
    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
}
