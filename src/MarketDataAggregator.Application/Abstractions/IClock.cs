namespace MarketDataAggregator.Application.Abstractions;

public interface IClock
{
    DateTimeOffset UtcNow { get; }
}
