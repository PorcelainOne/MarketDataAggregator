namespace MarketDataAggregator.Application;

public sealed record AggregationOptions(
    int RawChannelCapacity,
    int NormalizedChannelCapacity,
    int BatchSize,
    TimeSpan FlushInterval,
    TimeSpan DuplicateTtl,
    TimeSpan ReconnectInitialDelay,
    TimeSpan ReconnectMaxDelay)
{
    public static AggregationOptions Default { get; } = new(
        RawChannelCapacity: 1000,
        NormalizedChannelCapacity: 1000,
        BatchSize: 100,
        FlushInterval: TimeSpan.FromMilliseconds(500),
        DuplicateTtl: TimeSpan.FromMinutes(3),
        ReconnectInitialDelay: TimeSpan.FromMilliseconds(300),
        ReconnectMaxDelay: TimeSpan.FromSeconds(30));
}
