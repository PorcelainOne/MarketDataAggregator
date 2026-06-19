using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Application.Abstractions;

public interface ITickRepository
{
    Task EnsureSchemaAsync(CancellationToken cancellationToken);

    Task<int> WriteBatchAsync(IReadOnlyList<MarketTick> batch, CancellationToken cancellationToken);
}
