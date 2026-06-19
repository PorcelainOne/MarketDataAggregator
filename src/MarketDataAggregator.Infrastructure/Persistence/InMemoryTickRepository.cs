using System.Collections.Concurrent;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Infrastructure.Persistence;

public sealed class InMemoryTickRepository : ITickRepository
{
    private readonly ConcurrentQueue<MarketTick> _ticks = new();

    public Task EnsureSchemaAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<int> WriteBatchAsync(IReadOnlyList<MarketTick> batch, CancellationToken cancellationToken)
    {
        foreach (var tick in batch)
        {
            _ticks.Enqueue(tick);
        }

        return Task.FromResult(batch.Count);
    }

    public IReadOnlyCollection<MarketTick> Snapshot() => _ticks.ToArray();
}
