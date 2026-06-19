using System.Collections.Concurrent;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Infrastructure.Deduplication;

public sealed class SlidingWindowDuplicateDetector : IDuplicateDetector
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> _entries = new(StringComparer.Ordinal);
    private readonly IClock _clock;
    private readonly TimeSpan _ttl;
    private long _operations;

    public SlidingWindowDuplicateDetector(IClock clock, TimeSpan ttl)
    {
        _clock = clock;
        _ttl = ttl;
    }

    public bool TryRegister(MarketTick tick)
    {
        var now = _clock.UtcNow;
        if ((Interlocked.Increment(ref _operations) & 0xff) == 0)
        {
            PurgeExpired();
        }

        var expiresAt = now.Add(_ttl);

        while (true)
        {
            if (_entries.TryGetValue(tick.DedupHash, out var existing))
            {
                if (existing > now)
                {
                    return false;
                }

                if (_entries.TryUpdate(tick.DedupHash, expiresAt, existing))
                {
                    return true;
                }

                continue;
            }

            if (_entries.TryAdd(tick.DedupHash, expiresAt))
            {
                return true;
            }
        }
    }

    public int PurgeExpired()
    {
        var now = _clock.UtcNow;
        var removed = 0;

        foreach (var entry in _entries)
        {
            if (entry.Value <= now && _entries.TryRemove(entry.Key, out _))
            {
                removed++;
            }
        }

        return removed;
    }
}
