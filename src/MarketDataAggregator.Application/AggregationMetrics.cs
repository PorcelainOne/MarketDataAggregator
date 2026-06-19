namespace MarketDataAggregator.Application;

public sealed class AggregationMetrics
{
    private long _rawReceived;
    private long _rawQueued;
    private long _normalizedProduced;
    private long _normalizedQueued;
    private long _duplicatesSkipped;
    private long _parseErrors;
    private long _inserted;
    private long _dbConflicts;
    private long _reconnects;
    private long _batchFlushes;

    public void MarkRawReceived() => Interlocked.Increment(ref _rawReceived);
    public void MarkRawQueued() => Interlocked.Increment(ref _rawQueued);
    public void MarkRawDequeued() => Interlocked.Decrement(ref _rawQueued);
    public void MarkNormalizedProduced() => Interlocked.Increment(ref _normalizedProduced);
    public void MarkNormalizedQueued() => Interlocked.Increment(ref _normalizedQueued);
    public void MarkNormalizedDequeued() => Interlocked.Decrement(ref _normalizedQueued);
    public void MarkDuplicateSkipped() => Interlocked.Increment(ref _duplicatesSkipped);
    public void MarkParseError() => Interlocked.Increment(ref _parseErrors);
    public void MarkInserted(int count)
    {
        if (count > 0)
        {
            Interlocked.Add(ref _inserted, count);
        }
    }
    public void MarkDbConflicts(int count)
    {
        if (count > 0)
        {
            Interlocked.Add(ref _dbConflicts, count);
        }
    }
    public void MarkReconnect() => Interlocked.Increment(ref _reconnects);
    public void MarkBatchFlush() => Interlocked.Increment(ref _batchFlushes);

    public string Snapshot() =>
        $"received={Volatile.Read(ref _rawReceived)} " +
        $"normalized={Volatile.Read(ref _normalizedProduced)} " +
        $"inserted={Volatile.Read(ref _inserted)} " +
        $"duplicates={Volatile.Read(ref _duplicatesSkipped)} " +
        $"parseErrors={Volatile.Read(ref _parseErrors)} " +
        $"dbConflicts={Volatile.Read(ref _dbConflicts)} " +
        $"reconnects={Volatile.Read(ref _reconnects)} " +
        $"rawQueue={Volatile.Read(ref _rawQueued)} " +
        $"normalizedQueue={Volatile.Read(ref _normalizedQueued)}";
}
