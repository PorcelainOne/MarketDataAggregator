namespace MarketDataAggregator.Infrastructure.WebSockets;

public sealed class ReconnectPolicy
{
    private readonly TimeSpan _initialDelay;
    private readonly TimeSpan _maxDelay;
    private TimeSpan _currentDelay;

    public ReconnectPolicy(TimeSpan initialDelay, TimeSpan maxDelay)
    {
        _initialDelay = initialDelay;
        _maxDelay = maxDelay;
    }

    public void Reset() => _currentDelay = TimeSpan.Zero;

    public TimeSpan NextDelay()
    {
        var baseDelay = _currentDelay == TimeSpan.Zero ? _initialDelay : TimeSpan.FromMilliseconds(Math.Min(_maxDelay.TotalMilliseconds, _currentDelay.TotalMilliseconds * 2));
        _currentDelay = baseDelay;

        var jitterMs = Random.Shared.Next(0, Math.Max(1, (int)Math.Min(250, baseDelay.TotalMilliseconds / 4)));
        var candidate = baseDelay + TimeSpan.FromMilliseconds(jitterMs);
        return candidate > _maxDelay ? _maxDelay : candidate;
    }
}
