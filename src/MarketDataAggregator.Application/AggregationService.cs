using System.Threading.Channels;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;
using Microsoft.Extensions.Logging;

namespace MarketDataAggregator.Application;

public sealed class AggregationService
{
    private readonly IReadOnlyList<IExchangeClient> _clients;
    private readonly IReadOnlyDictionary<string, IExchangeMessageParser> _parsers;
    private readonly IDuplicateDetector _duplicateDetector;
    private readonly ITickRepository _repository;
    private readonly AggregationOptions _options;
    private readonly AggregationMetrics _metrics;
    private readonly ILogger<AggregationService> _logger;

    public AggregationService(
        IReadOnlyList<IExchangeClient> clients,
        IEnumerable<IExchangeMessageParser> parsers,
        IDuplicateDetector duplicateDetector,
        ITickRepository repository,
        AggregationOptions options,
        AggregationMetrics metrics,
        ILogger<AggregationService> logger)
    {
        _clients = clients;
        _parsers = parsers.ToDictionary(parser => parser.Source, StringComparer.OrdinalIgnoreCase);
        _duplicateDetector = duplicateDetector;
        _repository = repository;
        _options = options;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var rawChannel = Channel.CreateBounded<RawExchangeMessage>(new BoundedChannelOptions(_options.RawChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        var normalizedChannel = Channel.CreateBounded<MarketTick>(new BoundedChannelOptions(_options.NormalizedChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });
        using var metricsCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        await _repository.EnsureSchemaAsync(cancellationToken).ConfigureAwait(false);

        var metricsTask = RunMetricsLoopAsync(metricsCts.Token);
        var sourceTasks = _clients
            .Select(client => client.RunAsync(rawChannel.Writer, cancellationToken))
            .ToArray();

        var parserTask = ParseLoopAsync(rawChannel.Reader, normalizedChannel.Writer);
        var writerTask = WriteLoopAsync(normalizedChannel.Reader, cancellationToken);

        Exception? pipelineFailure = null;

        try
        {
            await Task.WhenAll(sourceTasks).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            pipelineFailure = ex;
        }
        finally
        {
            metricsCts.Cancel();
            rawChannel.Writer.TryComplete();
        }

        await parserTask.ConfigureAwait(false);
        await writerTask.ConfigureAwait(false);
        await metricsTask.ConfigureAwait(false);

        if (pipelineFailure is not null)
        {
            throw pipelineFailure;
        }
    }

    private async Task ParseLoopAsync(ChannelReader<RawExchangeMessage> reader, ChannelWriter<MarketTick> writer)
    {
        try
        {
            await foreach (var raw in reader.ReadAllAsync().ConfigureAwait(false))
            {
                _metrics.MarkRawDequeued();

                if (!_parsers.TryGetValue(raw.Source, out var parser))
                {
                    _logger.LogWarning("No parser registered for source {Source}", raw.Source);
                    _metrics.MarkParseError();
                    continue;
                }

                if (!parser.TryParse(raw, out var tick, out var error))
                {
                    _logger.LogWarning("Parse error for source {Source}: {Error}", raw.Source, error);
                    _metrics.MarkParseError();
                    continue;
                }

                await writer.WriteAsync(tick).ConfigureAwait(false);
                _metrics.MarkNormalizedQueued();
                _metrics.MarkNormalizedProduced();
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task WriteLoopAsync(ChannelReader<MarketTick> reader, CancellationToken cancellationToken)
    {
        var batch = new List<MarketTick>(_options.BatchSize);
        using var batchGate = new SemaphoreSlim(1, 1);
        using var flushCts = new CancellationTokenSource();
        var timerTask = RunFlushTimerAsync(batch, batchGate, flushCts.Token, cancellationToken);

        try
        {
            await foreach (var tick in reader.ReadAllAsync().ConfigureAwait(false))
            {
                _metrics.MarkNormalizedDequeued();

                if (!_duplicateDetector.TryRegister(tick))
                {
                    _metrics.MarkDuplicateSkipped();
                    continue;
                }

                List<MarketTick>? batchToFlush = null;
                await batchGate.WaitAsync().ConfigureAwait(false);
                try
                {
                    batch.Add(tick);
                    if (batch.Count >= _options.BatchSize)
                    {
                        batchToFlush = DrainBatch(batch);
                    }
                }
                finally
                {
                    batchGate.Release();
                }

                if (batchToFlush is not null)
                {
                    await FlushBatchAsync(batchToFlush, GetFlushToken(cancellationToken)).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            flushCts.Cancel();

            List<MarketTick> remaining;
            await batchGate.WaitAsync().ConfigureAwait(false);
            try
            {
                remaining = DrainBatch(batch);
            }
            finally
            {
                batchGate.Release();
            }

            if (remaining.Count > 0)
            {
                await FlushBatchAsync(remaining, CancellationToken.None).ConfigureAwait(false);
            }

            await timerTask.ConfigureAwait(false);
        }
    }

    private async Task RunFlushTimerAsync(List<MarketTick> batch, SemaphoreSlim batchGate, CancellationToken cancellationToken, CancellationToken shutdownToken)
    {
        try
        {
            using var timer = new PeriodicTimer(_options.FlushInterval);
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                List<MarketTick>? batchToFlush = null;
                var lockTaken = false;
                try
                {
                    await batchGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                    lockTaken = true;
                    if (batch.Count > 0)
                    {
                        batchToFlush = DrainBatch(batch);
                    }
                }
                finally
                {
                    if (lockTaken)
                    {
                        batchGate.Release();
                    }
                }

                if (batchToFlush is not null)
                {
                    await FlushBatchAsync(batchToFlush, GetFlushToken(shutdownToken)).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private CancellationToken GetFlushToken(CancellationToken cancellationToken) => cancellationToken.IsCancellationRequested ? CancellationToken.None : cancellationToken;

    private static List<MarketTick> DrainBatch(List<MarketTick> batch)
    {
        var snapshot = batch.ToList();
        batch.Clear();
        return snapshot;
    }

    private async Task FlushBatchAsync(IReadOnlyList<MarketTick> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
        {
            return;
        }

        var inserted = await _repository.WriteBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        _metrics.MarkInserted(inserted);
        _metrics.MarkDbConflicts(batch.Count - inserted);
        _metrics.MarkBatchFlush();

        _logger.LogInformation(
            "Batch flushed: size={Size}, inserted={Inserted}, conflicts={Conflicts}",
            batch.Count,
            inserted,
            batch.Count - inserted);
    }

    private async Task RunMetricsLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                _logger.LogInformation("Metrics: {Snapshot}", _metrics.Snapshot());
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
