using System.Net.WebSockets;
using System.Threading.Channels;
using MarketDataAggregator.Application;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;
using Microsoft.Extensions.Logging;

namespace MarketDataAggregator.Infrastructure.WebSockets;

public sealed class WebSocketExchangeClient : IExchangeClient
{
    private readonly ExchangeSourceDefinition _source;
    private readonly ILogger<WebSocketExchangeClient> _logger;
    private readonly IClock _clock;
    private readonly ReconnectPolicy _reconnectPolicy;
    private readonly AggregationMetrics _metrics;

    public WebSocketExchangeClient(
        ExchangeSourceDefinition source,
        ILogger<WebSocketExchangeClient> logger,
        IClock clock,
        TimeSpan reconnectInitialDelay,
        TimeSpan reconnectMaxDelay,
        AggregationMetrics metrics)
    {
        _source = source;
        _logger = logger;
        _clock = clock;
        _metrics = metrics;
        _reconnectPolicy = new ReconnectPolicy(reconnectInitialDelay, reconnectMaxDelay);
    }

    public string Source => _source.Source;

    public async Task RunAsync(ChannelWriter<RawExchangeMessage> writer, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            using var socket = new ClientWebSocket();

            try
            {
                await socket.ConnectAsync(_source.Uri, cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("Connected to {Source} at {Uri}", _source.Source, _source.Uri);
                _reconnectPolicy.Reset();

                await ReadLoopAsync(socket, writer, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "WebSocket source {Source} disconnected", _source.Source);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            _metrics.MarkReconnect();
            var delay = _reconnectPolicy.NextDelay();
            _logger.LogInformation("Reconnecting {Source} in {Delay} ms", _source.Source, delay.TotalMilliseconds);

            try
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task ReadLoopAsync(ClientWebSocket socket, ChannelWriter<RawExchangeMessage> writer, CancellationToken cancellationToken)
    {
        while (socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
        {
            var payload = await WebSocketMessageReader.ReadTextMessageAsync(socket, cancellationToken).ConfigureAwait(false);
            if (payload is null)
            {
                return;
            }

            _metrics.MarkRawReceived();

            await writer.WriteAsync(new RawExchangeMessage(_source.Source, payload, _clock.UtcNow), cancellationToken).ConfigureAwait(false);
            _metrics.MarkRawQueued();
        }
    }
}
