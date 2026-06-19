using System.Net.WebSockets;
using System.Globalization;
using System.Text;

namespace MarketDataAggregator.Simulator;

public static class SimulatedExchangeServer
{
    public static void Map(WebApplication app)
    {
        app.Map("/ws/alpha", context => HandleFeedAsync(context, CreateAlphaPayload, disconnectEvery: 25, duplicateEvery: 8, malformedEvery: 13, TimeSpan.FromMilliseconds(80), context.RequestAborted));
        app.Map("/ws/beta", context => HandleFeedAsync(context, CreateBetaPayload, disconnectEvery: 31, duplicateEvery: 10, malformedEvery: 17, TimeSpan.FromMilliseconds(100), context.RequestAborted));
        app.Map("/ws/gamma", context => HandleFeedAsync(context, CreateGammaPayload, disconnectEvery: 37, duplicateEvery: 9, malformedEvery: 19, TimeSpan.FromMilliseconds(120), context.RequestAborted));
    }

    private static async Task HandleFeedAsync(
        HttpContext context,
        Func<int, string> payloadFactory,
        int disconnectEvery,
        int duplicateEvery,
        int malformedEvery,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsync("WebSocket connection expected.", cancellationToken).ConfigureAwait(false);
            return;
        }

        using var socket = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
        var lastPayload = string.Empty;

        for (var sequence = 1; socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested; sequence++)
        {
            if (sequence % disconnectEvery == 0)
            {
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "simulated disconnect", cancellationToken).ConfigureAwait(false);
                return;
            }

            var payload = sequence % malformedEvery == 0
                ? "{ malformed json"
                : sequence % duplicateEvery == 0 && !string.IsNullOrEmpty(lastPayload)
                    ? lastPayload
                    : payloadFactory(sequence);

            if (payload != "{ malformed json")
            {
                lastPayload = payload;
            }

            var bytes = Encoding.UTF8.GetBytes(payload);
            await socket.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken).ConfigureAwait(false);
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private static string CreateAlphaPayload(int sequence) =>
        $"{{\"symbol\":\"BTCUSDT\",\"price\":{(43000 + sequence).ToString(CultureInfo.InvariantCulture)},\"qty\":{(0.1m + sequence / 100m).ToString(CultureInfo.InvariantCulture)},\"ts\":{DateTimeOffset.UtcNow.AddSeconds(-sequence).ToUnixTimeMilliseconds()}}}";

    private static string CreateBetaPayload(int sequence) =>
        $"{{\"ticker\":\"eth/usdt\",\"last\":\"{(3100 + sequence).ToString(CultureInfo.InvariantCulture)}\",\"volume\":\"{(1.5m + sequence / 10m).ToString(CultureInfo.InvariantCulture)}\",\"time\":\"{DateTimeOffset.UtcNow.AddSeconds(-sequence):O}\"}}";

    private static string CreateGammaPayload(int sequence) =>
        $"{{\"data\":{{\"pair\":\"sol_usdt\",\"p\":{(120 + sequence).ToString(CultureInfo.InvariantCulture)},\"v\":{(5.5m + sequence / 10m).ToString(CultureInfo.InvariantCulture)},\"timestamp\":{DateTimeOffset.UtcNow.AddSeconds(-sequence).ToUnixTimeSeconds()}}}}}";
}
