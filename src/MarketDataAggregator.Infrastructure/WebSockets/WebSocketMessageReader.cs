using System.Buffers;
using System.Net.WebSockets;
using System.Text;

namespace MarketDataAggregator.Infrastructure.WebSockets;

internal static class WebSocketMessageReader
{
    public static async Task<string?> ReadTextMessageAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(8192);
        try
        {
            using var stream = new MemoryStream();

            while (true)
            {
                var result = await socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return null;
                }

                if (result.MessageType != WebSocketMessageType.Text)
                {
                    throw new InvalidOperationException($"Unsupported WebSocket message type: {result.MessageType}");
                }

                stream.Write(buffer, 0, result.Count);

                if (result.EndOfMessage)
                {
                    return Encoding.UTF8.GetString(stream.ToArray());
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
