using System.Text.Json;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Infrastructure.Parsers;

public sealed class AlphaParser : IExchangeMessageParser
{
    public string Source => "alpha";

    public bool TryParse(RawExchangeMessage message, out MarketTick tick, out string? error)
    {
        try
        {
            using var document = JsonDocument.Parse(message.Payload);
            var root = document.RootElement;

            var ticker = ParserUtilities.NormalizeTicker(root.GetProperty("symbol").GetString());
            var price = ParserUtilities.ReadDecimal(root, "price");
            var volume = ParserUtilities.ReadDecimal(root, "qty");
            var exchangeTimestampUtc = ParserUtilities.ReadTimestamp(root, "ts");

            tick = new MarketTick(
                message.Source,
                ticker,
                price,
                volume,
                exchangeTimestampUtc,
                message.ReceivedAtUtc,
                message.Payload,
                ParserUtilities.BuildDedupHash(message.Source, ticker, exchangeTimestampUtc, price, volume));

            error = null;
            return true;
        }
        catch (Exception ex)
        {
            tick = default!;
            error = ex.Message;
            return false;
        }
    }
}
