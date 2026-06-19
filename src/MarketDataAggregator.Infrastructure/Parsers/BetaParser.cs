using System.Text.Json;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Infrastructure.Parsers;

public sealed class BetaParser : IExchangeMessageParser
{
    public string Source => "beta";

    public bool TryParse(RawExchangeMessage message, out MarketTick tick, out string? error)
    {
        try
        {
            using var document = JsonDocument.Parse(message.Payload);
            var root = document.RootElement;

            var ticker = ParserUtilities.NormalizeTicker(root.GetProperty("ticker").GetString());
            var price = ParserUtilities.ReadDecimal(root, "last");
            var volume = ParserUtilities.ReadDecimal(root, "volume");
            var exchangeTimestampUtc = ParserUtilities.ReadTimestamp(root, "time");

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
