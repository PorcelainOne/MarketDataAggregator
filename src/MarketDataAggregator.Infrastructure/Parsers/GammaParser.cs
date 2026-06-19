using System.Text.Json;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Infrastructure.Parsers;

public sealed class GammaParser : IExchangeMessageParser
{
    public string Source => "gamma";

    public bool TryParse(RawExchangeMessage message, out MarketTick tick, out string? error)
    {
        try
        {
            using var document = JsonDocument.Parse(message.Payload);
            var data = document.RootElement.GetProperty("data");

            var ticker = ParserUtilities.NormalizeTicker(data.GetProperty("pair").GetString());
            var price = ParserUtilities.ReadDecimal(data, "p");
            var volume = ParserUtilities.ReadDecimal(data, "v");
            var exchangeTimestampUtc = ParserUtilities.ReadTimestamp(data, "timestamp");

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
