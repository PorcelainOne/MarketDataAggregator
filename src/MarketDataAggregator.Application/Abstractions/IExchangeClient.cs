using System.Threading.Channels;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Application.Abstractions;

public interface IExchangeClient
{
    string Source { get; }

    Task RunAsync(ChannelWriter<RawExchangeMessage> writer, CancellationToken cancellationToken);
}
