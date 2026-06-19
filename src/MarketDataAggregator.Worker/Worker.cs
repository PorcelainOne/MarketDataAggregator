using MarketDataAggregator.Application;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MarketDataAggregator.Worker;

public sealed class Worker : BackgroundService
{
    private readonly AggregationService _aggregationService;
    private readonly ILogger<Worker> _logger;

    public Worker(AggregationService aggregationService, ILogger<Worker> logger)
    {
        _aggregationService = aggregationService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Market data worker started");
        await _aggregationService.RunAsync(stoppingToken).ConfigureAwait(false);
    }
}
