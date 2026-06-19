using MarketDataAggregator.Application;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;
using MarketDataAggregator.Infrastructure;
using MarketDataAggregator.Infrastructure.Deduplication;
using MarketDataAggregator.Infrastructure.Parsers;
using MarketDataAggregator.Infrastructure.Persistence;
using MarketDataAggregator.Infrastructure.WebSockets;
using MarketDataAggregator.Worker;
using System.Data.Common;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton(AggregationOptions.Default);
builder.Services.AddSingleton<IClock, SystemClock>();
builder.Services.AddSingleton<AggregationMetrics>();
builder.Services.AddSingleton<IDuplicateDetector>(sp =>
    new SlidingWindowDuplicateDetector(sp.GetRequiredService<IClock>(), sp.GetRequiredService<AggregationOptions>().DuplicateTtl));
builder.Services.AddSingleton<ITickRepository>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    return CreateRepository(configuration);
});

builder.Services.AddSingleton<IReadOnlyList<IExchangeClient>>(sp =>
{
    var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
    var clock = sp.GetRequiredService<IClock>();
    var metrics = sp.GetRequiredService<AggregationMetrics>();
    var options = sp.GetRequiredService<AggregationOptions>();

    return new IExchangeClient[]
    {
        new WebSocketExchangeClient(
            new ExchangeSourceDefinition("alpha", new Uri("ws://localhost:5071/ws/alpha")),
            loggerFactory.CreateLogger<WebSocketExchangeClient>(),
            clock,
            options.ReconnectInitialDelay,
            options.ReconnectMaxDelay,
            metrics),
        new WebSocketExchangeClient(
            new ExchangeSourceDefinition("beta", new Uri("ws://localhost:5071/ws/beta")),
            loggerFactory.CreateLogger<WebSocketExchangeClient>(),
            clock,
            options.ReconnectInitialDelay,
            options.ReconnectMaxDelay,
            metrics),
        new WebSocketExchangeClient(
            new ExchangeSourceDefinition("gamma", new Uri("ws://localhost:5071/ws/gamma")),
            loggerFactory.CreateLogger<WebSocketExchangeClient>(),
            clock,
            options.ReconnectInitialDelay,
            options.ReconnectMaxDelay,
            metrics)
    };
});

builder.Services.AddSingleton<IExchangeMessageParser, AlphaParser>();
builder.Services.AddSingleton<IExchangeMessageParser, BetaParser>();
builder.Services.AddSingleton<IExchangeMessageParser, GammaParser>();
builder.Services.AddSingleton<AggregationService>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();

static ITickRepository CreateRepository(IConfiguration configuration)
{
    var mode = configuration["Storage:Mode"];
    if (string.IsNullOrWhiteSpace(mode) || string.Equals(mode, "inmemory", StringComparison.OrdinalIgnoreCase))
    {
        return new InMemoryTickRepository();
    }

    if (!string.Equals(mode, "postgres", StringComparison.OrdinalIgnoreCase))
    {
        throw new InvalidOperationException($"Unsupported Storage:Mode value '{mode}'.");
    }

    var providerName = configuration["Storage:ProviderName"];
    var connectionString = configuration["Storage:ConnectionString"];
    if (string.IsNullOrWhiteSpace(providerName) || string.IsNullOrWhiteSpace(connectionString))
    {
        throw new InvalidOperationException("Storage:Mode=postgres requires Storage:ProviderName and Storage:ConnectionString.");
    }

    return new PostgresTickRepository(_ =>
    {
        var factory = DbProviderFactories.GetFactory(providerName);
        var connection = factory.CreateConnection()
            ?? throw new InvalidOperationException($"Provider '{providerName}' could not create a database connection.");
        connection.ConnectionString = connectionString;
        return Task.FromResult(connection);
    });
}
