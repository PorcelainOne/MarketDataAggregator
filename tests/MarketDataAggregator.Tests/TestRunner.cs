using System.Net.WebSockets;
using System.Threading.Channels;
using MarketDataAggregator.Application;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;
using MarketDataAggregator.Infrastructure;
using MarketDataAggregator.Infrastructure.Deduplication;
using MarketDataAggregator.Infrastructure.Parsers;
using MarketDataAggregator.Infrastructure.Persistence;
using MarketDataAggregator.Infrastructure.WebSockets;
using MarketDataAggregator.Simulator;
using Microsoft.Extensions.Logging.Abstractions;

namespace MarketDataAggregator.Tests;

internal static class TestRunner
{
    public static async Task<int> RunAsync()
    {
        var tests = new (string Name, Func<Task> Run)[]
        {
            ("Parsers", ParserTests.RunAsync),
            ("DuplicateDetector", DuplicateDetectorTests.RunAsync),
            ("ReconnectPolicy", ReconnectPolicyTests.RunAsync),
            ("SqlBuilder", SqlBuilderTests.RunAsync),
            ("WebSocketSmoke", WebSocketSmokeTests.RunAsync)
        };

        var failures = 0;
        foreach (var (name, run) in tests)
        {
            try
            {
                await run().ConfigureAwait(false);
                Console.WriteLine($"[PASS] {name}");
            }
            catch (Exception ex)
            {
                failures++;
                Console.Error.WriteLine($"[FAIL] {name}: {ex.Message}");
                Console.Error.WriteLine(ex);
            }
        }

        Console.WriteLine(failures == 0 ? "ALL TESTS PASSED" : $"{failures} TEST(S) FAILED");
        return failures == 0 ? 0 : 1;
    }
}

internal static class ParserTests
{
    public static Task RunAsync()
    {
        var clock = new FakeClock(DateTimeOffset.Parse("2026-06-19T12:00:00Z"));
        var alpha = new AlphaParser();
        var beta = new BetaParser();
        var gamma = new GammaParser();

        var alphaMessage = new RawExchangeMessage("alpha", """{"symbol":"btc/usdt","price":43123.12,"qty":0.15,"ts":1718798400000}""", clock.UtcNow);
        AssertEx.True(alpha.TryParse(alphaMessage, out var alphaTick, out var alphaError), alphaError ?? "alpha parser failed");
        AssertEx.Equal("BTC-USDT", alphaTick.Ticker);
        AssertEx.Equal(43123.12m, alphaTick.Price);
        AssertEx.Equal(0.15m, alphaTick.Volume);
        AssertEx.Equal("alpha", alphaTick.Source);

        var betaMessage = new RawExchangeMessage("beta", """{"ticker":"eth/usdt","last":"3100.10","volume":"1.25","time":"2026-06-19T11:59:59Z"}""", clock.UtcNow);
        AssertEx.True(beta.TryParse(betaMessage, out var betaTick, out var betaError), betaError ?? "beta parser failed");
        AssertEx.Equal("ETH-USDT", betaTick.Ticker);
        AssertEx.Equal(3100.10m, betaTick.Price);
        AssertEx.Equal(1.25m, betaTick.Volume);

        var gammaMessage = new RawExchangeMessage("gamma", """{"data":{"pair":"sol_usdt","p":120.25,"v":5.5,"timestamp":1718798400}}""", clock.UtcNow);
        AssertEx.True(gamma.TryParse(gammaMessage, out var gammaTick, out var gammaError), gammaError ?? "gamma parser failed");
        AssertEx.Equal("SOL-USDT", gammaTick.Ticker);
        AssertEx.Equal(120.25m, gammaTick.Price);
        AssertEx.Equal(5.5m, gammaTick.Volume);

        var invalidMessage = new RawExchangeMessage("alpha", """{"symbol":"btc/usdt","price":"bad","qty":0.15,"ts":1718798400000}""", clock.UtcNow);
        AssertEx.False(alpha.TryParse(invalidMessage, out _, out var invalidError), "expected parse failure");
        AssertEx.True(!string.IsNullOrWhiteSpace(invalidError), "expected parse error text");

        return Task.CompletedTask;
    }
}

internal static class DuplicateDetectorTests
{
    public static Task RunAsync()
    {
        var clock = new FakeClock(DateTimeOffset.Parse("2026-06-19T12:00:00Z"));
        var detector = new SlidingWindowDuplicateDetector(clock, TimeSpan.FromSeconds(30));
        var tick = BuildTick(clock.UtcNow);

        AssertEx.True(detector.TryRegister(tick), "first observation should pass");
        AssertEx.False(detector.TryRegister(tick), "second observation should be filtered");

        clock.Advance(TimeSpan.FromSeconds(31));
        AssertEx.True(detector.TryRegister(tick), "entry should expire after TTL");
        return Task.CompletedTask;
    }

    private static MarketTick BuildTick(DateTimeOffset receivedAt)
    {
        var exchangeTimestamp = DateTimeOffset.Parse("2026-06-19T11:59:59Z");
        var ticker = "BTC-USDT";
        var price = 43123.12m;
        var volume = 0.15m;

        return new MarketTick(
            "alpha",
            ticker,
            price,
            volume,
            exchangeTimestamp,
            receivedAt,
            "{}",
            ParserUtilities.BuildDedupHash("alpha", ticker, exchangeTimestamp, price, volume));
    }
}

internal static class ReconnectPolicyTests
{
    public static Task RunAsync()
    {
        var policy = new ReconnectPolicy(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500));
        var first = policy.NextDelay();
        var second = policy.NextDelay();

        AssertEx.True(first >= TimeSpan.FromMilliseconds(100), "first delay too small");
        AssertEx.True(first <= TimeSpan.FromMilliseconds(500), "first delay too large");
        AssertEx.True(second <= TimeSpan.FromMilliseconds(500), "second delay should respect cap");

        policy.Reset();
        var reset = policy.NextDelay();
        AssertEx.True(reset >= TimeSpan.FromMilliseconds(100), "reset delay too small");
        return Task.CompletedTask;
    }
}

internal static class SqlBuilderTests
{
    public static Task RunAsync()
    {
        var batch = new[]
        {
            new MarketTick("alpha", "BTC-USDT", 43123.12m, 0.15m, DateTimeOffset.Parse("2026-06-19T11:59:59Z"), DateTimeOffset.Parse("2026-06-19T12:00:00Z"), """{"a":1}""", "hash-1"),
            new MarketTick("beta", "ETH-USDT", 3100.10m, 1.25m, DateTimeOffset.Parse("2026-06-19T11:59:58Z"), DateTimeOffset.Parse("2026-06-19T12:00:00Z"), """{"b":2}""", "hash-2")
        };

        var plan = PostgresBatchSqlBuilder.BuildInsertPlan(batch);
        AssertEx.Contains("ON CONFLICT (dedup_hash) DO NOTHING", plan.Sql);
        AssertEx.Equal(batch.Length * 8, plan.Parameters.Count);
        AssertEx.Equal("@p0_source", plan.Parameters[0].Name);
        AssertEx.Equal("alpha", plan.Parameters[0].Value);
        return Task.CompletedTask;
    }
}

internal static class WebSocketSmokeTests
{
    public static async Task RunAsync()
    {
        const int port = 5088;
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        var app = builder.Build();
        app.UseWebSockets();
        SimulatedExchangeServer.Map(app);

        await app.StartAsync().ConfigureAwait(false);

        try
        {
            var source = new ExchangeSourceDefinition("alpha", new Uri($"ws://127.0.0.1:{port}/ws/alpha"));
            var metrics = new AggregationMetrics();
            var client = new WebSocketExchangeClient(source, NullLogger<WebSocketExchangeClient>.Instance, new SystemClock(), TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(250), metrics);
            var channel = Channel.CreateUnbounded<RawExchangeMessage>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));
            var clientTask = client.RunAsync(channel.Writer, cts.Token);

            var received = 0;
            while (received < 30 && !cts.IsCancellationRequested)
            {
                if (await channel.Reader.WaitToReadAsync(cts.Token).ConfigureAwait(false))
                {
                    while (channel.Reader.TryRead(out var _))
                    {
                        received++;
                        if (received >= 30)
                        {
                            break;
                        }
                    }
                }
            }

            cts.Cancel();
            await clientTask.ConfigureAwait(false);

            AssertEx.True(received >= 30, "expected to receive messages from websocket feed");
        }
        finally
        {
            await app.StopAsync().ConfigureAwait(false);
            await app.DisposeAsync().ConfigureAwait(false);
        }
    }
}

internal static class AssertEx
{
    public static void True(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    public static void False(bool condition, string message)
    {
        if (condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    public static void Equal<T>(T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            throw new InvalidOperationException($"Expected '{expected}', got '{actual}'.");
        }
    }

    public static void Contains(string expectedSubstring, string actual)
    {
        if (!actual.Contains(expectedSubstring, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Expected text to contain '{expectedSubstring}'.");
        }
    }
}

internal sealed class FakeClock : IClock
{
    public FakeClock(DateTimeOffset initialUtcNow)
    {
        UtcNow = initialUtcNow;
    }

    public DateTimeOffset UtcNow { get; private set; }

    public void Advance(TimeSpan amount) => UtcNow = UtcNow.Add(amount);
}
