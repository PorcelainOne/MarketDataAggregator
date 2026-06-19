namespace MarketDataAggregator.Infrastructure.Persistence;

public static class SchemaSqlProvider
{
    private const string ResourceName = "MarketDataAggregator.Infrastructure.Persistence.Schema.sql";
    private static readonly Lazy<string> Cached = new(LoadInternal);

    public static string Load() => Cached.Value;

    private static string LoadInternal()
    {
        var assembly = typeof(SchemaSqlProvider).Assembly;
        using var stream = assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{ResourceName}' was not found.");

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
