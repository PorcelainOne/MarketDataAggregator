using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace MarketDataAggregator.Infrastructure.Parsers;

public static class ParserUtilities
{
    internal static string NormalizeTicker(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new FormatException("Ticker is missing.");
        }

        var normalized = value.Trim().Replace('_', '-').Replace('/', '-').ToUpperInvariant();
        return normalized;
    }

    internal static decimal ReadDecimal(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            throw new FormatException($"Property '{propertyName}' is missing.");
        }

        if (property.ValueKind == JsonValueKind.Number)
        {
            return property.GetDecimal();
        }

        if (property.ValueKind == JsonValueKind.String && decimal.TryParse(property.GetString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        throw new FormatException($"Property '{propertyName}' is not a decimal.");
    }

    internal static DateTimeOffset ReadTimestamp(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property))
        {
            throw new FormatException($"Property '{propertyName}' is missing.");
        }

        if (property.ValueKind == JsonValueKind.Number && property.TryGetInt64(out var numeric))
        {
            return numeric > 1_000_000_000_000
                ? DateTimeOffset.FromUnixTimeMilliseconds(numeric).ToUniversalTime()
                : DateTimeOffset.FromUnixTimeSeconds(numeric).ToUniversalTime();
        }

        if (property.ValueKind == JsonValueKind.String)
        {
            var text = property.GetString();
            if (string.IsNullOrWhiteSpace(text))
            {
                throw new FormatException($"Property '{propertyName}' is empty.");
            }

            if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            {
                return parsed.ToUniversalTime();
            }
        }

        throw new FormatException($"Property '{propertyName}' is not a valid timestamp.");
    }

    public static string BuildDedupHash(string source, string ticker, DateTimeOffset exchangeTimestampUtc, decimal price, decimal volume)
    {
        var canonical = string.Join('|',
            source.Trim().ToLowerInvariant(),
            ticker.Trim().ToUpperInvariant(),
            exchangeTimestampUtc.UtcTicks.ToString(CultureInfo.InvariantCulture),
            price.ToString(CultureInfo.InvariantCulture),
            volume.ToString(CultureInfo.InvariantCulture));

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    internal static bool TryGetString(JsonElement element, string propertyName, out string? value)
    {
        value = null;
        if (!element.TryGetProperty(propertyName, out var property) || property.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        value = property.GetString();
        return true;
    }
}
