using System.Data.Common;
using System.Text;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Infrastructure.Persistence;

public sealed record SqlParameterSpec(string Name, object Value);

public sealed record SqlCommandPlan(string Sql, IReadOnlyList<SqlParameterSpec> Parameters);

public static class PostgresBatchSqlBuilder
{
    public static SqlCommandPlan BuildInsertPlan(IReadOnlyList<MarketTick> batch)
    {
        var parameters = new List<SqlParameterSpec>(batch.Count * 8);
        var sql = new StringBuilder();
        sql.AppendLine("""
            INSERT INTO market_ticks (
                source,
                ticker,
                price,
                volume,
                exchange_timestamp,
                received_at,
                raw_payload,
                dedup_hash
            )
            VALUES
            """);

        for (var index = 0; index < batch.Count; index++)
        {
            if (index > 0)
            {
                sql.AppendLine(",");
            }

            var tick = batch[index];
            sql.Append($"(@p{index}_source, @p{index}_ticker, @p{index}_price, @p{index}_volume, @p{index}_exchange_timestamp, @p{index}_received_at, @p{index}_raw_payload, @p{index}_dedup_hash)");

            parameters.Add(new SqlParameterSpec($"@p{index}_source", tick.Source));
            parameters.Add(new SqlParameterSpec($"@p{index}_ticker", tick.Ticker));
            parameters.Add(new SqlParameterSpec($"@p{index}_price", tick.Price));
            parameters.Add(new SqlParameterSpec($"@p{index}_volume", tick.Volume));
            parameters.Add(new SqlParameterSpec($"@p{index}_exchange_timestamp", tick.ExchangeTimestampUtc));
            parameters.Add(new SqlParameterSpec($"@p{index}_received_at", tick.ReceivedAtUtc));
            parameters.Add(new SqlParameterSpec($"@p{index}_raw_payload", tick.RawPayload));
            parameters.Add(new SqlParameterSpec($"@p{index}_dedup_hash", tick.DedupHash));
        }

        sql.AppendLine();
        sql.AppendLine("ON CONFLICT (dedup_hash) DO NOTHING;");
        return new SqlCommandPlan(sql.ToString(), parameters);
    }
}
