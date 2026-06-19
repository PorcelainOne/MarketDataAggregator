using System.Data.Common;
using MarketDataAggregator.Application.Abstractions;
using MarketDataAggregator.Domain;

namespace MarketDataAggregator.Infrastructure.Persistence;

public sealed class PostgresTickRepository : ITickRepository
{
    private readonly Func<CancellationToken, Task<DbConnection>> _connectionFactory;

    public PostgresTickRepository(Func<CancellationToken, Task<DbConnection>> connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task EnsureSchemaAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _connectionFactory(cancellationToken).ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText = SchemaSqlProvider.Load();
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> WriteBatchAsync(IReadOnlyList<MarketTick> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
        {
            return 0;
        }

        await using var connection = await _connectionFactory(cancellationToken).ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        var plan = PostgresBatchSqlBuilder.BuildInsertPlan(batch);
        command.CommandText = plan.Sql;

        foreach (var parameterSpec in plan.Parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = parameterSpec.Name;
            parameter.Value = parameterSpec.Value;
            command.Parameters.Add(parameter);
        }

        var affected = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return Math.Max(0, affected);
    }

}
