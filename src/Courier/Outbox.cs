using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Courier;

public sealed record OutboxMessage(string Topic, string Key, string Payload, IReadOnlyDictionary<string, string>? Headers = null);

public static class Outbox
{
    public const int Shards = 16;

    private const string Schema = """
        CREATE TABLE IF NOT EXISTS courier_outbox (
            id bigserial PRIMARY KEY,
            message_id text NOT NULL UNIQUE,
            topic text NOT NULL,
            key text NOT NULL,
            shard int NOT NULL,
            payload text NOT NULL,
            headers jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            published_at timestamptz
        ) WITH (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);
        CREATE INDEX IF NOT EXISTS courier_outbox_pending ON courier_outbox (shard, id) WHERE published_at IS NULL;

        CREATE TABLE IF NOT EXISTS courier_inbox (
            consumer text NOT NULL,
            message_id text NOT NULL,
            processed_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (consumer, message_id)
        );
        """;

    public static async Task EnsureSchemaAsync(NpgsqlDataSource dataSource, CancellationToken ct = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var advisory = new NpgsqlCommand("SELECT pg_advisory_xact_lock(727003)", connection, tx))
        {
            await advisory.ExecuteNonQueryAsync(ct);
        }

        await using (var create = new NpgsqlCommand(Schema, connection, tx))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    public static async Task<string> EnqueueAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, OutboxMessage message, CancellationToken ct = default)
    {
        var messageId = Guid.NewGuid().ToString("N");
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO courier_outbox (message_id, topic, key, shard, payload, headers) VALUES (@id, @topic, @key, @shard, @payload, @headers)",
            connection, transaction);
        cmd.Parameters.AddWithValue("id", messageId);
        cmd.Parameters.AddWithValue("topic", message.Topic);
        cmd.Parameters.AddWithValue("key", message.Key);
        cmd.Parameters.AddWithValue("shard", ShardOf(message.Key));
        cmd.Parameters.AddWithValue("payload", message.Payload);
        cmd.Parameters.Add(new NpgsqlParameter("headers", NpgsqlDbType.Jsonb)
        {
            Value = message.Headers is null ? DBNull.Value : JsonSerializer.Serialize(message.Headers),
        });
        await cmd.ExecuteNonQueryAsync(ct);
        return messageId;
    }

    public static int ShardOf(string key)
    {
        var hash = 2166136261u;
        foreach (var b in Encoding.UTF8.GetBytes(key))
        {
            hash = (hash ^ b) * 16777619u;
        }

        return (int)(hash % Shards);
    }
}
