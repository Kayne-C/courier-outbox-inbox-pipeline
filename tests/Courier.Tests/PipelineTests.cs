using System.Collections.Concurrent;
using Npgsql;
using Xunit;

namespace Courier.Tests;

[Collection("infra")]
public class PipelineTests(InfraFixture fixture)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private static async Task PrepareAsync(Env env)
    {
        await env.ExecuteAsync("CREATE TABLE received (message_id text, key text, payload text, delivery_count int)");
        await env.ExecuteAsync("CREATE TABLE violations (key text, expected int, actual int)");
        await env.ExecuteAsync("CREATE TABLE last_seq (key text PRIMARY KEY, seq int NOT NULL)");
    }

    private static MessageHandler Recording() => async (message, connection, tx, ct) =>
    {
        await using var cmd = new NpgsqlCommand("INSERT INTO received (message_id, key, payload) VALUES (@id, @key, @payload)", connection, tx);
        cmd.Parameters.AddWithValue("id", message.MessageId);
        cmd.Parameters.AddWithValue("key", message.Key ?? "");
        cmd.Parameters.AddWithValue("payload", message.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    };

    private static MessageHandler OrderChecking() => async (message, connection, tx, ct) =>
    {
        var parts = message.Value.Split(':');
        var seq = int.Parse(parts[1]);
        await using var current = new NpgsqlCommand("SELECT seq FROM last_seq WHERE key = @key FOR UPDATE", connection, tx);
        current.Parameters.AddWithValue("key", message.Key!);
        var previous = await current.ExecuteScalarAsync(ct) is int last ? last : 0;
        if (seq != previous + 1)
        {
            await using var violation = new NpgsqlCommand("INSERT INTO violations VALUES (@key, @expected, @actual)", connection, tx);
            violation.Parameters.AddWithValue("key", message.Key!);
            violation.Parameters.AddWithValue("expected", previous + 1);
            violation.Parameters.AddWithValue("actual", seq);
            await violation.ExecuteNonQueryAsync(ct);
        }

        await using var upsert = new NpgsqlCommand(
            "INSERT INTO last_seq VALUES (@key, @seq) ON CONFLICT (key) DO UPDATE SET seq = @seq", connection, tx);
        upsert.Parameters.AddWithValue("key", message.Key!);
        upsert.Parameters.AddWithValue("seq", seq);
        await upsert.ExecuteNonQueryAsync(ct);
        await Recording()(message, connection, tx, ct);
    };

    [Fact]
    public async Task A_committed_outbox_row_reaches_the_consumer_exactly_once_with_its_message_id()
    {
        await using var env = await fixture.CreateEnvAsync();
        await PrepareAsync(env);
        var relay = env.NewRelay();
        var consumer = env.NewConsumer(Recording());
        relay.Start();
        consumer.Start();

        await env.EnqueueAsync("k1", "hello");

        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") == 1, Patience));
        var outboxId = await env.ScalarAsync<string>("SELECT message_id FROM courier_outbox");
        Assert.Equal(outboxId, await env.ScalarAsync<string>("SELECT message_id FROM received"));
        Assert.Equal("hello", await env.ScalarAsync<string>("SELECT payload FROM received"));
        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM courier_outbox WHERE published_at IS NULL") == 0, Patience));
    }

    [Fact]
    public async Task A_rolled_back_transaction_publishes_nothing()
    {
        await using var env = await fixture.CreateEnvAsync();
        await PrepareAsync(env);
        var relay = env.NewRelay();
        var consumer = env.NewConsumer(Recording());
        relay.Start();
        consumer.Start();

        await env.EnqueueAsync("k1", "never", commit: false);
        await env.EnqueueAsync("k1", "visible");

        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") == 1, Patience));
        await Task.Delay(1000);
        Assert.Equal("visible", await env.ScalarAsync<string>("SELECT payload FROM received"));
        Assert.Equal(1L, await env.ScalarAsync<long>("SELECT count(*) FROM courier_outbox"));
    }

    [Fact]
    public async Task A_relay_that_dies_between_publishing_and_marking_causes_duplicates_the_inbox_absorbs()
    {
        await using var env = await fixture.CreateEnvAsync();
        await PrepareAsync(env);
        for (var i = 0; i < 40; i++)
        {
            await env.EnqueueAsync($"k{i % 4}", $"m{i}");
        }

        var crashes = 0;
        var relay = env.NewRelay();
        relay.AfterProduceBeforeMark = _ => Interlocked.Increment(ref crashes) <= 3
            ? throw new InvalidOperationException("simulated crash after the broker acknowledged")
            : Task.CompletedTask;
        var consumer = env.NewConsumer(Recording());
        relay.Start();
        consumer.Start();

        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM courier_outbox WHERE published_at IS NULL") == 0, Patience));
        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") == 40, Patience));
        await Task.Delay(1500);

        Assert.Equal(40L, await env.ScalarAsync<long>("SELECT count(*) FROM received"));
        Assert.Equal(40L, await env.ScalarAsync<long>("SELECT count(DISTINCT message_id) FROM received"));
        Assert.True(consumer.Duplicates > 0, "the republished rows should have been recognized as duplicates");
    }

    [Fact]
    public async Task Events_of_one_key_stay_in_order_with_two_relays_and_two_consumers()
    {
        await using var env = await fixture.CreateEnvAsync();
        await PrepareAsync(env);
        var relays = new[] { env.NewRelay(8), env.NewRelay(8) };
        var consumers = new[] { env.NewConsumer(OrderChecking()), env.NewConsumer(OrderChecking()) };
        foreach (var relay in relays)
        {
            relay.Start();
        }

        foreach (var consumer in consumers)
        {
            consumer.Start();
        }

        const int keys = 20;
        const int perKey = 60;
        await Task.WhenAll(Enumerable.Range(0, keys).Select(k => Task.Run(async () =>
        {
            for (var seq = 1; seq <= perKey; seq++)
            {
                await env.EnqueueAsync($"key-{k}", $"key-{k}:{seq}");
            }
        })));

        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") == keys * perKey, Patience));
        Assert.Equal(0L, await env.ScalarAsync<long>("SELECT count(*) FROM violations"));
        Assert.Equal((long)keys, await env.ScalarAsync<long>($"SELECT count(*) FROM last_seq WHERE seq = {perKey}"));
        Assert.Equal(16, relays.Sum(r => r.HeldShards.Count));
    }

    [Fact]
    public async Task A_consumer_killed_before_its_offsets_were_committed_is_replaced_without_double_effects()
    {
        await using var env = await fixture.CreateEnvAsync();
        await PrepareAsync(env);
        var relay = env.NewRelay();
        relay.Start();
        for (var i = 0; i < 300; i++)
        {
            await env.EnqueueAsync($"k{i % 10}", $"m{i}");
        }

        var first = env.NewConsumer(Recording(), b => b.AutoCommitIntervalMs = 60_000);
        first.SkipOffsetStore = () => true;
        first.Start();
        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") >= 100, Patience));
        await first.KillAsync();
        var seenByFirst = await env.ScalarAsync<long>("SELECT count(*) FROM received");

        var second = env.NewConsumer(Recording());
        second.Start();

        Assert.True(await env.UntilAsync(() => Task.FromResult(second.Processed + second.Duplicates >= 300), Patience));
        await Task.Delay(500);
        Assert.Equal(300L, await env.ScalarAsync<long>("SELECT count(*) FROM received"));
        Assert.Equal(300L, await env.ScalarAsync<long>("SELECT count(DISTINCT message_id) FROM received"));
        Assert.True(second.Duplicates > 0, $"the replacement should have re-read messages the first consumer finished (first saw {seenByFirst})");
    }

    [Fact]
    public async Task A_transient_failure_is_retried_without_dead_lettering()
    {
        await using var env = await fixture.CreateEnvAsync();
        await PrepareAsync(env);
        var attempts = new ConcurrentDictionary<string, int>();
        MessageHandler flaky = async (message, connection, tx, ct) =>
        {
            if (attempts.AddOrUpdate(message.Value, 1, (_, n) => n + 1) < 3)
            {
                throw new InvalidOperationException("temporarily unavailable");
            }

            await Recording()(message, connection, tx, ct);
        };
        var relay = env.NewRelay();
        var consumer = env.NewConsumer(flaky);
        relay.Start();
        consumer.Start();

        await env.EnqueueAsync("k", "wobbly");

        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") == 1, Patience));
        Assert.Equal(3, attempts["wobbly"]);
        Assert.Equal(0, consumer.DeadLettered);
    }

    [Fact]
    public async Task A_poison_message_is_dead_lettered_after_retries_and_does_not_block_the_partition()
    {
        await using var env = await fixture.CreateEnvAsync(partitions: 1);
        await PrepareAsync(env);
        var poisonCalls = 0;
        MessageHandler handler = async (message, connection, tx, ct) =>
        {
            if (message.Value == "poison")
            {
                Interlocked.Increment(ref poisonCalls);
                throw new InvalidOperationException("cannot process this");
            }

            await Recording()(message, connection, tx, ct);
        };
        var relay = env.NewRelay();
        var consumer = env.NewConsumer(handler, b => b.MaxAttempts = 3);
        relay.Start();
        consumer.Start();

        await env.EnqueueAsync("k", "before");
        await env.EnqueueAsync("k", "poison");
        await env.EnqueueAsync("k", "after");

        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") == 2, Patience));
        Assert.True(await env.UntilAsync(async () => consumer.DeadLettered == 1, Patience));
        Assert.InRange(poisonCalls, 3, 4);
        Assert.Equal(["after", "before"], (await ReceivedAsync(env)).Order());
    }

    [Fact]
    public async Task A_permanent_failure_skips_the_retries()
    {
        await using var env = await fixture.CreateEnvAsync();
        await PrepareAsync(env);
        var calls = 0;
        MessageHandler handler = (message, connection, tx, ct) =>
        {
            Interlocked.Increment(ref calls);
            throw new PermanentFailureException("schema version 9 is not supported");
        };
        var relay = env.NewRelay();
        var consumer = env.NewConsumer(handler, b => b.MaxAttempts = 5);
        relay.Start();
        consumer.Start();

        await env.EnqueueAsync("k", "{}");

        Assert.True(await env.UntilAsync(async () => consumer.DeadLettered == 1, Patience));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Dead_lettered_messages_can_be_replayed_once_the_bug_is_fixed()
    {
        await using var env = await fixture.CreateEnvAsync(partitions: 1);
        await PrepareAsync(env);
        var broken = true;
        MessageHandler handler = async (message, connection, tx, ct) =>
        {
            if (Volatile.Read(ref broken) && message.Value.StartsWith("bad", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("bug");
            }

            await Recording()(message, connection, tx, ct);
        };
        var relay = env.NewRelay();
        var consumer = env.NewConsumer(handler, b => b.MaxAttempts = 2);
        relay.Start();
        consumer.Start();

        await env.EnqueueAsync("k", "good-1");
        await env.EnqueueAsync("k", "bad-1");
        await env.EnqueueAsync("k", "bad-2");
        Assert.True(await env.UntilAsync(async () => consumer.DeadLettered == 2, Patience));

        Volatile.Write(ref broken, false);
        var replayed = await DeadLetters.ReplayAsync(env.Bootstrap, env.Topic + ".dlq", env.Group + "-replay", 100);

        Assert.Equal(2, replayed);
        Assert.True(await env.UntilAsync(async () => await env.ScalarAsync<long>("SELECT count(*) FROM received") == 3, Patience));
        Assert.Equal(["bad-1", "bad-2", "good-1"], (await ReceivedAsync(env)).Order());
    }

    private static async Task<List<string>> ReceivedAsync(Env env)
    {
        var list = new List<string>();
        await using var cmd = env.DataSource.CreateCommand("SELECT payload FROM received");
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetString(0));
        }

        return list;
    }
}
