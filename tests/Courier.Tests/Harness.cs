using Npgsql;
using Testcontainers.PostgreSql;
using Testcontainers.Redpanda;
using Xunit;

namespace Courier.Tests;

public sealed class InfraFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("public.ecr.aws/docker/library/postgres:17")
        .WithCommand("-c", "max_connections=400")
        .Build();

    private readonly RedpandaContainer _redpanda = new RedpandaBuilder("docker.redpanda.com/redpandadata/redpanda:v24.3.11").Build();

    public string Bootstrap => _redpanda.GetBootstrapAddress();

    public async ValueTask InitializeAsync() => await Task.WhenAll(_postgres.StartAsync(), _redpanda.StartAsync());

    public async ValueTask DisposeAsync()
    {
        await _postgres.DisposeAsync();
        await _redpanda.DisposeAsync();
    }

    public async Task<Env> CreateEnvAsync(int partitions = 6)
    {
        var name = "t_" + Guid.NewGuid().ToString("N");
        await using (var admin = new NpgsqlConnection(_postgres.GetConnectionString()))
        {
            await admin.OpenAsync();
            await using var cmd = new NpgsqlCommand($"CREATE DATABASE {name}", admin);
            await cmd.ExecuteNonQueryAsync();
        }

        var cs = new NpgsqlConnectionStringBuilder(_postgres.GetConnectionString()) { Database = name, MaxPoolSize = 80 }.ConnectionString;
        var dataSource = NpgsqlDataSource.Create(cs);
        await Ledger.LedgerService.EnsureSchemaAsync(dataSource);
        var env = new Env(dataSource, cs, Bootstrap, "t" + Guid.NewGuid().ToString("N")[..10]);
        await Topics.EnsureAsync(Bootstrap, env.Topic, partitions);
        await Topics.EnsureAsync(Bootstrap, env.Topic + ".dlq", 1);
        return env;
    }
}

[CollectionDefinition("infra")]
public sealed class InfraCollection : ICollectionFixture<InfraFixture>;

public sealed class Env(NpgsqlDataSource dataSource, string connectionString, string bootstrap, string id) : IAsyncDisposable
{
    private readonly List<IAsyncDisposable> _owned = [];

    public NpgsqlDataSource DataSource { get; } = dataSource;

    public string ConnectionString { get; } = connectionString;

    public string Bootstrap { get; } = bootstrap;

    public string Topic { get; } = id + ".events";

    public string Group { get; } = id + "-group";

    public OutboxRelay NewRelay(int maxShards = Outbox.Shards)
    {
        var relay = new OutboxRelay(DataSource, new RelayOptions { BootstrapServers = Bootstrap, MaxShards = maxShards, ShardRetryInterval = TimeSpan.FromMilliseconds(200) });
        _owned.Add(relay);
        return relay;
    }

    public InboxConsumer NewConsumer(MessageHandler handler, Action<ConsumerOptionsBuilder>? tweak = null)
    {
        var builder = new ConsumerOptionsBuilder { Group = Group, Topic = Topic };
        tweak?.Invoke(builder);
        var consumer = new InboxConsumer(DataSource, new ConsumerOptions
        {
            BootstrapServers = Bootstrap,
            GroupId = builder.Group,
            Topics = [builder.Topic],
            MaxAttempts = builder.MaxAttempts,
            RetryBackoff = TimeSpan.FromMilliseconds(20),
            AutoCommitIntervalMs = builder.AutoCommitIntervalMs,
        }, handler);
        _owned.Add(consumer);
        return consumer;
    }

    public async Task EnqueueAsync(string key, string payload, bool commit = true)
    {
        await using var connection = await DataSource.OpenConnectionAsync();
        await using var tx = await connection.BeginTransactionAsync();
        await Outbox.EnqueueAsync(connection, tx, new OutboxMessage(Topic, key, payload));
        if (commit)
        {
            await tx.CommitAsync();
        }
    }

    public async Task<T> ScalarAsync<T>(string sql)
    {
        await using var cmd = DataSource.CreateCommand(sql);
        return (T)Convert.ChangeType((await cmd.ExecuteScalarAsync())!, typeof(T));
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var cmd = DataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<bool> UntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(100);
        }

        return await condition();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var owned in _owned)
        {
            try
            {
                await owned.DisposeAsync();
            }
            catch (Exception)
            {
            }
        }

        await DataSource.DisposeAsync();
        NpgsqlConnection.ClearAllPools();
    }
}

public sealed class ConsumerOptionsBuilder
{
    public required string Group { get; set; }

    public required string Topic { get; set; }

    public int MaxAttempts { get; set; } = 3;

    public int AutoCommitIntervalMs { get; set; } = 200;
}
