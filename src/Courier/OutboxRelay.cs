using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Npgsql;

namespace Courier;

public sealed class RelayOptions
{
    public required string BootstrapServers { get; init; }
    public int MaxShards { get; init; } = Outbox.Shards;
    public int BatchSize { get; init; } = 500;
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(20);
    public TimeSpan ShardRetryInterval { get; init; } = TimeSpan.FromMilliseconds(500);
    public Action<ClientConfig>? Configure { get; init; }
}

public sealed class OutboxRelay(NpgsqlDataSource dataSource, RelayOptions options) : IAsyncDisposable
{
    private const int LockNamespace = 7270;

    private readonly Dictionary<int, NpgsqlConnection> _held = new();
    private readonly CancellationTokenSource _stop = new();
    private IProducer<string, string>? _producer;
    private Task? _loop;
    private long _published;

    public long Published => Interlocked.Read(ref _published);

    public IReadOnlyCollection<int> HeldShards => _held.Keys.ToArray();

    public Func<IReadOnlyList<long>, Task>? AfterProduceBeforeMark { get; set; }

    public void Start() => _loop ??= Task.Run(Loop);

    public async Task StopAsync()
    {
        await _stop.CancelAsync();
        if (_loop is not null)
        {
            try
            {
                await _loop;
            }
            catch (OperationCanceledException)
            {
            }
        }

        await ReleaseAllAsync();
        _producer?.Dispose();
        _producer = null;
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    private async Task Loop()
    {
        var lastAcquire = DateTime.MinValue;
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                if (DateTime.UtcNow - lastAcquire > options.ShardRetryInterval)
                {
                    await AcquireShardsAsync();
                    lastAcquire = DateTime.UtcNow;
                }

                var sent = 0;
                foreach (var shard in _held.Keys.ToArray())
                {
                    sent += await DrainShardAsync(shard);
                }

                if (sent == 0)
                {
                    await Task.Delay(options.PollInterval, _stop.Token);
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception)
            {
                _producer?.Dispose();
                _producer = null;
                await DropBrokenShardsAsync();
                try
                {
                    await Task.Delay(100, _stop.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private async Task AcquireShardsAsync()
    {
        foreach (var shard in Enumerable.Range(0, Outbox.Shards).OrderBy(_ => Random.Shared.Next()))
        {
            if (_held.Count >= options.MaxShards)
            {
                return;
            }

            if (_held.ContainsKey(shard))
            {
                continue;
            }

            var connection = await dataSource.OpenConnectionAsync(_stop.Token);
            await using var cmd = new NpgsqlCommand("SELECT pg_try_advisory_lock(@ns, @shard)", connection);
            cmd.Parameters.AddWithValue("ns", LockNamespace);
            cmd.Parameters.AddWithValue("shard", shard);
            if ((bool)(await cmd.ExecuteScalarAsync(_stop.Token))!)
            {
                _held[shard] = connection;
            }
            else
            {
                await connection.DisposeAsync();
            }
        }
    }

    private async Task DropBrokenShardsAsync()
    {
        foreach (var (shard, connection) in _held.ToArray())
        {
            var alive = false;
            try
            {
                await using var ping = new NpgsqlCommand("SELECT 1", connection);
                await ping.ExecuteScalarAsync();
                alive = true;
            }
            catch (Exception)
            {
            }

            if (!alive)
            {
                _held.Remove(shard);
                await connection.DisposeAsync();
            }
        }
    }

    private async Task ReleaseAllAsync()
    {
        foreach (var (_, connection) in _held)
        {
            await connection.DisposeAsync();
        }

        _held.Clear();
    }

    private IProducer<string, string> Producer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 5,
            MessageTimeoutMs = 20000,
        };
        options.Configure?.Invoke(config);
        return _producer ??= new ProducerBuilder<string, string>(config).Build();
    }

    private async Task<int> DrainShardAsync(int shard)
    {
        var rows = new List<(long Id, string MessageId, string Topic, string Key, string Payload, string? Headers)>();
        await using (var select = dataSource.CreateCommand(
            "SELECT id, message_id, topic, key, payload, headers::text FROM courier_outbox WHERE shard = @shard AND published_at IS NULL ORDER BY id LIMIT @limit"))
        {
            select.Parameters.AddWithValue("shard", shard);
            select.Parameters.AddWithValue("limit", options.BatchSize);
            await using var reader = await select.ExecuteReaderAsync(_stop.Token);
            while (await reader.ReadAsync(_stop.Token))
            {
                rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5)));
            }
        }

        if (rows.Count == 0)
        {
            return 0;
        }

        var producer = Producer();
        var delivered = new ConcurrentDictionary<long, bool>();
        foreach (var row in rows)
        {
            var headers = new Headers { { "message-id", Encoding.UTF8.GetBytes(row.MessageId) } };
            if (row.Headers is not null)
            {
                foreach (var (name, value) in JsonSerializer.Deserialize<Dictionary<string, string>>(row.Headers)!)
                {
                    headers.Add(name, Encoding.UTF8.GetBytes(value));
                }
            }

            var id = row.Id;
            producer.Produce(row.Topic, new Message<string, string> { Key = row.Key, Value = row.Payload, Headers = headers },
                report => delivered[id] = report.Error.Code == ErrorCode.NoError);
        }

        producer.Flush(TimeSpan.FromSeconds(30));
        var ok = rows.Where(r => delivered.TryGetValue(r.Id, out var good) && good).Select(r => r.Id).ToList();
        if (ok.Count < rows.Count)
        {
            _producer?.Dispose();
            _producer = null;
        }

        if (AfterProduceBeforeMark is { } hook)
        {
            await hook(ok);
        }

        if (ok.Count > 0)
        {
            await using var mark = dataSource.CreateCommand("UPDATE courier_outbox SET published_at = now() WHERE id = ANY(@ids)");
            mark.Parameters.AddWithValue("ids", ok.ToArray());
            await mark.ExecuteNonQueryAsync(_stop.Token);
            Interlocked.Add(ref _published, ok.Count);
        }

        return ok.Count;
    }
}
