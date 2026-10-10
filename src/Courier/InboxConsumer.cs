using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;

namespace Courier;

public sealed record ConsumedMessage(
    string Topic,
    int Partition,
    long Offset,
    string? Key,
    string Value,
    string MessageId,
    IReadOnlyDictionary<string, string> Headers);

public delegate Task MessageHandler(ConsumedMessage message, NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct);

public sealed class PermanentFailureException(string message, Exception? inner = null) : Exception(message, inner);

public sealed class ConsumerOptions
{
    public required string BootstrapServers { get; init; }
    public required string GroupId { get; init; }
    public required IReadOnlyList<string> Topics { get; init; }
    public int MaxAttempts { get; init; } = 4;
    public TimeSpan RetryBackoff { get; init; } = TimeSpan.FromMilliseconds(100);
    public int AutoCommitIntervalMs { get; init; } = 200;
    public int BatchSize { get; init; } = 100;
    public string DeadLetterSuffix { get; init; } = ".dlq";
    public Action<ClientConfig>? Configure { get; init; }
}

public sealed class InboxConsumer(NpgsqlDataSource dataSource, ConsumerOptions options, MessageHandler handler) : IAsyncDisposable
{
    private readonly CancellationTokenSource _stop = new();
    private Task? _loop;
    private volatile bool _killed;
    private IProducer<string, string>? _dlqProducer;
    private long _processed;
    private long _duplicates;
    private long _deadLettered;

    public long Processed => Interlocked.Read(ref _processed);

    public long Duplicates => Interlocked.Read(ref _duplicates);

    public long DeadLettered => Interlocked.Read(ref _deadLettered);

    public void Start() => _loop ??= Task.Factory.StartNew(Loop, TaskCreationOptions.LongRunning).Unwrap();

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

        _dlqProducer?.Dispose();
    }

    public async Task KillAsync()
    {
        _killed = true;
        await StopAsync();
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    private async Task Loop()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs = 10000,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        };
        options.Configure?.Invoke(config);
        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsRevokedHandler((c, _) => CommitStored(c))
            .Build();
        consumer.Subscribe(options.Topics);
        var lastCommit = DateTime.UtcNow;
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var first = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (first is not null && !first.IsPartitionEOF)
                {
                    var batch = new List<ConsumeResult<string, string>> { first };
                    while (batch.Count < options.BatchSize && consumer.Consume(TimeSpan.Zero) is { } more)
                    {
                        if (!more.IsPartitionEOF)
                        {
                            batch.Add(more);
                        }
                    }

                    await ProcessBatchAsync(batch);
                    foreach (var result in batch)
                    {
                        consumer.StoreOffset(result);
                    }
                }

                if ((DateTime.UtcNow - lastCommit).TotalMilliseconds >= options.AutoCommitIntervalMs)
                {
                    CommitStored(consumer);
                    lastCommit = DateTime.UtcNow;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            if (!_killed)
            {
                CommitStored(consumer);
                try
                {
                    consumer.Close();
                }
                catch (Exception)
                {
                }
            }
        }
    }

    private async Task ProcessBatchAsync(List<ConsumeResult<string, string>> batch)
    {
        if (batch.Count > 1)
        {
            try
            {
                await HandleBatchOnceAsync(batch.Select(ToMessage).ToList());
                return;
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
            }
        }

        foreach (var result in batch)
        {
            await ProcessWithRetryAsync(ToMessage(result), result);
        }
    }

    private async Task HandleBatchOnceAsync(List<ConsumedMessage> messages)
    {
        await using var connection = await dataSource.OpenConnectionAsync(_stop.Token);
        await using var tx = await connection.BeginTransactionAsync(_stop.Token);
        var fresh = new HashSet<string>();
        await using (var inbox = new NpgsqlCommand(
            "INSERT INTO courier_inbox (consumer, message_id) SELECT @consumer, unnest(@ids::text[]) ON CONFLICT DO NOTHING RETURNING message_id", connection, tx))
        {
            inbox.Parameters.AddWithValue("consumer", options.GroupId);
            inbox.Parameters.AddWithValue("ids", messages.Select(m => m.MessageId).Distinct().ToArray());
            await using var reader = await inbox.ExecuteReaderAsync(_stop.Token);
            while (await reader.ReadAsync(_stop.Token))
            {
                fresh.Add(reader.GetString(0));
            }
        }

        var handled = 0;
        foreach (var message in messages)
        {
            if (fresh.Remove(message.MessageId))
            {
                await handler(message, connection, tx, _stop.Token);
                handled++;
            }
        }

        await tx.CommitAsync(_stop.Token);
        Interlocked.Add(ref _processed, handled);
        Interlocked.Add(ref _duplicates, messages.Count - handled);
    }

    private async Task ProcessWithRetryAsync(ConsumedMessage message, ConsumeResult<string, string> result)
    {
        Exception? last = null;
        for (var attempt = 1; attempt <= options.MaxAttempts; attempt++)
        {
            _stop.Token.ThrowIfCancellationRequested();
            try
            {
                await HandleOnceAsync(message);
                return;
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                throw;
            }
            catch (PermanentFailureException ex)
            {
                last = ex;
                break;
            }
            catch (Exception ex)
            {
                last = ex;
                if (attempt < options.MaxAttempts)
                {
                    await Task.Delay(TimeSpan.FromTicks(options.RetryBackoff.Ticks * (1L << (attempt - 1))), _stop.Token);
                }
            }
        }

        await DeadLetterAsync(result, message, last!);
    }

    private async Task HandleOnceAsync(ConsumedMessage message)
    {
        await using var connection = await dataSource.OpenConnectionAsync(_stop.Token);
        await using var tx = await connection.BeginTransactionAsync(_stop.Token);
        await using (var inbox = new NpgsqlCommand(
            "INSERT INTO courier_inbox (consumer, message_id) VALUES (@consumer, @id) ON CONFLICT DO NOTHING", connection, tx))
        {
            inbox.Parameters.AddWithValue("consumer", options.GroupId);
            inbox.Parameters.AddWithValue("id", message.MessageId);
            if (await inbox.ExecuteNonQueryAsync(_stop.Token) == 0)
            {
                Interlocked.Increment(ref _duplicates);
                return;
            }
        }

        await handler(message, connection, tx, _stop.Token);
        await tx.CommitAsync(_stop.Token);
        Interlocked.Increment(ref _processed);
    }

    private async Task DeadLetterAsync(ConsumeResult<string, string> result, ConsumedMessage message, Exception error)
    {
        if (_dlqProducer is null)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true,
            };
            options.Configure?.Invoke(producerConfig);
            _dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        var headers = new Headers
        {
            { "message-id", Encoding.UTF8.GetBytes(message.MessageId) },
            { "x-original-topic", Encoding.UTF8.GetBytes(message.Topic) },
            { "x-original-partition", Encoding.UTF8.GetBytes(message.Partition.ToString()) },
            { "x-original-offset", Encoding.UTF8.GetBytes(message.Offset.ToString()) },
            { "x-consumer", Encoding.UTF8.GetBytes(options.GroupId) },
            { "x-error", Encoding.UTF8.GetBytes(Truncate($"{error.GetType().Name}: {error.Message}", 500)) },
        };
        foreach (var (name, value) in message.Headers.Where(h => h.Key != "message-id"))
        {
            headers.Add(name, Encoding.UTF8.GetBytes(value));
        }

        await _dlqProducer.ProduceAsync(
            message.Topic + options.DeadLetterSuffix,
            new Message<string, string> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers },
            _stop.Token);
        Interlocked.Increment(ref _deadLettered);
    }

    private static void CommitStored(IConsumer<string, string> consumer)
    {
        try
        {
            consumer.Commit();
        }
        catch (KafkaException)
        {
        }
    }

    private static string Truncate(string value, int length) => value.Length <= length ? value : value[..length];

    private static ConsumedMessage ToMessage(ConsumeResult<string, string> result)
    {
        var headers = new Dictionary<string, string>();
        foreach (var header in result.Message.Headers)
        {
            headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
        }

        var id = headers.GetValueOrDefault("message-id") ?? $"{result.Topic}/{result.Partition.Value}/{result.Offset.Value}";
        return new ConsumedMessage(result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key, result.Message.Value, id, headers);
    }
}

public static class Topics
{
    public static async Task EnsureAsync(string bootstrapServers, string topic, int partitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }
    }
}

public static class DeadLetters
{
    public static async Task<int> ReplayAsync(string bootstrapServers, string deadLetterTopic, string groupId, int max, CancellationToken ct = default)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
        }).Build();
        consumer.Subscribe(deadLetterTopic);

        var replayed = 0;
        var idleSince = DateTime.UtcNow;
        while (replayed < max && DateTime.UtcNow - idleSince < TimeSpan.FromSeconds(4) && !ct.IsCancellationRequested)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(250));
            if (result is null || result.IsPartitionEOF)
            {
                continue;
            }

            idleSince = DateTime.UtcNow;
            var original = result.Message.Headers.FirstOrDefault(h => h.Key == "x-original-topic")?.GetValueBytes();
            if (original is null)
            {
                consumer.Commit(result);
                continue;
            }

            var headers = new Headers();
            foreach (var header in result.Message.Headers.Where(h => !h.Key.StartsWith("x-", StringComparison.Ordinal)))
            {
                headers.Add(header.Key, header.GetValueBytes());
            }

            await producer.ProduceAsync(Encoding.UTF8.GetString(original),
                new Message<string, string> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers }, ct);
            consumer.Commit(result);
            replayed++;
        }

        return replayed;
    }
}
