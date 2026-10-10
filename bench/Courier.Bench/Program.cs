using System.Diagnostics;
using Courier;
using Courier.Ledger;
using Npgsql;
using Testcontainers.PostgreSql;
using Testcontainers.Redpanda;

var transfers = int.Parse(Arg("transfers", "20000"));
var accountCount = int.Parse(Arg("accounts", "500"));
var writers = int.Parse(Arg("writers", "8"));
var consumers = int.Parse(Arg("consumers", "3"));
var drain = Arg("drain", "false") == "true";

await using var postgres = new PostgreSqlBuilder("public.ecr.aws/docker/library/postgres:17").WithCommand("-c", "max_connections=200").Build();
await using var redpanda = new RedpandaBuilder("docker.redpanda.com/redpandadata/redpanda:v24.3.11").Build();
await Task.WhenAll(postgres.StartAsync(), redpanda.StartAsync());
var bootstrap = redpanda.GetBootstrapAddress();
var cs = new NpgsqlConnectionStringBuilder(postgres.GetConnectionString()) { MaxPoolSize = 100 }.ConnectionString;
await using var dataSource = NpgsqlDataSource.Create(cs);
await LedgerService.EnsureSchemaAsync(dataSource);
const string topic = "bench.account-events";
await Topics.EnsureAsync(bootstrap, topic, 12);
var ledger = new LedgerService(dataSource, topic);

var accounts = Enumerable.Range(0, accountCount).Select(i => $"a{i}").ToArray();
await Parallel.ForEachAsync(accounts, new ParallelOptions { MaxDegreeOfParallelism = 16 }, async (id, ct) => await ledger.OpenAccountAsync(id, 1_000_000, ct));

var relay = new OutboxRelay(dataSource, new RelayOptions { BootstrapServers = bootstrap });
var group = "bench-projector";
var projectors = Enumerable.Range(0, consumers)
    .Select(_ => new InboxConsumer(dataSource, new ConsumerOptions { BootstrapServers = bootstrap, GroupId = group, Topics = [topic] }, LedgerService.Projector()))
    .ToList();

var watch = Stopwatch.StartNew();
if (!drain)
{
    relay.Start();
    projectors.ForEach(p => p.Start());
}

var next = -1;
var writeLatencies = new List<double>();
await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
{
    var rng = new Random(w);
    while (true)
    {
        var n = Interlocked.Increment(ref next);
        if (n >= transfers)
        {
            return;
        }

        var fromIndex = rng.Next(accounts.Length);
        var toIndex = (fromIndex + 1 + rng.Next(accounts.Length - 1)) % accounts.Length;
        var sw = Stopwatch.StartNew();
        await ledger.TransferAsync($"t{n}", accounts[fromIndex], accounts[toIndex], rng.Next(1, 100));
        lock (writeLatencies)
        {
            writeLatencies.Add(sw.Elapsed.TotalMilliseconds);
        }
    }
})));
var written = watch.Elapsed;
if (drain)
{
    watch.Restart();
    relay.Start();
    projectors.ForEach(p => p.Start());
}

var expectedEvents = transfers * 2L + accountCount;
while (true)
{
    await using var cmd = dataSource.CreateCommand("SELECT count(*) FROM courier_inbox");
    if ((long)(await cmd.ExecuteScalarAsync())! >= expectedEvents)
    {
        break;
    }

    await Task.Delay(100);
}

var total = watch.Elapsed;
writeLatencies.Sort();
Console.WriteLine($"{transfers:N0} transfers ({expectedEvents:N0} events), {writers} writers, {consumers} projector consumers, 12 partitions");
Console.WriteLine($"  source writes: {transfers / written.TotalSeconds:N0} transfers/s, p50={writeLatencies[writeLatencies.Count / 2]:F1} ms p99={writeLatencies[(int)(writeLatencies.Count * 0.99)]:F1} ms");
Console.WriteLine(drain
    ? $"  backlog drain (relay + projectors from a cold start, {expectedEvents:N0} events queued): {total.TotalSeconds:F1}s = {expectedEvents / total.TotalSeconds:N0} events/s"
    : $"  end to end (projection fully caught up): {total.TotalSeconds:F1}s = {expectedEvents / total.TotalSeconds:N0} events/s");
await using var lag = dataSource.CreateCommand("SELECT count(*) FROM view_balances v JOIN ledger_accounts a ON a.id = v.account WHERE v.balance <> a.balance OR v.applied_version <> a.version");
Console.WriteLine($"  accounts where the projection differs from the source: {await lag.ExecuteScalarAsync()}");

foreach (var p in projectors)
{
    await p.StopAsync();
}

await relay.StopAsync();

static string Arg(string name, string fallback)
{
    var args = Environment.GetCommandLineArgs();
    var index = Array.IndexOf(args, "--" + name);
    return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback;
}
