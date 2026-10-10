using Courier.Ledger;
using Xunit;

namespace Courier.Tests;

[Collection("infra")]
public class LedgerTests(InfraFixture fixture)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(90);

    private static async Task<bool> CaughtUpAsync(Env env, LedgerService ledger, IEnumerable<string> accounts)
    {
        foreach (var id in accounts)
        {
            var source = await ledger.GetAccountAsync(id);
            var view = await ledger.GetViewAsync(id);
            if (view is null || view.Version != source!.Version || view.Balance != source.Balance)
            {
                return false;
            }
        }

        return true;
    }

    [Fact]
    public async Task A_transfer_updates_the_source_and_the_projection_catches_up()
    {
        await using var env = await fixture.CreateEnvAsync();
        var ledger = new LedgerService(env.DataSource, env.Topic);
        var relay = env.NewRelay();
        var projector = env.NewConsumer(LedgerService.Projector());
        relay.Start();
        projector.Start();

        await ledger.OpenAccountAsync("alice", 1000);
        await ledger.OpenAccountAsync("bob", 50);
        Assert.Equal(TransferResult.Completed, await ledger.TransferAsync("t1", "alice", "bob", 300));

        Assert.True(await env.UntilAsync(() => CaughtUpAsync(env, ledger, ["alice", "bob"]), Patience));
        Assert.Equal(new AccountState("alice", 700, 2), await ledger.GetAccountAsync("alice"));
        Assert.Equal(new AccountState("bob", 350, 2), await ledger.GetViewAsync("bob"));
    }

    [Fact]
    public async Task An_overdraft_changes_nothing_and_emits_nothing()
    {
        await using var env = await fixture.CreateEnvAsync();
        var ledger = new LedgerService(env.DataSource, env.Topic);
        await ledger.OpenAccountAsync("alice", 100);
        await ledger.OpenAccountAsync("bob", 0);
        var eventsBefore = await env.ScalarAsync<long>("SELECT count(*) FROM courier_outbox");

        await Assert.ThrowsAsync<InsufficientFundsException>(() => ledger.TransferAsync("t1", "alice", "bob", 101));

        Assert.Equal(100, (await ledger.GetAccountAsync("alice"))!.Balance);
        Assert.Equal(0L, await env.ScalarAsync<long>("SELECT count(*) FROM ledger_transfers"));
        Assert.Equal(eventsBefore, await env.ScalarAsync<long>("SELECT count(*) FROM courier_outbox"));
    }

    [Fact]
    public async Task Repeating_a_transfer_id_moves_money_once()
    {
        await using var env = await fixture.CreateEnvAsync();
        var ledger = new LedgerService(env.DataSource, env.Topic);
        await ledger.OpenAccountAsync("alice", 100);
        await ledger.OpenAccountAsync("bob", 0);

        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Task.Run(() => ledger.TransferAsync("same", "alice", "bob", 40))));

        Assert.Equal(1, results.Count(r => r == TransferResult.Completed));
        Assert.Equal(60, (await ledger.GetAccountAsync("alice"))!.Balance);
        Assert.Equal(40, (await ledger.GetAccountAsync("bob"))!.Balance);
    }

    [Fact]
    public async Task The_projection_converges_exactly_while_relays_and_consumers_are_killed_and_restarted()
    {
        await using var env = await fixture.CreateEnvAsync();
        var ledger = new LedgerService(env.DataSource, env.Topic);
        var accounts = Enumerable.Range(0, 24).Select(i => $"acct-{i}").ToArray();
        foreach (var account in accounts)
        {
            await ledger.OpenAccountAsync(account, 5_000);
        }

        var relays = new OutboxRelay?[2];
        var consumers = new InboxConsumer?[2];
        var crashRng = new Random(11);
        OutboxRelay SpawnRelay()
        {
            var relay = env.NewRelay(8);
            relay.AfterProduceBeforeMark = _ =>
            {
                lock (crashRng)
                {
                    return crashRng.NextDouble() < 0.04 ? throw new InvalidOperationException("simulated crash before marking") : Task.CompletedTask;
                }
            };
            relay.Start();
            return relay;
        }

        InboxConsumer SpawnConsumer()
        {
            var consumer = env.NewConsumer(LedgerService.Projector(), b => b.AutoCommitIntervalMs = 3000);
            consumer.Start();
            return consumer;
        }

        for (var i = 0; i < 2; i++)
        {
            relays[i] = SpawnRelay();
            consumers[i] = SpawnConsumer();
        }

        var transfers = 1500;
        var next = -1;
        var insufficient = 0;
        var writers = Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            var rng = new Random(100 + w);
            while (true)
            {
                var n = Interlocked.Increment(ref next);
                if (n >= transfers)
                {
                    return;
                }

                var from = accounts[rng.Next(accounts.Length)];
                var to = accounts[rng.Next(accounts.Length)];
                if (from == to)
                {
                    continue;
                }

                try
                {
                    await ledger.TransferAsync($"t-{n}", from, to, rng.Next(1, 400));
                }
                catch (InsufficientFundsException)
                {
                    Interlocked.Increment(ref insufficient);
                }
            }
        })).ToList();

        var killer = Task.Run(async () =>
        {
            var rng = new Random(3);
            for (var round = 0; round < 10; round++)
            {
                await Task.Delay(rng.Next(400, 900));
                var slot = rng.Next(2);
                if (rng.Next(2) == 0)
                {
                    await relays[slot]!.StopAsync();
                    relays[slot] = SpawnRelay();
                }
                else
                {
                    await consumers[slot]!.KillAsync();
                    consumers[slot] = SpawnConsumer();
                }
            }
        });

        await Task.WhenAll(writers.Append(killer));

        Assert.True(await env.UntilAsync(() => CaughtUpAsync(env, ledger, accounts), Patience), "the projection never caught up");
        Assert.Equal(accounts.Length * 5_000L, await env.ScalarAsync<long>("SELECT sum(balance) FROM ledger_accounts"));
        Assert.Equal(accounts.Length * 5_000L, await env.ScalarAsync<long>("SELECT sum(balance) FROM view_balances"));
        Assert.Equal(0L, await env.ScalarAsync<long>("SELECT count(*) FROM courier_outbox WHERE published_at IS NULL"));
        var completed = await env.ScalarAsync<long>("SELECT count(*) FROM ledger_transfers");
        Assert.Equal(completed * 2 + accounts.Length, await env.ScalarAsync<long>("SELECT count(*) FROM courier_inbox"));
        Assert.True(completed > transfers / 2, $"only {completed} transfers completed ({insufficient} overdrafts)");
        Assert.Equal(0L, await env.ScalarAsync<long>("SELECT count(*) FROM view_balances v JOIN ledger_accounts a ON a.id = v.account WHERE v.balance <> a.balance OR v.applied_version <> a.version"));
    }
}
