using System.Text.Json;
using Npgsql;

namespace Courier.Ledger;

public sealed record AccountChanged(string TransferId, string Account, long Delta, long Version);

public sealed record AccountState(string Id, long Balance, long Version);

public sealed class InsufficientFundsException(string account, long balance, long amount)
    : Exception($"account '{account}' holds {balance} but {amount} is needed");

public sealed class OutOfOrderEventException(string account, long applied, long received)
    : Exception($"account '{account}': expected version {applied + 1} but received {received}");

public enum TransferResult
{
    Completed,
    AlreadyProcessed,
}

public sealed class LedgerService(NpgsqlDataSource dataSource, string topic = LedgerService.EventsTopic)
{
    public const string EventsTopic = "ledger.account-events";
    public const string ProjectorGroup = "ledger-projector";

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private const string Schema = """
        CREATE TABLE IF NOT EXISTS ledger_accounts (
            id text PRIMARY KEY,
            balance bigint NOT NULL CHECK (balance >= 0),
            version bigint NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ledger_transfers (
            id text PRIMARY KEY,
            from_account text NOT NULL,
            to_account text NOT NULL,
            amount bigint NOT NULL CHECK (amount > 0),
            created_at timestamptz NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS view_balances (
            account text PRIMARY KEY,
            balance bigint NOT NULL,
            applied_version bigint NOT NULL
        );
        """;

    public static async Task EnsureSchemaAsync(NpgsqlDataSource dataSource, CancellationToken ct = default)
    {
        await Outbox.EnsureSchemaAsync(dataSource, ct);
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var advisory = new NpgsqlCommand("SELECT pg_advisory_xact_lock(727004)", connection, tx))
        {
            await advisory.ExecuteNonQueryAsync(ct);
        }

        await using (var create = new NpgsqlCommand(Schema, connection, tx))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    public async Task<bool> OpenAccountAsync(string id, long initialBalance, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(id) || id.Length > 128 || initialBalance < 0)
        {
            throw new ArgumentException("an account needs an id of 1..128 characters and a non-negative balance");
        }

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO ledger_accounts (id, balance, version) VALUES (@id, @balance, 1) ON CONFLICT DO NOTHING", connection, tx))
        {
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("balance", initialBalance);
            if (await insert.ExecuteNonQueryAsync(ct) == 0)
            {
                return false;
            }
        }

        await Emit(connection, tx, new AccountChanged($"open-{id}", id, initialBalance, 1), ct);
        await tx.CommitAsync(ct);
        return true;
    }

    public async Task<TransferResult> TransferAsync(string transferId, string from, string to, long amount, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(transferId) || transferId.Length > 128 || amount <= 0 || from == to)
        {
            throw new ArgumentException("a transfer needs an id, a positive amount and two different accounts");
        }

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO ledger_transfers (id, from_account, to_account, amount) VALUES (@id, @from, @to, @amount) ON CONFLICT DO NOTHING", connection, tx))
        {
            insert.Parameters.AddWithValue("id", transferId);
            insert.Parameters.AddWithValue("from", from);
            insert.Parameters.AddWithValue("to", to);
            insert.Parameters.AddWithValue("amount", amount);
            if (await insert.ExecuteNonQueryAsync(ct) == 0)
            {
                return TransferResult.AlreadyProcessed;
            }
        }

        var accounts = new Dictionary<string, (long Balance, long Version)>();
        await using (var lockRows = new NpgsqlCommand(
            "SELECT id, balance, version FROM ledger_accounts WHERE id = ANY(@ids) ORDER BY id FOR UPDATE", connection, tx))
        {
            lockRows.Parameters.AddWithValue("ids", new[] { from, to });
            await using var reader = await lockRows.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                accounts[reader.GetString(0)] = (reader.GetInt64(1), reader.GetInt64(2));
            }
        }

        if (!accounts.TryGetValue(from, out var source) || !accounts.TryGetValue(to, out var target))
        {
            throw new ArgumentException("both accounts must exist");
        }

        if (source.Balance < amount)
        {
            throw new InsufficientFundsException(from, source.Balance, amount);
        }

        await Apply(connection, tx, from, -amount, ct);
        await Apply(connection, tx, to, amount, ct);
        await Emit(connection, tx, new AccountChanged(transferId, from, -amount, source.Version + 1), ct);
        await Emit(connection, tx, new AccountChanged(transferId, to, amount, target.Version + 1), ct);
        await tx.CommitAsync(ct);
        return TransferResult.Completed;
    }

    public async Task<AccountState?> GetAccountAsync(string id, CancellationToken ct = default)
    {
        await using var cmd = dataSource.CreateCommand("SELECT id, balance, version FROM ledger_accounts WHERE id = @id");
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new AccountState(reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)) : null;
    }

    public async Task<AccountState?> GetViewAsync(string id, CancellationToken ct = default)
    {
        await using var cmd = dataSource.CreateCommand("SELECT account, balance, applied_version FROM view_balances WHERE account = @id");
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new AccountState(reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)) : null;
    }

    public static MessageHandler Projector(Func<ConsumedMessage, Exception?>? fault = null) => async (message, connection, tx, ct) =>
    {
        if (fault?.Invoke(message) is { } injected)
        {
            throw injected;
        }

        var change = JsonSerializer.Deserialize<AccountChanged>(message.Value, Json)
            ?? throw new PermanentFailureException("the event is not a valid AccountChanged");

        await using (var update = new NpgsqlCommand(
            "UPDATE view_balances SET balance = balance + @delta, applied_version = @version WHERE account = @a AND applied_version = @version - 1 RETURNING 1",
            connection, tx))
        {
            update.Parameters.AddWithValue("a", change.Account);
            update.Parameters.AddWithValue("delta", change.Delta);
            update.Parameters.AddWithValue("version", change.Version);
            if (await update.ExecuteScalarAsync(ct) is not null)
            {
                return;
            }
        }

        if (change.Version == 1)
        {
            await using var first = new NpgsqlCommand(
                "INSERT INTO view_balances (account, balance, applied_version) VALUES (@a, @delta, 1) ON CONFLICT DO NOTHING RETURNING 1", connection, tx);
            first.Parameters.AddWithValue("a", change.Account);
            first.Parameters.AddWithValue("delta", change.Delta);
            if (await first.ExecuteScalarAsync(ct) is not null)
            {
                return;
            }
        }

        long applied;
        await using (var current = new NpgsqlCommand("SELECT applied_version FROM view_balances WHERE account = @a", connection, tx))
        {
            current.Parameters.AddWithValue("a", change.Account);
            applied = await current.ExecuteScalarAsync(ct) is long found ? found : 0;
        }

        if (change.Version > applied)
        {
            throw new OutOfOrderEventException(change.Account, applied, change.Version);
        }
    };

    private static async Task Apply(NpgsqlConnection connection, NpgsqlTransaction tx, string account, long delta, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("UPDATE ledger_accounts SET balance = balance + @delta, version = version + 1 WHERE id = @id", connection, tx);
        cmd.Parameters.AddWithValue("delta", delta);
        cmd.Parameters.AddWithValue("id", account);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private Task Emit(NpgsqlConnection connection, NpgsqlTransaction tx, AccountChanged change, CancellationToken ct) =>
        Outbox.EnqueueAsync(connection, tx, new OutboxMessage(topic, change.Account, JsonSerializer.Serialize(change, Json)), ct);
}
