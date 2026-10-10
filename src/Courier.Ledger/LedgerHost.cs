using System.Security.Cryptography;
using System.Text;
using Npgsql;

namespace Courier.Ledger;

public sealed record OpenAccountDto(long Balance);

public sealed record TransferDto(string Id, string From, string To, long Amount);

public static class LedgerHost
{
    public static WebApplication Build(string[] args, Action<WebApplicationBuilder>? configure = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        configure?.Invoke(builder);

        var apiKey = builder.Configuration["Ledger:ApiKey"];
        if (string.IsNullOrWhiteSpace(apiKey) || apiKey.Length < 16)
        {
            throw new InvalidOperationException("Ledger:ApiKey must be configured with at least 16 characters");
        }

        var connectionString = builder.Configuration["Ledger:ConnectionString"]
            ?? throw new InvalidOperationException("Ledger:ConnectionString is required");
        var bootstrap = builder.Configuration["Ledger:Kafka:BootstrapServers"]
            ?? throw new InvalidOperationException("Ledger:Kafka:BootstrapServers is required");

        var dataSource = NpgsqlDataSource.Create(connectionString);
        builder.Services.AddSingleton(dataSource);
        builder.Services.AddSingleton(new LedgerService(dataSource));
        builder.Services.AddHostedService(sp => new PipelineService(
            dataSource,
            bootstrap,
            builder.Configuration.GetValue("Ledger:RunRelay", true),
            builder.Configuration.GetValue("Ledger:RunProjector", true)));

        var app = builder.Build();
        var keyBytes = Encoding.UTF8.GetBytes(apiKey);

        app.Use(async (context, next) =>
        {
            if (context.Request.Path.StartsWithSegments("/health"))
            {
                await next();
                return;
            }

            var header = context.Request.Headers.Authorization.ToString();
            var presented = header.StartsWith("Bearer ", StringComparison.Ordinal) ? Encoding.UTF8.GetBytes(header[7..]) : [];
            if (presented.Length != keyBytes.Length || !CryptographicOperations.FixedTimeEquals(presented, keyBytes))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            try
            {
                await next();
            }
            catch (ArgumentException ex)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                await context.Response.WriteAsJsonAsync(new { error = ex.Message });
            }
            catch (InsufficientFundsException ex)
            {
                context.Response.StatusCode = StatusCodes.Status422UnprocessableEntity;
                await context.Response.WriteAsJsonAsync(new { error = ex.Message });
            }
        });

        app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

        app.MapPut("/accounts/{id}", async (string id, OpenAccountDto dto, LedgerService ledger) =>
            await ledger.OpenAccountAsync(id, dto.Balance) ? Results.Created($"/accounts/{id}", new { id }) : Results.Ok(new { id, existing = true }));

        app.MapGet("/accounts/{id}", async (string id, LedgerService ledger) =>
            await ledger.GetAccountAsync(id) is { } account ? Results.Ok(account) : Results.NotFound());

        app.MapGet("/view/{id}", async (string id, LedgerService ledger) =>
            await ledger.GetViewAsync(id) is { } view ? Results.Ok(view) : Results.NotFound());

        app.MapPost("/transfers", async (TransferDto dto, LedgerService ledger) =>
            await ledger.TransferAsync(dto.Id, dto.From, dto.To, dto.Amount) == TransferResult.Completed
                ? Results.Created($"/transfers/{dto.Id}", new { dto.Id })
                : Results.Ok(new { dto.Id, alreadyProcessed = true }));

        return app;
    }
}

internal sealed class PipelineService(NpgsqlDataSource dataSource, string bootstrap, bool runRelay, bool runProjector) : IHostedService
{
    private OutboxRelay? _relay;
    private InboxConsumer? _consumer;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await LedgerService.EnsureSchemaAsync(dataSource, cancellationToken);
        await Topics.EnsureAsync(bootstrap, LedgerService.EventsTopic, 6);
        if (runRelay)
        {
            _relay = new OutboxRelay(dataSource, new RelayOptions { BootstrapServers = bootstrap });
            _relay.Start();
        }

        if (runProjector)
        {
            _consumer = new InboxConsumer(
                dataSource,
                new ConsumerOptions { BootstrapServers = bootstrap, GroupId = LedgerService.ProjectorGroup, Topics = [LedgerService.EventsTopic] },
                LedgerService.Projector());
            _consumer.Start();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumer is not null)
        {
            await _consumer.StopAsync();
        }

        if (_relay is not null)
        {
            await _relay.StopAsync();
        }
    }
}
