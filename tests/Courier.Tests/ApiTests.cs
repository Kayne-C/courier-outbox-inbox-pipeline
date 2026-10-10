using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Courier.Ledger;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Courier.Tests;

[Collection("infra")]
public class ApiTests(InfraFixture fixture)
{
    private const string Key = "test-api-key-0000000001";

    private async Task<(WebApplication App, HttpClient Http)> StartAsync(string connectionString)
    {
        var app = LedgerHost.Build([], b =>
        {
            b.Logging.ClearProviders();
            b.WebHost.UseUrls("http://127.0.0.1:0");
            b.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Ledger:ConnectionString"] = connectionString,
                ["Ledger:ApiKey"] = Key,
                ["Ledger:Kafka:BootstrapServers"] = fixture.Bootstrap,
            });
        });
        await app.StartAsync();
        var http = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Key);
        return (app, http);
    }

    [Fact]
    public async Task Requests_without_the_key_are_rejected_and_health_is_open()
    {
        await using var env = await fixture.CreateEnvAsync();
        var (app, http) = await StartAsync(env.ConnectionString);
        using var anonymous = new HttpClient { BaseAddress = http.BaseAddress };

        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync("/accounts/a")).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await anonymous.GetAsync("/health")).StatusCode);
        await app.StopAsync();
    }

    [Fact]
    public async Task A_transfer_made_over_http_shows_up_in_the_read_model()
    {
        await using var env = await fixture.CreateEnvAsync();
        var (app, http) = await StartAsync(env.ConnectionString);

        Assert.Equal(HttpStatusCode.Created, (await http.PutAsJsonAsync("/accounts/alice", new { balance = 500 })).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await http.PutAsJsonAsync("/accounts/bob", new { balance = 0 })).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await http.PutAsJsonAsync("/accounts/bob", new { balance = 0 })).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await http.PostAsJsonAsync("/transfers", new { id = "t1", from = "alice", to = "bob", amount = 120 })).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await http.PostAsJsonAsync("/transfers", new { id = "t1", from = "alice", to = "bob", amount = 120 })).StatusCode);

        Assert.True(await env.UntilAsync(async () =>
        {
            var response = await http.GetAsync("/view/bob");
            return response.StatusCode == HttpStatusCode.OK
                && (await response.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("balance").GetInt64() == 120;
        }, TimeSpan.FromSeconds(60)));
        var source = await http.GetFromJsonAsync<JsonElement>("/accounts/alice");
        Assert.Equal(380, source.GetProperty("balance").GetInt64());
        await app.StopAsync();
    }

    [Fact]
    public async Task Bad_transfers_are_400_or_422_and_the_server_needs_a_key_to_start()
    {
        await using var env = await fixture.CreateEnvAsync();
        var (app, http) = await StartAsync(env.ConnectionString);
        await http.PutAsJsonAsync("/accounts/alice", new { balance = 10 });
        await http.PutAsJsonAsync("/accounts/bob", new { balance = 0 });

        Assert.Equal(HttpStatusCode.BadRequest, (await http.PostAsJsonAsync("/transfers", new { id = "x", from = "alice", to = "alice", amount = 1 })).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await http.PostAsJsonAsync("/transfers", new { id = "x", from = "alice", to = "bob", amount = 0 })).StatusCode);
        Assert.Equal(HttpStatusCode.UnprocessableEntity, (await http.PostAsJsonAsync("/transfers", new { id = "y", from = "alice", to = "bob", amount = 999 })).StatusCode);
        Assert.Throws<InvalidOperationException>(() => LedgerHost.Build([], b =>
            b.Configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["Ledger:ConnectionString"] = "Host=x", ["Ledger:Kafka:BootstrapServers"] = "x:1" })));
        await app.StopAsync();
    }
}
