using MongoDB.Driver;
using NUnit.Framework;
using Testcontainers.MongoDb;
using Testcontainers.Toxiproxy;
using Toxiproxy.Net;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration.Latency;

[SetUpFixture]
public sealed class ToxiproxyFixture
{
    private const int ProxyPort = ToxiproxyBuilder.FirstProxiedPort;

    private ToxiproxyContainer _toxiproxyContainer = null!;
    private Connection _connection = null!;

    public static string ConnectionString { get; private set; } = null!;
    public static Proxy MongoProxy { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _toxiproxyContainer = new ToxiproxyBuilder("ghcr.io/shopify/toxiproxy:2.12.0")
            .WithNetwork(MongoReplicaSetFixture.Network)
            .Build();

        await _toxiproxyContainer.StartAsync();

        _connection = new Connection(_toxiproxyContainer.Hostname, _toxiproxyContainer.GetMappedPublicPort());

        var client = _connection.Client();

        // Upstream uses the network alias - resolvable from inside the Toxiproxy container because both containers are
        // on the same Docker network
        var proxy = await client.AddAsync(new Proxy
        {
            Name = "mongo",
            Listen = $"0.0.0.0:{ProxyPort}",
            Upstream = $"{MongoReplicaSetFixture.MongoNetworkAlias}:{MongoDbBuilder.MongoDbPort}",
            Enabled = true
        });

        MongoProxy = proxy;

        var host = _toxiproxyContainer.Hostname;
        var port = _toxiproxyContainer.GetMappedPublicPort(ProxyPort);
        var serverAddress = new MongoServerAddress(host, port);

        var builder = new MongoUrlBuilder(MongoReplicaSetFixture.ConnectionString)
        {
            Server = serverAddress
        };

        ConnectionString = builder.ToMongoUrl().ToString();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _connection.Dispose();
        await _toxiproxyContainer.DisposeAsync();
    }
}