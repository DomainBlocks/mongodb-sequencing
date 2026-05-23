using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using NUnit.Framework;
using Testcontainers.MongoDb;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

[SetUpFixture]
public sealed class MongoReplicaSetFixture
{
    public const string MongoNetworkAlias = "mongo";

    private MongoDbContainer _mongoContainer = null!;

    public static INetwork Network { get; private set; } = null!;
    public static string ConnectionString { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Network = new NetworkBuilder()
            .WithName($"mongo-test-{Guid.NewGuid():N}")
            .Build();

        await Network.CreateAsync();

        _mongoContainer = new MongoDbBuilder("mongo:7.0")
            .WithReplicaSet()
            .WithNetwork(Network)
            .WithNetworkAliases(MongoNetworkAlias)
            .Build();

        await _mongoContainer.StartAsync();

        ConnectionString = _mongoContainer.GetConnectionString();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _mongoContainer.DisposeAsync();
        await Network.DisposeAsync();
        Network = null!;
    }
}