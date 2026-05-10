using NUnit.Framework;
using Testcontainers.MongoDb;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

[SetUpFixture]
public sealed class MongoReplicaSetFixture
{
    private MongoDbContainer _container = null!;

    public static string ConnectionString { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _container = new MongoDbBuilder("mongo:7.0").WithReplicaSet().Build();
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.DisposeAsync();
    }
}