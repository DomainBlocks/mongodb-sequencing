using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MongoDB.Bson;
using MongoDB.Driver;
using NUnit.Framework;

namespace DomainBlocks.MongoDB.Sequencing.Examples.Tests.Integration;

[SetUpFixture]
public sealed class MongoReplicaSetFixture
{
    private const int MongoPort = 27017;

    private IContainer _container = null!;

    public static string ConnectionString { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _container = new ContainerBuilder("mongo:latest")
            .WithPortBinding(MongoPort, assignRandomHostPort: true)
            .WithCommand("--replSet", "rs0", "--bind_ip_all")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(MongoPort))
            .Build();

        await _container.StartAsync();

        var host = _container.Hostname;
        var mappedPort = _container.GetMappedPublicPort(MongoPort);

        // Bootstrap connection before replica set is ready.
        var bootstrapConnectionString = $"mongodb://{host}:{mappedPort}/?directConnection=true";
        using var bootstrapClient = new MongoClient(bootstrapConnectionString);

        // Initiate replica set (idempotent for reruns in same container lifetime).
        var adminDb = bootstrapClient.GetDatabase("admin");

        try
        {
            var config = new BsonDocument
            {
                { "_id", "rs0" },
                {
                    "members",
                    new BsonArray
                    {
                        new BsonDocument
                        {
                            { "_id", 0 },
                            { "host", $"{host}:{MongoPort}" }
                        }
                    }
                }
            };

            await adminDb.RunCommandAsync<BsonDocument>(new BsonDocument("replSetInitiate", config));
        }
        catch (MongoCommandException ex) when (ex.CodeName is "AlreadyInitialized")
        {
            // Safe if already initialized.
        }

        // Wait until writable primary.
        var timeout = TimeSpan.FromSeconds(60);
        var start = DateTime.UtcNow;

        while (DateTime.UtcNow - start < timeout)
        {
            try
            {
                var hello = await adminDb.RunCommandAsync<BsonDocument>(new BsonDocument("hello", 1));
                var isWritablePrimary = hello.GetValue("isWritablePrimary", false).ToBoolean();
                if (isWritablePrimary)
                    break;
            }
            catch
            {
                // Ignore transient startup errors.
            }

            await Task.Delay(500);
        }

        ConnectionString = $"mongodb://{host}:{mappedPort}/?replicaSet=rs0";
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.DisposeAsync();
    }
}