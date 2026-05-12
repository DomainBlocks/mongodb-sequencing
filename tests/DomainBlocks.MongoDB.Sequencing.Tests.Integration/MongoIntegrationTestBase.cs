using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using NUnit.Framework;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public abstract class MongoIntegrationTestBase
{
    private const string TestDbPrefix = "seq_test_";
    protected const string SequenceId = "test_seq";

    protected MongoClient MongoClient { get; private set; } = null!;
    protected string DatabaseName { get; private set; } = null!;
    protected CollectionNamespace SequenceNs { get; private set; } = null!;
    protected CollectionNamespace TargetNs { get; private set; } = null!;
    private ILoggerFactory LoggerFactory { get; set; } = null!;

    [SetUp]
    public async Task SetUp()
    {
        MongoClient = new MongoClient(MongoReplicaSetFixture.ConnectionString);
        DatabaseName = $"{TestDbPrefix}{Guid.NewGuid():N}";
        SequenceNs = new CollectionNamespace(DatabaseName, "sequences");
        TargetNs = new CollectionNamespace(DatabaseName, "targets");

        LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(x => x
            .AddSimpleConsole(opt =>
            {
                opt.IncludeScopes = true;
                opt.TimestampFormat = "HH:mm:ss.fff ";
            })
            .SetMinimumLevel(LogLevel.Debug));

        var db = MongoClient.GetDatabase(DatabaseName);
        var targetCollection = db.GetCollection<TargetDoc>(TargetNs.CollectionName);

        var indexModel = new CreateIndexModel<TargetDoc>(
            Builders<TargetDoc>.IndexKeys.Ascending(x => x.Nested!.Sequence),
            new CreateIndexOptions { Unique = true });

        await targetCollection.Indexes.CreateOneAsync(indexModel);

        await SetUpAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        await TearDownAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        try
        {
            await MongoClient.DropDatabaseAsync(DatabaseName, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // best-effort
        }
        finally
        {
            LoggerFactory.Dispose();

#if !MONGO_DRIVER_V2
            MongoClient.Dispose();
#endif
        }
    }

    protected MongoSequencedAppender<TargetDoc, TContext> CreateAppender<TContext>(
        int index = 0,
        IMongoSequencedAppenderPolicy<TContext>? policy = null)
    {
        var binding = new MongoSequenceBinding<TargetDoc>(
            sequenceCollectionNamespace: SequenceNs,
            sequenceId: SequenceId,
            targetCollectionNamespace: TargetNs,
            targetField: new ExpressionFieldDefinition<TargetDoc, long>(x => x.Nested!.Sequence));

        return new MongoSequencedAppender<TargetDoc, TContext>(
            MongoClient,
            binding,
            policy,
            logger: LoggerFactory.CreateLogger($"appender_{index}"));
    }

    protected async Task<long[]> ReadSequenceAsync(CancellationToken ct = default)
    {
        var collection = MongoClient
            .GetDatabase(DatabaseName)
            .GetCollection<TargetDoc>(TargetNs.CollectionName);

        var docs = await collection
            .Find(FilterDefinition<TargetDoc>.Empty)
            .Sort(Builders<TargetDoc>.Sort.Ascending(x => x.Nested!.Sequence))
            .ToListAsync(ct);

        return [.. docs.Select(d => d.Nested!.Sequence)];
    }

    protected virtual Task SetUpAsync() => Task.CompletedTask;

    protected virtual Task TearDownAsync() => Task.CompletedTask;

    // ReSharper disable all
    protected record TargetDoc
    {
        public ObjectId Id { get; init; }
        public required string Value { get; init; }
        public NestedDoc? Nested { get; init; }
    }

    protected record NestedDoc
    {
        [BsonElement("seq")]
        public long Sequence { get; init; }
    }
    // ReSharper restore all
}