using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using NUnit.Framework;
using Shouldly;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public class MongoSequenceInitializerTests
{
    private const string TestDbPrefix = "seq_init_test_";
    private const string SequenceId = "test_seq";

    private MongoClient _mongoClient = null!;
    private string _databaseName = null!;
    private CollectionNamespace _seqNs = null!;

    [SetUp]
    public void SetUp()
    {
        _mongoClient = new MongoClient(MongoReplicaSetFixture.ConnectionString);
        _databaseName = $"{TestDbPrefix}{Guid.NewGuid():N}";
        _seqNs = new CollectionNamespace(_databaseName, "sequences");
    }

    [TearDown]
    public async Task TearDown()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        try
        {
            await _mongoClient.DropDatabaseAsync(_databaseName, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // best-effort
        }
        finally
        {
            cts.Dispose();

#if !MONGO_DRIVER_V2
            _mongoClient.Dispose();
#endif
        }
    }

    [Test]
    public async Task InitializeAsync_WhenSequenceDoesNotExist_ReturnsTrueAndSeedsCounter()
    {
        var result = await MongoSequenceInitializer.TryInitializeAsync(_mongoClient, _seqNs, SequenceId, 1);

        result.ShouldBeTrue();

        var next = await ReadNextAsync();
        next.ShouldBe(1);
    }

    [Test]
    public async Task InitializeAsync_WhenSequenceAlreadyExists_ReturnsFalseAndLeavesCounterUnchanged()
    {
        await MongoSequenceInitializer.TryInitializeAsync(_mongoClient, _seqNs, SequenceId, 1);

        var result = await MongoSequenceInitializer.TryInitializeAsync(_mongoClient, _seqNs, SequenceId, 100);

        result.ShouldBeFalse();

        var next = await ReadNextAsync();
        next.ShouldBe(1, "counter must not be overwritten by second call");
    }

    [Test]
    public async Task InitializeAsync_WithZeroInitialSequenceNumber_Succeeds()
    {
        var result = await MongoSequenceInitializer.TryInitializeAsync(_mongoClient, _seqNs, SequenceId, 0);

        result.ShouldBeTrue();

        var next = await ReadNextAsync();
        next.ShouldBe(0);
    }

    [Test]
    public void InitializeAsync_WithNegativeInitialSequenceNumber_Throws()
    {
        Should.Throw<ArgumentOutOfRangeException>(() =>
            MongoSequenceInitializer.TryInitializeAsync(_mongoClient, _seqNs, SequenceId, -1));
    }

    [Test]
    [CancelAfter(30_000)]
    public async Task InitializeAsync_FirstAppend_StartsFromInitialSequenceNumber(CancellationToken ct)
    {
        await MongoSequenceInitializer.TryInitializeAsync(_mongoClient, _seqNs, SequenceId, 10, ct);

        var targetNs = new CollectionNamespace(_databaseName, "targets");
        var db = _mongoClient.GetDatabase(_databaseName);
        await db.CreateCollectionAsync(targetNs.CollectionName, cancellationToken: ct);

        var targetCollection = db.GetCollection<TargetDoc>(targetNs.CollectionName);

        await targetCollection.Indexes.CreateOneAsync(
            new CreateIndexModel<TargetDoc>(
                Builders<TargetDoc>.IndexKeys.Ascending(x => x.Sequence),
                new CreateIndexOptions { Unique = true }),
            cancellationToken: ct);

        var binding = new MongoSequenceBinding<TargetDoc>(
            sequenceCollectionNamespace: _seqNs,
            sequenceId: SequenceId,
            targetCollectionNamespace: targetNs,
            targetField: new ExpressionFieldDefinition<TargetDoc, long>(x => x.Sequence));

        await using var appender = new MongoSequencedAppender<TargetDoc, object>(_mongoClient, binding);

        await appender.AppendAsync(
            [
                new TargetDoc { Value = "a" },
                new TargetDoc { Value = "b" }
            ],
            new object(),
            cancellationToken: ct);

        var docs = await targetCollection
            .Find(FilterDefinition<TargetDoc>.Empty)
            .Sort(Builders<TargetDoc>.Sort.Ascending(x => x.Sequence))
            .ToListAsync(ct);

        docs[0].Sequence.ShouldBe(10);
        docs[1].Sequence.ShouldBe(11);
    }

    private async Task<long> ReadNextAsync()
    {
        var collection = _mongoClient
            .GetDatabase(_seqNs.DatabaseNamespace.DatabaseName)
            .GetCollection<BsonDocument>(_seqNs.CollectionName);

        var doc = await collection
            .Find(Builders<BsonDocument>.Filter.Eq("_id", SequenceId))
            .FirstOrDefaultAsync();

        doc.ShouldNotBeNull("sequence counter document should exist");
        return doc["next"].ToInt64();
    }

    // ReSharper disable all
    private record TargetDoc
    {
        public ObjectId Id { get; init; }
        public required string Value { get; init; }

        [BsonElement("seq")]
        public long Sequence { get; init; }
    }
    // ReSharper restore all
}