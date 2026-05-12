using MongoDB.Bson;
using MongoDB.Driver;
using NUnit.Framework;
using Shouldly;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public class MongoSequenceInitializerTests : MongoIntegrationTestBase
{
    private const int TimeoutMillis = 10_000;

    [Test]
    public async Task InitializeAsync_WhenSequenceDoesNotExist_ReturnsTrueAndSeedsCounter()
    {
        var result = await MongoSequenceInitializer.TryInitializeAsync(MongoClient, SequenceNs, SequenceId, 1);

        result.ShouldBeTrue();

        var next = await ReadNextAsync();
        next.ShouldBe(1);
    }

    [Test]
    public async Task InitializeAsync_WhenSequenceAlreadyExists_ReturnsFalseAndLeavesCounterUnchanged()
    {
        await MongoSequenceInitializer.TryInitializeAsync(MongoClient, SequenceNs, SequenceId, 1);

        var result = await MongoSequenceInitializer.TryInitializeAsync(MongoClient, SequenceNs, SequenceId, 100);

        result.ShouldBeFalse();

        var next = await ReadNextAsync();
        next.ShouldBe(1, "counter must not be overwritten by second call");
    }

    [Test]
    public async Task InitializeAsync_WithZeroInitialSequenceNumber_Succeeds()
    {
        var result = await MongoSequenceInitializer.TryInitializeAsync(MongoClient, SequenceNs, SequenceId, 0);

        result.ShouldBeTrue();

        var next = await ReadNextAsync();
        next.ShouldBe(0);
    }

    [Test]
    public void InitializeAsync_WithNegativeInitialSequenceNumber_Throws()
    {
        Should.Throw<ArgumentOutOfRangeException>(() =>
            MongoSequenceInitializer.TryInitializeAsync(MongoClient, SequenceNs, SequenceId, -1));
    }

    [Test]
    [CancelAfter(TimeoutMillis)]
    public async Task InitializeAsync_FirstAppend_StartsFromInitialSequenceNumber(CancellationToken ct)
    {
        await MongoSequenceInitializer.TryInitializeAsync(MongoClient, SequenceNs, SequenceId, 10, ct);

        var db = MongoClient.GetDatabase(DatabaseName);
        var targetCollection = db.GetCollection<TargetDoc>(TargetNs.CollectionName);

        var indexModel = new CreateIndexModel<TargetDoc>(
            Builders<TargetDoc>.IndexKeys.Ascending(x => x.Nested!.Sequence),
            new CreateIndexOptions { Unique = true });

        await targetCollection.Indexes.CreateOneAsync(indexModel, cancellationToken: ct);

        await using var appender = CreateAppender<object>();

        await appender.AppendAsync(
            [
                new TargetDoc { Value = "a" },
                new TargetDoc { Value = "b" }
            ],
            new object(),
            cancellationToken: ct);

        var docs = await targetCollection
            .Find(FilterDefinition<TargetDoc>.Empty)
            .Sort(Builders<TargetDoc>.Sort.Ascending(x => x.Nested!.Sequence))
            .ToListAsync(ct);

        docs[0].Nested!.Sequence.ShouldBe(10);
        docs[1].Nested!.Sequence.ShouldBe(11);
    }

    private async Task<long> ReadNextAsync()
    {
        var collection = MongoClient
            .GetDatabase(SequenceNs.DatabaseNamespace.DatabaseName)
            .GetCollection<BsonDocument>(SequenceNs.CollectionName);

        var doc = await collection
            .Find(Builders<BsonDocument>.Filter.Eq("_id", SequenceId))
            .FirstOrDefaultAsync();

        doc.ShouldNotBeNull("sequence counter document should exist");
        return doc["next"].ToInt64();
    }
}