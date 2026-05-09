using DomainBlocks.MongoDB.Sequencing.Examples.SimpleEventStore;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using NUnit.Framework;
using Shouldly;

namespace DomainBlocks.MongoDB.Sequencing.Examples.Tests.Integration.SimpleEventStore;

public class EventStoreTests
{
    private MongoClient _mongoClient = null!;
    private string _databaseName = null!;
    private EventStore _eventStore = null!;

    static EventStoreTests()
    {
        // Allow only these example event POCOs for object-payload BSON serialization in tests.
        HashSet<Type> allowedEventTypes =
        [
            typeof(OrderPlaced),
            typeof(OrderShipped),
            typeof(OrderCancelled),
            typeof(OrderDelivered)
        ];

        BsonSerializer.RegisterSerializer(
            new ObjectSerializer(type =>
                ObjectSerializer.DefaultAllowedTypes(type) || allowedEventTypes.Contains(type)));
    }

    [SetUp]
    public async Task SetUp()
    {
        _mongoClient = new MongoClient(MongoReplicaSetFixture.ConnectionString);
        _databaseName = $"es_test_{Guid.NewGuid():N}";
        _eventStore = new EventStore(_mongoClient, _databaseName);
        await _eventStore.EnsureIndexesAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        await _eventStore.DisposeAsync();
        await _mongoClient.DropDatabaseAsync(_databaseName);
        _mongoClient.Dispose();
    }

    [Test]
    public async Task Append_SingleStream_AssignsContiguousStreamVersions()
    {
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1"), new OrderShipped("order-1")]);

        var events = await _eventStore.ReadStreamAsync("order-1").ToListAsync();

        events.Count.ShouldBe(2);
        events[0].StreamVersion.ShouldBe(0);
        events[1].StreamVersion.ShouldBe(1);
    }

    [Test]
    public async Task Append_SingleStream_AssignsContiguousGlobalSequence()
    {
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1")]);
        await _eventStore.AppendAsync("order-2", [new OrderPlaced("order-2")]);

        var all = await _eventStore.ReadAllAsync().ToListAsync();

        all.Count.ShouldBe(2);
        all[0].GlobalSequence.ShouldBe(0);
        all[1].GlobalSequence.ShouldBe(1);
    }

    [Test]
    public async Task Append_MultipleStreams_GlobalSequenceIsContiguousAcrossStreams()
    {
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1"), new OrderShipped("order-1")]);
        await _eventStore.AppendAsync("order-2", [new OrderPlaced("order-2")]);

        var all = await _eventStore.ReadAllAsync().ToListAsync();

        all.Count.ShouldBe(3);
        all.Select(x => x.GlobalSequence).ShouldBe([0, 1, 2]);
    }

    [Test]
    public async Task Append_WithCorrectExpectedVersion_Succeeds()
    {
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1")], expectedVersion: null);
        await _eventStore.AppendAsync("order-1", [new OrderShipped("order-1")], expectedVersion: 0);

        var events = await _eventStore.ReadStreamAsync("order-1").ToListAsync();

        events.Count.ShouldBe(2);
        events[0].StreamVersion.ShouldBe(0);
        events[1].StreamVersion.ShouldBe(1);
    }

    [Test]
    public async Task Append_WithWrongExpectedVersion_ThrowsException()
    {
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1")]);

        await Should.ThrowAsync<InvalidOperationException>(() =>
            _eventStore.AppendAsync("order-1", [new OrderShipped("order-1")], expectedVersion: 99));
    }

    [Test]
    public async Task Append_ToNewStream_WithExpectedVersionNull_Succeeds()
    {
        await Should.NotThrowAsync(() =>
            _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1")], expectedVersion: null));
    }

    [Test]
    public async Task Append_ConcurrentWritesToSameStream_OnlyOneSucceeds()
    {
        // First write to establish the stream.
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1")]);

        // Two concurrent writes both expecting version 0 - only one should succeed.
        var task1 = _eventStore.AppendAsync("order-1", [new OrderShipped("order-1")], expectedVersion: 0);
        var task2 = _eventStore.AppendAsync("order-1", [new OrderCancelled("order-1")], expectedVersion: 0);

        var results = await Task.WhenAll(
            task1.ContinueWith(t => t.Exception is null),
            task2.ContinueWith(t => t.Exception is null));

        results.Count(x => x).ShouldBe(1, "exactly one concurrent write should succeed");
        results.Count(x => !x).ShouldBe(1, "exactly one concurrent write should fail");

        var events = await _eventStore.ReadStreamAsync("order-1").ToListAsync();
        events.Count.ShouldBe(2, "stream should contain exactly two events");
    }

    [Test]
    public async Task ReadStream_FromStart_ReturnsEventsInStreamVersionOrder()
    {
        await _eventStore.AppendAsync(
            "order-1",
            [
                new OrderPlaced("order-1"),
                new OrderShipped("order-1"),
                new OrderDelivered("order-1")
            ]);

        var events = await _eventStore.ReadStreamAsync("order-1").ToListAsync();

        events.Select(x => x.StreamVersion).ShouldBe([0, 1, 2]);
    }

    [Test]
    public async Task ReadStream_FromVersion_ReturnsOnlyEventsFromThatVersion()
    {
        await _eventStore.AppendAsync(
            "order-1",
            [
                new OrderPlaced("order-1"),
                new OrderShipped("order-1"),
                new OrderDelivered("order-1")
            ]);

        var events = await _eventStore.ReadStreamAsync("order-1", fromVersion: 1).ToListAsync();

        events.Count.ShouldBe(2);
        events[0].StreamVersion.ShouldBe(1);
        events[1].StreamVersion.ShouldBe(2);
    }

    [Test]
    public async Task ReadAll_FromStart_ReturnsOnlyEventsFromThatPosition()
    {
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1")]);
        await _eventStore.AppendAsync("order-2", [new OrderPlaced("order-2")]);
        await _eventStore.AppendAsync("order-3", [new OrderPlaced("order-3")]);

        var events = await _eventStore.ReadAllAsync().ToListAsync();

        events.Count.ShouldBe(3);
        events[0].GlobalSequence.ShouldBe(0);
        events[1].GlobalSequence.ShouldBe(1);
        events[2].GlobalSequence.ShouldBe(2);
    }

    [Test]
    public async Task ReadAll_FromSequence_ReturnsOnlyEventsFromThatPosition()
    {
        await _eventStore.AppendAsync("order-1", [new OrderPlaced("order-1")]);
        await _eventStore.AppendAsync("order-2", [new OrderPlaced("order-2")]);
        await _eventStore.AppendAsync("order-3", [new OrderPlaced("order-3")]);

        var events = await _eventStore.ReadAllAsync(fromSequence: 1).ToListAsync();

        events.Count.ShouldBe(2);
        events[0].GlobalSequence.ShouldBe(1);
        events[1].GlobalSequence.ShouldBe(2);
    }

    // Resharper disable all
    private sealed record OrderPlaced(string OrderId);

    private sealed record OrderShipped(string OrderId);

    private sealed record OrderCancelled(string OrderId);

    private sealed record OrderDelivered(string OrderId);
    // Resharper restore all
}