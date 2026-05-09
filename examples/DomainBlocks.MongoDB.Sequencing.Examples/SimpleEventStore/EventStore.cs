using System.Runtime.CompilerServices;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DomainBlocks.MongoDB.Sequencing.Examples.SimpleEventStore;

/// <summary>
/// Minimal event store that uses <see cref="MongoSequencedAppender{TDocument,TContext}"/> to assign globally ordered
/// sequence numbers.
/// </summary>
public sealed class EventStore : IAsyncDisposable
{
    private readonly IMongoCollection<EventDocument> _collection;
    private readonly MongoSequencedAppender<EventDocument, AppendContext> _appender;

    public EventStore(IMongoClient client, string databaseName)
    {
        var db = client.GetDatabase(databaseName);
        _collection = db.GetCollection<EventDocument>("events");

        // Bind the global sequence counter to the GlobalSequence field on our documents.
        var binding = new MongoSequenceBinding<EventDocument>(
            sequenceCollectionNamespace: new CollectionNamespace(databaseName, "sequences"),
            sequenceId: "event_sequence",
            targetCollectionNamespace: new CollectionNamespace(databaseName, "events"),
            targetField: new ExpressionFieldDefinition<EventDocument, long>(x => x.GlobalSequence));

        // Create the sequenced appender that will assign global sequence values to each appended document.
        _appender = new MongoSequencedAppender<EventDocument, AppendContext>(
            client,
            binding,
            appendPolicy: new AppenderPolicy(_collection));
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        // Enforces unique per-stream versioning and enables efficient stream reads.
        // Also drives the OnConflict callback when two writers clash on the same (streamId, streamVersion).
        await _collection.Indexes.CreateOneAsync(
            new CreateIndexModel<EventDocument>(
                Builders<EventDocument>.IndexKeys.Ascending(x => x.StreamId).Ascending(x => x.StreamVersion),
                new CreateIndexOptions { Unique = true }),
            cancellationToken: cancellationToken);

        // Enforces uniqueness of the global sequence number, assigned by MongoSequencedAppender.
        await _collection.Indexes.CreateOneAsync(
            new CreateIndexModel<EventDocument>(
                Builders<EventDocument>.IndexKeys.Ascending(x => x.GlobalSequence),
                new CreateIndexOptions { Unique = true }),
            cancellationToken: cancellationToken);
    }

    public async Task AppendAsync(
        string streamId,
        IEnumerable<object> events,
        int? expectedVersion = null,
        CancellationToken cancellationToken = default)
    {
        var documents = events.Select(e => new EventDocument
        {
            StreamId = streamId,
            EventType = e.GetType().Name,
            Payload = e
        });

        await _appender.AppendAsync(
            documents,
            new AppendContext(streamId, expectedVersion),
            cancellationToken: cancellationToken);
    }

    public async IAsyncEnumerable<EventDocument> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var cursor = await _collection
            .Find(x => x.StreamId == streamId && x.StreamVersion >= fromVersion)
            .SortBy(x => x.StreamVersion)
            .ToCursorAsync(cancellationToken);

        while (await cursor.MoveNextAsync(cancellationToken))
        {
            foreach (var doc in cursor.Current)
                yield return doc;
        }
    }

    public async IAsyncEnumerable<EventDocument> ReadAllAsync(
        long fromSequence = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var cursor = await _collection
            .Find(x => x.GlobalSequence >= fromSequence)
            .SortBy(x => x.GlobalSequence)
            .ToCursorAsync(cancellationToken);

        while (await cursor.MoveNextAsync(cancellationToken))
        {
            foreach (var doc in cursor.Current)
                yield return doc;
        }
    }

    public ValueTask DisposeAsync() => _appender.DisposeAsync();
}