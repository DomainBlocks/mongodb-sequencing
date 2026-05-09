using MongoDB.Bson;

namespace DomainBlocks.MongoDB.Sequencing.Examples.SimpleEventStore;

/// <summary>
/// Persisted event record used by the simple event store, containing stream-local versioning, global sequence ordering,
/// and event payload data.
/// </summary>
public class EventDocument
{
    public ObjectId Id { get; init; }
    public required string StreamId { get; init; }
    public int StreamVersion { get; init; }
    public long GlobalSequence { get; init; } // Assigned by MongoSequencedAppender
    public required string EventType { get; init; }
    public required object Payload { get; init; }
}