using MongoDB.Driver;

namespace DomainBlocks.MongoDB.Sequencing.Examples.SimpleEventStore;

/// <summary>
/// Example append policy for a simple event store. Assigns stream version values before commit and enforces optimistic
/// concurrency based on a unique index on (streamId, streamVersion).
/// </summary>
public class AppenderPolicy(IMongoCollection<EventDocument> collection) : IMongoSequencedAppenderPolicy<AppendContext>
{
    public async Task OnBatchCommittingAsync(
        IReadOnlyList<AppendRequest<AppendContext>> batch,
        CancellationToken cancellationToken)
    {
        // Collect unique stream IDs from this batch.
        var streamIds = batch.Select(x => x.Context.StreamId).Distinct().ToList();

        // Fetch the current max version for each stream.
        var currentVersions = await collection
            .WithReadConcern(ReadConcern.Majority)
            .WithReadPreference(ReadPreference.Primary)
            .Aggregate()
            .Match(x => streamIds.Contains(x.StreamId))
            .Group(
                x => x.StreamId,
                g => new { StreamId = g.Key, MaxVersion = g.Max(x => x.StreamVersion) })
            .ToListAsync(cancellationToken);

        var versionByStream = currentVersions.ToDictionary(x => x.StreamId, x => x.MaxVersion);

        foreach (var request in batch)
        {
            var (streamId, expectedVersion) = request.Context;
            var streamVersion = versionByStream.GetValueOrDefault(streamId, -1);

            // Fail early if expected version doesn't match actual.
            if (expectedVersion.HasValue && streamVersion != expectedVersion)
            {
                request.TryComplete(new InvalidOperationException("Expected version conflict"));
                continue;
            }

            // Assign the next contiguous version to each document being appended.
            foreach (var eventDoc in request.Documents)
                eventDoc["StreamVersion"] = ++streamVersion;

            // Track the latest version within this batch for same-stream requests.
            versionByStream[streamId] = streamVersion;
        }
    }

    public ConflictResolution OnConflict(AppendConflict<AppendContext> conflict)
    {
        // If a duplicate key error occurs on (streamId, streamVersion):
        // - Fail if we expected a specific version
        // - Retry if no expected version was specified
        // On retry, OnBatchCommittingAsync will run again, re-fetching and rebasing stream versions.
        return conflict.Context.ExpectedVersion.HasValue
            ? ConflictResolution.Fail(new InvalidOperationException("Expected version conflict"))
            : ConflictResolution.Retry;
    }
}