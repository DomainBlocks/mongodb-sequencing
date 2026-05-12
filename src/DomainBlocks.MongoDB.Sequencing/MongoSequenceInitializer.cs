using MongoDB.Bson;
using MongoDB.Driver;

namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Utility for initializing sequence counters prior to use with
/// <see cref="MongoSequencedAppender{TDocument,TContext}"/>.
/// </summary>
public static class MongoSequenceInitializer
{
    /// <summary>
    /// Initializes a sequence counter so that the first document appended will be assigned
    /// <paramref name="initialSequenceNumber"/>. Has no effect if the counter document already exists.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if the sequence counter was created; <see langword="false"/> if it already existed.
    /// </returns>
    public static async Task<bool> TryInitializeAsync(
        IMongoClient mongoClient,
        CollectionNamespace sequenceCollectionNamespace,
        string sequenceId,
        long initialSequenceNumber,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(initialSequenceNumber);

        var collection = mongoClient
            .GetDatabase(sequenceCollectionNamespace.DatabaseNamespace.DatabaseName)
            .GetCollection<BsonDocument>(sequenceCollectionNamespace.CollectionName)
            .WithWriteConcern(WriteConcern.WMajority.With(journal: true));

        var filter = Builders<BsonDocument>.Filter.Eq("_id", sequenceId);

        // SetOnInsert ensures this is a no-op if the counter document already exists.
        var update = Builders<BsonDocument>.Update.SetOnInsert(SequenceFieldNames.Next, initialSequenceNumber);

        var result = await collection
            .UpdateOneAsync(filter, update, new UpdateOptions { IsUpsert = true }, cancellationToken)
            .ConfigureAwait(false);

        return result.UpsertedId is not null;
    }
}