using MongoDB.Bson;

namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Represents an append conflict caused by duplicate key error during a commit attempt.
/// </summary>
public sealed class AppendConflict<TContext>(
    IReadOnlyList<BsonDocument> documents,
    TContext context,
    int documentIndex,
    string? errorMessage,
    Exception originatingException)
{
    /// <summary>
    /// Gets the BSON documents to be appended.
    /// </summary>
    public IReadOnlyList<BsonDocument> Documents { get; } = documents;

    /// <summary>
    /// Gets the caller-supplied context associated with the append operation.
    /// </summary>
    public TContext Context { get; } = context;

    /// <summary>
    /// Gets the index of the conflicting document within <see cref="Documents"/>.
    /// </summary>
    public int DocumentIndex { get; } = documentIndex;

    /// <summary>
    /// Gets the error message provided by MongoDB, or <c>null</c> if unavailable.
    /// </summary>
    public string? ErrorMessage { get; } = errorMessage;

    /// <summary>
    /// Gets the exception that originally signaled the conflict, thrown by the MongoDB driver. Suitable for use as an
    /// inner exception.
    /// </summary>
    public Exception OriginatingException { get; } = originatingException;
}