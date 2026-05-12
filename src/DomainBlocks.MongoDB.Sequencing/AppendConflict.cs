using MongoDB.Bson;

namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Represents an append conflict caused by duplicate key error during a commit attempt.
/// </summary>
public sealed class AppendConflict<TContext>
{
    /// <summary>
    /// Represents an append conflict caused by duplicate key error during a commit attempt.
    /// </summary>
    public AppendConflict(
        IReadOnlyList<BsonDocument> documents,
        TContext context,
        int documentIndex,
        string? errorMessage,
        Exception originatingException)
    {
        ArgumentNullException.ThrowIfNull(documents);
        ArgumentNullException.ThrowIfNull(originatingException);

        if (documents.Count == 0)
            throw new ArgumentException("At least one document is required.", nameof(documents));

        Documents = documents;
        Context = context;
        DocumentIndex = documentIndex;
        ErrorMessage = errorMessage;
        OriginatingException = originatingException;
    }

    /// <summary>
    /// Gets the BSON documents to be appended. Always contains at least one document.
    /// </summary>
    public IReadOnlyList<BsonDocument> Documents { get; }

    /// <summary>
    /// Gets the caller-supplied context associated with the append operation.
    /// </summary>
    public TContext Context { get; }

    /// <summary>
    /// Gets the index of the conflicting document within <see cref="Documents"/>.
    /// </summary>
    public int DocumentIndex { get; }

    /// <summary>
    /// Gets the error message provided by MongoDB, or <c>null</c> if unavailable.
    /// </summary>
    public string? ErrorMessage { get; }

    /// <summary>
    /// Gets the exception that originally signaled the conflict, thrown by the MongoDB driver. Suitable for use as an
    /// inner exception.
    /// </summary>
    public Exception OriginatingException { get; }
}