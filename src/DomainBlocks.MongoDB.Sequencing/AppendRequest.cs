using MongoDB.Bson;

namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Represents an append request, containing the BSON documents to append and caller-supplied context.
/// </summary>
public sealed class AppendRequest<TContext>
{
    private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public AppendRequest(IReadOnlyList<BsonDocument> documents, TContext context)
    {
        ArgumentNullException.ThrowIfNull(documents);

        if (documents.Count == 0)
            throw new ArgumentException("At least one document is required.", nameof(documents));

        Documents = documents;
        Context = context;
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
    /// Gets a value that indicates whether this request has completed.
    /// </summary>
    public bool IsCompleted => _tcs.Task.IsCompleted;

    internal Guid Id { get; } = Guid.NewGuid();
    internal Task Completion => _tcs.Task;

    /// <summary>
    /// Attempts to complete this request successfully, or with a failure if an exception is provided.
    /// </summary>
    /// <returns>True if the request has completed, or false if it has already been completed.</returns>
    public bool TryComplete(Exception? error = null)
    {
        return error is null ? _tcs.TrySetResult() : _tcs.TrySetException(error);
    }
}