namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Appends documents to a MongoDB collection with a globally ordered sequence number, providing a total ordering key
/// across change streams and historical reads.
/// </summary>
public interface IMongoSequencedAppender<in TDocument, in TContext> : IAsyncDisposable
{
    /// <summary>
    /// Appends one or more documents, assigning each a contiguous globally ordered sequence number.
    /// </summary>
    /// <param name="documents">The documents to append.</param>
    /// <param name="context">The caller-supplied context to associate with this append operation.</param>
    /// <param name="options">Optional per-call options.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task AppendAsync(
        IEnumerable<TDocument> documents,
        TContext context,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default);
}