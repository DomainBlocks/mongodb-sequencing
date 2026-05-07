namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Configuration options for <see cref="MongoSequencedAppender{TDocument,TContext}"/>.
/// </summary>
public class MongoSequencedAppenderOptions
{
    /// <summary>
    /// Gets or sets the maximum number of append operations that can be queued before callers block. The default value
    /// is 1,000.
    /// </summary>
    public int QueueCapacity { get; set; } = 1_000;

    /// <summary>
    /// Gets or sets the maximum number of appends processed in a single transaction. The default value is 500.
    /// </summary>
    public int BatchSize { get; set; } = 500;

    /// <summary>
    /// Gets or sets the maximum number of times a conflicting append will be retried before being faulted. The default
    /// value is 3.
    /// </summary>
    public int MaxConflictRetries { get; set; } = 3;

    /// <summary>
    /// Gets or sets the delay between conflict retry attempts. The default value is 100 milliseconds.
    /// </summary>
    public TimeSpan ConflictRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);
}