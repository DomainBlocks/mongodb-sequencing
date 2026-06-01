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
    public int MaxBatchSize { get; set; } = 500;

    /// <summary>
    /// Gets or sets the maximum time to wait for additional requests to accumulate before committing a batch. When
    /// multiple requests are queued together, the appender will wait up to this duration to coalesce further incoming
    /// requests into the same batch. The default value is <see cref="TimeSpan.Zero"/> (no delay).
    /// </summary>
    public TimeSpan BatchingDelay { get; set; }

    /// <summary>
    /// Gets or sets the minimum number of requests that must be immediately available upon waking before the
    /// <see cref="BatchingDelay"/> is applied. Set to 2 or higher to require a minimum number of concurrently available
    /// requests before delaying. Values less than 2 cause the delay to always apply. The default value is zero.
    /// </summary>
    public int BatchingDelayMinCount { get; set; }

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