namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Represents the policy decision in response to a duplicate key conflict.
/// </summary>
public abstract class ConflictResolution
{
    /// <summary>
    /// Retries the conflicting append operation. The appender will retry up to
    /// <see cref="MongoSequencedAppenderOptions.MaxConflictRetries"/> times.
    /// </summary>
    public static ConflictResolution Retry { get; } = new RetryResolution();

    /// <summary>
    /// Fails the conflicting append operation with an optional exception. If no exception is provided, a default
    /// <see cref="AppendConflictException"/> is used.
    /// </summary>
    public static ConflictResolution Fail(Exception? exception = null) => new FailResolution(exception);

    private ConflictResolution()
    {
    }

    public sealed class RetryResolution : ConflictResolution;

    public sealed class FailResolution(Exception? exception) : ConflictResolution
    {
        public Exception? Exception { get; } = exception;
    }
}