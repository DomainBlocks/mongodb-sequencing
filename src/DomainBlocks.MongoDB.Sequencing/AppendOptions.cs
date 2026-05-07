namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Per-call options for appending documents via <see cref="IMongoSequencedAppender{TDocument,TContext}.AppendAsync"/>.
/// </summary>
public class AppendOptions
{
    /// <summary>
    /// Gets or sets the maximum time to wait for the append operation to complete. Defaults to 10 seconds.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);
}