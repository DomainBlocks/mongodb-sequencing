namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// The default policy that performs no pre-commit logic and always retries on conflict. Used when no custom policy is
/// provided.
/// </summary>
public sealed class DefaultSequencedAppenderPolicy<TContext> : IMongoSequencedAppenderPolicy<TContext>
{
    public static readonly DefaultSequencedAppenderPolicy<TContext> Shared = new();

    private DefaultSequencedAppenderPolicy()
    {
    }

    public Task OnBatchCommittingAsync(
        IReadOnlyList<AppendRequest<TContext>> batch,
        CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public ConflictResolution OnConflict(AppendConflict<TContext> conflict)
    {
        return ConflictResolution.Retry;
    }
}