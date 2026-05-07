using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace DomainBlocks.MongoDB.Sequencing;

internal static class ClientSessionHandleExtensions
{
    private const int MaxCommitRetries = 3;
    private static readonly TimeSpan CommitRetryDelay = TimeSpan.FromMilliseconds(100);

    extension(IClientSessionHandle session)
    {
        public async Task CommitWithRetryAsync(ILogger? logger = null, CancellationToken cancellationToken = default)
        {
            for (var attempt = 0; attempt < MaxCommitRetries; attempt++)
            {
                try
                {
                    await session.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }
                catch (MongoException ex) when (ex.HasErrorLabel(MongoErrorLabels.UnknownTransactionCommitResult))
                {
                    logger?.LogWarning(ex, "Unknown transaction commit result; retrying");
                    await Task.Delay(CommitRetryDelay, cancellationToken).ConfigureAwait(false);
                }
            }

            // Final attempt - let it throw
            await session.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}