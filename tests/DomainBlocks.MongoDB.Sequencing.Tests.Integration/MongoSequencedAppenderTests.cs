using NUnit.Framework;
using Shouldly;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public class MongoSequencedAppenderTests : MongoIntegrationTestBase
{
    private const int TimeoutMillis = 10_000;

    [Test]
    [CancelAfter(TimeoutMillis)]
    public async Task BatchProcessingError_WhenExceptionThrown_FaultsRequestAndContinues(CancellationToken ct)
    {
        const string errorMessage = "Simulated batch failure";
        var failed = 0;

        var policy = new CallbackAppenderPolicy<object>(
            onBatchCommitting: (_, _) => Interlocked.Exchange(ref failed, 1) == 0
                ? throw new InvalidOperationException(errorMessage)
                : Task.CompletedTask);

        await using var appender = CreateAppender(policy: policy);

        // First append should be faulted by the policy.
        var ex = await Should.ThrowAsync<InvalidOperationException>(() => appender.AppendAsync(
            [new TargetDoc { Value = "fail" }],
            new object(),
            cancellationToken: ct));

        ex.Message.ShouldBe(errorMessage);

        // Nothing should be appended.
        var sequence = await ReadSequenceAsync(ct);
        sequence.ShouldBe([]);

        // Second append should succeed normally - worker loop must still be running.
        await appender.AppendAsync([new TargetDoc { Value = "succeed" }], new object(), cancellationToken: ct);

        sequence = await ReadSequenceAsync(ct);
        sequence.ShouldBe([0]);
    }

    private class CallbackAppenderPolicy<TContext>(
        Func<IReadOnlyList<AppendRequest<TContext>>, CancellationToken, Task>? onBatchCommitting = null) :
        IMongoSequencedAppenderPolicy<TContext>
    {
        public Task OnBatchCommittingAsync(IReadOnlyList<AppendRequest<TContext>> batch, CancellationToken ct)
        {
            return onBatchCommitting?.Invoke(batch, ct) ?? Task.CompletedTask;
        }

        public ConflictResolution OnConflict(AppendConflict<TContext> conflict)
        {
            return ConflictResolution.Fail();
        }
    }
}