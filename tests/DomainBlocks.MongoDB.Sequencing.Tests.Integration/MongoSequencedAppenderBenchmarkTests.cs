using NUnit.Framework;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public class MongoSequencedAppenderBenchmarkTests() : MongoIntegrationTestBase(MongoReplicaSetFixture.ConnectionString)
{
    private const int TimeoutMillis = 60_000;

    [Test]
    [Explicit("Benchmark")]
    [CancelAfter(TimeoutMillis)]
    public async Task AppendAsync_SingleAppend_MeasureLatency(CancellationToken ct)
    {
        const int warmupIterations = 100;
        const int iterations = 1000;

        await using var appender = CreateAppender<object>();

        await LatencyBenchmark.RunAsync(
            appender,
            AppendAsync,
            warmupIterations,
            iterations,
            cancellationToken: ct);
    }

    [Test]
    [Explicit("Benchmark")]
    [CancelAfter(TimeoutMillis)]
    public async Task AppendAsync_MeasureThroughputCeiling(CancellationToken ct)
    {
        const int appenderCount = 1;
        const int eventCount = 1_000_000;
        const int maxInFlight = 1_000;

        var options = new MongoSequencedAppenderOptions
        {
            MaxBatchSize = 1_000,
            BatchingDelay = TimeSpan.FromMilliseconds(5)
        };

        var appenders = Enumerable.Range(0, appenderCount)
            .Select(i => CreateAppender<object>(i, options: options))
            .ToArray();

        try
        {
            await ThroughputBenchmark.RunAsync(
                appenders,
                AppendAsync,
                eventCount,
                maxInFlight,
                cancellationToken: ct);
        }
        finally
        {
            foreach (var a in appenders)
                await a.DisposeAsync();
        }
    }

    private static Task AppendAsync(
        MongoSequencedAppender<TargetDoc, object> appender,
        CancellationToken ct)
    {
        return appender.AppendAsync(
            [new TargetDoc { Value = "Benchmark" }],
            context: new object(),
            cancellationToken: ct);
    }
}