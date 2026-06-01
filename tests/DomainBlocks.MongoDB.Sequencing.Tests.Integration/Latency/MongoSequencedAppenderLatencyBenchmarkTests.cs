using NUnit.Framework;
using Toxiproxy.Net.Toxics;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration.Latency;

public class MongoSequencedAppenderLatencyBenchmarkTests() : MongoIntegrationTestBase(ToxiproxyFixture.ConnectionString)
{
    private const int TimeoutMillis = 60_000;

    [Test]
    [Explicit("Benchmark")]
    [CancelAfter(TimeoutMillis)]
    public async Task AppendAsync_SingleAppend_MeasureLatency(CancellationToken ct)
    {
        const int warmupIterations = 10;
        const int iterations = 100;
        const int latencyMs = 5;
        const int jitterMs = 3;

        var options = new MongoSequencedAppenderOptions
        {
            BatchingDelay = TimeSpan.FromMilliseconds(10),
            BatchingDelayMinCount = 2
        };

        await using var appender = CreateAppender<object>(options: options);

        await ToxiproxyFixture.MongoProxy.AddAsync(new LatencyToxic
        {
            Name = "mongo-latency",
            Stream = ToxicDirection.DownStream,
            Attributes = new LatencyToxic.ToxicAttributes
            {
                Latency = latencyMs,
                Jitter = jitterMs
            }
        });

        await LatencyBenchmark.RunAsync(
            appender,
            AppendAsync,
            warmupIterations,
            iterations,
            $"latency {latencyMs} ms ± {jitterMs} ms",
            ct);
    }

    [Test]
    [Explicit("Benchmark")]
    [CancelAfter(TimeoutMillis)]
    public async Task AppendAsync_MeasureThroughputCeiling(CancellationToken ct)
    {
        const int appenderCount = 1;
        const int eventCount = 10_000;
        const int maxInFlight = 500;
        const int latencyMs = 30;
        const int jitterMs = 5;

        var options = new MongoSequencedAppenderOptions
        {
            BatchingDelay = TimeSpan.FromMilliseconds(30),
            BatchingDelayMinCount = 2
        };

        var appenders = Enumerable.Range(0, appenderCount)
            .Select(i => CreateAppender<object>(i, options: options))
            .ToArray();

        await ToxiproxyFixture.MongoProxy.AddAsync(new LatencyToxic
        {
            Name = "mongo-throughput",
            Stream = ToxicDirection.DownStream,
            Attributes = new LatencyToxic.ToxicAttributes
            {
                Latency = latencyMs,
                Jitter = jitterMs
            }
        });

        try
        {
            await ThroughputBenchmark.RunAsync(
                appenders,
                AppendAsync,
                eventCount,
                maxInFlight,
                $"latency {latencyMs} ms ± {jitterMs} ms",
                ct);
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