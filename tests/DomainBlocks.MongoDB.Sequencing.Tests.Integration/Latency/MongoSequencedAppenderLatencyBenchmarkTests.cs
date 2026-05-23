using NUnit.Framework;
using Toxiproxy.Net.Toxics;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration.Latency;

public class MongoSequencedAppenderLatencyBenchmarkTests() : MongoIntegrationTestBase(ToxiproxyFixture.ConnectionString)
{
    private const int TimeoutMillis = 60_000;

    [Test]
    [Explicit("Benchmark")]
    [CancelAfter(TimeoutMillis)]
    public async Task AppendAsync_MeasureThroughputCeiling_UnderLatency(CancellationToken ct)
    {
        const int appenderCount = 1;
        const int eventCount = 10_000;
        const int maxInFlight = 500;
        const int latencyMs = 30;
        const int jitterMs = 5;

        var appenders = Enumerable.Range(0, appenderCount)
            .Select(i => CreateAppender<object>(i))
            .ToArray();

        var proxy = ToxiproxyFixture.MongoProxy;

        await proxy.AddAsync(new LatencyToxic
        {
            Name = "mongo-latency",
            Stream = ToxicDirection.DownStream,
            Attributes = new LatencyToxic.ToxicAttributes
            {
                Latency = latencyMs,
                Jitter = jitterMs
            }
        });

        try
        {
            await ThroughputMeasurement.RunAsync(
                appenders,
                (appender, token) => appender.AppendAsync(
                    [new TargetDoc { Value = "Benchmark" }],
                    context: new object(),
                    cancellationToken: token),
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
}