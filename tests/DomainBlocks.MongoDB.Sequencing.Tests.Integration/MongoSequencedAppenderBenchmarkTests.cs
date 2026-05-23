using System.Diagnostics;
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

        for (var i = 0; i < warmupIterations; i++)
            await AppendAsync(appender, ct);

        var latencies = new List<double>(iterations);

        for (var i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            await AppendAsync(appender, ct);
            sw.Stop();
            latencies.Add(sw.Elapsed.TotalMilliseconds);
        }

        var sorted = latencies.OrderBy(x => x).ToList();
        await TestContext.Out.WriteLineAsync($"p50:  {sorted[Percentile(0.50)]:F1} ms");
        await TestContext.Out.WriteLineAsync($"p90:  {sorted[Percentile(0.90)]:F1} ms");
        await TestContext.Out.WriteLineAsync($"p99:  {sorted[Percentile(0.99)]:F1} ms");
        await TestContext.Out.WriteLineAsync($"min:  {sorted[0]:F1} ms");
        await TestContext.Out.WriteLineAsync($"max:  {sorted[^1]:F1} ms");
        await TestContext.Out.WriteLineAsync($"mean: {latencies.Average():F1} ms");

        return;

        int Percentile(double p) => (int)Math.Ceiling(sorted.Count * p) - 1;
    }

    [Test]
    [Explicit("Benchmark")]
    [CancelAfter(TimeoutMillis)]
    public async Task AppendAsync_MeasureThroughputCeiling(CancellationToken ct)
    {
        const int appenderCount = 1;
        const int eventCount = 1_000_000;
        const int maxInFlight = 1_000;

        var appenders = Enumerable.Range(0, appenderCount)
            .Select(i => CreateAppender<object>(i))
            .ToArray();

        try
        {
            await ThroughputMeasurement.RunAsync(
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