using System.Diagnostics;
using NUnit.Framework;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public static class LatencyBenchmark
{
    public static async Task RunAsync<TDocument>(
        MongoSequencedAppender<TDocument, object> appender,
        Func<MongoSequencedAppender<TDocument, object>, CancellationToken, Task> appendFunc,
        int warmupIterations,
        int iterations,
        string? label = null,
        CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < warmupIterations; i++)
            await appendFunc(appender, cancellationToken);

        var latencies = new List<double>(iterations);

        for (var i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            await appendFunc(appender, cancellationToken);
            sw.Stop();
            latencies.Add(sw.Elapsed.TotalMilliseconds);
        }

        if (label is not null)
            await TestContext.Out.WriteLineAsync($"label: {label}");

        var sorted = latencies.OrderBy(x => x).ToList();
        await TestContext.Out.WriteLineAsync($"p50:   {sorted[Percentile(0.50)]:F1} ms");
        await TestContext.Out.WriteLineAsync($"p90:   {sorted[Percentile(0.90)]:F1} ms");
        await TestContext.Out.WriteLineAsync($"p99:   {sorted[Percentile(0.99)]:F1} ms");
        await TestContext.Out.WriteLineAsync($"min:   {sorted[0]:F1} ms");
        await TestContext.Out.WriteLineAsync($"max:   {sorted[^1]:F1} ms");
        await TestContext.Out.WriteLineAsync($"mean:  {latencies.Average():F1} ms");

        return;

        int Percentile(double p) => (int)Math.Ceiling(sorted.Count * p) - 1;
    }
}