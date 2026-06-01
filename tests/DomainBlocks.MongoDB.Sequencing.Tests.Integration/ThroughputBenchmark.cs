using System.Diagnostics;
using NUnit.Framework;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public static class ThroughputBenchmark
{
    public static async Task RunAsync<TDocument>(
        IReadOnlyList<MongoSequencedAppender<TDocument, object>> appenders,
        Func<MongoSequencedAppender<TDocument, object>, CancellationToken, Task> appendFunc,
        int totalOps,
        int maxDop,
        string? label = null,
        CancellationToken cancellationToken = default)
    {
        var random = Random.Shared;
        var errors = 0;

        var sw = Stopwatch.StartNew();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, totalOps),
            new ParallelOptions { MaxDegreeOfParallelism = maxDop, CancellationToken = cancellationToken },
            async (_, ct) =>
            {
                var appender = appenders[random.Next(appenders.Count)];
                try
                {
                    await appendFunc(appender, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception)
                {
                    Interlocked.Increment(ref errors);
                }
            });

        sw.Stop();

        var throughput = totalOps / sw.Elapsed.TotalSeconds;

        if (label is not null)
            await TestContext.Out.WriteLineAsync($"label:      {label}");

        await TestContext.Out.WriteLineAsync($"total ops:  {totalOps:N0}");
        await TestContext.Out.WriteLineAsync($"max dop:    {maxDop:N0}");
        await TestContext.Out.WriteLineAsync($"errors:     {errors:N0}");
        await TestContext.Out.WriteLineAsync($"elapsed:    {sw.Elapsed.TotalMilliseconds:N0} ms");
        await TestContext.Out.WriteLineAsync($"throughput: {throughput:N0} ops/sec");
    }
}