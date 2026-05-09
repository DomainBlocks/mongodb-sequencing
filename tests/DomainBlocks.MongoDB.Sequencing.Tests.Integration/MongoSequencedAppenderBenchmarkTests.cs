using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using NUnit.Framework;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public class MongoSequencedAppenderBenchmarkTests
{
    private const int TestTimeoutMillis = 30 * 1_000;
    private const string TestDbPrefix = "seq_bench_";

    private MongoClient _mongoClient = null!;
    private string _databaseName = null!;
    private CollectionNamespace _seqNs = null!;
    private CollectionNamespace _targetNs = null!;
    private ILoggerFactory _loggerFactory = null!;

    [SetUp]
    public async Task SetUp()
    {
        _mongoClient = new MongoClient(TestMongoConnectionStrings.Default);
        _databaseName = $"{TestDbPrefix}{Guid.NewGuid():N}";
        _seqNs = new CollectionNamespace(_databaseName, "sequences");
        _targetNs = new CollectionNamespace(_databaseName, "targets");

        _loggerFactory = LoggerFactory.Create(x => x
            .AddSimpleConsole(opt =>
            {
                opt.IncludeScopes = true;
                opt.TimestampFormat = "HH:mm:ss.fff ";
            })
            .SetMinimumLevel(LogLevel.Debug));

        var db = _mongoClient.GetDatabase(_databaseName);
        await db.CreateCollectionAsync(_targetNs.CollectionName);

        var targetCollection = db.GetCollection<BenchmarkDoc>(_targetNs.CollectionName);

        var indexModel = new CreateIndexModel<BenchmarkDoc>(
            Builders<BenchmarkDoc>.IndexKeys.Ascending(x => x.Sequence),
            new CreateIndexOptions { Unique = true });

        await targetCollection.Indexes.CreateOneAsync(indexModel);
    }

    [TearDown]
    public async Task TearDown()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        try
        {
            await _mongoClient.DropDatabaseAsync(_databaseName, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // best-effort
        }
        finally
        {
            cts.Dispose();
            _loggerFactory.Dispose();

#if !MONGO_DRIVER_V2
            _mongoClient.Dispose();
#endif
        }
    }

    [Test]
    [Explicit("Benchmark")]
    [CancelAfter(TestTimeoutMillis)]
    public async Task AppendAsync_SingleAppend_MeasureLatency(CancellationToken ct)
    {
        const int warmupIterations = 100;
        const int iterations = 1000;

        await using var appender = CreateAppender();

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
    [CancelAfter(TestTimeoutMillis)]
    public async Task AppendAsync_MeasureThroughputCeiling(CancellationToken ct)
    {
        const int appenderCount = 5;
        const int maxInFlight = 1000;
        const int warmUpSeconds = 3;
        const int measureSeconds = 15;

        var appenders = Enumerable.Range(0, appenderCount)
            .Select(CreateAppender)
            .ToArray();

        try
        {
            var semaphore = new SemaphoreSlim(maxInFlight, maxInFlight);
            var ops = 0;
            var errors = 0;
            var isInMeasureWindow = new StrongBox<bool>(false);
            var random = Random.Shared;

            var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            runCts.CancelAfter(TimeSpan.FromSeconds(warmUpSeconds + measureSeconds));

            var pendingTasks = new ConcurrentBag<Task>();

            var producerLoopTask = Task.Run(
                async () =>
                {
                    using (runCts)
                    {
                        while (!runCts.IsCancellationRequested)
                        {
                            try
                            {
                                await semaphore.WaitAsync(runCts.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                break;
                            }

                            var isMeasuring = isInMeasureWindow.Value;
                            var appender = appenders[random.Next(appenderCount)];

                            var task = AppendAsync(appender, runCts.Token).ContinueWith(
                                t =>
                                {
                                    semaphore.Release();

                                    if (t.IsCompletedSuccessfully)
                                    {
                                        if (isMeasuring)
                                            Interlocked.Increment(ref ops);
                                    }
                                    else if (t.IsFaulted)
                                    {
                                        Interlocked.Increment(ref errors);
                                    }
                                },
                                TaskScheduler.Default);

                            pendingTasks.Add(task);
                        }
                    }
                },
                ct);

            // Warm-up
            await Task.Delay(TimeSpan.FromSeconds(warmUpSeconds), ct);

            // Measure
            isInMeasureWindow.Value = true;
            var start = Stopwatch.GetTimestamp();
            await Task.Delay(TimeSpan.FromSeconds(measureSeconds), ct);

            // Stop
            isInMeasureWindow.Value = false;
            var elapsed = Stopwatch.GetElapsedTime(start);
            await producerLoopTask;
            await Task.WhenAll(pendingTasks).WaitAsync(ct);

            var throughput = ops / elapsed.TotalSeconds;

            await TestContext.Out.WriteLineAsync($"appenders:     {appenderCount}");
            await TestContext.Out.WriteLineAsync($"max in-flight: {maxInFlight:N0}");
            await TestContext.Out.WriteLineAsync($"ops measured:  {ops:N0}");
            await TestContext.Out.WriteLineAsync($"errors:        {errors:N0}");
            await TestContext.Out.WriteLineAsync($"elapsed:       {elapsed.TotalMilliseconds:N0} ms");
            await TestContext.Out.WriteLineAsync($"throughput:    {throughput:N0} ops/sec");
        }
        finally
        {
            foreach (var a in appenders)
                await a.DisposeAsync();
        }
    }

    private static Task AppendAsync(
        MongoSequencedAppender<BenchmarkDoc, object> appender,
        CancellationToken ct)
    {
        return appender.AppendAsync(
            [new BenchmarkDoc { Value = "Benchmark" }],
            context: new object(),
            cancellationToken: ct);
    }

    private MongoSequencedAppender<BenchmarkDoc, object> CreateAppender(int index = 0)
    {
        var binding = new MongoSequenceBinding<BenchmarkDoc>(
            sequenceCollectionNamespace: _seqNs,
            sequenceId: "bench_seq",
            targetCollectionNamespace: _targetNs,
            targetField: new ExpressionFieldDefinition<BenchmarkDoc, long>(x => x.Sequence));

        return new MongoSequencedAppender<BenchmarkDoc, object>(
            _mongoClient,
            binding,
            logger: _loggerFactory.CreateLogger($"appender_{index}"));
    }

    // ReSharper disable all
    private record BenchmarkDoc
    {
        public ObjectId Id { get; init; }
        public required string Value { get; init; }

        [BsonElement("seq")]
        public long Sequence { get; init; }
    }
    // ReSharper restore all
}