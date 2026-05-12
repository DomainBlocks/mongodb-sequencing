using MongoDB.Bson;
using MongoDB.Driver;
using NUnit.Framework;
using Shouldly;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public class MongoSequencedAppenderConcurrencyTests : MongoIntegrationTestBase
{
    private const int TimeoutMillis = 10_000;
    private const int LongRunningTimeoutMillis = 30_000;
    private const int AppenderCount = 5;

    private MongoSequencedAppender<TargetDoc, object>[] _appenders = null!;

    protected override Task SetUpAsync()
    {
        _appenders =
        [
            .. Enumerable
                .Range(0, AppenderCount)
                .Select(i => CreateAppender<object>(i))
        ];

        return Task.CompletedTask;
    }

    protected override async Task TearDownAsync()
    {
        foreach (var a in _appenders)
            await a.DisposeAsync();
    }

    [Test]
    [CancelAfter(TimeoutMillis)]
    public async Task ConcurrentAppends_FromMultipleAppenders_AllSucceed(CancellationToken ct)
    {
        const int docsPerAppender = 20;
        const int expectedTotal = AppenderCount * docsPerAppender;

        var tasks = _appenders
            .SelectMany((appender, i) => Enumerable
                .Range(0, docsPerAppender)
                .Select(j => appender.AppendAsync(
                    [new TargetDoc { Value = $"a{i}-d{j}" }],
                    context: new object(),
                    cancellationToken: ct)));

        await Task.WhenAll(tasks);

        var sequence = await ReadSequenceAsync(ct);

        sequence.Length.ShouldBe(expectedTotal, "Every appended document must be committed");

        sequence.ShouldBe(
            Enumerable.Range(0, expectedTotal).Select(i => (long)i),
            "Sequence must be contiguous starting from 0");
    }

    [Test]
    [CancelAfter(TimeoutMillis)]
    public async Task ConcurrentAppends_HistoricalRead_SequenceIsStrictlyIncreasingAndContiguous(CancellationToken ct)
    {
        const int batchesPerAppender = 10;
        const int docsPerBatch = 3;
        const int expectedTotal = AppenderCount * batchesPerAppender * docsPerBatch;

        var tasks = _appenders
            .SelectMany((appender, i) => Enumerable
                .Range(0, batchesPerAppender)
                .Select(b => appender.AppendAsync(
                    Enumerable
                        .Range(0, docsPerBatch)
                        .Select(d => new TargetDoc { Value = $"a{i}-b{b}-d{d}" }),
                    context: new object(),
                    cancellationToken: ct)));

        await Task.WhenAll(tasks);

        var positions = await ReadSequenceAsync(ct);

        positions.Length.ShouldBe(expectedTotal, "All batch documents must be committed");

        positions.ShouldBe(
            Enumerable.Range(0, expectedTotal).Select(i => (long)i),
            "Sequence must be strictly contiguous across all batches");
    }

    [Test]
    [CancelAfter(LongRunningTimeoutMillis)]
    [Explicit("Long running")]
    public async Task ConcurrentAppends_ChangeStreamRead_SequenceIsStrictlyIncreasingAndContiguous(CancellationToken ct)
    {
        const int runSeconds = 15;
        const int minObservedDocs = 100;

        var db = MongoClient.GetDatabase(DatabaseName);
        var target = db.GetCollection<BsonDocument>(TargetNs.CollectionName);

        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        runCts.CancelAfter(TimeSpan.FromSeconds(runSeconds));
        var runToken = runCts.Token;

        Exception? firstFailure = null;

        var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
            .Match(x => x.OperationType == ChangeStreamOperationType.Insert);

        var observerTask = Task.Run(
            async () =>
            {
                long? lastSeq = null;
                var observed = 0;

                try
                {
                    using var cursor = await target.WatchAsync(
                        pipeline,
                        new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.Default },
                        runToken);

                    while (await cursor.MoveNextAsync(runToken))
                    {
                        foreach (var change in cursor.Current)
                        {
                            var doc = change.FullDocument;
                            if (doc is null)
                                continue;

                            var seq = doc["Nested"]["seq"].AsInt64;

                            if (lastSeq.HasValue && seq != lastSeq.Value + 1)
                            {
                                firstFailure ??= new ShouldAssertException(
                                    $"Sequence anomaly detected. Last={lastSeq.Value}, Current={seq}");

                                // ReSharper disable once AccessToDisposedClosure - observerTask awaited below
                                await runCts.CancelAsync();
                                return;
                            }

                            lastSeq = seq;
                            observed++;
                        }
                    }
                }
                catch (OperationCanceledException) when (runToken.IsCancellationRequested)
                {
                }

                if (firstFailure is null)
                {
                    observed.ShouldBeGreaterThanOrEqualTo(
                        minObservedDocs,
                        $"Expected at least {minObservedDocs} observed documents");
                }
            },
            runToken);

        var appendTasks = _appenders
            .Select((appender, i) => Task.Run(
                async () =>
                {
                    var seq = 0;

                    while (!runToken.IsCancellationRequested)
                    {
                        await appender.AppendAsync(
                            [
                                new TargetDoc { Value = $"w{i}-e{seq++}" },
                                new TargetDoc { Value = $"w{i}-e{seq++}" },
                                new TargetDoc { Value = $"w{i}-e{seq++}" }
                            ],
                            context: new object(),
                            cancellationToken: runToken);
                    }
                },
                runToken))
            .ToArray();

        try
        {
            await Task.WhenAll(appendTasks.Append(observerTask));
        }
        catch (OperationCanceledException) when (runToken.IsCancellationRequested)
        {
        }

        if (firstFailure is not null)
            throw firstFailure;
    }
}