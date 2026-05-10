using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using NUnit.Framework;
using Shouldly;

namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public class MongoSequencedAppenderConcurrencyTests
{
    private const int TestTimeoutMillis = 10 * 1_000;
    private const int AppenderCount = 5;
    private const string TestDbPrefix = "seq_test_";

    private MongoClient _mongoClient = null!;
    private string _databaseName = null!;
    private CollectionNamespace _seqNs = null!;
    private CollectionNamespace _targetNs = null!;
    private MongoSequencedAppender<TargetDoc, object>[] _appenders = null!;
    private ILoggerFactory _loggerFactory = null!;

    [SetUp]
    public async Task SetUp()
    {
        _mongoClient = new MongoClient(MongoReplicaSetFixture.ConnectionString);
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

        // Ensure the target collection exists and has a unique index on sequence.
        var db = _mongoClient.GetDatabase(_databaseName);
        await db.CreateCollectionAsync(_targetNs.CollectionName);

        var targetCollection = db.GetCollection<TargetDoc>(_targetNs.CollectionName);

        var indexModel = new CreateIndexModel<TargetDoc>(
            Builders<TargetDoc>.IndexKeys.Ascending(x => x.Nested!.Sequence),
            new CreateIndexOptions { Unique = true });

        await targetCollection.Indexes.CreateOneAsync(indexModel);

        _appenders = [.. Enumerable.Range(0, AppenderCount).Select(CreateAppender)];
    }

    [TearDown]
    public async Task TearDown()
    {
        foreach (var appender in _appenders)
            await appender.DisposeAsync();

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
    [CancelAfter(TestTimeoutMillis)]
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
    [CancelAfter(TestTimeoutMillis)]
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
    [CancelAfter(30_000)]
    [Explicit("Long running")]
    public async Task ConcurrentAppends_ChangeStreamRead_SequenceIsStrictlyIncreasingAndContiguous(CancellationToken ct)
    {
        const int runSeconds = 15;
        const int minObservedDocs = 100;

        var db = _mongoClient.GetDatabase(_databaseName);
        var target = db.GetCollection<BsonDocument>(_targetNs.CollectionName);

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

    private MongoSequencedAppender<TargetDoc, object> CreateAppender(int index)
    {
        var binding = new MongoSequenceBinding<TargetDoc>(
            sequenceCollectionNamespace: _seqNs,
            sequenceId: "test_seq",
            targetCollectionNamespace: _targetNs,
            targetField: new ExpressionFieldDefinition<TargetDoc, long>(x => x.Nested!.Sequence));

        return new MongoSequencedAppender<TargetDoc, object>(
            _mongoClient,
            binding,
            logger: _loggerFactory.CreateLogger($"appender_{index}"));
    }

    private async Task<long[]> ReadSequenceAsync(CancellationToken ct)
    {
        var db = _mongoClient.GetDatabase(_databaseName);

        var collection = db.GetCollection<TargetDoc>(_targetNs.CollectionName);

        var docs = await collection
            .Find(FilterDefinition<TargetDoc>.Empty)
            .Sort(Builders<TargetDoc>.Sort.Ascending(x => x.Nested!.Sequence))
            .ToListAsync(ct);

        return [.. docs.Select(d => d.Nested!.Sequence)];
    }

    // ReSharper disable all
    private record TargetDoc
    {
        public ObjectId Id { get; init; }

        public required string Value { get; init; }

        public NestedDoc? Nested { get; init; }
    }

    private record NestedDoc
    {
        [BsonElement("seq")]
        public long Sequence { get; init; }
    }
    // ReSharper restore all
}