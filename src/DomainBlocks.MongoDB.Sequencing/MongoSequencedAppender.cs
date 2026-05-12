using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace DomainBlocks.MongoDB.Sequencing;

internal static class MongoSequencedAppender
{
    public static readonly TransactionOptions TransactionOptions = new(
        ReadConcern.Snapshot,
        ReadPreference.Primary,
        WriteConcern.WMajority.With(journal: true));

    public static readonly InsertManyOptions InsertManyOptions = new() { IsOrdered = true };
}

/// <inheritdoc/>
public sealed class MongoSequencedAppender<TDocument, TContext> : IMongoSequencedAppender<TDocument, TContext>
{
    private readonly IMongoClient _mongoClient;
    private readonly IMongoCollection<BsonDocument> _sequenceCollection;
    private readonly IMongoCollection<BsonDocument> _targetCollection;
    private readonly string _sequenceId;
    private readonly string[] _targetFieldPathSegments;
    private readonly IMongoSequencedAppenderPolicy<TContext> _appenderPolicy;
    private readonly ILogger _logger;
    private readonly Channel<AppendRequest<TContext>> _channel;
    private readonly int _batchSize;
    private readonly int _maxConflictRetries;
    private readonly TimeSpan _conflictRetryDelay;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly Task _runAppendLoopTask;
    private readonly Buffers _buffers = new();
    private int _disposed;

    /// <summary>
    /// Initializes a new instance of <see cref="MongoSequencedAppender{TDocument,TContext}"/>.
    /// </summary>
    /// <param name="mongoClient">The MongoDB client used by this appender.</param>
    /// <param name="binding">The sequence binding used by this appender.</param>
    /// <param name="appendPolicy">
    /// An optional policy for pre-commit logic and conflict resolution. Defaults to
    /// <see cref="DefaultSequencedAppenderPolicy{TContext}"/> if not provided.
    /// </param>
    /// <param name="options">Optional configuration.</param>
    /// <param name="logger">An optional logger. No logging occurs if not provided.</param>
    public MongoSequencedAppender(
        IMongoClient mongoClient,
        MongoSequenceBinding<TDocument> binding,
        IMongoSequencedAppenderPolicy<TContext>? appendPolicy = null,
        MongoSequencedAppenderOptions? options = null,
        ILogger? logger = null)
    {
        var serializerRegistry = BsonSerializer.SerializerRegistry;
        var documentSerializer = serializerRegistry.GetSerializer<TDocument>();

#if MONGO_DRIVER_V2
        var targetFieldName = binding.TargetField.Render(documentSerializer, serializerRegistry).FieldName;
#else
        var renderArgs = new RenderArgs<TDocument>(
            documentSerializer,
            serializerRegistry,
            translationOptions: mongoClient.Settings.TranslationOptions);

        var targetFieldName = binding.TargetField.Render(renderArgs).FieldName;
#endif

        options ??= new MongoSequencedAppenderOptions();

        _mongoClient = mongoClient;

        _sequenceCollection = mongoClient
            .GetDatabase(binding.SequenceCollectionNamespace.DatabaseNamespace.DatabaseName)
            .GetCollection<BsonDocument>(binding.SequenceCollectionNamespace.CollectionName);

        _targetCollection = mongoClient
            .GetDatabase(binding.TargetCollectionNamespace.DatabaseNamespace.DatabaseName)
            .GetCollection<BsonDocument>(binding.TargetCollectionNamespace.CollectionName);

        _sequenceId = binding.SequenceId;
        _targetFieldPathSegments = targetFieldName.Split('.');
        _appenderPolicy = appendPolicy ?? DefaultSequencedAppenderPolicy<TContext>.Shared;
        _logger = logger ?? NullLogger.Instance;

        _channel = Channel.CreateBounded<AppendRequest<TContext>>(new BoundedChannelOptions(options.QueueCapacity)
        {
            SingleWriter = false,
            SingleReader = true
        });

        _batchSize = options.BatchSize;
        _maxConflictRetries = options.MaxConflictRetries;
        _conflictRetryDelay = options.ConflictRetryDelay;

        _runAppendLoopTask = RunAppendLoopAsync(_stopCts.Token);
    }

    public async Task AppendAsync(
        IEnumerable<TDocument> documents,
        TContext context,
        AppendOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var bsonDocuments = documents.Select(x => x.ToBsonDocument()).ToArray();
        if (bsonDocuments.Length == 0)
            return;

        options ??= new AppendOptions();
        var request = new AppendRequest<TContext>(bsonDocuments, context);

        using var linkedTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linkedTimeoutCts.CancelAfter(options.Timeout);

        try
        {
            await _channel.Writer.WriteAsync(request, linkedTimeoutCts.Token);
            await request.Completion.WaitAsync(linkedTimeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Caller canceled.
            throw;
        }
        catch (OperationCanceledException) when (linkedTimeoutCts.IsCancellationRequested)
        {
            throw new TimeoutException($"Append operation did not complete within {options.Timeout}.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _channel.Writer.TryComplete();
        await _stopCts.CancelAsync().ConfigureAwait(false);
        await _runAppendLoopTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        _stopCts.Dispose();
    }

    private async Task RunAppendLoopAsync(CancellationToken ct)
    {
        var batch = new List<AppendRequest<TContext>>(_batchSize);

        try
        {
            while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (batch.Count < _batchSize && _channel.Reader.TryRead(out var request))
                    batch.Add(request);

                if (batch.Count == 0)
                    continue;

                await ProcessBatchAsync(batch, ct).ConfigureAwait(false);
                batch.Clear();
            }
        }
        catch (OperationCanceledException ex) when (ct.IsCancellationRequested)
        {
            // Graceful stop: complete writer, then fault current batch + queued items.
            _channel.Writer.TryComplete();
            FaultAll(batch, ex);
            DrainWithFault(_channel, ex);
        }
        catch (Exception ex)
        {
            // Fatal worker failure: complete writer with error, then current batch + queued items.
            _logger.LogCritical(ex, "A fatal error occurred while processing appends");
            _channel.Writer.TryComplete(ex);
            FaultAll(batch, ex);
            DrainWithFault(_channel, ex);
        }
    }

    private async Task ProcessBatchAsync(List<AppendRequest<TContext>> batch, CancellationToken ct)
    {
        _buffers.ConflictRetryCounts.Clear();

        while (batch.Count > 0)
        {
            await PrepareCommitAsync(batch, ct).ConfigureAwait(false);
            var result = await CommitAsync(ct).ConfigureAwait(false);

            switch (result)
            {
                case CommitResult.Success:
                    foreach (var request in _buffers.Requests)
                        request.Value.TryComplete();
                    return;
                case CommitResult.Conflict conflict:
                    await HandleConflictAsync(batch, conflict, ct).ConfigureAwait(false);
                    break;
            }
        }
    }

    private async Task PrepareCommitAsync(List<AppendRequest<TContext>> batch, CancellationToken ct)
    {
        _buffers.Requests.Clear();
        _buffers.Documents.Clear();
        _buffers.RequestByDocumentIndex.Clear();

        await _appenderPolicy.OnBatchCommittingAsync(batch, ct);

        foreach (var request in batch)
        {
            if (request.IsCompleted)
                continue;

            _buffers.Requests.Add(request.Id, request);

            foreach (var document in request.Documents)
            {
                _buffers.Documents.Add(document);
                _buffers.RequestByDocumentIndex.Add(request);
            }
        }
    }

    private async Task<CommitResult> CommitAsync(CancellationToken ct)
    {
        var docs = _buffers.Documents;
        var docCount = docs.Count;

        if (docCount == 0)
            return new CommitResult.Success();

        while (true)
        {
            using var session = await _mongoClient.StartSessionAsync(cancellationToken: ct).ConfigureAwait(false);
            session.StartTransaction(MongoSequencedAppender.TransactionOptions);

            try
            {
                var startSeq = await ClaimSequenceAsync(session, docCount, ct).ConfigureAwait(false);

                for (var i = 0; i < docCount; i++)
                    SetSequenceField(docs[i], _targetFieldPathSegments, startSeq + i);

                await _targetCollection
                    .InsertManyAsync(session, docs, MongoSequencedAppender.InsertManyOptions, ct)
                    .ConfigureAwait(false);

                await session.CommitWithRetryAsync(_logger, ct).ConfigureAwait(false);
                return new CommitResult.Success();
            }
            catch (MongoException ex) when (ex.HasErrorLabel(MongoErrorLabels.TransientTransactionError))
            {
                await SafeAbortTransactionAsync(session, ct).ConfigureAwait(false);
                _logger.LogTrace("Transient transaction error; retrying transaction (size={BatchSize})", docCount);
            }
            catch (MongoBulkWriteException ex) when (ex.WriteErrors.Any(x => x.Code == MongoErrorCodes.DuplicateKey))
            {
                await SafeAbortTransactionAsync(session, ct).ConfigureAwait(false);

                var firstError = ex.WriteErrors.First(e => e.Code == MongoErrorCodes.DuplicateKey);
                var request = _buffers.RequestByDocumentIndex[firstError.Index];

                var conflict = new AppendConflict<TContext>(
                    request.Documents,
                    request.Context,
                    firstError.Index,
                    firstError.Message,
                    ex);

                return new CommitResult.Conflict(request.Id, conflict);
            }
            catch (Exception)
            {
                await SafeAbortTransactionAsync(session, ct).ConfigureAwait(false);
                throw;
            }
        }
    }

    private async Task HandleConflictAsync(
        List<AppendRequest<TContext>> batch,
        CommitResult.Conflict conflict,
        CancellationToken ct)
    {
        var requestId = conflict.RequestId;
        var appendConflict = conflict.AppendConflict;

        using var scope = _logger.BeginScope(new { RequestId = requestId });
        _logger.LogDebug("Duplicate key conflict detected; invoking policy");

        var resolution = _appenderPolicy.OnConflict(appendConflict);

        if (resolution is ConflictResolution.FailResolution fail)
        {
            _logger.LogDebug("Conflict resolved by policy");

            CompleteAndRemoveFromBatch(
                fail.Exception ??
                new AppendConflictException(innerException: appendConflict.OriginatingException));

            return;
        }

        if (resolution is not ConflictResolution.RetryResolution)
            throw new UnreachableException($"Unknown conflict resolution type '{resolution.GetType()}'.");

        var retryCount = _buffers.ConflictRetryCounts.GetValueOrDefault(requestId);
        if (retryCount >= _maxConflictRetries)
        {
            _logger.LogWarning(
                "Conflict not resolved by policy after {MaxRetries} retries; evicting request",
                _maxConflictRetries);

            CompleteAndRemoveFromBatch(new AppendConflictException(
                $"Conflict not resolved after {_maxConflictRetries} retries.",
                innerException: appendConflict.OriginatingException));
        }
        else
        {
            _logger.LogDebug(
                "Conflict not resolved by policy; retrying batch in {RetryDelay} (attempt {Attempt}/{Max})",
                _conflictRetryDelay,
                retryCount + 1,
                _maxConflictRetries);

            _buffers.ConflictRetryCounts[requestId] = retryCount + 1;

            await Task.Delay(_conflictRetryDelay, ct).ConfigureAwait(false);
        }

        void CompleteAndRemoveFromBatch(Exception exception)
        {
            var request = _buffers.Requests[requestId];
            request.TryComplete(exception);
            batch.Remove(request);
        }
    }

    private async Task<long> ClaimSequenceAsync(IClientSessionHandle session, long count, CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", _sequenceId);
        var update = Builders<BsonDocument>.Update.Inc(SequenceFieldNames.Next, count);

        var options = new FindOneAndUpdateOptions<BsonDocument>
        {
            IsUpsert = true,
            Projection = Builders<BsonDocument>.Projection.Include(SequenceFieldNames.Next),
            ReturnDocument = ReturnDocument.Before
        };

        var result = await _sequenceCollection
            .FindOneAndUpdateAsync(session, filter, update, options, ct)
            .ConfigureAwait(false);

        var start = result?[SequenceFieldNames.Next].ToInt64() ?? 0;

        return start;
    }

    private static void SetSequenceField(BsonDocument doc, ReadOnlySpan<string> pathSegments, long value)
    {
        var currentDoc = doc;

        for (var i = 0; i < pathSegments.Length - 1; i++)
        {
            var segment = pathSegments[i];

            if (!currentDoc.TryGetValue(segment, out var child) || child.IsBsonNull)
            {
                var childDoc = new BsonDocument();
                currentDoc[segment] = childDoc;
                currentDoc = childDoc;
                continue;
            }

            {
                if (child is not BsonDocument childDoc)
                {
                    var fullPath = string.Join('.', pathSegments);
                    throw new InvalidOperationException(
                        $"Path '{fullPath}' invalid at '{segment}': expected BsonDocument, got {child.BsonType}.");
                }

                currentDoc = childDoc;
            }
        }

        currentDoc[pathSegments[^1]] = value;
    }

    private async Task SafeAbortTransactionAsync(IClientSessionHandle session, CancellationToken ct)
    {
        if (!session.IsInTransaction)
            return;

        try
        {
            await session.AbortTransactionAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogTrace("Transaction canceled");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to abort transaction");
        }
    }

    private static void FaultAll(List<AppendRequest<TContext>> entries, Exception exception)
    {
        foreach (var request in entries)
            request.TryComplete(exception);
    }

    private static void DrainWithFault(ChannelReader<AppendRequest<TContext>> reader, Exception exception)
    {
        while (reader.TryRead(out var request))
            request.TryComplete(exception);
    }

    private sealed class Buffers
    {
        public readonly Dictionary<Guid, AppendRequest<TContext>> Requests = [];
        public readonly List<BsonDocument> Documents = [];
        public readonly List<AppendRequest<TContext>> RequestByDocumentIndex = [];
        public readonly Dictionary<Guid, int> ConflictRetryCounts = [];
    }

    private abstract class CommitResult
    {
        public sealed class Success : CommitResult;

        public sealed class Conflict(Guid requestId, AppendConflict<TContext> appendConflict) : CommitResult
        {
            public Guid RequestId { get; } = requestId;
            public AppendConflict<TContext> AppendConflict { get; } = appendConflict;
        }
    }
}