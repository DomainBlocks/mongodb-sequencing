using MongoDB.Driver;

namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Defines a binding between a sequence counter collection and a target collection for use with
/// <see cref="MongoSequencedAppender{TDocument,TContext}"/>.
/// </summary>
public sealed class MongoSequenceBinding<TDocument>
{
    /// <summary>
    /// Defines a binding between a sequence counter collection and a target collection for use with
    /// <see cref="MongoSequencedAppender{TDocument,TContext}"/>.
    /// </summary>
    /// <param name="sequenceCollectionNamespace">The namespace of the sequence counter collection.</param>
    /// <param name="sequenceId">The identifier of the sequence counter within the sequence collection.</param>
    /// <param name="targetCollectionNamespace">
    /// The namespace of the target collection into which documents are appended.
    /// </param>
    /// <param name="targetField">
    /// The definition of the document field into which sequence numbers are written.
    /// </param>
    public MongoSequenceBinding(
        CollectionNamespace sequenceCollectionNamespace,
        string sequenceId,
        CollectionNamespace targetCollectionNamespace,
        FieldDefinition<TDocument, long> targetField)
    {
        ArgumentNullException.ThrowIfNull(sequenceCollectionNamespace);
        ArgumentException.ThrowIfNullOrWhiteSpace(sequenceId);
        ArgumentNullException.ThrowIfNull(targetCollectionNamespace);
        ArgumentNullException.ThrowIfNull(targetField);

        SequenceCollectionNamespace = sequenceCollectionNamespace;
        SequenceId = sequenceId;
        TargetCollectionNamespace = targetCollectionNamespace;
        TargetField = targetField;
    }

    /// <summary>
    /// Gets the namespace of the collection used to store and increment the sequence counter.
    /// </summary>
    public CollectionNamespace SequenceCollectionNamespace { get; }

    /// <summary>
    /// Gets the identifier of the sequence counter within the sequence collection.
    /// </summary>
    public string SequenceId { get; }

    /// <summary>
    /// Gets the namespace of the target collection into which documents are appended.
    /// </summary>
    public CollectionNamespace TargetCollectionNamespace { get; }

    /// <summary>
    /// Gets the definition of the document field into which sequence numbers are written.
    /// </summary>
    public FieldDefinition<TDocument, long> TargetField { get; }
}