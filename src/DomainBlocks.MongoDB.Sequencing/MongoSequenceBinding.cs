using MongoDB.Driver;

namespace DomainBlocks.MongoDB.Sequencing;

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
public sealed class MongoSequenceBinding<TDocument>(
    CollectionNamespace sequenceCollectionNamespace,
    string sequenceId,
    CollectionNamespace targetCollectionNamespace,
    FieldDefinition<TDocument, long> targetField)
{
    /// <summary>
    /// Gets the namespace of the collection used to store and increment the sequence counter.
    /// </summary>
    public CollectionNamespace SequenceCollectionNamespace { get; } = sequenceCollectionNamespace;

    /// <summary>
    /// Gets the identifier of the sequence counter within the sequence collection.
    /// </summary>
    public string SequenceId { get; } = sequenceId;

    /// <summary>
    /// Gets the namespace of the target collection into which documents are appended.
    /// </summary>
    public CollectionNamespace TargetCollectionNamespace { get; } = targetCollectionNamespace;

    /// <summary>
    /// Gets the definition of the document field into which sequence numbers are written.
    /// </summary>
    public FieldDefinition<TDocument, long> TargetField { get; } = targetField;
}