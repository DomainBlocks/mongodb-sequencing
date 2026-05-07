namespace DomainBlocks.MongoDB.Sequencing;

internal static class MongoErrorLabels
{
    public const string TransientTransactionError = "TransientTransactionError";
    public const string UnknownTransactionCommitResult = "UnknownTransactionCommitResult";
}