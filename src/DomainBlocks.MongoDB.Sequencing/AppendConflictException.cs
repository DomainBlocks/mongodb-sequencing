namespace DomainBlocks.MongoDB.Sequencing;

/// <summary>
/// Thrown when an append operation fails due to an unresolved conflict.
/// </summary>
public class AppendConflictException(string? message = null, Exception? innerException = null) :
    Exception(message, innerException);