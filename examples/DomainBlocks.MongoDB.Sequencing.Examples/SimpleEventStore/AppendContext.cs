namespace DomainBlocks.MongoDB.Sequencing.Examples.SimpleEventStore;

/// <summary>
/// Context passed with an append request, identifying the target stream and the optional expected stream version used
/// for optimistic concurrency checks.
/// </summary>
public record AppendContext(string StreamId, int? ExpectedVersion);