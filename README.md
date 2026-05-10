# DomainBlocks.MongoDB.Sequencing

A .NET library for appending documents to a MongoDB collection with a globally ordered, strictly increasing, contiguous
sequence number - providing a reliable total ordering across historical reads and change stream observations.

## Table of contents

- [Prerequisites](#prerequisites)
- [The problem](#the-problem)
- [How it works](#how-it-works)
- [Customizing behavior with a policy](#customizing-behavior-with-a-policy)
- [Use cases](#use-cases)
    - [Why not change stream resume tokens?](#why-not-change-stream-resume-tokens)
    - [Example: implementing a simple event store](#example-implementing-a-simple-event-store)
- [Benchmarks](#benchmarks)
    - [Peak throughput](#peak-throughput)
    - [Latency](#latency)

## Prerequisites

- MongoDB 7.0+
- MongoDB replica set deployment
- .NET 10

## The problem

Out of the box, MongoDB does not provide a way to assign a globally ordered sequence number to documents that reflects
true insertion order across multiple concurrent writers. This makes it difficult to:

- Know that a historical read (sorted by sequence) and a change stream share the same total order, so a consumer can
  seamlessly transition from one to the other without gaps or ambiguity.
- Reason about causality - if B has a higher sequence number than A, then B causally follows A.

Without this, event-driven processing becomes unreliable - consumers cannot checkpoint their position with confidence,
and the boundary between catching up historically and tailing live cannot be reasoned about correctly.

## How it works

[MongoSequencedAppender](src/DomainBlocks.MongoDB.Sequencing/MongoSequencedAppender.cs) solves this by:

1. Atomically claiming a range of sequence numbers from a counter document using `findOneAndUpdate` inside a MongoDB
   multi-document transaction.
2. Stamping the claimed sequence numbers onto a target field on each document being appended.
3. Appending the documents into the target collection within the same transaction, using an ordered `insertMany`.
4. Batching concurrent append requests into a single transaction, so that throughput scales under load without
   sacrificing ordering guarantees.

Because the counter increment and the inserts are committed atomically, sequence numbers are guaranteed to be contiguous
and globally ordered - even across multiple concurrent appenders. When two transactions attempt to increment the counter
simultaneously, MongoDB's optimistic concurrency control ensures only one can commit; the other fails with a write
conflict and retries. This forces appends to serialize, eliminating any possibility of out-of-order sequence number
assignment. The result is that sorting by sequence number reflects true insertion order across the entire collection.

## Customizing behavior with a policy

An `IMongoSequencedAppenderPolicy<TContext>` can be supplied to hook into the append pipeline at two points: before
each batch is committed, and when a duplicate key conflict is detected on the target collection. Uses include:

- Stamping per-stream version numbers onto event documents before commit.
- Implementing idempotent commits by detecting and skipping already-applied requests.
- Completing requests early based on pre-commit validation.
- Implementing optimistic concurrency checks.

For an example implementation,
see [AppenderPolicy](examples/DomainBlocks.MongoDB.Sequencing.Examples/SimpleEventStore/AppenderPolicy.cs).

## Use cases

This library is particularly useful for introducing global ordering into a MongoDB-based event store, where events
(represented by documents) must be consumable in a guaranteed total order by downstream event handlers and projections.

Such global sequencing enables reliable checkpointing when tailing an event collection via a change stream. Because
historical reads and change stream observations share the same total order, transitioning from catch-up to live
processing is straightforward to reason about and implement correctly.

### Why not change stream resume tokens?

Unlike change stream resume tokens, a sequence number checkpoint is durable and self-contained. Resume tokens eventually
roll off the oplog (making resume impossible without replaying from scratch), can be invalidated if pipeline options
change, and have no meaningful correlation to position within the collection itself. A sequence number, by contrast,
survives oplog rotation, is easy to reason about across historical reads and change streams, and always reflects a
document's true position in the global order.

To be clear, this library does not attempt to replace change stream resume tokens - they remain the mechanism for
resuming a change stream itself. Rather, a sequence number provides a reliable, durable way to track your position
within a collection that's being used as an append-only log, independent of the change stream infrastructure.

### Example: implementing a simple event store

See [SimpleEventStore](examples) for an example showing how to build a simple, globally ordered event store using this
library.

## Benchmarks

Benchmarks run against a local three-node MongoDB replica set running in Docker on an Apple M3 MacBook Air.

The benchmark tests are included in the repository and can be run against your own environment to get representative
numbers for your setup.

### Peak throughput

Peak throughput measured over 15 seconds after a 3-second warm-up, with each operation appending a single document. The
following parameters were used:

- **Max in-flight operations:** 1,000 total across all concurrent appender instances
- **Batch size:** 500 (maximum append operations committed in a single transaction)
- **Queue capacity:** 1,000 (maximum append operations buffered before callers block)

| Appender instance count* | Throughput (ops/sec) |
|-------------------------:|---------------------:|
|                        1 |               65,502 |
|                        3 |               46,464 |
|                        5 |               30,126 |

\* Values greater than 1 indicate multiple appender instances issuing append operations concurrently.

Throughput decreases as the number of concurrent appenders increases due to greater write conflict contention on the
counter document, causing more transactions to abort and retry.

### Latency

Single-appender latency, measured over 1,000 sequential appends following 100 warm-up iterations, one document per
operation.

| Metric | Latency (ms) |
|-------:|-------------:|
|    p50 |          1.4 |
|    p90 |          1.7 |
|    p99 |          2.3 |
|    min |          1.0 |
|    max |          5.5 |
|   mean |          1.4 |