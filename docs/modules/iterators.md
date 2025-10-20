# Iterator Framework Module

## Purpose
Provide composable cursor primitives that unify access across memtables, WAL replayers, and SSTables. Reliable iterators keep the baseline implementation deterministic and simplify student-written compactions.

## Key Types
- `Iterator` interface with positioning (`First`, `Last`, `Seek`), navigation (`Next`, `Prev`), and accessors (`Key`, `Value`, `Valid`).
- `MergingIterator` that performs k-way merges over sorted child iterators using the configured comparator.
- `BoundedIterator` wrapper enforcing key range restrictions for level-specific scans.

## Integration Points
- Memtable flushes expose iterators consumed by `TableBuilder`.
- Compaction tasks compose multiple table iterators into a single stream, respecting sequence numbers and tombstones.
- Read paths compose a memtable iterator with the leveled SSTable hierarchy, enabling consistent snapshot reads.

## Extension Hooks
- Allow comparator injection (bytewise vs. user-defined) to highlight order-dependent behavior.
- Implement lazy iterator materialization to study backpressure and resource usage.

## Suggested Exercises
- Add assertion-enabled iterators that validate ordering invariants in tests.
- Expose a tracing wrapper that records iterator access patterns for visualization tooling.
