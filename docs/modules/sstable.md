# SSTable Module

## Purpose
Persist sorted key/value runs that power fast range scans and compaction-friendly merges. Level-0 tables accept overlapping key ranges; higher levels guarantee disjoint ranges for efficient searches.

## Key Types
- `TableBuilder` constructs blocks, indexes, filters, and footers while flushing memtable iterators.
- `TableReader` exposes block-cached iterators, bloom filter probes, and statistics (`ApproximateOffsetOf`).
- `Block` structures hold compressed key/value records with restart intervals tuned for prefix sharing.

## Integration Points
- Flush and compaction tasks stream data into `TableBuilder`, which relies on `internal/fs` for file creation.
- `VersionSet` references table metadata (smallest/largest keys, sequence snapshots) when assembling read paths.
- Bloom filters from `internal/filter` attach to table footers, and bitmaps accelerate point-lookups.

## Extension Hooks
- Experiment with compression choices (`Snappy`, `Noop`) or block sizes through builder options.
- Swap the footer layout to include user-defined statistics (e.g., tombstone counts) for research exercises.

## Suggested Exercises
- Implement block-level caching and observe how it changes read amplification.
- Write integration tests that verify table iterators respect sequence numbers and tombstones.
