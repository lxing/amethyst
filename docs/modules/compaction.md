# Compaction Manager Module

## Purpose
Maintain read efficiency and bounded storage growth by merging memtables and SSTables into progressively larger, sorted levels. Students explore scheduling heuristics and performance tradeoffs within a controlled environment.

## Key Types
- `Manager` orchestrates background jobs, tracks level metadata, and exposes `MaybeSchedule`.
- `Task` pairs source tables with a target level and owns life-cycle callbacks (`Prepare`, `Run`, `Install`).
- `MergeIterator` bridges memtable and SSTable iterators to deliver sorted key/value streams.

## Integration Points
- Flush tasks materialize active memtables into level-0 tables via `TableBuilder`.
- Leveling tasks consume output manifests, replacing old files atomically and triggering deletion via `fs.RemoveObsolete`.
- Metrics feed into the instrumentation layer to expose compaction debt and queue length.

## Extension Hooks
- Introduce pluggable picking strategies (size-tiered vs. leveled) via interfaces on `Picker`.
- Allow students to experiment with rate limiting or cooperative scheduling with the WAL sync loop.

## Suggested Exercises
- Enforce overlap rules for L0→L1 promotions and add assertions that keep level boundaries disjoint.
- Simulate long-running compactions and teach how to surface backpressure to writers when too many immutable tables pile up.
