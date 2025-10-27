# Memtable Module

## Purpose
Provide the in-memory write buffer that absorbs mutations before they are persisted to disk. The reference build offers a simple map-backed implementation that students will later replace with a skiplist to gain sorted iteration semantics.

## Key Types
- `Memtable` interface with `Put`, `Get`, `Delete`, `ApproximateSize`, and `NewIterator`.
- `mapMemtableImpl` reference class living in `internal/memtable/map.go` for baseline behavior.
- `Iterator` abstraction shared with SSTables, exposing `First`, `Seek`, `Key`, and `Value`.

## Integration Points
- Appends are durably recorded in the WAL before hitting the memtable.
- Flush events hand the iterator to the SSTable builder, so iteration order must be deterministic.
- Snapshot reads access the memtable alongside immutable flush candidates; concurrency control should stay simple (e.g., `RWMutex`).

## Extension Hooks
- Introduce `SkiplistMemtable` by implementing your own constructor (mirroring `NewMapMemtable`) to choose between map and skiplist.
- Experiment with reference counting or epoch-based reclamation to remove per-operation locks once concurrency lessons begin.

## Suggested Exercises
- Add metrics for mutation counts and flush latency.
- Implement a limited-size allocator to simulate memory pressure and trigger flushes deterministically.
