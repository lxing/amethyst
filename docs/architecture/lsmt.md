# LSMT Architecture

## Goals & Scope
This repository hosts a teaching-oriented log-structured merge tree (LSMT). Each subsystem exposes small, testable seams so students can replace the reference code with richer data structures (e.g., swap a map for a skiplist) while observing system-level effects. The architecture favors clarity and determinism over maximum throughput.

## Write Path Overview
1. `DB.Put` validates keys, allocates a monotonic sequence number, and appends the mutation to the batched write-ahead log segment.
2. The append is chained to the active memtable implementation (`Memtable`). The reference variant uses `internal/memtable`'s map-backed type, but the interface hides the future skiplist upgrade.
3. When the memtable crosses configurable soft limits, the compaction manager schedules a flush task that converts the in-memory view into a new level-0 SSTable.

## Read Path Overview
1. Reads consult the mutable memtable first, falling back to any immutable memtables currently flushing.
2. A leveled SSTable iterator merges results from level-0 files (treated as overlapping) and progressively higher levels (disjoint key ranges).
3. Bloom filters attached to each table shortcut negative lookups before blocks are touched, keeping I/O tractable for classroom-scale workloads.

## Compaction Lifecycle
- **Scheduling**: `internal/compaction.Manager` monitors size-tiered and time-based triggers, enqueuing flushes or level promotions.
- **Execution**: A merge iterator spans the source memtable/SSTables, streams sorted key/value pairs into `TableBuilder`, and emits replacement tables.
- **Manifest Update**: Once files land on disk, the version set atomically swaps pointers and queues obsolete files for deletion.

## Recovery & Durability
During start-up, `internal/wal.Recover` replays committed batches into a fresh memtable. If the prior process crashed mid-flush, the manifest retains the last consistent snapshot so partial SSTable writes are ignored. Checkpoints run after each successful compaction, bounding WAL replay time and giving students a deterministic baseline for experiments.
