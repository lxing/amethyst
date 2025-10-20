# Write-Ahead Log Module

## Purpose
Guarantee durability by batching mutations onto disk before they reach the memtable. The WAL also drives crash recovery, ensuring that no acknowledged write is lost when the process restarts.

## Key Types
- `Writer` accepts batches (`Batch` struct) and appends framed records with checksums.
- `Segment` encapsulates an on-disk file, rotation thresholds, and sync policy.
- `Reader` iterates segments on recovery, verifying checksums and sequence numbers.

## Integration Points
- `DB.Put` funnels mutations through `Writer.AppendBatch` prior to touching the memtable.
- Compaction checkpoints invoke `Manifest` updates that allow pruning fully-replayed segments.
- Recovery rehydrates a fresh memtable by replaying batches in monotonically increasing sequence order.

## Extension Hooks
- Toggle between `SyncAlways`, `SyncEveryN`, or `OSBuffered` modes to demonstrate durability-latency tradeoffs.
- Allow pluggable record encodings so students can experiment with varint headers versus fixed-width frames.

## Suggested Exercises
- Implement checksum mismatch detection and page-aligned preallocation.
- Write property tests that inject truncated WAL records and ensure recovery stops cleanly at the last valid batch.
