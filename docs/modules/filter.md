# Filter Module

## Purpose
Reduce disk reads for negative lookups by attaching probabilistic filters and compact bitmaps to each SSTable. These structures keep the workload responsive even when datasets outgrow memory.

## Key Types
- `Builder` ingests keys during table construction and emits serialized bloom filter and bitmap blocks.
- `Filter` exposes `MayContain(key []byte)` for reads and compaction planning.
- `Hasher` defines the hash strategy; default implementation uses double hashing seeded by table ID.

## Integration Points
- `TableBuilder` invokes the filter builder per data block and persists outputs near the table footer.
- `TableReader` loads filters lazily, storing them in a cache aligned with block cache policy.
- Compaction planning consults per-table bitmaps to estimate overlap before scheduling merges.

## Extension Hooks
- Allow students to plug in counting bloom filters or tiered bitmap layouts for deletions.
- Surface configuration knobs (bits per key, hash functions) through `DBOptions` for comparative experiments.

## Suggested Exercises
- Add diagnostic tooling that reports false positive rates using synthetic workloads.
- Implement SIMD-friendly hashing or GPU offload to demonstrate advanced optimization paths.
