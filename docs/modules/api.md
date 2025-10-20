# Public API Module

## Purpose
Expose a clean `DB` interface that orchestrates lower-level modules without leaking internal details. The API is intentionally small so students can reason about transactional guarantees while focusing on storage mechanics.

## Key Types
- `DB` struct with `Open`, `Close`, `Get`, `Put`, `Delete`, `Flush`, and `Compact` methods.
- `Options` bundle tuning knobs (memtable size, WAL sync mode, filter bits per key).
- `Snapshot` handle that pins a sequence number for consistent reads.

## Integration Points
- Construction wires together WAL, memtable factory, compaction manager, and storage adapters.
- API methods enforce ordering: writes go WAL → memtable → scheduler, reads consult snapshot state.
- Shutdown coordinates WAL sync, flush completion, and manifest checkpointing to guarantee durability.

## Extension Hooks
- Add batched `Write` paths to explore group commit and pipelining.
- Introduce column-family style namespaces by multiplexing module instances.

## Suggested Exercises
- Implement a simple CLI using the API to demonstrate key-value workload patterns.
- Add context-aware operations (`GetContext`) to propagate cancellation and deadlines through the stack.
