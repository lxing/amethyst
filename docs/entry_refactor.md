# Entry Refactor Memo

Context: students benefit when concepts are isolated like the `internal/dsa` lessons. Entry serde currently sits in `internal/common/types.go`, mixed with generic helpers.

Proposed structure:
- `internal/entry/model.go` (package `entry`)
  - `Type`, `Entry`, `CompareEntries`
  - dedicated focus on entry semantics
- `internal/entry/serde/serde.go` (package `entryserde`)
  - `Encode` / `Decode` using existing helpers in `internal/common`
  - optional scratch buffer parameter to reduce allocations; keep a copy-based helper for existing callers
- `internal/iter/entry.go` (package `iter`)
  - iterator interface and test helpers that operate on `entry.Entry`

Open questions / follow-ups:
- Evaluate moving numeric primitives from `internal/common` or wrapping them for tighter coupling
- Decide default ergonomics for the scratch-buffer API vs. simpler copies
- Sequence migration across WAL, SSTable, memtable without disruptive diffs

Notes:
- Keep abstractions minimal; expose only what the current lessons need
- Ensure tests document buffer-reuse patterns if we surface them

---

## Ring Buffer & Pooling Notes

### Batched Write Path
- Replace per-batch slice allocations in `internal/dsa/batcher` with a fixed-capacity ring that stores `(item, resultCh)` pairs.
- Ring doubles as a deterministic pool: capacity is `maxBatchSize`; oldest slots are reused when full.
- Provides predictable batching pedagogy and avoids extra GC churn.

### WAL / Serde Layer
- Introduce an entry pool exposed alongside the serde API: `Get()` returns a reusable `*entry.Entry`; `Put()` returns it to the ring.
- Decoder can accept a `*Entry` to fill in place, eliminating fresh allocations when calling `ReadEntry`.
- Keys/values should reuse backing buffers via scratch slices to minimize copying.

### Merge Iterator Path
- Ring buffer less helpful because consumption order is heap-driven; prefer a simple pool.
- Preallocate one entry per child iterator (or source) and recycle via `pool.Put` after emitting the merged entry.
- Shards or per-iterator pools keep things lock-free and deterministic for students.

### Custom Pool Design
- Implemented as a small ring (`[]*entry.Entry` + head/tail/size) guarded by optional mutex if shared across goroutines.
- `Get` pops from head or allocates when empty; `Put` writes at tail and overwrites oldest when full.
- Reset slices on `Put` (set length to zero) to avoid stale data exposure.
- Optional metrics: hit/miss/drop counters and debugging hooks to illustrate reuse benefits.
