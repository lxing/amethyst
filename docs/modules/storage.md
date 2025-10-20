# Storage & Metadata Module

## Purpose
Abstract filesystem interactions and track versioned metadata so the database can atomically install new tables. The separation keeps disk specifics outside of core components and provides safe experimentation surfaces.

## Key Types
- `FS` interface offering `CreateFile`, `OpenFile`, `Rename`, and `Remove` with deterministic error contracts.
- `FileRegistry` hands out monotonically increasing file numbers for WAL segments and SSTables.
- `Manifest` persists level descriptors, live file sets, and checkpoint epochs.

## Integration Points
- WAL segments and table builders acquire file descriptors via `FS` rather than using `os` directly.
- Compaction tasks write provisional manifests and atomically swap them in once new tables succeed.
- Recovery reads the manifest to decide which tables to reopen and which WAL segments need replay.

## Extension Hooks
- Implement an in-memory `FS` for unit testing or a cloud-backed variant to explore remote storage tradeoffs.
- Add manifest compaction to prune old versions and demonstrate consistent checkpointing.

## Suggested Exercises
- Introduce crash-only tests that power-cycle the process and ensure obsolete files vanish after restart.
- Teach students to detect leaked file descriptors by instrumenting the `FS` wrapper with counters.
