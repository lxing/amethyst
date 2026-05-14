# Compaction Roadmap

## Current Behavior
- `CompactL0` merges every L0 SSTable plus any overlapping L1 tables into a single new L1 file.
- Inputs are streamed through `merge_iterator.NewMergeIterator`, which keeps the newest entry per key and preserves tombstones.
- The manifest applies a `CompactionEdit` that removes the source files and adds the new file, then flushes to disk.
- Old SSTable files are left on disk; there is no lifecycle management to delete superseded data.
- Only L0→L1 compactions exist; higher levels never compact.

## Gaps & Risks
- **File reclamation**: Without version pinning or reference counting, we cannot safely delete compacted files—risking unbounded disk growth.
- **Output sizing**: A single large L1 file is emitted per run, ignoring target level sizes and leading to skewed fan-out.
- **Tombstone retention**: Deletes are never dropped, so stale tombstones accumulate even when older values are gone.
- **Scheduling**: Compaction is manual; no background worker monitors L0 overlap or schedule debt.
- **Error handling**: Failure halfway through `writeCompactedFiles` can orphan files because cleanup is not coordinated.

## Next Steps
1. Introduce version pinning (reference counts or epoch guards) so readers can safely access old versions while compaction proceeds, enabling file deletion once unused.
2. Teach the writer to split output into multiple files based on a size target per level and update manifest edits accordingly.
3. Add a background compaction loop that triggers L0→L1 work when overlap or file count crosses a threshold, later extending to deeper levels.
4. Implement tombstone GC policies tied to lower-level key ranges, dropping deletes when no older versions remain below the output level.
5. Harden the workflow with per-step error handling and rollback/cleanup to avoid orphaned SSTables if a compaction run aborts.
