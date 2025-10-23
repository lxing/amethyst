# Manifest and Recovery Protocol

## Overview

The manifest is the source of truth for the LSM tree structure. It tracks active WAL files and SSTable files organized by level. The manifest uses versioning for snapshot isolation, allowing readers to safely access a consistent view while the structure changes.

## Manifest Structure

```go
type Version struct {
    CurrentWAL        FileNumber     // Current WAL being written
    Levels            [][]FileNumber // SSTables per level
    NextWALNumber     FileNumber     // Next WAL to allocate
    NextSSTableNumber FileNumber     // Next SSTable to allocate
}
```

The manifest maintains an immutable `Version` representing the current state. Mutations create a new version via deep copy, then atomically swap the current pointer.

## WAL Rotation Protocol (Synchronous Flush)

When the active WAL reaches size threshold:

1. **Allocate**: Get `NextWALNumber` from manifest
2. **Create file**: Write new WAL file to disk
3. **Sync**: Ensure file is durable
4. **Flush**: Synchronously flush memtable to SSTable (blocks writers)
5. **Update manifest**: Call `SetWAL(newWALNumber)` - this is the commit point
6. **Swap**: Atomically start writing to new WAL and new memtable
7. **Clean up**: Delete old WAL file from disk

## Compaction Protocol

When compacting SSTables from level N to N+1:

1. **Select inputs**: Choose SSTables to compact based on compaction policy
2. **Merge**: Read inputs and write new merged SSTables
3. **Sync**: Ensure new SSTables are durable
4. **Update manifest**: Call `Apply(CompactionEdit)` - this is the commit point
   - `DeleteSSTables`: Input files from both levels
   - `AddSSTables`: New output files at level N+1
5. **Clean up**: Delete old input files from disk

## Crash Recovery Invariant

**Files are written first, then the manifest is updated, then old files are deleted.**

The manifest update is the atomic commit point. This ensures:

- **Before commit**: New files exist but are not yet visible. Crash leaves orphaned files that can be vacuumed.
- **After commit**: New files are visible in manifest. Old files may still exist but are not referenced.
- **On recovery**: Load manifest to reconstruct LSM tree. Any files not in manifest are orphans and can be deleted.

## Snapshot Isolation

The `Current()` method returns an immutable `*Version`. Readers hold this pointer for the duration of their operation. Because versions are immutable and Go garbage collects unreferenced versions, readers never see partial compactions.

Writers hold the write lock only while creating a new version and swapping the pointer. This allows concurrent reads during most of the compaction work.

## File Orphaning

Orphaned files can occur if:
- Crash happens after writing new files but before manifest update
- Crash happens after manifest update but before deleting old files

Recovery handles this by treating the manifest as truth: any files on disk not referenced in the manifest are orphans and can be safely deleted.
