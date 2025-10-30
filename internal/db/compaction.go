package db

import (
	"bytes"
	"fmt"
	"os"

	"amethyst/internal/common"
	"amethyst/internal/manifest"
	"amethyst/internal/merge_iterator"
	"amethyst/internal/sstable"
)

// CompactL0 merges all L0 files with overlapping L1 files into new L1 files.
// This is a manual trigger - automatic background compaction will be added later.
//
// TODO: File lifecycle - when to delete old SSTable files?
// TODO: Reference counting on Versions to prevent deleting files in use
// TODO: Background worker for automatic compaction
// TODO: Metrics and monitoring
func (d *DB) CompactL0() error {
	m := d.Manifest()
	v := m.Current()

	// 1. Get all L0 files
	l0Files := v.Levels[0]
	if len(l0Files) == 0 {
		return nil // Nothing to compact
	}

	common.Logf("compaction: starting L0 → L1 (%d L0 files)\n", len(l0Files))

	// 2. Find overlapping L1 files
	l1Files := findOverlappingL1Files(v.Levels[1], l0Files)
	common.Logf("compaction: found %d overlapping L1 files\n", len(l1Files))

	// 3. Open iterators for all files to be merged
	iters, err := openIterators(m, l0Files, 0)
	if err != nil {
		return fmt.Errorf("failed to open L0 iterators: %w", err)
	}
	l1Iters, err := openIterators(m, l1Files, 1)
	if err != nil {
		return fmt.Errorf("failed to open L1 iterators: %w", err)
	}
	iters = append(iters, l1Iters...)

	// 4. Create merge iterator
	// TODO: MergeIterator is currently stubbed - needs k-way merge implementation
	mergeIter := merge_iterator.NewMergeIterator(iters)

	// 5. Write new L1 files
	newL1Files, err := d.writeCompactedFiles(mergeIter, 1)
	if err != nil {
		// TODO: Better error handling - currently orphans files
		return fmt.Errorf("failed to write compacted files: %w", err)
	}

	// 6. Apply CompactionEdit atomically
	edit := &manifest.CompactionEdit{
		DeleteSSTables: map[int]map[common.FileNo]struct{}{
			0: fileMetadataToSet(l0Files),
			1: fileMetadataToSet(l1Files),
		},
		AddSSTables: map[int][]manifest.FileMetadata{
			1: newL1Files,
		},
	}
	m.Apply(edit)

	// 7. Persist manifest to disk
	if err := m.Flush(); err != nil {
		return fmt.Errorf("failed to flush manifest: %w", err)
	}

	// TODO: Delete old SSTable files (currently orphaned)
	// Need to wait until no readers hold references to old Version

	common.Logf("compaction: completed, wrote %d new L1 files\n", len(newL1Files))
	return nil
}

// findOverlappingL1Files returns L1 files whose key range overlaps with any L0 file.
// Since L0 files can overlap each other, we need the union of all L0 key ranges.
func findOverlappingL1Files(l1Files []manifest.FileMetadata, l0Files []manifest.FileMetadata) []manifest.FileMetadata {
	if len(l1Files) == 0 || len(l0Files) == 0 {
		return nil
	}

	// Find min and max keys across all L0 files
	var minKey, maxKey []byte
	for _, f := range l0Files {
		if minKey == nil || bytes.Compare(f.SmallestKey, minKey) < 0 {
			minKey = f.SmallestKey
		}
		if maxKey == nil || bytes.Compare(f.LargestKey, maxKey) > 0 {
			maxKey = f.LargestKey
		}
	}

	// Find L1 files that overlap with [minKey, maxKey]
	var overlapping []manifest.FileMetadata
	for _, f := range l1Files {
		// File overlaps if: f.SmallestKey <= maxKey AND f.LargestKey >= minKey
		if bytes.Compare(f.SmallestKey, maxKey) <= 0 && bytes.Compare(f.LargestKey, minKey) >= 0 {
			overlapping = append(overlapping, f)
		}
	}

	return overlapping
}

// openIterators opens SSTable iterators for the given files.
func openIterators(m *manifest.Manifest, files []manifest.FileMetadata, level int) ([]common.EntryIterator, error) {
	iters := make([]common.EntryIterator, 0, len(files))
	for _, f := range files {
		table, err := m.GetTable(f.FileNo, level)
		if err != nil {
			return nil, fmt.Errorf("failed to open table %d: %w", f.FileNo, err)
		}
		iters = append(iters, table.Iterator())
	}
	return iters, nil
}

// writeCompactedFiles writes merged entries to new SSTable files in the target level.
// For now, writes all entries to a single file (TODO: split into multiple files).
//
// TODO: Split into multiple files based on size threshold
// TODO: Proper file size target (separate config option)
// TODO: Tombstone dropping (safe to drop if no older versions exist)
// TODO: Entry deduplication (keep newest by sequence number)
func (d *DB) writeCompactedFiles(iter common.EntryIterator, level int) ([]manifest.FileMetadata, error) {
	// TODO: MergeIterator is stubbed, so this won't produce any output yet
	// For now, just write everything to a single SSTable file

	v := d.Manifest().Current()
	fileNo := v.NextSSTableNumber
	path := d.paths.SSTablePath(level, fileNo)

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer f.Close()

	// Write SSTable using existing WriteSSTable function
	// Use MemtableFlushThreshold as size hint for now
	result, err := sstable.WriteSSTable(f, iter, uint32(d.Opts.MemtableFlushThreshold), d.Opts.BloomFilterFPR)
	if err != nil {
		return nil, fmt.Errorf("failed to write SSTable: %w", err)
	}

	common.Logf("  wrote compacted file %d.sst (%d entries)\n", fileNo, result.EntryCount)

	return []manifest.FileMetadata{
		{
			FileNo:      fileNo,
			SmallestKey: result.SmallestKey,
			LargestKey:  result.LargestKey,
		},
	}, nil
}

// fileMetadataToSet converts a slice of FileMetadata to a set (map) of file numbers.
func fileMetadataToSet(files []manifest.FileMetadata) map[common.FileNo]struct{} {
	set := make(map[common.FileNo]struct{}, len(files))
	for _, f := range files {
		set[f.FileNo] = struct{}{}
	}
	return set
}
