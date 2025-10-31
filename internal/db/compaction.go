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

// compactionManager handles compaction operations for the database.
// TODO: Add background worker goroutine for automatic compaction
// TODO: Add stopChan and triggerChan for coordination
type compactionManager struct {
	manifest *manifest.Manifest
	paths    *common.PathManager
	opts     *Options
}

// newCompactionManager creates a new compaction manager.
func newCompactionManager(m *manifest.Manifest, paths *common.PathManager, opts *Options) *compactionManager {
	return &compactionManager{
		manifest: m,
		paths:    paths,
		opts:     opts,
	}
}

// CompactL0 merges all L0 files with overlapping L1 files into new L1 files.
// This is a manual trigger - automatic background compaction will be added later.
//
// TODO: File lifecycle - when to delete old SSTable files?
// TODO: Reference counting on Versions to prevent deleting files in use
// TODO: Background worker for automatic compaction
// TODO: Metrics and monitoring
func (d *DB) CompactL0() error {
	return d.compactionMgr.compactL0()
}

func (cm *compactionManager) compactL0() error {
	v := cm.manifest.Current()

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
	iters, err := cm.openIterators(l0Files, 0)
	if err != nil {
		return fmt.Errorf("failed to open L0 iterators: %w", err)
	}
	l1Iters, err := cm.openIterators(l1Files, 1)
	if err != nil {
		return fmt.Errorf("failed to open L1 iterators: %w", err)
	}
	iters = append(iters, l1Iters...)

	// 4. Create merge iterator
	mergeIter := merge_iterator.NewMergeIterator(iters)

	// 5. Write new L1 files
	// Estimate size: each input file likely has ~MemtableFlushThreshold entries
	// This is a rough approximation for bloom filter sizing
	totalInputFiles := len(l0Files) + len(l1Files)
	estimatedSize := uint32(totalInputFiles) * uint32(cm.opts.MemtableFlushThreshold)
	newL1Files, err := cm.writeCompactedFiles(mergeIter, 1, estimatedSize)
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
	cm.manifest.Apply(edit)

	// 7. Persist manifest to disk
	if err := cm.manifest.Flush(); err != nil {
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
func (cm *compactionManager) openIterators(files []manifest.FileMetadata, level int) ([]common.EntryIterator, error) {
	iters := make([]common.EntryIterator, 0, len(files))
	for _, f := range files {
		table, err := cm.manifest.GetTable(f.FileNo, level)
		if err != nil {
			return nil, fmt.Errorf("failed to open table %d: %w", f.FileNo, err)
		}
		iters = append(iters, table.Iterator())
	}
	return iters, nil
}

// writeCompactedFiles writes merged entries to a single large SSTable file in the target level.
// The merge iterator handles deduplication (keeps newest by sequence number) and preserves tombstones.
//
// TODO: Split into multiple files based on size threshold
// TODO: Tombstone dropping (safe to drop if no older versions exist at deeper levels)
func (cm *compactionManager) writeCompactedFiles(iter common.EntryIterator, level int, sizeHint uint32) ([]manifest.FileMetadata, error) {

	v := cm.manifest.Current()
	fileNo := v.NextSSTableNumber
	path := cm.paths.SSTablePath(level, fileNo)

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer f.Close()

	// Write SSTable using size hint based on sum of input files
	result, err := sstable.WriteSSTable(f, iter, sizeHint, cm.opts.BloomFilterFPR)
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
