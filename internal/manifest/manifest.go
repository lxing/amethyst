package manifest

import (
	"fmt"
	"sync"

	"amethyst/internal/block_cache"
	"amethyst/internal/common"
	"amethyst/internal/sstable"
)

// Version represents an immutable snapshot of the LSM tree structure.
type Version struct {
	// Current WAL being written
	CurrentWAL common.FileNo

	// Levels[0] = L0 tables, Levels[1] = L1 tables, etc.
	Levels [][]common.FileNo

	// Next file number to allocate for new WAL
	NextWALNumber common.FileNo

	// Next file number to allocate for new SSTable
	NextSSTableNumber common.FileNo
}

// Manifest tracks the structural state of the LSM tree with snapshot isolation.
//
// TODO: Version and SSTable lifecycle management
// Currently, old Versions are not explicitly cleaned up and SSTable handles stay open
// indefinitely in tableCache. This works for now but will eventually cause issues:
//
// 1. Memory leaks: Old Version objects accumulate (Go GC handles this, but still wasteful)
// 2. File descriptor leaks: SSTables removed by compaction stay open forever
// 3. Disk space leaks: Can't delete obsolete SST files while handles are open
//
// Solutions to implement later:
// - Manual reference counting on Versions (like RocksDB's Version::Ref/Unref)
// - Track which SSTables are referenced by live Versions
// - Close and delete SSTable files when no Version references them
// - Add DB.Close() to explicitly release all resources
type Manifest struct {
	mu sync.RWMutex

	// Current version (latest state)
	current *Version

	// Table cache: shared pool of open SSTable handles
	tableCache map[common.FileNo]sstable.SSTable

	// Block cache: shared across all SSTables
	blockCache block_cache.BlockCache
}

// NewManifest creates a new manifest with the given number of levels.
func NewManifest(numLevels int) *Manifest {
	return &Manifest{
		current: &Version{
			Levels: make([][]common.FileNo, numLevels),
		},
		tableCache: make(map[common.FileNo]sstable.SSTable),
		blockCache: block_cache.NewBlockCache(),
	}
}

// Current returns a snapshot of the current version for reading.
func (m *Manifest) Current() *Version {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// SetWAL sets the current WAL and increments NextWALNumber.
func (m *Manifest) SetWAL(num common.FileNo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newVersion := m.deepCopy(m.current)
	newVersion.CurrentWAL = num
	newVersion.NextWALNumber = num + 1
	m.current = newVersion
}

// CompactionEdit describes an atomic change to the manifest.
type CompactionEdit struct {
	// SSTables to add/remove per level
	AddSSTables    map[int]map[common.FileNo]struct{}
	DeleteSSTables map[int]map[common.FileNo]struct{}
}

// Apply atomically applies a compaction edit, creating a new version.
func (m *Manifest) Apply(edit *CompactionEdit) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy current version
	newVersion := m.deepCopy(m.current)

	// Apply SSTable deletions
	for level, deleteSet := range edit.DeleteSSTables {
		filtered := make([]common.FileNo, 0, len(newVersion.Levels[level]))
		for _, f := range newVersion.Levels[level] {
			if _, deleted := deleteSet[f]; !deleted {
				filtered = append(filtered, f)
			}
		}
		newVersion.Levels[level] = filtered
	}

	// Apply SSTable additions
	var maxSSTable common.FileNo
	for level, addSet := range edit.AddSSTables {
		for f := range addSet {
			newVersion.Levels[level] = append(newVersion.Levels[level], f)
			if f > maxSSTable {
				maxSSTable = f
			}
		}
	}
	if maxSSTable >= newVersion.NextSSTableNumber {
		newVersion.NextSSTableNumber = maxSSTable + 1
	}

	m.current = newVersion
}

func (m *Manifest) deepCopy(v *Version) *Version {
	newVersion := &Version{
		CurrentWAL:        v.CurrentWAL,
		Levels:            make([][]common.FileNo, len(v.Levels)),
		NextWALNumber:     v.NextWALNumber,
		NextSSTableNumber: v.NextSSTableNumber,
	}
	for i := range v.Levels {
		newVersion.Levels[i] = make([]common.FileNo, len(v.Levels[i]))
		copy(newVersion.Levels[i], v.Levels[i])
	}
	return newVersion
}

// GetTable returns the SSTable for the given file number, opening it if not cached.
// The level parameter is currently unused but will be needed when we support multiple levels.
func (m *Manifest) GetTable(fileNo common.FileNo, level int) (sstable.SSTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already open
	if table, ok := m.tableCache[fileNo]; ok {
		return table, nil
	}

	// Open the SSTable file
	// TODO: Support multiple levels, for now hardcoded to L0
	path := fmt.Sprintf("sstable/0/%d.sst", fileNo)
	table, err := sstable.OpenSSTable(path, fileNo, m.blockCache)
	if err != nil {
		return nil, err
	}

	// Cache it
	m.tableCache[fileNo] = table
	return table, nil
}
