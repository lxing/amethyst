package manifest

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"amethyst/internal/block_cache"
	"amethyst/internal/common"
	"amethyst/internal/sstable"
)

// FileMetadata tracks metadata for a single SSTable file.
type FileMetadata struct {
	FileNo      common.FileNo
	SmallestKey []byte
	LargestKey  []byte
}

// Version represents an immutable snapshot of the LSM tree structure.
type Version struct {
	// Current WAL being written
	CurrentWAL common.FileNo

	// Levels[0] = L0 tables, Levels[1] = L1 tables, etc.
	Levels [][]FileMetadata

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
			Levels: make([][]FileMetadata, numLevels),
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

// LoadVersion replaces the current version with the provided one (used during recovery).
func (m *Manifest) LoadVersion(v *Version) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = v
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
	AddSSTables    map[int][]FileMetadata
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
		filtered := make([]FileMetadata, 0, len(newVersion.Levels[level]))
		for _, fm := range newVersion.Levels[level] {
			if _, deleted := deleteSet[fm.FileNo]; !deleted {
				filtered = append(filtered, fm)
			}
		}
		newVersion.Levels[level] = filtered
	}

	// Apply SSTable additions
	var maxSSTable common.FileNo
	for level, addList := range edit.AddSSTables {
		for _, fm := range addList {
			newVersion.Levels[level] = append(newVersion.Levels[level], fm)
			if fm.FileNo > maxSSTable {
				maxSSTable = fm.FileNo
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
		Levels:            make([][]FileMetadata, len(v.Levels)),
		NextWALNumber:     v.NextWALNumber,
		NextSSTableNumber: v.NextSSTableNumber,
	}
	for i := range v.Levels {
		newVersion.Levels[i] = make([]FileMetadata, len(v.Levels[i]))
		copy(newVersion.Levels[i], v.Levels[i])
	}
	return newVersion
}

// GetTable returns the SSTable for the given file number, opening it if not cached.
func (m *Manifest) GetTable(fileNo common.FileNo, level int) (sstable.SSTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already open
	if table, ok := m.tableCache[fileNo]; ok {
		return table, nil
	}

	// Open the SSTable file
	path := common.SSTablePath(level, fileNo)
	table, err := sstable.OpenSSTable(path, fileNo, m.blockCache)
	if err != nil {
		return nil, err
	}

	// Cache it
	m.tableCache[fileNo] = table
	return table, nil
}

// WriteManifest serializes a Version to JSON.
func WriteManifest(w io.Writer, v *Version) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(v)
}

// ReadManifest deserializes a Version from JSON.
func ReadManifest(r io.Reader) (*Version, error) {
	var v Version
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Flush atomically writes the current version to disk (MANIFEST file).
func (m *Manifest) Flush() error {
	m.mu.RLock()
	v := m.current
	m.mu.RUnlock()

	// Atomic write: write to temp file, then rename
	tmpPath := "MANIFEST.tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", tmpPath, err)
	}

	if err := WriteManifest(f, v); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Atomic rename
	return os.Rename(tmpPath, "MANIFEST")
}
