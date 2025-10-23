package manifest

import "sync"

// FileNo identifies a file (SSTable or WAL).
type FileNo uint64

// Version represents an immutable snapshot of the LSM tree structure.
type Version struct {
	// Current WAL being written
	CurrentWAL FileNo

	// Levels[0] = L0 tables, Levels[1] = L1 tables, etc.
	Levels [][]FileNo

	// Next file number to allocate for new WAL
	NextWALNumber FileNo

	// Next file number to allocate for new SSTable
	NextSSTableNumber FileNo
}

// Manifest tracks the structural state of the LSM tree with snapshot isolation.
type Manifest struct {
	mu sync.RWMutex

	// Current version (latest state)
	current *Version
}

// NewManifest creates a new manifest with the given number of levels.
func NewManifest(numLevels int) *Manifest {
	return &Manifest{
		current: &Version{
			Levels: make([][]FileNo, numLevels),
		},
	}
}

// Current returns a snapshot of the current version for reading.
func (m *Manifest) Current() *Version {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// SetWAL sets the current WAL and increments NextWALNumber.
func (m *Manifest) SetWAL(num FileNo) {
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
	AddSSTables    map[int]map[FileNo]struct{}
	DeleteSSTables map[int]map[FileNo]struct{}
}

// Apply atomically applies a compaction edit, creating a new version.
func (m *Manifest) Apply(edit *CompactionEdit) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy current version
	newVersion := m.deepCopy(m.current)

	// Apply SSTable deletions
	for level, deleteSet := range edit.DeleteSSTables {
		filtered := make([]FileNo, 0, len(newVersion.Levels[level]))
		for _, f := range newVersion.Levels[level] {
			if _, deleted := deleteSet[f]; !deleted {
				filtered = append(filtered, f)
			}
		}
		newVersion.Levels[level] = filtered
	}

	// Apply SSTable additions
	var maxSSTable FileNo
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
		Levels:            make([][]FileNo, len(v.Levels)),
		NextWALNumber:     v.NextWALNumber,
		NextSSTableNumber: v.NextSSTableNumber,
	}
	for i := range v.Levels {
		newVersion.Levels[i] = make([]FileNo, len(v.Levels[i]))
		copy(newVersion.Levels[i], v.Levels[i])
	}
	return newVersion
}
