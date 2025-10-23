package manifest

import "sync"

// FileNumber identifies a file (SSTable or WAL).
type FileNumber uint64

// Version represents an immutable snapshot of the LSM tree structure.
type Version struct {
	// Active WALs needed for recovery. Last entry is the current WAL being written.
	ActiveWALs []FileNumber

	// Levels[0] = L0 tables, Levels[1] = L1 tables, etc.
	Levels [][]FileNumber

	// Next file number to allocate for new WAL
	NextWALNumber FileNumber

	// Next file number to allocate for new SSTable
	NextSSTableNumber FileNumber
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
			Levels: make([][]FileNumber, numLevels),
		},
	}
}

// Current returns a snapshot of the current version for reading.
func (m *Manifest) Current() *Version {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// AddWAL adds a new WAL to the active set and increments NextWALNumber.
func (m *Manifest) AddWAL(num FileNumber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newVersion := m.deepCopy(m.current)
	newVersion.ActiveWALs = append(newVersion.ActiveWALs, num)
	newVersion.NextWALNumber = num + 1
	m.current = newVersion
}

// DeleteWAL removes a WAL from the active set.
func (m *Manifest) DeleteWAL(num FileNumber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newVersion := m.deepCopy(m.current)
	filtered := make([]FileNumber, 0, len(newVersion.ActiveWALs))
	for _, wal := range newVersion.ActiveWALs {
		if wal != num {
			filtered = append(filtered, wal)
		}
	}
	newVersion.ActiveWALs = filtered
	m.current = newVersion
}

// CompactionEdit describes an atomic change to the manifest.
type CompactionEdit struct {
	// SSTables to add/remove per level
	AddSSTables    map[int][]FileNumber
	DeleteSSTables map[int][]FileNumber
}

// Apply atomically applies a compaction edit, creating a new version.
func (m *Manifest) Apply(edit *CompactionEdit) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy current version
	newVersion := m.deepCopy(m.current)

	// Apply SSTable changes
	for level, files := range edit.DeleteSSTables {
		deleteSet := make(map[FileNumber]bool)
		for _, f := range files {
			deleteSet[f] = true
		}
		filtered := make([]FileNumber, 0, len(newVersion.Levels[level]))
		for _, f := range newVersion.Levels[level] {
			if !deleteSet[f] {
				filtered = append(filtered, f)
			}
		}
		newVersion.Levels[level] = filtered
	}

	var maxSSTable FileNumber
	for level, files := range edit.AddSSTables {
		newVersion.Levels[level] = append(newVersion.Levels[level], files...)
		for _, f := range files {
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
		ActiveWALs:        make([]FileNumber, len(v.ActiveWALs)),
		Levels:            make([][]FileNumber, len(v.Levels)),
		NextWALNumber:     v.NextWALNumber,
		NextSSTableNumber: v.NextSSTableNumber,
	}
	copy(newVersion.ActiveWALs, v.ActiveWALs)
	for i := range v.Levels {
		newVersion.Levels[i] = make([]FileNumber, len(v.Levels[i]))
		copy(newVersion.Levels[i], v.Levels[i])
	}
	return newVersion
}
