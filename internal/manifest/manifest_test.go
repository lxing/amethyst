package manifest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewManifest(t *testing.T) {
	m := NewManifest(7)
	v := m.Current()
	require.NotNil(t, v)
	require.Equal(t, 7, len(v.Levels))
	require.Equal(t, FileNo(0), v.CurrentWAL)
	require.Equal(t, FileNo(0), v.NextWALNumber)
	require.Equal(t, FileNo(0), v.NextSSTableNumber)
}

func TestSetWAL(t *testing.T) {
	m := NewManifest(7)

	// Set initial WAL
	m.SetWAL(1)
	v := m.Current()
	require.Equal(t, FileNo(1), v.CurrentWAL)
	require.Equal(t, FileNo(2), v.NextWALNumber)

	// Set another WAL
	m.SetWAL(2)
	v = m.Current()
	require.Equal(t, FileNo(2), v.CurrentWAL)
	require.Equal(t, FileNo(3), v.NextWALNumber)
}

func TestApplyCompactionEdit(t *testing.T) {
	m := NewManifest(7)

	// Add tables to L0 and L1
	edit1 := &CompactionEdit{
		AddSSTables: map[int]map[FileNo]struct{}{
			0: {1: {}, 2: {}, 3: {}, 4: {}},
			1: {10: {}, 11: {}},
		},
		DeleteSSTables: map[int]map[FileNo]struct{}{},
	}
	m.Apply(edit1)

	v := m.Current()
	require.Equal(t, 4, len(v.Levels[0]))
	require.Equal(t, 2, len(v.Levels[1]))
	require.Equal(t, FileNo(12), v.NextSSTableNumber)

	// Delete some tables from L0
	edit2 := &CompactionEdit{
		AddSSTables: map[int]map[FileNo]struct{}{},
		DeleteSSTables: map[int]map[FileNo]struct{}{
			0: {2: {}, 4: {}},
		},
	}
	m.Apply(edit2)

	v = m.Current()
	require.Equal(t, 2, len(v.Levels[0]))
	require.Contains(t, v.Levels[0], FileNo(1))
	require.Contains(t, v.Levels[0], FileNo(3))
	require.NotContains(t, v.Levels[0], FileNo(2))
	require.NotContains(t, v.Levels[0], FileNo(4))
}

func TestApplyCompactionEditSimulateCompaction(t *testing.T) {
	m := NewManifest(7)

	// Add initial L0 and L1 tables
	edit1 := &CompactionEdit{
		AddSSTables: map[int]map[FileNo]struct{}{
			0: {1: {}, 2: {}, 3: {}},
			1: {10: {}, 11: {}},
		},
		DeleteSSTables: map[int]map[FileNo]struct{}{},
	}
	m.Apply(edit1)

	// Simulate compaction: compact L0 tables 1,2 and L1 table 10 into new L1 tables 20,21
	edit2 := &CompactionEdit{
		DeleteSSTables: map[int]map[FileNo]struct{}{
			0: {1: {}, 2: {}},
			1: {10: {}},
		},
		AddSSTables: map[int]map[FileNo]struct{}{
			1: {20: {}, 21: {}},
		},
	}
	m.Apply(edit2)

	v := m.Current()

	// L0 should only have table 3
	require.Equal(t, 1, len(v.Levels[0]))
	require.Contains(t, v.Levels[0], FileNo(3))

	// L1 should have tables 11, 20, 21
	require.Equal(t, 3, len(v.Levels[1]))
	require.Contains(t, v.Levels[1], FileNo(11))
	require.Contains(t, v.Levels[1], FileNo(20))
	require.Contains(t, v.Levels[1], FileNo(21))

	// NextSSTableNumber should be updated
	require.Equal(t, FileNo(22), v.NextSSTableNumber)
}

func TestVersionIsolation(t *testing.T) {
	m := NewManifest(7)

	// Add initial tables
	edit1 := &CompactionEdit{
		AddSSTables: map[int]map[FileNo]struct{}{
			0: {1: {}, 2: {}},
		},
		DeleteSSTables: map[int]map[FileNo]struct{}{},
	}
	m.Apply(edit1)

	// Get snapshot
	v1 := m.Current()
	require.Equal(t, 2, len(v1.Levels[0]))

	// Apply another edit
	edit2 := &CompactionEdit{
		AddSSTables: map[int]map[FileNo]struct{}{
			0: {3: {}},
		},
		DeleteSSTables: map[int]map[FileNo]struct{}{},
	}
	m.Apply(edit2)

	// Get new snapshot
	v2 := m.Current()
	require.Equal(t, 3, len(v2.Levels[0]))

	// Old snapshot should be unchanged
	require.Equal(t, 2, len(v1.Levels[0]))
	require.Contains(t, v1.Levels[0], FileNo(1))
	require.Contains(t, v1.Levels[0], FileNo(2))
	require.NotContains(t, v1.Levels[0], FileNo(3))
}

func TestNextSSTableNumberPreservation(t *testing.T) {
	m := NewManifest(7)

	// Add tables with high file numbers
	edit := &CompactionEdit{
		AddSSTables: map[int]map[FileNo]struct{}{
			0: {100: {}, 200: {}},
		},
		DeleteSSTables: map[int]map[FileNo]struct{}{},
	}
	m.Apply(edit)

	v := m.Current()
	require.Equal(t, FileNo(201), v.NextSSTableNumber)

	// Delete tables but counter should remain
	edit2 := &CompactionEdit{
		AddSSTables: map[int]map[FileNo]struct{}{},
		DeleteSSTables: map[int]map[FileNo]struct{}{
			0: {100: {}, 200: {}},
		},
	}
	m.Apply(edit2)

	v = m.Current()
	require.Equal(t, 0, len(v.Levels[0]))
	require.Equal(t, FileNo(201), v.NextSSTableNumber)
}
