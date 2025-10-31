package db_test

import (
	"testing"

	"amethyst/internal/common"
	"amethyst/internal/db"
	"github.com/stretchr/testify/require"
)

func TestCompactL0(t *testing.T) {
	testDir := t.TempDir()

	// Create DB with low memtable flush threshold to create multiple L0 files
	d, err := db.Open(db.WithDBPath(testDir), db.WithMemtableFlushThreshold(3))
	require.NoError(t, err)

	// Create 4 L0 SSTables with known entries
	// Each batch will flush to a separate SSTable
	// Threshold is 3, so 4th write triggers flush

	// SSTable 0: keys a, b, c (x1 will trigger flush)
	require.NoError(t, d.Put([]byte("a"), []byte("a0")))
	require.NoError(t, d.Put([]byte("b"), []byte("b0")))
	require.NoError(t, d.Put([]byte("c"), []byte("c0")))
	require.NoError(t, d.Put([]byte("x1"), []byte("x1"))) // Trigger flush

	// SSTable 1: keys b (newer), d, e (x2 will trigger flush)
	require.NoError(t, d.Put([]byte("b"), []byte("b1-new"))) // Overwrites b from SSTable 0
	require.NoError(t, d.Put([]byte("d"), []byte("d1")))
	require.NoError(t, d.Put([]byte("e"), []byte("e1")))
	require.NoError(t, d.Put([]byte("x2"), []byte("x2"))) // Trigger flush

	// SSTable 2: keys f, g, h (delete) (x3 will trigger flush)
	require.NoError(t, d.Put([]byte("f"), []byte("f2")))
	require.NoError(t, d.Put([]byte("g"), []byte("g2")))
	require.NoError(t, d.Delete([]byte("h"))) // Tombstone
	require.NoError(t, d.Put([]byte("x3"), []byte("x3"))) // Trigger flush

	// SSTable 3: keys a (newer), i, j (x4 will trigger flush)
	require.NoError(t, d.Put([]byte("a"), []byte("a3-new"))) // Overwrites a from SSTable 0
	require.NoError(t, d.Put([]byte("i"), []byte("i3")))
	require.NoError(t, d.Put([]byte("j"), []byte("j3")))
	require.NoError(t, d.Put([]byte("x4"), []byte("x4"))) // Trigger flush

	// Check how many L0 SSTables we actually created
	manifest := d.Manifest()
	version := manifest.Current()
	actualCount := len(version.Levels[0])
	require.True(t, actualCount >= 4, "Should have at least 4 L0 SSTables, got %d", actualCount)
	require.Len(t, version.Levels[1], 0, "Should have no L1 SSTables initially")

	// Run compaction
	err = d.CompactL0()
	require.NoError(t, err)

	// After compaction, should have:
	// - 0 L0 files
	// - 1 L1 file
	version = manifest.Current()
	require.Len(t, version.Levels[0], 0, "Should have no L0 SSTables after compaction")
	require.Len(t, version.Levels[1], 1, "Should have 1 L1 SSTable after compaction")

	// Verify all keys are readable with correct values
	// Expected results after merge (highest seq wins for duplicates):
	expected := map[string]string{
		"a":  "a3-new", // From SSTable 3 (newest)
		"b":  "b1-new", // From SSTable 1 (newer than SSTable 0)
		"c":  "c0",     // From SSTable 0
		"d":  "d1",     // From SSTable 1
		"e":  "e1",     // From SSTable 1
		"f":  "f2",     // From SSTable 2
		"g":  "g2",     // From SSTable 2
		"i":  "i3",     // From SSTable 3
		"j":  "j3",     // From SSTable 3
		"x1": "x1",     // From SSTable 0
		"x2": "x2",     // From SSTable 1
		"x3": "x3",     // From SSTable 2
		"x4": "x4",     // From SSTable 3
	}

	for key, expectedValue := range expected {
		value, err := d.Get([]byte(key))
		require.NoError(t, err, "key %q should exist", key)
		require.Equal(t, expectedValue, string(value), "key %q should have correct value", key)
	}

	// Verify tombstone is preserved - key "h" should be deleted
	_, err = d.Get([]byte("h"))
	require.ErrorIs(t, err, db.ErrNotFound, "deleted key 'h' should not be found")

	// Verify all keys are now in L1 by checking we can read them from L1
	// (memtable is empty, L0 is empty, so reads must come from L1)
	common.Logf("\n=== Verifying keys are in L1 ===\n")
	for key := range expected {
		value, err := d.Get([]byte(key))
		require.NoError(t, err, "key %q should be readable from L1", key)
		require.Equal(t, expected[key], string(value), "key %q from L1 should have correct value", key)
	}
}

func TestCompactL0_WithOverlappingL1(t *testing.T) {
	testDir := t.TempDir()

	// Create DB with low threshold
	d, err := db.Open(db.WithDBPath(testDir), db.WithMemtableFlushThreshold(3))
	require.NoError(t, err)

	// Create 2 L0 SSTables
	require.NoError(t, d.Put([]byte("a"), []byte("a0")))
	require.NoError(t, d.Put([]byte("b"), []byte("b0")))
	require.NoError(t, d.Put([]byte("c"), []byte("c0")))
	require.NoError(t, d.Put([]byte("trigger1"), []byte("t1")))

	require.NoError(t, d.Put([]byte("d"), []byte("d1")))
	require.NoError(t, d.Put([]byte("e"), []byte("e1")))
	require.NoError(t, d.Put([]byte("f"), []byte("f1")))
	require.NoError(t, d.Put([]byte("trigger2"), []byte("t2")))

	// Compact to create L1 file
	require.NoError(t, d.CompactL0())

	// Verify we have 1 L1 file
	version := d.Manifest().Current()
	require.Len(t, version.Levels[1], 1, "Should have 1 L1 file")
	require.Len(t, version.Levels[0], 0, "Should have 0 L0 files")

	// Now write new L0 files that overlap with existing L1
	// Overwrite some existing keys
	require.NoError(t, d.Put([]byte("b"), []byte("b-newer"))) // Overlaps with L1
	require.NoError(t, d.Put([]byte("c"), []byte("c-newer"))) // Overlaps with L1
	require.NoError(t, d.Put([]byte("z"), []byte("z-new")))   // New key
	require.NoError(t, d.Put([]byte("trigger3"), []byte("t3")))

	// Should have 1 new L0 file
	version = d.Manifest().Current()
	require.Len(t, version.Levels[0], 1, "Should have 1 new L0 file")
	require.Len(t, version.Levels[1], 1, "Should still have 1 L1 file")

	// Compact again - should merge new L0 with overlapping L1
	require.NoError(t, d.CompactL0())

	// Should have 0 L0 files and 1 L1 file (merged)
	version = d.Manifest().Current()
	require.Len(t, version.Levels[0], 0, "Should have 0 L0 files after second compaction")
	require.Len(t, version.Levels[1], 1, "Should have 1 L1 file after second compaction")

	// Verify newer values won
	value, err := d.Get([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, "b-newer", string(value))

	value, err = d.Get([]byte("c"))
	require.NoError(t, err)
	require.Equal(t, "c-newer", string(value))

	// Verify old values still exist
	value, err = d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, "a0", string(value))

	// Verify new key exists
	value, err = d.Get([]byte("z"))
	require.NoError(t, err)
	require.Equal(t, "z-new", string(value))
}

func TestCompactL0_Empty(t *testing.T) {
	testDir := t.TempDir()
	d, err := db.Open(db.WithDBPath(testDir))
	require.NoError(t, err)

	// Compacting empty L0 should be a no-op
	err = d.CompactL0()
	require.NoError(t, err)

	version := d.Manifest().Current()
	require.Len(t, version.Levels[0], 0)
	require.Len(t, version.Levels[1], 0)
}
