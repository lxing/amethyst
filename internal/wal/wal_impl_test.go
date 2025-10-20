package wal_test

import (
	"context"
	"path/filepath"
	"testing"

	"amethyst/internal/wal"
	"github.com/stretchr/testify/require"
)

func TestFileLogAppendAndIterate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.OpenWAL(path)
	require.NoError(t, err)
	defer log.Close()

	batch := []wal.Entry{
		{Type: wal.EntryTypePut, Seq: 1, Key: []byte("a"), Value: []byte("A")},
		{Type: wal.EntryTypeDelete, Seq: 2, Key: []byte("b")},
	}

	require.NoError(t, log.Append(context.Background(), batch))

	iter, err := log.Iterator(context.Background())
	require.NoError(t, err)
	defer iter.Close()

	var got []wal.Entry
	for {
		entry, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, entry)
	}

	require.Equal(t, len(batch), len(got))
	for i := range batch {
		require.True(t, got[i].Equal(batch[i]))
	}
}

func TestFileLogPersistsAcrossOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.OpenWAL(path)
	require.NoError(t, err)

	batch1 := []wal.Entry{{Type: wal.EntryTypePut, Seq: 10, Key: []byte("k1"), Value: []byte("v1")}}
	require.NoError(t, log.Append(context.Background(), batch1))
	require.NoError(t, log.Close())

	log, err = wal.OpenWAL(path)
	require.NoError(t, err)
	defer log.Close()

	batch2 := []wal.Entry{{Type: wal.EntryTypePut, Seq: 11, Key: []byte("k2"), Value: []byte("v2")}}
	require.NoError(t, log.Append(context.Background(), batch2))

	iter, err := log.Iterator(context.Background())
	require.NoError(t, err)
	defer iter.Close()

	var seqs []uint64
	for {
		entry, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		seqs = append(seqs, entry.Seq)
	}

	require.Equal(t, []uint64{10, 11}, seqs)
}

func TestFileLogContextCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.OpenWAL(path)
	require.NoError(t, err)
	defer log.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = log.Append(ctx, []wal.Entry{{Type: wal.EntryTypePut, Seq: 1, Key: []byte("k"), Value: []byte("v")}})
	require.Error(t, err)
}
