package wal_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"amethyst/internal/wal"

	"github.com/stretchr/testify/require"
)

func TestAppendAndIterate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.NewWAL(path)
	require.NoError(t, err)
	defer log.Close()

	batch := []wal.Entry{
		{
			Type:  wal.EntryTypePut,
			Seq:   1,
			Key:   []byte("a"),
			Value: []byte("A"),
		},
		{
			Type: wal.EntryTypeDelete,
			Seq:  2,
			Key:  []byte("b"),
		},
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

func TestPersistsAcrossOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.NewWAL(path)
	require.NoError(t, err)

	batch1 := []wal.Entry{
		{
			Type:  wal.EntryTypePut,
			Seq:   10,
			Key:   []byte("k1"),
			Value: []byte("v1"),
		},
	}
	require.NoError(t, log.Append(context.Background(), batch1))
	require.NoError(t, log.Close())

	log, err = wal.NewWAL(path)
	require.NoError(t, err)
	defer log.Close()

	batch2 := []wal.Entry{
		{
			Type:  wal.EntryTypePut,
			Seq:   11,
			Key:   []byte("k2"),
			Value: []byte("v2"),
		},
	}
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

func TestBulkAppendBatches(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.NewWAL(path)
	require.NoError(t, err)
	defer log.Close()

	const (
		batches   = 4
		perBatch  = 128
		totalSeqs = batches * perBatch
	)

	expected := make([]wal.Entry, 0, totalSeqs)
	seq := uint64(1)

	for batch := 0; batch < batches; batch++ {
		current := make([]wal.Entry, 0, perBatch)
		for i := 0; i < perBatch; i++ {
			entry := wal.Entry{
				Type:  wal.EntryTypePut,
				Seq:   seq,
				Key:   []byte(fmt.Sprintf("b%02d-key-%03d", batch, i)),
				Value: []byte(fmt.Sprintf("payload-%02d-%03d", batch, i)),
			}
			seq++
			current = append(current, entry)
			expected = append(expected, entry)
		}
		require.NoError(t, log.Append(context.Background(), current))
	}

	iter, err := log.Iterator(context.Background())
	require.NoError(t, err)
	defer iter.Close()

	index := 0
	for {
		entry, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		require.True(t, entry.Equal(expected[index]))
		index++
	}

	require.Equal(t, len(expected), index)
}
