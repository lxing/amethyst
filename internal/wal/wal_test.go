package wal_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"amethyst/internal/common"
	"amethyst/internal/wal"

	"github.com/stretchr/testify/require"
)

func TestAppendAndIterate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.NewWAL(path)
	require.NoError(t, err)
	defer log.Close()

	batch := []*common.Entry{
		{
			Type:  common.EntryTypePut,
			Seq:   1,
			Key:   []byte("a"),
			Value: []byte("A"),
		},
		{
			Type: common.EntryTypeDelete,
			Seq:  2,
			Key:  []byte("b"),
		},
	}

	require.NoError(t, log.Append(context.Background(), batch))

	iter, err := log.Iterator(context.Background())
	require.NoError(t, err)
	defer iter.Close()

	for i := range batch {
		entry, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.True(t, wal.Equal(entry, batch[i]))
	}

	entry, err := iter.Next()
	require.NoError(t, err)
	require.Nil(t, entry)
}

func TestPersistsAcrossOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.wal")

	log, err := wal.NewWAL(path)
	require.NoError(t, err)

	batch1 := []*common.Entry{
		{
			Type:  common.EntryTypePut,
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

	batch2 := []*common.Entry{
		{
			Type:  common.EntryTypePut,
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
		entry, err := iter.Next()
		require.NoError(t, err)
		if entry == nil {
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
		batches  = 4
		perBatch = 128
	)

	expected := make([]*common.Entry, 0, batches*perBatch)
	seq := uint64(1)

	for batch := 0; batch < batches; batch++ {
		current := make([]*common.Entry, 0, perBatch)
		for i := 0; i < perBatch; i++ {
			entry := &common.Entry{
				Type:  common.EntryTypePut,
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
		entry, err := iter.Next()
		require.NoError(t, err)
		if entry == nil {
			break
		}
		require.True(t, wal.Equal(entry, expected[index]))
		index++
	}

	require.Equal(t, len(expected), index)
}
