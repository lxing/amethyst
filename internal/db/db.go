package db

import (
	"bytes"
	"errors"
	"sync"

	"amethyst/internal/common"
	"amethyst/internal/memtable"
	"amethyst/internal/wal"
)

type Options struct{}

type DB struct {
	mu       sync.RWMutex
	nextSeq  uint64
	memtable memtable.Memtable
	wal      *wal.WALImpl
}

func Open(opts Options) (*DB, error) {
	log, err := wal.NewWAL("wal.log")
	if err != nil {
		return nil, err
	}
	return &DB{
		memtable: memtable.NewMapMemtable(),
		wal:      log,
	}, nil
}

func (d *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("db: key must be non-empty")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextSeq++

	entry := &common.Entry{
		Type:  common.EntryTypePut,
		Seq:   d.nextSeq,
		Key:   bytes.Clone(key),
		Value: bytes.Clone(value),
	}
	if err := d.wal.Append([]*common.Entry{entry}); err != nil {
		return err
	}

	d.memtable.Put(key, value)
	return nil
}

func (d *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("db: key must be non-empty")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextSeq++

	entry := &common.Entry{
		Type: common.EntryTypeDelete,
		Seq:  d.nextSeq,
		Key:  bytes.Clone(key),
	}
	if err := d.wal.Append([]*common.Entry{entry}); err != nil {
		return err
	}

	d.memtable.Delete(key)
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	value, ok := d.memtable.Get(key)
	if !ok {
		return nil, errors.New("db: not found")
	}
	return bytes.Clone(value), nil
}
