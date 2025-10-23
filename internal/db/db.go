package db

import (
    "bytes"
    "context"
    "errors"
    "sync"

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

func Open(ctx context.Context, opts Options) (*DB, error) {
    log, err := wal.NewWAL("wal.log")
    if err != nil {
        return nil, err
    }
    return &DB{
        memtable: memtable.NewMapMemtable(),
        wal:      log,
    }, nil
}

func (d *DB) Put(ctx context.Context, key, value []byte) error {
    if len(key) == 0 {
        return errors.New("db: key must be non-empty")
    }

    d.mu.Lock()
    defer d.mu.Unlock()

    d.nextSeq++

    entry := wal.Entry{
        Type:  wal.EntryTypePut,
        Seq:   d.nextSeq,
        Key:   bytes.Clone(key),
        Value: bytes.Clone(value),
    }
    if err := d.wal.Append(ctx, []wal.Entry{entry}); err != nil {
        return err
    }

    return d.memtable.Put(d.nextSeq, key, value)
}

func (d *DB) Delete(ctx context.Context, key []byte) error {
    if len(key) == 0 {
        return errors.New("db: key must be non-empty")
    }

    d.mu.Lock()
    defer d.mu.Unlock()

    d.nextSeq++

    entry := wal.Entry{
        Type: wal.EntryTypeDelete,
        Seq:  d.nextSeq,
        Key:  bytes.Clone(key),
    }
    if err := d.wal.Append(ctx, []wal.Entry{entry}); err != nil {
        return err
    }

    return d.memtable.Delete(d.nextSeq, key)
}

func (d *DB) Get(ctx context.Context, key []byte) ([]byte, error) {
    d.mu.RLock()
    defer d.mu.RUnlock()

    entry, ok := d.memtable.Get(key)
    if !ok || entry.Tombstone {
        return nil, errors.New("db: not found")
    }
    return bytes.Clone(entry.Value), nil
}
