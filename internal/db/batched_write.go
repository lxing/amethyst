package db

import (
	"time"

	"amethyst/internal/common"
)

// writeRequest represents a pending write operation waiting for group commit.
type writeRequest struct {
	entry    *common.Entry
	resultCh chan error
}

// Locking rationale:
// - processBatch is single-threaded via groupCommitLoop
// - d.memtable/d.wal/d.manifest use atomic.Pointer for lock-free access
// - memtable is internally thread-safe with its own lock
// - d.nextSeq doesn't need lock (single writer)
func (d *DB) processBatch(batch []*writeRequest) error {
	// Check if flush needed (read-only on memtable)
	if d.Memtable().Len() >= d.Opts.MemtableFlushThreshold {
		if err := d.flushMemtable(); err != nil {
			return err
		}
	}

	// Assign sequence numbers to all entries in batch
	entries := make([]*common.Entry, 0, len(batch))
	for _, req := range batch {
		d.nextSeq++
		req.entry.Seq = d.nextSeq
		entries = append(entries, req.entry)
	}

	// Write entire batch to WAL with single sync
	if err := d.WAL().WriteEntry(entries); err != nil {
		return err
	}

	// Update memtable (memtable internally locks for thread-safety)
	mt := d.Memtable()
	for _, req := range batch {
		switch req.entry.Type {
		case common.EntryTypePut:
			mt.Put(req.entry.Key, req.entry.Value, req.entry.Seq)
		case common.EntryTypeDelete:
			mt.Delete(req.entry.Key, req.entry.Seq)
		}
	}

	return nil
}

// groupCommitLoop is the main batching coordinator.
// It runs in a background goroutine, collecting batches of write requests
// and committing them together with a single WAL sync.
func (d *DB) groupCommitLoop() {
	maxBatchSize := d.Opts.MaxBatchSize
	batchTimeout := d.Opts.BatchTimeout
	timer := time.NewTimer(batchTimeout)

	for {
		batch := make([]*writeRequest, 0, maxBatchSize)

		// Collect requests until timeout or batch full
		timer.Reset(batchTimeout)
		done := false
		for len(batch) < maxBatchSize && !done {
			if len(batch) == 0 {
				// Block waiting for first request
				batch = append(batch, <-d.writeChan)
			} else {
				// Have at least one request, collect more with timeout
				select {
				case req := <-d.writeChan:
					batch = append(batch, req)
				case <-timer.C:
					done = true
				}
			}
		}

		// Process the batch
		err := d.processBatch(batch)

		// Notify all writers in batch
		for _, req := range batch {
			req.resultCh <- err
		}
	}
}
