package db

import (
	"amethyst/internal/common"
)

// writeRequest represents a pending write operation waiting for group commit.
type writeRequest struct {
	entry    *common.Entry
	resultCh chan error
}

// collectBatch collects a batch of write requests from the channel.
// It blocks waiting for the first request, then greedily collects
// additional requests that are immediately available (up to MaxBatchSize).
func (d *DB) collectBatch() []*writeRequest {
	maxBatchSize := d.Opts.MaxBatchSize

	batch := make([]*writeRequest, 0, maxBatchSize)

	// Block waiting for first request
	first := <-d.writeChan
	batch = append(batch, first)

	// Collect more requests that are immediately available
	for len(batch) < maxBatchSize {
		select {
		case req := <-d.writeChan:
			batch = append(batch, req)
		default:
			// No more immediately available
			return batch
		}
	}

	return batch
}

// processBatch processes a batch of write requests under the DB lock.
// It handles flushing, sequence assignment, WAL writes, and memtable updates.
// Returns an error if any step fails.
func (d *DB) processBatch(batch []*writeRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if flush needed (synchronous, under lock)
	if d.memtable.Len() >= d.Opts.MemtableFlushThreshold {
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
	if err := d.wal.WriteEntry(entries); err != nil {
		return err
	}

	// Update memtable
	for _, req := range batch {
		switch req.entry.Type {
		case common.EntryTypePut:
			d.memtable.Put(req.entry.Key, req.entry.Value)
		case common.EntryTypeDelete:
			d.memtable.Delete(req.entry.Key)
		}
	}

	return nil
}

// groupCommitLoop is the main batching coordinator.
// It runs in a background goroutine, collecting batches of write requests
// and committing them together with a single WAL sync.
func (d *DB) groupCommitLoop() {
	for {
		batch := d.collectBatch()
		err := d.processBatch(batch)

		// Notify all writers in batch
		for _, req := range batch {
			req.resultCh <- err
		}
	}
}
