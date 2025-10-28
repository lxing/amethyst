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

// groupCommitLoop is the main batching coordinator.
// It runs in a background goroutine, collecting batches of write requests
// and committing them together with a single WAL sync.
func (d *DB) groupCommitLoop() {
	for {
		batch := d.collectBatch()

		d.mu.Lock()

		// Check if flush needed (synchronous, under lock)
		if d.memtable.Len() >= d.Opts.MemtableFlushThreshold {
			if err := d.flushMemtable(); err != nil {
				// Flush failed - notify all writers and continue
				d.mu.Unlock()
				for _, req := range batch {
					req.resultCh <- err
				}
				continue
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
		err := d.wal.WriteEntry(entries)

		// Update memtable if WAL write succeeded
		if err == nil {
			for _, req := range batch {
				switch req.entry.Type {
				case common.EntryTypePut:
					d.memtable.Put(req.entry.Key, req.entry.Value)
				case common.EntryTypeDelete:
					d.memtable.Delete(req.entry.Key)
				}
			}
		}

		d.mu.Unlock()

		// Notify all writers in batch
		for _, req := range batch {
			req.resultCh <- err
		}
	}
}
