package batcher

import "time"

// Batcher coordinates group commit for arbitrary request types.
type Batcher[T any] struct {
	requestChan  chan request[T]
	process      func(batch []T) error
	maxBatchSize int
	batchTimeout time.Duration
}

type request[T any] struct {
	item     T
	resultCh chan error
}

// New creates a new batcher that processes batches using the provided function.
func New[T any](maxBatchSize int, batchTimeout time.Duration, process func(batch []T) error) *Batcher[T] {
	b := &Batcher[T]{
		requestChan:  make(chan request[T], maxBatchSize),
		process:      process,
		maxBatchSize: maxBatchSize,
		batchTimeout: batchTimeout,
	}
	go b.loop()
	return b
}

// Submit queues an item for batched processing and blocks until complete.
func (b *Batcher[T]) Submit(item T) error {
	resultCh := make(chan error, 1)
	b.requestChan <- request[T]{item: item, resultCh: resultCh}
	return <-resultCh
}

// loop is the main batching coordinator.
func (b *Batcher[T]) loop() {
	timer := time.NewTimer(b.batchTimeout)

	for {
		requests := make([]request[T], 0, b.maxBatchSize)

		// Collect requests until timeout or batch full
		timer.Reset(b.batchTimeout)
		done := false
		for len(requests) < b.maxBatchSize && !done {
			if len(requests) == 0 {
				// Block waiting for first request
				requests = append(requests, <-b.requestChan)
			} else {
				// Have at least one request, collect more with timeout
				select {
				case req := <-b.requestChan:
					requests = append(requests, req)
				case <-timer.C:
					done = true
				}
			}
		}

		// Extract items for processing
		batch := make([]T, len(requests))
		for i, req := range requests {
			batch[i] = req.item
		}

		// Process the batch
		err := b.process(batch)

		// Notify all requesters in batch
		for _, req := range requests {
			req.resultCh <- err
		}
	}
}
