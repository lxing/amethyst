package batcher_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"amethyst/internal/dsa/batcher"
	"github.com/stretchr/testify/require"
)

func TestBatcher_SingleItem(t *testing.T) {
	var processedBatches [][]int
	var mu sync.Mutex

	b := batcher.New(10, 100*time.Millisecond, func(batch []int) error {
		mu.Lock()
		processedBatches = append(processedBatches, batch)
		mu.Unlock()
		return nil
	})

	err := b.Submit(42)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, processedBatches, 1)
	require.Equal(t, []int{42}, processedBatches[0])
}

func TestBatcher_BatchesBySize(t *testing.T) {
	var processedBatches [][]int
	var mu sync.Mutex

	maxBatchSize := 5
	b := batcher.New(maxBatchSize, 1*time.Second, func(batch []int) error {
		mu.Lock()
		processedBatches = append(processedBatches, batch)
		mu.Unlock()
		return nil
	})

	// Submit exactly maxBatchSize items rapidly
	var wg sync.WaitGroup
	for i := 0; i < maxBatchSize; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			err := b.Submit(val)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, processedBatches, 1)
	require.Len(t, processedBatches[0], maxBatchSize)
}

func TestBatcher_BatchesByTimeout(t *testing.T) {
	var processedBatches [][]int
	var mu sync.Mutex

	batchTimeout := 50 * time.Millisecond
	b := batcher.New(100, batchTimeout, func(batch []int) error {
		mu.Lock()
		processedBatches = append(processedBatches, batch)
		mu.Unlock()
		return nil
	})

	// Submit 3 items concurrently so they arrive before timeout
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			err := b.Submit(val)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// All 3 should be in same batch
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, processedBatches, 1)
	require.Len(t, processedBatches[0], 3)
}

func TestBatcher_ConcurrentSubmits(t *testing.T) {
	var processed atomic.Int32

	b := batcher.New(10, 50*time.Millisecond, func(batch []int) error {
		processed.Add(int32(len(batch)))
		return nil
	})

	numGoroutines := 100
	itemsPerGoroutine := 10

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				err := b.Submit(start + j)
				require.NoError(t, err)
			}
		}(i * itemsPerGoroutine)
	}
	wg.Wait()

	require.Equal(t, int32(numGoroutines*itemsPerGoroutine), processed.Load())
}

func TestBatcher_ErrorPropagation(t *testing.T) {
	expectedErr := errors.New("test error")

	b := batcher.New(5, 100*time.Millisecond, func(batch []int) error {
		return expectedErr
	})

	err := b.Submit(1)
	require.ErrorIs(t, err, expectedErr)
}

func TestBatcher_MultipleBatches(t *testing.T) {
	var batchSizes []int
	var mu sync.Mutex

	maxBatchSize := 3
	b := batcher.New(maxBatchSize, 50*time.Millisecond, func(batch []int) error {
		mu.Lock()
		batchSizes = append(batchSizes, len(batch))
		mu.Unlock()
		return nil
	})

	// First batch: fill to max size concurrently
	var wg sync.WaitGroup
	for i := 0; i < maxBatchSize; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			err := b.Submit(val)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Second batch: submit fewer items concurrently
	for i := 100; i < 102; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			err := b.Submit(val)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batchSizes, 2)
	require.Equal(t, maxBatchSize, batchSizes[0])
	require.Equal(t, 2, batchSizes[1])
}
