# Amethyst TODO

## Core Features

### WAL
- [x] ~~Batched WAL writes~~ **COMPLETED**
  - ~~Batch multiple entries before sync for better throughput~~
  - ~~Configurable batch size and timeout~~
  - Implemented in `internal/db/batched_write.go` with group commit loop

### Memtable
- [ ] Skiplist memtable implementation
  - Replace current map-based memtable with skiplist
  - Better concurrency and performance characteristics

### SSTable
- [ ] Bloom filter implementation
  - Reduce unnecessary disk reads for non-existent keys
  - Configurable false positive rate
  - TODOs at `sstable.go:111, 180`

- [ ] Bitmap implementation
  - Efficient storage and querying
  - For compaction planning

### Block Cache
- [ ] LRU block cache implementation
  - Currently stub only (always returns cache miss)
  - Implement proper LRU eviction policy
  - Significant performance improvement for hot data

### Compaction
- [ ] K-way merge with heap
  - Implement heap-based merge iterator
  - Merge multiple sorted SSTable streams efficiently

- [ ] Compaction scheduler
  - Level-based compaction policy
  - Background goroutine for compaction tasks

### Database Lifecycle
- [ ] DB.Close() implementation
  - Close WAL properly
  - Close manifest and table cache
  - Flush pending writes
  - Release file descriptors

- [ ] Version and SSTable lifecycle management
  - Clean up old versions
  - Close SSTables removed by compaction
  - Delete obsolete SST files
  - See comment at `manifest.go:39`

### Query Optimization
- [ ] L1+ lookup optimization
  - Binary search by key range for non-overlapping levels
  - Currently checks all files even in L1+
  - See TODO at `db.go:261`

### Testing
- [ ] Load test scaffolding
  - Benchmark framework for write/read operations
  - Multi-threaded workload simulation
  - Performance metrics and reporting

- [ ] Crash recovery tests
  - Verify WAL replay correctness
  - Test various failure scenarios

- [ ] Concurrent read/write stress tests
  - Heavy concurrent workloads
  - Race condition detection
