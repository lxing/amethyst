# Amethyst TODO

## Core Features

### WAL
- [ ] Batched WAL writes
  - Batch multiple entries before sync for better throughput
  - Configurable batch size and timeout

### Memtable
- [ ] Skiplist memtable implementation
  - Replace current map-based memtable with skiplist
  - Better concurrency and performance characteristics

### SSTable
- [ ] Bloom filter implementation
  - Reduce unnecessary disk reads for non-existent keys
  - Configurable false positive rate

- [ ] Bitmap implementation
  - Efficient storage and querying

### Testing
- [ ] Load test scaffolding
  - Benchmark framework for write/read operations
  - Multi-threaded workload simulation
  - Performance metrics and reporting
