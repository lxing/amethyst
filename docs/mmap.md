# Zero-Copy vs mmap: Understanding I/O Performance

This document explains two different optimization techniques that are often confused: **zero-copy parsing** and **mmap**. They solve different problems and can be used independently or together.

## Table of Contents
- [The Problem: Too Many Copies](#the-problem-too-many-copies)
- [Zero-Copy: Eliminating Userspace Copies](#zero-copy-eliminating-userspace-copies)
- [mmap: Eliminating Kernel→Userspace Copies](#mmap-eliminating-kerneluserspace-copies)
- [Combining Both: True Zero-Copy](#combining-both-true-zero-copy)
- [Performance Comparison](#performance-comparison)
- [LSM-Tree Specific Considerations](#lsm-tree-specific-considerations)

---

## The Problem: Too Many Copies

When reading data from disk and parsing it, traditional code makes multiple copies:

### Traditional Read + Copy

```go
// Step 1: Read from disk into buffer
blockData := make([]byte, 1024)
file.ReadAt(blockData, offset)  // Copy 1: kernel buffer → blockData

// Step 2: Parse entry, copy fields
keyLen := binary.LittleEndian.Uint16(blockData[0:2])
entry.Key = make([]byte, keyLen)          // Allocate new buffer
copy(entry.Key, blockData[2:2+keyLen])    // Copy 2: blockData → entry.Key

valueLen := binary.LittleEndian.Uint16(blockData[2+keyLen:])
entry.Value = make([]byte, valueLen)      // Allocate new buffer
copy(entry.Value, blockData[4+keyLen:])   // Copy 3: blockData → entry.Value
```

**Data flow:**
```
┌──────┐   disk I/O  ┌──────────┐   copy    ┌──────────┐   copy    ┌───────────┐
│ Disk │ ─────────> │ Page     │ ────────> │ blockData│ ────────> │ entry.Key │
│      │            │ Cache    │           │ buffer   │           │ entry.Val │
└──────┘            │ (kernel) │           │ (user)   │           │  (user)   │
                    └──────────┘           └──────────┘           └───────────┘
                         ↑                      ↑                       ↑
                     copy 1                 copy 2                  copy 3
```

**Cost for 1KB block with 10 entries:**
- 1 syscall (pread)
- ~1KB copy (kernel → userspace)
- ~500 bytes copies (10 entries × ~50 bytes/entry)
- 20+ allocations (10 Entry structs + keys + values)

---

## Zero-Copy: Eliminating Userspace Copies

**Zero-copy** means avoiding copies **within userspace** by using slices instead of copying data.

### Implementation

```go
// Step 1: Read from disk (same as before)
blockData := make([]byte, 1024)
file.ReadAt(blockData, offset)  // Still pays kernel → userspace copy

// Step 2: Parse using slices instead of copies
func (e *Entry) UnmarshalFrom(data []byte) (int, error) {
    pos := 0

    keyLen := binary.LittleEndian.Uint16(data[pos:])
    pos += 2
    e.Key = data[pos:pos+int(keyLen)]  // Slice, not copy!
    pos += int(keyLen)

    valueLen := binary.LittleEndian.Uint16(data[pos:])
    pos += 2
    e.Value = data[pos:pos+int(valueLen)]  // Slice, not copy!
    pos += int(valueLen)

    e.Seq = binary.LittleEndian.Uint32(data[pos:])
    pos += 4

    e.Type = EntryType(data[pos])
    pos += 1

    return pos, nil  // Return bytes consumed
}
```

**Data flow:**
```
┌──────┐   disk I/O  ┌──────────┐   copy    ┌──────────┐
│ Disk │ ─────────> │ Page     │ ────────> │ blockData│ ←─────┐
│      │            │ Cache    │           │ buffer   │       │
└──────┘            │ (kernel) │           │ (user)   │       │
                    └──────────┘           └──────────┘       │
                         ↑                      ↑              │
                     copy 1                  copy 2         (slice)
                                                               │
                                                         ┌───────────┐
                                                         │ entry.Key │
                                                         │ entry.Val │
                                                         └───────────┘
```

### Benefits

✅ **Eliminated userspace copies** - Key and Value are slices, not copies
✅ **Fewer allocations** - Only Entry struct, no separate Key/Value buffers
✅ **Works with any I/O** - Files, network sockets, pipes
✅ **Simple to implement** - Just change parsing code
✅ **Predictable** - Same syscall behavior as before

### Limitations

⚠️ **Lifetime coupling** - `entry.Key` is only valid while `blockData` is alive
⚠️ **Still pays syscall cost** - One `pread()` per block read
⚠️ **Still copies from kernel** - Page cache → userspace buffer

### When to Use

- Hot path parsing (SSTable blocks, WAL entries)
- When data is already buffered in memory
- When you control buffer lifetime (block cache, etc.)

---

## mmap: Eliminating Kernel→Userspace Copies

**mmap** (memory-mapped I/O) maps file pages directly into your address space, eliminating the kernel→userspace copy.

### How mmap Works

```go
// Map entire file into address space
file, _ := os.Open("data.sst")
fileSize, _ := file.Seek(0, io.SeekEnd)
mmapData, _ := syscall.Mmap(
    int(file.Fd()),
    0,              // offset
    int(fileSize),  // length
    syscall.PROT_READ,
    syscall.MAP_SHARED,
)

// Now mmapData directly points to kernel's page cache
// No ReadAt() needed!
blockData := mmapData[blockOffset:blockOffset+blockSize]

// Parse the block
entry.UnmarshalFrom(blockData)
```

**Data flow:**
```
┌──────┐   disk I/O  ┌──────────┐
│ Disk │ ─────────> │ Page     │ ←─────┐
│      │            │ Cache    │       │
└──────┘            │ (kernel) │       │
                    └──────────┘       │
                         ↑          (mapped)
                     copy 1            │
                                  ┌──────────┐
                                  │ mmapData │
                                  │ (user)   │
                                  └──────────┘
                                       ↑
                                    (slice)
                                       │
                                 ┌───────────┐
                                 │ entry.Key │
                                 │ entry.Val │
                                 └───────────┘
```

### How It Really Works

**Traditional read():**
1. App calls `read(fd, buf, size)`
2. Kernel checks if pages are in page cache
3. If not, loads from disk → page cache
4. **Copies** from page cache → user buffer
5. Returns to userspace

**With mmap:**
1. App calls `mmap(fd, ...)`
2. Kernel adds pages to process address space (virtual memory)
3. Returns to userspace (no data copied yet!)
4. App accesses `mmapData[offset]`
5. If page not in RAM: **page fault** → kernel loads page → retry
6. If page in RAM: direct memory access (no copy!)

### Benefits

✅ **No syscalls after mmap** - Accessing data is just memory access
✅ **No kernel→userspace copy** - Directly access page cache
✅ **OS manages caching** - Kernel decides what stays in RAM
✅ **Perfect for immutable files** - SSTables never change

### Limitations

❌ **Address space consumption** - 32-bit: can't map >2-4GB total
❌ **Unpredictable latency** - Page faults can happen anytime
❌ **Lifecycle complexity** - When to `munmap()`? What if file deleted?
❌ **Not portable to all I/O** - Can't mmap sockets, pipes
❌ **Can waste RAM** - OS may keep cold data in memory

### When to Use

- Small files (<64MB) accessed frequently
- Read-only, immutable data (like SSTables)
- When you want OS to manage caching
- When address space is plentiful (64-bit systems)

**Industry practice (RocksDB, LevelDB):**
- Files <64MB: mmap entire file
- Files >64MB: use pread() + block cache

---

## Combining Both: True Zero-Copy

When you combine mmap with zero-copy parsing, you get **truly zero copies** (except the unavoidable disk→RAM):

```go
// Map file
mmapData := syscall.Mmap(fd, 0, fileSize, PROT_READ, MAP_SHARED)

// Access block (no syscall, possibly page fault)
blockData := mmapData[blockOffset:blockOffset+blockSize]

// Parse with zero-copy (no allocations for fields)
entry.UnmarshalFrom(blockData)

// Result: entry.Key and entry.Value point directly into page cache!
```

**Data flow:**
```
┌──────┐   disk I/O  ┌──────────┐
│ Disk │ ─────────> │ Page     │ ←─────────────────────┐
│      │            │ Cache    │                        │
└──────┘            │ (kernel) │                        │
                    └──────────┘                        │
                         ↑                           (mapped)
                     copy 1                             │
                   (unavoidable)                   ┌──────────┐
                                                   │ mmapData │ ←──────┐
                                                   └──────────┘        │
                                                        ↑            (slice)
                                                     (slice)           │
                                                        │         ┌───────────┐
                                                        └────────>│ entry.Key │
                                                                  │ entry.Val │
                                                                  └───────────┘
```

**Absolutely minimal copies:**
- Disk → RAM: 1 copy (unavoidable - physics!)
- Kernel → userspace: 0 copies (mmap)
- Userspace → userspace: 0 copies (zero-copy parsing)

---

## Performance Comparison

### Scenario: Read 1000 Random Entries from SSTable

**Assumptions:**
- 100 unique blocks (10 entries per block)
- 1KB blocks
- Block cache available

#### Traditional: read() + copy parsing

```
Syscalls:
- 100 pread() calls (one per unique block)
- 900 cache hits

Copies:
- 100KB copied (kernel → blockData for 100 blocks)
- ~50KB copied (blockData → entry fields for 1000 entries)

Allocations:
- 1000 Entry structs
- 2000 field allocations (key + value per entry)

Total: 100 syscalls, ~150KB copied, 3000 allocations
```

#### read() + zero-copy parsing (proposed)

```
Syscalls:
- 100 pread() calls (one per unique block)
- 900 cache hits

Copies:
- 100KB copied (kernel → blockData for 100 blocks)
- 0KB copied in userspace (slicing only)

Allocations:
- 1000 Entry structs
- 0 field allocations (slices into blockData)

Total: 100 syscalls, 100KB copied, 1000 allocations
```

**Improvement: 3x fewer allocations, 33% less copying, same syscalls**

#### mmap + zero-copy parsing

```
Syscalls:
- 0 pread() calls
- ~10-50 page faults (if not in RAM)

Copies:
- 0KB copied in userspace
- Page faults load data directly into page cache

Allocations:
- 1000 Entry structs
- 0 field allocations (slices into mmap)

Total: 0 syscalls, 0KB copied, 1000 allocations
```

**Improvement: 100x fewer syscalls, 0 userspace copies**

### Realistic Timings

**For 1000 random reads:**

```
Traditional (read + copy):
- Syscalls: 100 × 1μs = 100μs
- Kernel copies: 100KB × 5 GB/s = 20μs
- Userspace copies: 50KB × 10 GB/s = 5μs
- Allocations: 3000 × 50ns = 150μs
Total: ~275μs

read() + zero-copy:
- Syscalls: 100 × 1μs = 100μs
- Kernel copies: 100KB × 5 GB/s = 20μs
- Userspace copies: 0μs
- Allocations: 1000 × 50ns = 50μs
Total: ~170μs (38% faster)

mmap + zero-copy:
- Page faults: 30 × 1μs = 30μs
- Copies: 0μs
- Allocations: 1000 × 50ns = 50μs
Total: ~80μs (71% faster)
```

**But:** These assume no block cache. With a warm cache, the difference shrinks significantly.

---

## LSM-Tree Specific Considerations

### Current Implementation

**WAL reads (recovery):**
```go
// Sequential read with bufio.Reader
file, _ := os.Open("wal.log")
reader := bufio.NewReader(file)  // Buffers 4-8KB

for {
    entry, _ := ReadEntry(reader)
    memtable.Put(entry.Key, entry.Value, entry.Seq)
}
```

**Analysis:**
- ✅ Sequential access pattern - `bufio` amortizes syscalls
- ✅ One-time operation during recovery
- ✅ Simple, predictable
- ❌ Allocates new buffers for each entry (not critical)

**Recommendation: Keep as-is.** Not a hot path.

---

**SSTable reads (query path):**
```go
func (s *SSTable) Get(key []byte) (*common.Entry, error) {
    // 1. Check bloom filter (in-memory)
    if !s.filter.MayContain(key) {
        return nil, ErrNotFound
    }

    // 2. Find block offset (in-memory index)
    blockOffset, _ := s.index.FindBlockOffset(key)

    // 3. Check block cache
    if block, ok := s.blockCache.Get(blockKey); ok {
        return block.Get(key)  // Cache hit - no syscall!
    }

    // 4. Cache miss - read block
    blockData := make([]byte, blockSize)
    s.file.ReadAt(blockData, int64(blockOffset))  // pread() syscall

    // 5. Parse block (allocates for each entry)
    block, _ := NewBlock(blockData)

    // 6. Cache for next time
    s.blockCache.Put(blockKey, block)

    return block.Get(key)
}
```

**Analysis:**
- ✅ Block cache prevents most syscalls
- ✅ Bloom filter prevents unnecessary reads
- ❌ Parsing allocates new buffers (happens on every cache miss)
- ❌ Could use zero-copy parsing for better GC behavior

**Recommendation: Add zero-copy parsing first**, then consider mmap for small files.

---

### Optimization Roadmap

**Phase 1: Zero-Copy Parsing (Easy Win)**

Add `UnmarshalFrom` method:

```go
func (e *Entry) UnmarshalFrom(data []byte) (int, error) {
    pos := 0

    keyLen := binary.LittleEndian.Uint16(data[pos:])
    pos += 2
    e.Key = data[pos:pos+int(keyLen)]  // Slice, not copy
    pos += int(keyLen)

    valueLen := binary.LittleEndian.Uint16(data[pos:])
    pos += 2
    e.Value = data[pos:pos+int(valueLen)]  // Slice, not copy
    pos += int(valueLen)

    e.Seq = binary.LittleEndian.Uint32(data[pos:])
    pos += 4
    e.Type = EntryType(data[pos])
    pos += 1

    return pos, nil
}
```

Use in block parsing:

```go
func NewBlock(data []byte) (*Block, error) {
    // Parse footer to get entry count
    numEntries := binary.LittleEndian.Uint16(data[len(data)-2:])

    entries := make([]Entry, numEntries)  // Not pointers!
    pos := 0

    for i := 0; i < int(numEntries); i++ {
        n, err := entries[i].UnmarshalFrom(data[pos:])
        if err != nil {
            return nil, err
        }
        pos += n
    }

    return &Block{
        Data:    data,     // Keep buffer alive
        Entries: entries,  // Entries slice into Data
    }, nil
}
```

**Benefits:**
- 3x fewer allocations
- Better GC behavior
- No API changes
- Works with existing block cache

---

**Phase 2: mmap for Small Files (Optional)**

```go
type SSTable struct {
    // Union type: either mmapped or file-based
    mmapData []byte
    file     *os.File

    // ... rest same
}

func OpenSSTable(path string, fileNo FileNo, cache *BlockCache) (*SSTable, error) {
    f, _ := os.Open(path)
    info, _ := f.Stat()

    var mmapData []byte
    if info.Size() < 64*1024*1024 {  // <64MB: mmap it
        mmapData, _ = syscall.Mmap(
            int(f.Fd()), 0, int(info.Size()),
            syscall.PROT_READ, syscall.MAP_SHARED,
        )
        f.Close()  // Can close file descriptor after mmap
    }

    return &SSTable{
        mmapData: mmapData,
        file:     f,
        // ... init rest
    }, nil
}

func (s *SSTable) readBlock(offset uint32, size uint32) ([]byte, error) {
    if s.mmapData != nil {
        // mmap'd: just slice
        return s.mmapData[offset:offset+size], nil
    } else {
        // regular file: pread
        data := make([]byte, size)
        _, err := s.file.ReadAt(data, int64(offset))
        return data, err
    }
}
```

**Benefits:**
- Eliminates syscalls for small files
- OS manages caching
- Good for L0 SSTables (small, hot)

**Caution:**
- Need proper cleanup (`munmap` on close)
- Watch for address space exhaustion
- Profile first!

---

## Summary

| Technique | What It Optimizes | When to Use | Complexity |
|-----------|------------------|-------------|------------|
| **Traditional** | Nothing (baseline) | Default, simple code | Low |
| **Zero-Copy Parse** | Userspace copies & allocations | Hot parsing paths | Low |
| **mmap** | Syscalls & kernel copies | Small, frequently-accessed files | Medium |
| **Both Combined** | Everything | Maximum performance | Medium-High |

**Recommended approach for this LSM-tree:**
1. ✅ Keep current approach for WAL (sequential, buffered)
2. 🔧 Add zero-copy parsing for SSTable blocks (easy win)
3. 🤔 Consider mmap for small SSTables if profiling shows syscalls are a bottleneck

**The key insight:** Zero-copy and mmap are **different optimizations** that can be applied independently. Zero-copy helps even without mmap, and mmap helps even without zero-copy. But combining them gives the best results.
