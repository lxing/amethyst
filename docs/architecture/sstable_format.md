# Simplified SSTable Format (Teaching Cut)

## File Layout
```
| data region | index | footer |
```
- **Data region**: one entry per line, sorted by ascending `key` and descending `seq`.
- **Index**: fixed-size records with `key`, `offset` pointing into data region.
- **Footer**: 16-byte trailer: `indexOffset (uint64)` + `indexCount (uint32)` + `magic (uint32)`.

## Entry Encoding
Each entry is stored as:
```
keyLen (varint)
valueLen (varint)
seq (uint64)
flags (uint8) // 0 = value present, 1 = tombstone
key bytes
value bytes (omitted when tombstone)
```

This keeps parsing trivial and gives students a clear spec to implement.

## Single-Level Lookup
1. Binary search the in-memory index (array of `{key, offset}`) to find the smallest key ≥ target.
2. Seek to `offset` in the data file and decode entries sequentially until the key changes.
3. Take the first matching entry (highest sequence) and respect tombstone flag.

A single block read per lookup, and no restart array to manage.
