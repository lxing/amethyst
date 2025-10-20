# WAL Format Sketch

This LSMT will eventually write batches of mutations to disk before they reach the memtable. Below is a working draft of the record layout we can iterate on as the implementation matures.

## Segment Layout
- Segments live under a `wal/` directory with sequential numeric file names (e.g., `000001.wal`).
- Each segment stores a concatenation of variable-length records. No fixed header beyond a magic/version prefix to guard against stale files.

```
| magic(4) | version(1) | repeated { record } |
```

## Record Encoding (candidate)
Each record corresponds to a logical batch. Within the batch we encode individual mutations. Proposed outer framing:

```
| recordLen(4) | crc32(4) | batchSeq(8) | entryCount(4) | entries... |
```

- `recordLen` counts bytes after the length field, enabling skip-ahead during recovery.
- `crc32` covers `batchSeq`, `entryCount`, and the raw entry payload.
- `batchSeq` is the global sequence assigned by the DB for ordering.
- `entryCount` allows fast iteration without scanning for sentinels.

### Entry Encoding
Within a batch the entries could use a compact TLV structure:

```
| keyLen(varint) | valueLen(varint) | flags(1) | key | value |
```

- `flags` bit 0 marks tombstones. Additional bits could represent merge operands later.
- Empty value with tombstone flag represents delete.

## Rationale
- Framing with length + checksum simplifies detection of torn writes; recovery stops when checksum fails or EOF is reached mid-record.
- Varint lengths keep keys compact without fixed padding.
- Batches make group commit natural and align with the DB’s sequence assignment.

## Open Decisions
- Align records to 4KB boundaries for faster preallocation?
- Allow compression per batch? Probably not for the teaching path, but leave room in `flags` or a future header.
- Append a footer with the last durable sequence number to accelerate recovery, or keep it manifest-driven?

Let’s revisit after we stub the writer API (append, sync, rotate) and see how complicated we want the parser to be for students.
