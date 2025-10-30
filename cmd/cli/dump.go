package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"amethyst/internal/common"
	"amethyst/internal/db"
	"amethyst/internal/sstable"
	"amethyst/internal/wal"
)

func dumpIterator(iter common.EntryIterator) {
	// Print header
	fmt.Printf("%-6s %-8s %-20s  %s\n", "OP", "SEQ", "KEY", "VALUE")
	fmt.Println()

	count := 0
	for {
		entry, err := iter.Next()
		if err != nil {
			fmt.Printf("error reading entry: %v\n", err)
			return
		}
		if entry == nil {
			break
		}

		count++
		typeStr := "PUT"
		if entry.Type == common.EntryTypeDelete {
			typeStr = "DEL"
		}

		// Truncate key if longer than 20 chars
		key := string(entry.Key)
		if len(key) > 20 {
			key = key[:20]
		}

		// Format with fixed-width columns
		if entry.Type == common.EntryTypePut {
			fmt.Printf("%-6s %-8d %-20s  %s\n", typeStr, entry.Seq, key, string(entry.Value))
		} else {
			fmt.Printf("%-6s %-8d %-20s\n", typeStr, entry.Seq, key)
		}
	}

	fmt.Println()
	fmt.Printf("Total entries: %d\n", count)
}

func dumpMemtable(engine *db.DB) {
	fmt.Println("Dumping Memtable")
	fmt.Println()
	dumpIterator(engine.Memtable().Iterator())
}

func dumpWAL(engine *db.DB, relativePath string) {
	fmt.Printf("Dumping WAL: %s\n", relativePath)
	fmt.Println()

	// Extract file number from path (e.g., "wal/123.log" -> 123)
	filename := filepath.Base(relativePath)
	fileNoStr := strings.TrimSuffix(filename, ".log")
	var fileNo common.FileNo
	if _, err := fmt.Sscanf(fileNoStr, "%d", &fileNo); err != nil {
		fmt.Printf("failed to parse file number from %s: %v\n", filename, err)
		return
	}

	// Use PathManager to construct full path
	fullPath := engine.Paths().WALPath(fileNo)

	w, err := wal.OpenWAL(fullPath)
	if err != nil {
		fmt.Printf("failed to open WAL: %v\n", err)
		return
	}
	defer w.Close()

	iter, err := w.Iterator()
	if err != nil {
		fmt.Printf("failed to create iterator: %v\n", err)
		return
	}

	dumpIterator(iter)
}

func dumpSSTable(engine *db.DB, relativePath string) {
	fmt.Printf("Dumping SSTable: %s\n", relativePath)
	fmt.Println()

	// Extract level and file number from path (e.g., "sstable/0/123.sst" -> level=0, fileNo=123)
	parts := strings.Split(filepath.ToSlash(relativePath), "/")
	if len(parts) < 3 {
		fmt.Printf("invalid SSTable path format: %s (expected sstable/<level>/<file>.sst)\n", relativePath)
		return
	}

	var level int
	if _, err := fmt.Sscanf(parts[1], "%d", &level); err != nil {
		fmt.Printf("failed to parse level from %s: %v\n", parts[1], err)
		return
	}

	filename := parts[2]
	fileNoStr := strings.TrimSuffix(filename, ".sst")
	var fileNo common.FileNo
	if _, err := fmt.Sscanf(fileNoStr, "%d", &fileNo); err != nil {
		fmt.Printf("failed to parse file number from %s: %v\n", filename, err)
		return
	}

	// Use PathManager to construct full path
	fullPath := engine.Paths().SSTablePath(level, fileNo)

	table, err := sstable.OpenSSTable(fullPath, fileNo, nil)
	if err != nil {
		fmt.Printf("failed to open SSTable: %v\n", err)
		return
	}
	defer table.Close()

	dumpIterator(table.Iterator())
}

func dumpFile(engine *db.DB, relativePath string) {
	ext := strings.ToLower(filepath.Ext(relativePath))

	switch ext {
	case ".log":
		dumpWAL(engine, relativePath)
	case ".sst":
		dumpSSTable(engine, relativePath)
	default:
		fmt.Printf("unknown file type: %s (expected .log or .sst)\n", ext)
	}
}

func dump(parts []string, engine *db.DB) {
	if len(parts) != 2 {
		fmt.Println("usage: dump <memtable|file.log|file.sst>")
		return
	}
	if parts[1] == "memtable" {
		dumpMemtable(engine)
	} else {
		dumpFile(engine, parts[1])
	}
}
