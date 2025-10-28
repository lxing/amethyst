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

func dumpWAL(path string) {
	fmt.Printf("Dumping WAL: %s\n", path)
	fmt.Println()

	w, err := wal.OpenWAL(path)
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

func dumpSSTable(path string) {
	fmt.Printf("Dumping SSTable: %s\n", path)
	fmt.Println()

	// Extract file number from path (e.g., "sstable/0/123.sst" -> 123)
	filename := filepath.Base(path)
	fileNoStr := strings.TrimSuffix(filename, ".sst")
	var fileNo common.FileNo
	if _, err := fmt.Sscanf(fileNoStr, "%d", &fileNo); err != nil {
		fmt.Printf("failed to parse file number from %s: %v\n", filename, err)
		return
	}

	table, err := sstable.OpenSSTable(path, fileNo, nil)
	if err != nil {
		fmt.Printf("failed to open SSTable: %v\n", err)
		return
	}
	defer table.Close()

	dumpIterator(table.Iterator())
}

func dumpFile(path string) {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".log":
		dumpWAL(path)
	case ".sst":
		dumpSSTable(path)
	default:
		fmt.Printf("unknown file type: %s (expected .log or .sst)\n", ext)
	}
}
