package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"amethyst/internal/common"
	"amethyst/internal/sstable"
	"amethyst/internal/wal"
)

func inspectFile(path string) {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".log":
		inspectWAL(path)
	case ".sst":
		inspectSSTable(path)
	default:
		fmt.Printf("unknown file type: %s (expected .log or .sst)\n", ext)
	}
}

func inspectWAL(path string) {
	fmt.Printf("Inspecting WAL: %s\n", path)
	fmt.Println()

	w, err := wal.NewWAL(path)
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
	}

	fmt.Printf("Total entries: %d\n", count)
	fmt.Println()
}

func inspectSSTable(path string) {
	fmt.Printf("Inspecting SSTable: %s\n", path)
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

	indexEntries := table.GetIndex()

	fmt.Printf("Total blocks: %d\n", len(indexEntries))
	fmt.Println()
	fmt.Println("Index entries (first key of each block):")
	fmt.Println()

	for i, entry := range indexEntries {
		fmt.Printf("Block %d: offset=%d key=%q\n", i, entry.BlockOffset, string(entry.Key))
	}
	fmt.Println()
}
