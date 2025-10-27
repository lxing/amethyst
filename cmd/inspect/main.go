package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"amethyst/internal/block_cache"
	"amethyst/internal/common"
	"amethyst/internal/sstable"
	"amethyst/internal/wal"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <file.log|file.sst>\n", os.Args[0])
		os.Exit(1)
	}

	path := os.Args[1]
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".log":
		inspectWAL(path)
	case ".sst":
		inspectSSTable(path)
	default:
		fmt.Fprintf(os.Stderr, "unknown file type: %s (expected .log or .sst)\n", ext)
		os.Exit(1)
	}
}

func inspectWAL(path string) {
	fmt.Printf("Inspecting WAL: %s\n", path)
	fmt.Println()

	w, err := wal.NewWAL(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open WAL: %v\n", err)
		os.Exit(1)
	}
	defer w.Close()

	iter, err := w.Iterator()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create iterator: %v\n", err)
		os.Exit(1)
	}

	count := 0
	for {
		entry, err := iter.Next()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading entry: %v\n", err)
			os.Exit(1)
		}
		if entry == nil {
			break
		}
		count++
	}

	fmt.Printf("Total entries: %d\n", count)
}

func inspectSSTable(path string) {
	fmt.Printf("Inspecting SSTable: %s\n", path)
	fmt.Println()

	// Extract file number from path (e.g., "sstable/0/123.sst" -> 123)
	filename := filepath.Base(path)
	fileNoStr := strings.TrimSuffix(filename, ".sst")
	var fileNo common.FileNo
	if _, err := fmt.Sscanf(fileNoStr, "%d", &fileNo); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse file number from %s: %v\n", filename, err)
		os.Exit(1)
	}

	blockCache := block_cache.NewBlockCache()
	table, err := sstable.OpenSSTable(path, fileNo, blockCache)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open SSTable: %v\n", err)
		os.Exit(1)
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
}
