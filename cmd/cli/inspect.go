package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"amethyst/internal/common"
	"amethyst/internal/db"
	"amethyst/internal/sstable"
	"amethyst/internal/wal"
)

// inspectCompleter provides tab completion for inspect command filenames
// This is the global completer for liner, but it only completes for "inspect " commands
// Runs on every tab press, using ReadDir for performance
func inspectCompleter(line string) []string {
	// Only complete for inspect command
	if !strings.HasPrefix(line, "inspect ") {
		return nil
	}

	// Get the partial path after "inspect "
	partial := strings.TrimPrefix(line, "inspect ")

	var matches []string

	// Case 1: Starting completion - suggest top-level directories
	if partial == "" || !strings.Contains(partial, "/") {
		if strings.HasPrefix("wal/", partial) {
			matches = append(matches, "inspect wal/")
		}
		if strings.HasPrefix("sstable/", partial) {
			matches = append(matches, "inspect sstable/")
		}
		return matches
	}

	// Case 2: Completing wal/ directory - show .log files
	if strings.HasPrefix(partial, "wal/") {
		entries, err := os.ReadDir("wal")
		if err != nil {
			return nil
		}
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
				fullPath := "wal/" + entry.Name()
				if strings.HasPrefix(fullPath, partial) {
					matches = append(matches, "inspect "+fullPath)
				}
			}
		}
		return matches
	}

	// Case 3: Completing sstable/ directory
	if strings.HasPrefix(partial, "sstable/") {
		rest := strings.TrimPrefix(partial, "sstable/")

		// Case 3a: Completing level directory (sstable/<TAB> or sstable/0<TAB>)
		if !strings.Contains(rest, "/") {
			entries, err := os.ReadDir("sstable")
			if err != nil {
				return nil
			}
			for _, entry := range entries {
				if entry.IsDir() {
					levelDir := "sstable/" + entry.Name() + "/"
					if strings.HasPrefix(levelDir, partial) {
						matches = append(matches, "inspect "+levelDir)
					}
				}
			}
			return matches
		}

		// Case 3b: Completing .sst files within a level (sstable/0/<TAB>)
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 2 {
			levelDir := "sstable/" + parts[0]
			entries, err := os.ReadDir(levelDir)
			if err != nil {
				return nil
			}
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
					fullPath := levelDir + "/" + entry.Name()
					if strings.HasPrefix(fullPath, partial) {
						matches = append(matches, "inspect "+fullPath)
					}
				}
			}
			return matches
		}
	}

	return matches
}

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
	entryCount, err := table.GetEntryCount()
	if err != nil {
		fmt.Printf("failed to get entry count: %v\n", err)
		return
	}

	fmt.Printf("Total blocks: %d\n", len(indexEntries))
	fmt.Printf("Total entries: %d\n", entryCount)
	fmt.Println()
	fmt.Println("Index entries (first key of each block):")
	fmt.Println()

	for i, entry := range indexEntries {
		fmt.Printf("Block %d: offset=%d key=%q\n", i, entry.BlockOffset, string(entry.Key))
	}
	fmt.Println()
}

func inspectMemtable(engine *db.DB) {
	fmt.Println("Inspecting Memtable")
	fmt.Println()

	count := engine.Memtable().Len()
	fmt.Printf("Total entries: %d\n", count)
	fmt.Println()
}
