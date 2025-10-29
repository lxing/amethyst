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

// fileCompleter provides tab completion for inspect and dump commands
// This is the global completer for liner
// Runs on every tab press, using ReadDir for performance
func fileCompleter(line string) []string {
	var prefix, partial string

	// Check which command we're completing for
	if strings.HasPrefix(line, "inspect ") {
		prefix = "inspect "
		partial = strings.TrimPrefix(line, prefix)
	} else if strings.HasPrefix(line, "dump ") {
		prefix = "dump "
		partial = strings.TrimPrefix(line, prefix)
	} else {
		return nil
	}

	var matches []string

	// Case 1: Starting completion - suggest memtable and top-level directories
	if partial == "" || !strings.Contains(partial, "/") {
		if strings.HasPrefix("memtable", partial) {
			matches = append(matches, prefix+"memtable")
		}
		if strings.HasPrefix("wal/", partial) {
			matches = append(matches, prefix+"wal/")
		}
		if strings.HasPrefix("sstable/", partial) {
			matches = append(matches, prefix+"sstable/")
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
					matches = append(matches, prefix+fullPath)
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
						matches = append(matches, prefix+levelDir)
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
						matches = append(matches, prefix+fullPath)
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

	w, err := wal.OpenWAL(path)
	if err != nil {
		fmt.Printf("failed to open WAL: %v\n", err)
		return
	}
	defer w.Close()

	fmt.Printf("Total entries: %d\n", w.Len())
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

	index := table.GetIndex()
	entryCount := table.Len()

	fmt.Printf("Total blocks: %d\n", len(index.Entries))
	fmt.Printf("Total entries: %d\n", entryCount)
	fmt.Println("Index entries:")

	for i, entry := range index.Entries {
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

// renderBoxRow prints boxes side-by-side with ASCII borders and 1 space padding.
// If there are more than 10 boxes, they wrap to multiple rows.
// Example output:
//   LM: ┌──────────────┐ ┌──────────────┐
//       │ Memtable     │ │ WAL 0.log    │
//       │ 5 entries    │ │ 5 entries    │
//       └──────────────┘ └──────────────┘
func renderBoxRow(label string, boxes [][]string, width int) {
	if len(boxes) == 0 {
		return
	}

	const maxBoxesPerRow = 10

	// Process boxes in chunks of maxBoxesPerRow
	for chunkStart := 0; chunkStart < len(boxes); chunkStart += maxBoxesPerRow {
		chunkEnd := chunkStart + maxBoxesPerRow
		if chunkEnd > len(boxes) {
			chunkEnd = len(boxes)
		}
		chunk := boxes[chunkStart:chunkEnd]

		maxLines := 0
		for _, box := range chunk {
			if len(box) > maxLines {
				maxLines = len(box)
			}
		}

		// Print label only for first chunk
		if chunkStart == 0 {
			fmt.Printf("%s: ", label)
		} else {
			fmt.Print("    ")
		}

		// Top borders
		for i := 0; i < len(chunk); i++ {
			fmt.Print("┌")
			for j := 0; j < width+2; j++ {
				fmt.Print("─")
			}
			fmt.Print("┐ ")
		}
		fmt.Println()

		// Content lines
		for lineIdx := 0; lineIdx < maxLines; lineIdx++ {
			fmt.Print("    ")
			for _, box := range chunk {
				content := ""
				if lineIdx < len(box) {
					content = box[lineIdx]
				}
				if len(content) > width {
					content = content[:width]
				}
				fmt.Printf("│ %-*s │ ", width, content)
			}
			fmt.Println()
		}

		// Bottom borders
		fmt.Print("    ")
		for i := 0; i < len(chunk); i++ {
			fmt.Print("└")
			for j := 0; j < width+2; j++ {
				fmt.Print("─")
			}
			fmt.Print("┘ ")
		}
		fmt.Println()
	}
}

func inspectAll(engine *db.DB) {
	const boxWidth = 18

	version := engine.Manifest().Current()

	// LM: Memory level
	memCount := engine.Memtable().Len()
	walCount := engine.WAL().Len()

	memBox := []string{
		"Memtable",
		fmt.Sprintf("%d entries", memCount),
	}
	walBox := []string{
		fmt.Sprintf("WAL %d.log", version.CurrentWAL),
		fmt.Sprintf("%d entries", walCount),
	}
	renderBoxRow("LM", [][]string{memBox, walBox}, boxWidth)

	// Each SSTable level
	for level, fileMetas := range version.Levels {
		if len(fileMetas) == 0 {
			fmt.Printf("L%d: (empty)\n", level)
			continue
		}

		var boxes [][]string
		for _, fm := range fileMetas {
			table, err := engine.Manifest().GetTable(fm.FileNo, level)
			if err != nil {
				boxes = append(boxes, []string{
					fmt.Sprintf("%d.sst", fm.FileNo),
					"error",
				})
				continue
			}

			entryCount := table.Len()

			firstKey := string(fm.SmallestKey)
			lastKey := string(fm.LargestKey)

			boxes = append(boxes, []string{
				fmt.Sprintf("%d.sst", fm.FileNo),
				fmt.Sprintf("%d entries", entryCount),
				firstKey,
				lastKey,
			})
		}

		renderBoxRow(fmt.Sprintf("L%d", level), boxes, boxWidth)
	}
}

func inspect(parts []string, engine *db.DB) {
	if len(parts) == 1 {
		// Default: inspect all
		inspectAll(engine)
		return
	}

	if len(parts) != 2 {
		fmt.Println("usage: inspect [memtable|file.log|file.sst]")
		return
	}

	if parts[1] == "memtable" {
		inspectMemtable(engine)
	} else {
		inspectFile(parts[1])
	}
}
