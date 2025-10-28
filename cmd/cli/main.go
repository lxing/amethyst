package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"amethyst/internal/common"
	"amethyst/internal/db"
	"amethyst/internal/sstable"
	"amethyst/internal/wal"
)

var kvPairs = [][2]string{
	{"apple", "artichoke"},
	{"banana", "broccoli"},
	{"cherry", "cabbage"},
	{"durian", "daikon"},
	{"elderberry", "eggplant"},
	{"fig", "fennel"},
	{"grapefruit", "ginger"},
	{"honeydew", "horseradish"},
	{"imbe", "ivygourd"},
	{"jackfruit", "jicama"},
	{"kiwi", "kale"},
	{"lime", "leek"},
	{"mango", "mushroom"},
	{"nectarine", "nopale"},
	{"orange", "okra"},
	{"peach", "peas"},
	{"quince", "quinoa"},
	{"raspberry", "radish"},
	{"strawberry", "spinach"},
	{"tangerine", "tomato"},
	{"ugni", "ube"},
	{"voavanga", "vanilla"},
	{"watermelon", "watercress"},
	{"ximenia", "xanthan"},
	{"yuzu", "yam"},
	{"zarzamora", "zucchini"},
}

func main() {
	walThreshold := 100
	maxSSTableLevel := 3

	engine, err := db.Open(db.WithWALThreshold(walThreshold), db.WithMaxSSTableLevel(maxSSTableLevel))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("adb - amethyst database")
	fmt.Printf("config: wal_flush_size=%d max_levels=%d\n", walThreshold, maxSSTableLevel)
	fmt.Println("commands: put <key> <value> | get <key> | delete <key> | seed <x> | inspect <file> | exit")

	// Load seed index from DB
	seedIndex := 0
	if val, err := engine.Get([]byte("__cli_seed_index__")); err == nil {
		if idx, err := strconv.Atoi(string(val)); err == nil {
			seedIndex = idx
			fmt.Printf("resumed seed index from %d\n", seedIndex)
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "put":
			if len(parts) != 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			if err := engine.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Printf("put error: %v\n", err)
				continue
			}
			fmt.Println("ok")
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			value, err := engine.Get([]byte(parts[1]))
			if err != nil {
				fmt.Printf("get error: %v\n", err)
				continue
			}
			fmt.Printf("%s\n", string(value))
		case "delete":
			if len(parts) != 2 {
				fmt.Println("usage: delete <key>")
				continue
			}
			if err := engine.Delete([]byte(parts[1])); err != nil {
				fmt.Printf("delete error: %v\n", err)
				continue
			}
			fmt.Println("ok")
		case "seed":
			if len(parts) != 2 {
				fmt.Println("usage: seed <x>")
				continue
			}
			x, err := strconv.Atoi(parts[1])
			if err != nil || x < 1 {
				fmt.Println("seed: x must be a positive integer")
				continue
			}
			count := 0
			startIndex := seedIndex
			for _, pair := range kvPairs {
				for i := 0; i < x; i++ {
					key := fmt.Sprintf("%s%d", pair[0], seedIndex+i)
					value := fmt.Sprintf("%s%d", pair[1], seedIndex+i)
					if err := engine.Put([]byte(key), []byte(value)); err != nil {
						fmt.Printf("seed error: %v\n", err)
						continue
					}
					count++
				}
			}
			seedIndex += x

			// Persist seed index to DB
			if err := engine.Put([]byte("__cli_seed_index__"), []byte(fmt.Sprint(seedIndex))); err != nil {
				fmt.Printf("warning: failed to persist seed index: %v\n", err)
			}

			fmt.Printf("seeded %d entries (26 * %d, index %d-%d)\n", count, x, startIndex, seedIndex-1)
		case "inspect":
			if len(parts) != 2 {
				fmt.Println("usage: inspect <file.log|file.sst>")
				continue
			}
			inspectFile(parts[1])
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "input error: %v\n", err)
	}
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

	fmt.Printf("Total blocks: %d\n", len(indexEntries))
	fmt.Println()
	fmt.Println("Index entries (first key of each block):")
	fmt.Println()

	for i, entry := range indexEntries {
		fmt.Printf("Block %d: offset=%d key=%q\n", i, entry.BlockOffset, string(entry.Key))
	}
	fmt.Println()
}
