package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"amethyst/internal/db"
	"github.com/peterh/liner"
)

type cmdContext struct {
	engine    *db.DB
	seedIndex int
}

func printHelp() {
	fmt.Println("commands:")
	fmt.Println("  put     <key> <value>  - write a key-value pair")
	fmt.Println("  get     <key>          - read a value")
	fmt.Println("  delete  <key>          - delete a key")
	fmt.Println("  seed    <x>            - load 26*x fruit/vegetable pairs")
	fmt.Println("  inspect <memtable|file.log|file.sst> - inspect memtable or file")
	fmt.Println("  clear                  - delete all .log and .sst files")
	fmt.Println("  help                   - show this help")
	fmt.Println("  exit, quit             - exit the program")
	fmt.Println()
}

func clearDatabase(ctx *cmdContext) error {
	// Close the database to stop all operations
	if err := ctx.engine.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	// Remove wal/ and sstable/ directories entirely
	os.RemoveAll("wal")
	os.RemoveAll("sstable")

	// Reopen engine (will recreate directories)
	newEngine, err := db.Open()
	if err != nil {
		return fmt.Errorf("failed to reopen database: %w", err)
	}

	ctx.engine = newEngine
	ctx.seedIndex = 0

	fmt.Println("cleared database")
	return nil
}

func main() {
	engine, err := db.Open()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("adb - amethyst database")
	fmt.Printf("config: wal_flush_size=%d max_levels=%d\n", engine.Opts.WALThreshold, engine.Opts.MaxSSTableLevel)
	fmt.Println()
	printHelp()

	// Initialize context
	ctx := &cmdContext{engine: engine}

	// Initialize liner for command line editing
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)
	line.SetCompleter(inspectCompleter)

	// Load history from file
	history, err := newHistory()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load history: %v\n", err)
		os.Exit(1)
	}
	defer history.save()

	// Load history into liner
	for _, cmd := range history.commands {
		line.AppendHistory(cmd)
	}

	// Load seed index from DB
	ctx.seedIndex = loadSeedIndex(ctx.engine)

	for {
		input, err := line.Prompt("> ")
		if err != nil {
			if err == liner.ErrPromptAborted || err == io.EOF {
				fmt.Println()
				break
			}
			fmt.Fprintf(os.Stderr, "input error: %v\n", err)
			break
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Add to history (both liner and our persistent history)
		line.AppendHistory(input)
		history.add(input)

		parts := strings.Fields(input)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "put":
			if len(parts) != 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			if err := ctx.engine.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Printf("put error: %v\n", err)
				continue
			}
			fmt.Println("ok")
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			value, err := ctx.engine.Get([]byte(parts[1]))
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
			if err := ctx.engine.Delete([]byte(parts[1])); err != nil {
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
			runSeed(ctx.engine, x, &ctx.seedIndex)
		case "inspect":
			if len(parts) != 2 {
				fmt.Println("usage: inspect <memtable|file.log|file.sst>")
				continue
			}
			if parts[1] == "memtable" {
				inspectMemtable(ctx.engine)
			} else {
				inspectFile(parts[1])
			}
		case "clear":
			if err := clearDatabase(ctx); err != nil {
				fmt.Printf("clear error: %v\n", err)
			}
		case "help":
			printHelp()
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command")
			printHelp()
		}
	}
}
