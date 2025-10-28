package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"amethyst/internal/db"
)

func printHelp() {
	fmt.Println("commands:")
	fmt.Println("  put     <key> <value>  - write a key-value pair")
	fmt.Println("  get     <key>          - read a value")
	fmt.Println("  delete  <key>       - delete a key")
	fmt.Println("  seed    <x>           - load 26*x fruit/vegetable pairs")
	fmt.Println("  inspect <file>     - inspect .log or .sst file")
	fmt.Println("  help               - show this help")
	fmt.Println("  exit, quit         - exit the program")
	fmt.Println()
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
	fmt.Println()
	printHelp()

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
			runSeed(engine, x, &seedIndex)
		case "inspect":
			if len(parts) != 2 {
				fmt.Println("usage: inspect <file.log|file.sst>")
				continue
			}
			inspectFile(parts[1])
		case "help":
			printHelp()
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command")
			printHelp()
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "input error: %v\n", err)
	}
}
