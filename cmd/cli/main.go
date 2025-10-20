package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"amethyst/internal/db"
)

func main() {
	engine, err := db.Open(context.Background(), db.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("amethyst LSMT")
	fmt.Println("commands: put <key> <value> | get <key> | delete <key> | exit")

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
			if err := engine.Put(context.Background(), []byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Printf("put error: %v\n", err)
				continue
			}
			fmt.Println("ok")
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			value, err := engine.Get(context.Background(), []byte(parts[1]))
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
			if err := engine.Delete(context.Background(), []byte(parts[1])); err != nil {
				fmt.Printf("delete error: %v\n", err)
				continue
			}
			fmt.Println("ok")
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
