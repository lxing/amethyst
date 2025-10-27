package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"amethyst/internal/db"
)

var fruitPairs = [][2]string{
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
	engine, err := db.Open()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("amethyst LSMT")
	fmt.Println("commands: put <key> <value> | get <key> | delete <key> | seed <x> | exit")

	seedIndex := 0 // Global seed index counter
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
			for _, pair := range fruitPairs {
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
			fmt.Printf("seeded %d entries (26 * %d, index %d-%d)\n", count, x, startIndex, seedIndex-1)
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
