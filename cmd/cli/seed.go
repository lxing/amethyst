package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"amethyst/internal/common"
	"amethyst/internal/db"
	"golang.org/x/sync/errgroup"
)

const seedIndexFile = "CLI_SEED_INDEX"

func loadSeedIndex() int {
	data, err := os.ReadFile(seedIndexFile)
	if err != nil {
		return 0
	}
	idx, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return idx
}

func saveSeedIndex(idx int) error {
	return os.WriteFile(seedIndexFile, []byte(fmt.Sprint(idx)), 0644)
}

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

func runSeed(engine *db.DB, x int, seedIndex *int) {
	start := time.Now()
	startIndex := *seedIndex

	// Write concurrently with 26 goroutines (one per key pair)
	// to leverage group commit batching
	var g errgroup.Group
	for i := 0; i < x; i++ {
		currentIndex := *seedIndex + i
		for _, pair := range kvPairs {
			pair := pair // capture loop variable
			g.Go(func() error {
				key := fmt.Sprintf("%s%d", pair[0], currentIndex)
				value := fmt.Sprintf("%s%d", pair[1], currentIndex)
				return engine.Put([]byte(key), []byte(value))
			})
		}
	}

	// Wait for all writes to complete
	if err := g.Wait(); err != nil {
		fmt.Printf("seed error: %v\n", err)
		return
	}

	*seedIndex += x
	count := 26 * x

	// Persist seed index to file
	if err := saveSeedIndex(*seedIndex); err != nil {
		fmt.Printf("warning: failed to persist seed index: %v\n", err)
	}

	avgPerEntry := time.Since(start) / time.Duration(count)
	common.LogDuration(start, "seeded %d entries (26 * %d, index %d-%d) - %v/entry",
		count, x, startIndex, *seedIndex-1, avgPerEntry)
}
