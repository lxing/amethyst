package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"amethyst/internal/common"
	"amethyst/internal/db"
)

func loadSeedIndex(engine *db.DB) int {
	if val, err := engine.Get([]byte("__cli_seed_index__")); err == nil {
		if idx, err := strconv.Atoi(string(val)); err == nil {
			fmt.Printf("resumed seed index from %d\n", idx)
			return idx
		}
	}
	return 0
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
	count := 0
	startIndex := *seedIndex

	// Randomize the order of fruits for more realistic workload
	shuffled := make([][2]string, len(kvPairs))
	copy(shuffled, kvPairs)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	for _, pair := range shuffled {
		for i := 0; i < x; i++ {
			key := fmt.Sprintf("%s%d", pair[0], *seedIndex+i)
			value := fmt.Sprintf("%s%d", pair[1], *seedIndex+i)
			if err := engine.Put([]byte(key), []byte(value)); err != nil {
				fmt.Printf("seed error: %v\n", err)
				continue
			}
			count++
		}
	}
	*seedIndex += x

	// Persist seed index to DB
	if err := engine.Put([]byte("__cli_seed_index__"), []byte(fmt.Sprint(*seedIndex))); err != nil {
		fmt.Printf("warning: failed to persist seed index: %v\n", err)
	}

	avgPerEntry := time.Since(start) / time.Duration(count)
	common.LogDuration(start, "seeded %d entries (26 * %d, index %d-%d) - %v/entry",
		count, x, startIndex, *seedIndex-1, avgPerEntry)
}
