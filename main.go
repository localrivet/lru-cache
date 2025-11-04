package main

import (
	"fmt"
	"log"
	lru "lru-cache/cache"
	"time"
)

func main() {
	fmt.Println("=== LRU Cache - Production Ready with Insane Performance ===\n")

	// 1. Standard mode (non-sharded)
	fmt.Println("1. Standard Mode (single lock):")
	standardCache, err := lru.New[string, int](5)
	if err != nil {
		log.Fatal(err)
	}
	standardCache.Put("apple", 1)
	standardCache.Put("banana", 2)
	fmt.Printf("   %s\n\n", standardCache.String())

	// 2. High-throughput mode (sharded for ETL)
	fmt.Println("2. High-Throughput Mode (64 shards for ETL):")
	etlCache, err := lru.NewWithOptions[string, int](
		100000,
		lru.HighThroughputOptions(), // 64 shards + stats
	)
	if err != nil {
		log.Fatal(err)
	}
	etlCache.Put("key1", 100)
	etlCache.Put("key2", 200)
	fmt.Printf("   %s\n\n", etlCache.String())

	// 3. Demonstrate performance with concurrent access
	fmt.Println("3. Performance Demo (10,000 operations):")

	cache, _ := lru.NewWithOptions[int, int](
		10000,
		lru.Options{Shards: 64, TrackStats: true},
	)

	start := time.Now()
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}
	for i := 0; i < 10000; i++ {
		cache.Get(i)
	}
	elapsed := time.Since(start)

	stats := cache.Stats()
	fmt.Printf("   Time: %v\n", elapsed)
	fmt.Printf("   Throughput: %.0f ops/sec\n", 20000/elapsed.Seconds())
	fmt.Printf("   Hit rate: %.2f%%\n", stats.HitRate*100)
	fmt.Printf("   Cache: %d/%d items\n\n", stats.Size, stats.Capacity)

	// 4. GetOrCompute pattern (perfect for ETL lookups)
	fmt.Println("4. GetOrCompute (ETL lookup pattern):")

	lookupCache, _ := lru.NewWithOptions[string, string](
		1000,
		lru.Options{Shards: 32, TrackStats: true},
	)

	// First call - computes
	user1, cached := lookupCache.GetOrCompute("user_123", func() string {
		fmt.Println("   → Cache miss, fetching from database...")
		return "John Doe"
	})
	fmt.Printf("   Result: %s (cached: %v)\n", user1, cached)

	// Second call - from cache
	user2, cached := lookupCache.GetOrCompute("user_123", func() string {
		fmt.Println("   → This should not print!")
		return "Never called"
	})
	fmt.Printf("   Result: %s (cached: %v)\n\n", user2, cached)

	// 5. Batch operations (ETL bulk processing)
	fmt.Println("5. Batch Operations (bulk ETL processing):")

	batchCache, _ := lru.NewWithOptions[int, string](
		10000,
		lru.HighThroughputOptions(),
	)

	// Batch Put
	items := map[int]string{
		1: "one",
		2: "two",
		3: "three",
	}
	batchCache.BatchPut(items)

	// Batch Get
	keys := []int{1, 2, 3, 999}
	results := batchCache.BatchGet(keys)
	fmt.Printf("   Batch Get results: %v\n", results)
	fmt.Printf("   (key 999 not found, as expected)\n\n")

	// 6. Show different shard configurations
	fmt.Println("6. Shard Configuration Options:")

	configs := []struct {
		name   string
		shards int
	}{
		{"No sharding (standard)", 0},
		{"Light sharding", 16},
		{"Medium sharding", 32},
		{"Heavy sharding (ETL)", 64},
		{"Maximum throughput", 128},
	}

	for _, cfg := range configs {
		var c *lru.LRUCache[int, int]
		if cfg.shards == 0 {
			c, _ = lru.New[int, int](1000)
		} else {
			c, _ = lru.NewWithOptions[int, int](1000, lru.Options{Shards: cfg.shards})
		}
		c.Put(1, 10)
		fmt.Printf("   %-30s: %s\n", cfg.name, c.String())
	}

	// 7. Real-world ETL example
	fmt.Println("\n7. Real-World ETL Example (Deduplication):")

	dedupCache, _ := lru.NewWithOptions[string, bool](
		100000,
		lru.Options{Shards: 64, TrackStats: true},
	)

	// Simulate processing records
	records := []string{
		"record_1", "record_2", "record_3",
		"record_1", // duplicate
		"record_4",
		"record_2", // duplicate
	}

	processed := 0
	duplicates := 0

	for _, recordID := range records {
		if _, seen := dedupCache.Get(recordID); seen {
			fmt.Printf("   ✗ Duplicate: %s\n", recordID)
			duplicates++
		} else {
			fmt.Printf("   ✓ Processing: %s\n", recordID)
			dedupCache.Put(recordID, true)
			processed++
		}
	}

	fmt.Printf("\n   Summary: %d processed, %d duplicates\n", processed, duplicates)

	// Final stats
	finalStats := dedupCache.Stats()
	fmt.Printf("   Cache stats: %d items, %.2f%% hit rate\n\n",
		finalStats.Size, finalStats.HitRate*100)

	fmt.Println("=== Demo Complete ===")
	fmt.Println("\nFor ETL workloads, use:")
	fmt.Println("  cache, _ := lru.NewWithOptions[K, V](capacity, lru.HighThroughputOptions())")
	fmt.Println("\nSee ETL_PERFORMANCE.md for benchmarks and detailed guide!")
}
