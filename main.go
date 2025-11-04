package main

import (
	"fmt"
	"log"
	lru "lru-cache/cache"
)

func main() {
	fmt.Println("=== Production-Ready LRU Cache Demo ===\n")

	// Create a new cache with capacity 5
	cache, err := lru.New[string, int](5)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created cache with capacity: %d\n\n", cache.Cap())

	// Basic operations: Put and Get
	fmt.Println("1. Basic Put and Get operations:")
	cache.Put("apple", 1)
	cache.Put("banana", 2)
	cache.Put("cherry", 3)
	cache.Put("date", 4)
	cache.Put("elderberry", 5)

	if val, ok := cache.Get("apple"); ok {
		fmt.Printf("   Get('apple') = %d\n", val)
	}
	if val, ok := cache.Get("cherry"); ok {
		fmt.Printf("   Get('cherry') = %d\n", val)
	}
	fmt.Printf("   Cache size: %d/%d\n\n", cache.Len(), cache.Cap())

	// Demonstrate eviction
	fmt.Println("2. LRU Eviction (adding 6th item to capacity-5 cache):")
	evicted, key, val := cache.Put("fig", 6)
	if evicted {
		fmt.Printf("   Evicted: '%s' = %d\n", key, val)
	}
	fmt.Printf("   Cache size: %d/%d\n\n", cache.Len(), cache.Cap())

	// Show all keys
	fmt.Println("3. Current keys in cache:")
	keys := cache.Keys()
	fmt.Printf("   Keys: %v\n\n", keys)

	// Peek without updating recency
	fmt.Println("4. Peek (doesn't update recency):")
	if val, ok := cache.Peek("banana"); ok {
		fmt.Printf("   Peek('banana') = %d\n", val)
	}

	// Contains check
	fmt.Println("\n5. Contains check:")
	fmt.Printf("   Contains('banana'): %v\n", cache.Contains("banana"))
	fmt.Printf("   Contains('apple'): %v (was evicted)\n\n", cache.Contains("apple"))

	// Oldest and Newest
	fmt.Println("6. Oldest and Newest items:")
	if key, val, err := cache.Oldest(); err == nil {
		fmt.Printf("   Oldest: '%s' = %d\n", key, val)
	}
	if key, val, err := cache.Newest(); err == nil {
		fmt.Printf("   Newest: '%s' = %d\n\n", key, val)
	}

	// Update existing key
	fmt.Println("7. Update existing key:")
	cache.Put("banana", 22)
	if val, ok := cache.Get("banana"); ok {
		fmt.Printf("   Updated 'banana' to %d\n\n", val)
	}

	// Remove operation
	fmt.Println("8. Remove operation:")
	if val, ok := cache.Remove("cherry"); ok {
		fmt.Printf("   Removed 'cherry' = %d\n", val)
	}
	fmt.Printf("   Cache size: %d/%d\n\n", cache.Len(), cache.Cap())

	// RemoveOldest
	fmt.Println("9. RemoveOldest:")
	if key, val, err := cache.RemoveOldest(); err == nil {
		fmt.Printf("   Removed oldest: '%s' = %d\n", key, val)
	}
	fmt.Printf("   Cache size: %d/%d\n\n", cache.Len(), cache.Cap())

	// Resize cache
	fmt.Println("10. Resize cache:")
	fmt.Printf("   Before resize: capacity=%d, size=%d\n", cache.Cap(), cache.Len())
	if err := cache.Resize(3); err == nil {
		fmt.Printf("   After resize: capacity=%d, size=%d\n\n", cache.Cap(), cache.Len())
	}

	// String representation
	fmt.Println("11. Cache string representation:")
	fmt.Printf("   %s\n\n", cache.String())

	// Clear cache
	fmt.Println("12. Clear cache:")
	cache.Clear()
	fmt.Printf("   After clear: size=%d\n\n", cache.Len())

	// Demonstrate with different types
	fmt.Println("=== Different Type Examples ===\n")

	// Integer keys and values
	intCache, _ := lru.New[int, int](3)
	intCache.Put(1, 100)
	intCache.Put(2, 200)
	intCache.Put(3, 300)
	fmt.Println("Integer cache:")
	fmt.Printf("   Get(2) = %v\n\n", getOrDefault(intCache.Get(2)))

	// String keys, struct values
	type User struct {
		Name  string
		Email string
	}

	userCache, _ := lru.New[string, User](3)
	userCache.Put("user1", User{"Alice", "alice@example.com"})
	userCache.Put("user2", User{"Bob", "bob@example.com"})

	fmt.Println("User cache:")
	if user, ok := userCache.Get("user1"); ok {
		fmt.Printf("   Get('user1') = {Name: %s, Email: %s}\n\n", user.Name, user.Email)
	}

	// Error handling example
	fmt.Println("=== Error Handling ===\n")

	// Invalid capacity
	if _, err := lru.New[string, int](0); err != nil {
		fmt.Printf("Invalid capacity error: %v\n", err)
	}

	// Empty cache operations
	emptyCache, _ := lru.New[string, int](5)
	if _, _, err := emptyCache.Oldest(); err != nil {
		fmt.Printf("Empty cache error: %v\n", err)
	}
	if _, _, err := emptyCache.Newest(); err != nil {
		fmt.Printf("Empty cache error: %v\n", err)
	}
	if _, _, err := emptyCache.RemoveOldest(); err != nil {
		fmt.Printf("Empty cache error: %v\n\n", err)
	}

	fmt.Println("=== Demo Complete ===")
}

// Helper function to handle tuple returns
func getOrDefault[V any](val V, ok bool) V {
	return val
}
