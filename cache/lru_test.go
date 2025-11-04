package lru

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestNew tests cache creation
func TestNew(t *testing.T) {
	tests := []struct {
		name        string
		capacity    int
		expectError bool
	}{
		{"Valid capacity", 10, false},
		{"Capacity of 1", 1, false},
		{"Large capacity", 10000, false},
		{"Zero capacity", 0, true},
		{"Negative capacity", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := New[int, int](tt.capacity)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for capacity %d, got nil", tt.capacity)
				}
				if cache != nil {
					t.Errorf("Expected nil cache for invalid capacity, got %v", cache)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if cache == nil {
					t.Error("Expected non-nil cache")
				}
				if cache.Cap() != tt.capacity {
					t.Errorf("Expected capacity %d, got %d", tt.capacity, cache.Cap())
				}
			}
		})
	}
}

// TestPutAndGet tests basic put and get operations
func TestPutAndGet(t *testing.T) {
	cache, err := New[string, int](3)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Test putting and getting values
	cache.Put("one", 1)
	cache.Put("two", 2)
	cache.Put("three", 3)

	if val, ok := cache.Get("one"); !ok || val != 1 {
		t.Errorf("Expected (1, true), got (%d, %v)", val, ok)
	}
	if val, ok := cache.Get("two"); !ok || val != 2 {
		t.Errorf("Expected (2, true), got (%d, %v)", val, ok)
	}
	if val, ok := cache.Get("three"); !ok || val != 3 {
		t.Errorf("Expected (3, true), got (%d, %v)", val, ok)
	}

	// Test non-existent key
	if val, ok := cache.Get("nonexistent"); ok {
		t.Errorf("Expected (0, false), got (%d, true)", val)
	}
}

// TestEviction tests LRU eviction behavior
func TestEviction(t *testing.T) {
	cache, _ := New[int, string](3)

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")

	// Access key 1 to make it recently used
	cache.Get(1)

	// Add key 4, should evict key 2 (least recently used)
	evicted, key, val := cache.Put(4, "four")
	if !evicted {
		t.Error("Expected eviction to occur")
	}
	if key != 2 || val != "two" {
		t.Errorf("Expected eviction of (2, 'two'), got (%d, '%s')", key, val)
	}

	// Verify key 2 is gone
	if _, ok := cache.Get(2); ok {
		t.Error("Key 2 should have been evicted")
	}

	// Verify other keys still exist
	if _, ok := cache.Get(1); !ok {
		t.Error("Key 1 should still exist")
	}
	if _, ok := cache.Get(3); !ok {
		t.Error("Key 3 should still exist")
	}
	if _, ok := cache.Get(4); !ok {
		t.Error("Key 4 should exist")
	}
}

// TestUpdate tests updating existing keys
func TestUpdate(t *testing.T) {
	cache, _ := New[string, int](2)

	cache.Put("key", 1)
	evicted, _, _ := cache.Put("key", 2)

	if evicted {
		t.Error("Update should not cause eviction")
	}

	if val, ok := cache.Get("key"); !ok || val != 2 {
		t.Errorf("Expected updated value 2, got %d", val)
	}

	if cache.Len() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Len())
	}
}

// TestRemove tests remove operation
func TestRemove(t *testing.T) {
	cache, _ := New[int, string](3)

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")

	// Remove existing key
	val, ok := cache.Remove(2)
	if !ok || val != "two" {
		t.Errorf("Expected ('two', true), got ('%s', %v)", val, ok)
	}

	// Verify it's gone
	if _, ok := cache.Get(2); ok {
		t.Error("Removed key should not exist")
	}

	// Remove non-existent key
	if _, ok := cache.Remove(99); ok {
		t.Error("Removing non-existent key should return false")
	}

	if cache.Len() != 2 {
		t.Errorf("Expected size 2, got %d", cache.Len())
	}
}

// TestContains tests the Contains method
func TestContains(t *testing.T) {
	cache, _ := New[string, int](2)

	cache.Put("exists", 1)

	if !cache.Contains("exists") {
		t.Error("Contains should return true for existing key")
	}

	if cache.Contains("notexists") {
		t.Error("Contains should return false for non-existent key")
	}
}

// TestPeek tests the Peek method (doesn't update recency)
func TestPeek(t *testing.T) {
	cache, _ := New[int, string](2)

	cache.Put(1, "one")
	cache.Put(2, "two")

	// Peek at key 1 (shouldn't update recency)
	if val, ok := cache.Peek(1); !ok || val != "one" {
		t.Errorf("Peek failed: expected ('one', true), got ('%s', %v)", val, ok)
	}

	// Add key 3, should evict key 1 (oldest) since Peek didn't update it
	cache.Put(3, "three")

	if _, ok := cache.Get(1); ok {
		t.Error("Key 1 should have been evicted (Peek shouldn't update recency)")
	}
}

// TestKeys tests the Keys method
func TestKeys(t *testing.T) {
	cache, _ := New[int, int](3)

	cache.Put(1, 10)
	cache.Put(2, 20)
	cache.Put(3, 30)

	keys := cache.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	keyMap := make(map[int]bool)
	for _, k := range keys {
		keyMap[k] = true
	}

	for _, expected := range []int{1, 2, 3} {
		if !keyMap[expected] {
			t.Errorf("Expected key %d not found", expected)
		}
	}
}

// TestLen tests the Len method
func TestLen(t *testing.T) {
	cache, _ := New[int, int](5)

	if cache.Len() != 0 {
		t.Errorf("New cache should have length 0, got %d", cache.Len())
	}

	for i := 0; i < 3; i++ {
		cache.Put(i, i*10)
		if cache.Len() != i+1 {
			t.Errorf("Expected length %d, got %d", i+1, cache.Len())
		}
	}

	cache.Remove(1)
	if cache.Len() != 2 {
		t.Errorf("After removal, expected length 2, got %d", cache.Len())
	}
}

// TestOldestAndNewest tests Oldest and Newest methods
func TestOldestAndNewest(t *testing.T) {
	cache, _ := New[int, string](3)

	// Test empty cache
	if _, _, err := cache.Oldest(); err != ErrEmptyCache {
		t.Errorf("Expected ErrEmptyCache, got %v", err)
	}
	if _, _, err := cache.Newest(); err != ErrEmptyCache {
		t.Errorf("Expected ErrEmptyCache, got %v", err)
	}

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")

	// Newest should be 3 (most recently added)
	if key, val, err := cache.Newest(); err != nil || key != 3 || val != "three" {
		t.Errorf("Expected (3, 'three', nil), got (%d, '%s', %v)", key, val, err)
	}

	// Oldest should be 1
	if key, val, err := cache.Oldest(); err != nil || key != 1 || val != "one" {
		t.Errorf("Expected (1, 'one', nil), got (%d, '%s', %v)", key, val, err)
	}

	// Access key 1, making it newest
	cache.Get(1)

	// Now newest should be 1
	if key, _, _ := cache.Newest(); key != 1 {
		t.Errorf("Expected newest key 1, got %d", key)
	}

	// Oldest should be 2
	if key, _, _ := cache.Oldest(); key != 2 {
		t.Errorf("Expected oldest key 2, got %d", key)
	}
}

// TestRemoveOldest tests RemoveOldest method
func TestRemoveOldest(t *testing.T) {
	cache, _ := New[int, string](3)

	// Test empty cache
	if _, _, err := cache.RemoveOldest(); err != ErrEmptyCache {
		t.Errorf("Expected ErrEmptyCache, got %v", err)
	}

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")

	// Remove oldest (should be 1)
	key, val, err := cache.RemoveOldest()
	if err != nil || key != 1 || val != "one" {
		t.Errorf("Expected (1, 'one', nil), got (%d, '%s', %v)", key, val, err)
	}

	if cache.Len() != 2 {
		t.Errorf("Expected length 2, got %d", cache.Len())
	}

	if _, ok := cache.Get(1); ok {
		t.Error("Removed key should not exist")
	}
}

// TestClear tests the Clear method
func TestClear(t *testing.T) {
	cache, _ := New[int, int](3)

	cache.Put(1, 10)
	cache.Put(2, 20)
	cache.Put(3, 30)

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("After clear, expected length 0, got %d", cache.Len())
	}

	if _, ok := cache.Get(1); ok {
		t.Error("All keys should be cleared")
	}

	// Verify capacity is unchanged
	if cache.Cap() != 3 {
		t.Errorf("Capacity should remain 3, got %d", cache.Cap())
	}
}

// TestResize tests the Resize method
func TestResize(t *testing.T) {
	cache, _ := New[int, string](5)

	for i := 1; i <= 5; i++ {
		cache.Put(i, fmt.Sprintf("val%d", i))
	}

	// Resize to smaller capacity
	err := cache.Resize(3)
	if err != nil {
		t.Errorf("Resize failed: %v", err)
	}

	if cache.Cap() != 3 {
		t.Errorf("Expected capacity 3, got %d", cache.Cap())
	}

	if cache.Len() != 3 {
		t.Errorf("Expected length 3, got %d", cache.Len())
	}

	// Keys 1 and 2 should have been evicted (oldest)
	if _, ok := cache.Get(1); ok {
		t.Error("Key 1 should have been evicted")
	}
	if _, ok := cache.Get(2); ok {
		t.Error("Key 2 should have been evicted")
	}

	// Keys 3, 4, 5 should remain
	for i := 3; i <= 5; i++ {
		if _, ok := cache.Get(i); !ok {
			t.Errorf("Key %d should still exist", i)
		}
	}

	// Test invalid resize
	if err := cache.Resize(0); err != ErrInvalidCapacity {
		t.Errorf("Expected ErrInvalidCapacity, got %v", err)
	}
}

// TestConcurrency tests thread-safety
func TestConcurrency(t *testing.T) {
	cache, _ := New[int, int](100)
	var wg sync.WaitGroup

	// Number of goroutines
	numGoroutines := 10
	numOperations := 1000

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := id*numOperations + j
				cache.Put(key, key*10)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := id*numOperations + j
				cache.Get(key)
			}
		}(i)
	}

	// Concurrent mixed operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for j := 0; j < numOperations; j++ {
				key := r.Intn(1000)
				switch r.Intn(4) {
				case 0:
					cache.Put(key, key*10)
				case 1:
					cache.Get(key)
				case 2:
					cache.Remove(key)
				case 3:
					cache.Contains(key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Cache should still be functional
	cache.Put(9999, 99990)
	if val, ok := cache.Get(9999); !ok || val != 99990 {
		t.Error("Cache corrupted after concurrent operations")
	}
}

// TestString tests the String method
func TestString(t *testing.T) {
	cache, _ := New[int, int](10)
	cache.Put(1, 10)
	cache.Put(2, 20)

	str := cache.String()
	expected := "LRUCache{sharded: false, capacity: 10, size: 2}"
	if str != expected {
		t.Errorf("Expected '%s', got '%s'", expected, str)
	}
}

// TestGenericTypes tests various generic type combinations
func TestGenericTypes(t *testing.T) {
	// String keys, int values
	cache1, _ := New[string, int](2)
	cache1.Put("key", 123)
	if val, ok := cache1.Get("key"); !ok || val != 123 {
		t.Error("String -> int cache failed")
	}

	// Int keys, string values
	cache2, _ := New[int, string](2)
	cache2.Put(1, "value")
	if val, ok := cache2.Get(1); !ok || val != "value" {
		t.Error("Int -> string cache failed")
	}

	// Struct keys and values
	type Key struct{ id int }
	type Value struct{ data string }

	cache3, _ := New[Key, Value](2)
	cache3.Put(Key{1}, Value{"test"})
	if val, ok := cache3.Get(Key{1}); !ok || val.data != "test" {
		t.Error("Struct -> struct cache failed")
	}

	// Pointer values
	cache4, _ := New[int, *string](2)
	str := "pointer"
	cache4.Put(1, &str)
	if val, ok := cache4.Get(1); !ok || *val != "pointer" {
		t.Error("Pointer value cache failed")
	}
}

// TestEdgeCases tests various edge cases
func TestEdgeCases(t *testing.T) {
	// Cache with capacity 1
	cache, _ := New[int, int](1)
	cache.Put(1, 10)
	cache.Put(2, 20) // Should evict 1

	if _, ok := cache.Get(1); ok {
		t.Error("Key 1 should have been evicted")
	}
	if val, ok := cache.Get(2); !ok || val != 20 {
		t.Error("Key 2 should exist")
	}

	// Updating the only key in cache
	cache.Put(2, 25)
	if val, ok := cache.Get(2); !ok || val != 25 {
		t.Error("Update failed in capacity-1 cache")
	}
}

// TestZeroValues tests handling of zero values
func TestZeroValues(t *testing.T) {
	cache, _ := New[int, int](3)

	// Put zero value
	cache.Put(0, 0)

	if val, ok := cache.Get(0); !ok || val != 0 {
		t.Error("Zero values should be handled correctly")
	}

	// Ensure zero value is returned for missing key
	if val, ok := cache.Get(999); ok || val != 0 {
		t.Error("Missing key should return zero value")
	}
}
