package lru

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidCapacity is returned when capacity is less than 1
	ErrInvalidCapacity = errors.New("capacity must be greater than 0")
	// ErrEmptyCache is returned when operations are performed on an empty cache
	ErrEmptyCache = errors.New("cache is empty")
	// ErrKeyNotFound is returned when a key is not found in the cache
	ErrKeyNotFound = errors.New("key not found")
)

// LRUCache is a thread-safe implementation of a Least Recently Used (LRU) cache.
// It provides O(1) time complexity for Get, Put, and Remove operations.
//
// The cache automatically evicts the least recently used item when it reaches capacity.
// All operations are safe for concurrent use by multiple goroutines.
//
// Type parameters:
//   - K: The key type, must be comparable
//   - V: The value type, can be any type
type LRUCache[K comparable, V any] struct {
	capacity int                     // Maximum number of items the cache can hold
	list     *list.List              // Doubly linked list to maintain access order
	elements map[K]*list.Element     // Map for O(1) lookups
	mu       sync.RWMutex            // Read-Write mutex for thread-safety
}

// entry represents a key-value pair stored in the cache
type entry[K comparable, V any] struct {
	key   K
	value V
}

// New creates a new LRUCache with the specified capacity.
//
// Parameters:
//   - capacity: Maximum number of items the cache can hold
//
// Returns:
//   - *LRUCache[K, V]: A new cache instance
//   - error: ErrInvalidCapacity if capacity is less than 1
//
// Time complexity: O(1)
// Space complexity: O(capacity)
//
// Example:
//
//	cache, err := lru.New[string, int](100)
//	if err != nil {
//	    log.Fatal(err)
//	}
func New[K comparable, V any](capacity int) (*LRUCache[K, V], error) {
	if capacity < 1 {
		return nil, ErrInvalidCapacity
	}

	return &LRUCache[K, V]{
		capacity: capacity,
		list:     list.New(),
		elements: make(map[K]*list.Element, capacity),
	}, nil
}

// Get retrieves the value associated with the key and marks it as recently used.
//
// Parameters:
//   - key: The key to look up
//
// Returns:
//   - V: The value associated with the key
//   - bool: true if the key was found, false otherwise
//
// Time complexity: O(1)
//
// Example:
//
//	if value, ok := cache.Get("mykey"); ok {
//	    fmt.Println("Found:", value)
//	}
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	if node, ok := c.elements[key]; ok {
		c.list.MoveToFront(node)
		return node.Value.(entry[K, V]).value, true
	}
	return zero, false
}

// Put inserts or updates a key-value pair in the cache.
// If the key already exists, its value is updated and it's marked as recently used.
// If the cache is at capacity, the least recently used item is evicted.
//
// Parameters:
//   - key: The key to insert or update
//   - value: The value to associate with the key
//
// Returns:
//   - evicted: true if an item was evicted to make room
//   - evictedKey: the key that was evicted (only valid if evicted is true)
//   - evictedValue: the value that was evicted (only valid if evicted is true)
//
// Time complexity: O(1)
//
// Example:
//
//	evicted, key, val := cache.Put("mykey", 42)
//	if evicted {
//	    fmt.Printf("Evicted: %v=%v\n", key, val)
//	}
func (c *LRUCache[K, V]) Put(key K, value V) (evicted bool, evictedKey K, evictedValue V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing key
	if node, ok := c.elements[key]; ok {
		c.list.MoveToFront(node)
		node.Value = entry[K, V]{key: key, value: value}
		return false, evictedKey, evictedValue
	}

	// Evict if at capacity
	if c.list.Len() >= c.capacity {
		oldest := c.list.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(entry[K, V])
			delete(c.elements, oldEntry.key)
			c.list.Remove(oldest)
			evicted = true
			evictedKey = oldEntry.key
			evictedValue = oldEntry.value
		}
	}

	// Add new entry
	newEntry := entry[K, V]{key: key, value: value}
	node := c.list.PushFront(newEntry)
	c.elements[key] = node

	return evicted, evictedKey, evictedValue
}

// Remove deletes the entry associated with the key.
//
// Parameters:
//   - key: The key to remove
//
// Returns:
//   - V: The value that was removed
//   - bool: true if the key was found and removed, false otherwise
//
// Time complexity: O(1)
//
// Example:
//
//	if value, ok := cache.Remove("mykey"); ok {
//	    fmt.Println("Removed:", value)
//	}
func (c *LRUCache[K, V]) Remove(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	if node, ok := c.elements[key]; ok {
		val := node.Value.(entry[K, V]).value
		delete(c.elements, key)
		c.list.Remove(node)
		return val, true
	}
	return zero, false
}

// Contains checks if a key exists in the cache without updating its recency.
//
// Parameters:
//   - key: The key to check
//
// Returns:
//   - bool: true if the key exists, false otherwise
//
// Time complexity: O(1)
//
// Example:
//
//	if cache.Contains("mykey") {
//	    fmt.Println("Key exists")
//	}
func (c *LRUCache[K, V]) Contains(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.elements[key]
	return ok
}

// Peek retrieves the value without updating its recency.
// This is useful when you want to inspect a value without affecting LRU order.
//
// Parameters:
//   - key: The key to look up
//
// Returns:
//   - V: The value associated with the key
//   - bool: true if the key was found, false otherwise
//
// Time complexity: O(1)
//
// Example:
//
//	if value, ok := cache.Peek("mykey"); ok {
//	    fmt.Println("Peeked:", value)
//	}
func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var zero V
	if node, ok := c.elements[key]; ok {
		return node.Value.(entry[K, V]).value, true
	}
	return zero, false
}

// Keys returns all keys in the cache in arbitrary order.
// The order does not reflect the LRU order.
//
// Returns:
//   - []K: A slice containing all keys
//
// Time complexity: O(n) where n is the number of items in the cache
// Space complexity: O(n)
//
// Example:
//
//	keys := cache.Keys()
//	fmt.Println("All keys:", keys)
func (c *LRUCache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]K, 0, len(c.elements))
	for k := range c.elements {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the current number of items in the cache.
//
// Returns:
//   - int: The number of items currently in the cache
//
// Time complexity: O(1)
//
// Example:
//
//	fmt.Printf("Cache contains %d items\n", cache.Len())
func (c *LRUCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.list.Len()
}

// Cap returns the maximum capacity of the cache.
//
// Returns:
//   - int: The maximum number of items the cache can hold
//
// Time complexity: O(1)
//
// Example:
//
//	fmt.Printf("Cache capacity: %d\n", cache.Cap())
func (c *LRUCache[K, V]) Cap() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.capacity
}

// Oldest returns the least recently used item without removing it.
//
// Returns:
//   - K: The key of the oldest item
//   - V: The value of the oldest item
//   - error: ErrEmptyCache if the cache is empty
//
// Time complexity: O(1)
//
// Example:
//
//	if key, value, err := cache.Oldest(); err == nil {
//	    fmt.Printf("Oldest: %v=%v\n", key, value)
//	}
func (c *LRUCache[K, V]) Oldest() (K, V, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var zeroK K
	var zeroV V

	if c.list.Len() == 0 {
		return zeroK, zeroV, ErrEmptyCache
	}

	oldest := c.list.Back()
	e := oldest.Value.(entry[K, V])
	return e.key, e.value, nil
}

// Newest returns the most recently used item without removing it.
//
// Returns:
//   - K: The key of the newest item
//   - V: The value of the newest item
//   - error: ErrEmptyCache if the cache is empty
//
// Time complexity: O(1)
//
// Example:
//
//	if key, value, err := cache.Newest(); err == nil {
//	    fmt.Printf("Newest: %v=%v\n", key, value)
//	}
func (c *LRUCache[K, V]) Newest() (K, V, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var zeroK K
	var zeroV V

	if c.list.Len() == 0 {
		return zeroK, zeroV, ErrEmptyCache
	}

	newest := c.list.Front()
	e := newest.Value.(entry[K, V])
	return e.key, e.value, nil
}

// RemoveOldest removes and returns the least recently used item.
//
// Returns:
//   - K: The key of the removed item
//   - V: The value of the removed item
//   - error: ErrEmptyCache if the cache is empty
//
// Time complexity: O(1)
//
// Example:
//
//	if key, value, err := cache.RemoveOldest(); err == nil {
//	    fmt.Printf("Removed oldest: %v=%v\n", key, value)
//	}
func (c *LRUCache[K, V]) RemoveOldest() (K, V, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zeroK K
	var zeroV V

	if c.list.Len() == 0 {
		return zeroK, zeroV, ErrEmptyCache
	}

	oldest := c.list.Back()
	e := oldest.Value.(entry[K, V])
	delete(c.elements, e.key)
	c.list.Remove(oldest)

	return e.key, e.value, nil
}

// Clear removes all items from the cache.
//
// Time complexity: O(1)
//
// Example:
//
//	cache.Clear()
//	fmt.Println("Cache cleared")
func (c *LRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list.Init()
	c.elements = make(map[K]*list.Element, c.capacity)
}

// Resize changes the capacity of the cache.
// If the new capacity is smaller than the current size, the oldest items are evicted.
//
// Parameters:
//   - newCapacity: The new maximum capacity
//
// Returns:
//   - error: ErrInvalidCapacity if newCapacity is less than 1
//
// Time complexity: O(k) where k is the number of items to evict
//
// Example:
//
//	if err := cache.Resize(50); err != nil {
//	    log.Fatal(err)
//	}
func (c *LRUCache[K, V]) Resize(newCapacity int) error {
	if newCapacity < 1 {
		return ErrInvalidCapacity
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.capacity = newCapacity

	// Evict items if necessary
	for c.list.Len() > c.capacity {
		oldest := c.list.Back()
		if oldest != nil {
			e := oldest.Value.(entry[K, V])
			delete(c.elements, e.key)
			c.list.Remove(oldest)
		}
	}

	return nil
}

// String returns a string representation of the cache for debugging.
//
// Returns:
//   - string: A formatted string showing cache statistics
//
// Time complexity: O(1)
//
// Example:
//
//	fmt.Println(cache.String())
func (c *LRUCache[K, V]) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return fmt.Sprintf("LRUCache{capacity: %d, size: %d}", c.capacity, c.list.Len())
}
