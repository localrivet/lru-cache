package lru

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidCapacity is returned when capacity is less than 1
	ErrInvalidCapacity = errors.New("capacity must be greater than 0")
	// ErrEmptyCache is returned when operations are performed on an empty cache
	ErrEmptyCache = errors.New("cache is empty")
	// ErrKeyNotFound is returned when a key is not found in the cache
	ErrKeyNotFound = errors.New("key not found")
	// ErrInvalidShardCount is returned when shard count is not a power of 2
	ErrInvalidShardCount = errors.New("shard count must be 0 or a power of 2")
)

// LRUCache is a thread-safe, high-performance LRU cache implementation.
//
// It provides O(1) time complexity for all operations and supports optional
// sharding for extreme concurrent throughput (millions of ops/sec).
//
// Usage:
//   - Use New() for standard performance
//   - Use NewWithOptions(capacity, HighThroughputOptions()) for ETL workloads
//
// Type parameters:
//   - K: The key type, must be comparable
//   - V: The value type, can be any type
type LRUCache[K comparable, V any] struct {
	// For non-sharded mode
	capacity int
	list     *list.List
	elements map[K]*list.Element
	mu       sync.RWMutex

	// For sharded mode
	shards    []shard[K, V]
	shardMask uint32
	isSharded bool

	// Statistics (optional)
	hits       atomic.Uint64
	misses     atomic.Uint64
	trackStats bool
}

// entry represents a key-value pair stored in the cache
type entry[K comparable, V any] struct {
	key   K
	value V
}

// New creates a new LRUCache with default options (no sharding).
//
// For high-performance ETL workloads, use NewWithOptions() instead.
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
	return NewWithOptions[K, V](capacity, DefaultOptions())
}

// NewWithOptions creates a new LRUCache with custom options for maximum performance.
//
// Use this for ETL workloads and high-concurrency scenarios.
//
// Parameters:
//   - capacity: Maximum number of items the cache can hold
//   - opts: Configuration options (use HighThroughputOptions() for ETL)
//
// Returns:
//   - *LRUCache[K, V]: A new cache instance
//   - error: Error if parameters are invalid
//
// Example for ETL:
//
//	cache, err := lru.NewWithOptions[string, Record](1000000, lru.HighThroughputOptions())
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewWithOptions[K comparable, V any](capacity int, opts Options) (*LRUCache[K, V], error) {
	if capacity < 1 {
		return nil, ErrInvalidCapacity
	}

	cache := &LRUCache[K, V]{
		trackStats: opts.TrackStats,
	}

	// Non-sharded mode
	if opts.Shards == 0 {
		cache.capacity = capacity
		cache.list = list.New()
		cache.elements = make(map[K]*list.Element, capacity)
		cache.isSharded = false
		return cache, nil
	}

	// Sharded mode - validate shard count is power of 2
	if opts.Shards&(opts.Shards-1) != 0 {
		return nil, ErrInvalidShardCount
	}

	cache.isSharded = true
	cache.shardMask = uint32(opts.Shards - 1)
	cache.shards = make([]shard[K, V], opts.Shards)

	capacityPerShard := capacity / opts.Shards
	if capacityPerShard < 1 {
		capacityPerShard = 1
	}

	for i := 0; i < opts.Shards; i++ {
		cache.shards[i] = shard[K, V]{
			cache: &simpleLRU[K, V]{
				capacity: capacityPerShard,
				list:     list.New(),
				elements: make(map[K]*list.Element, capacityPerShard),
			},
		}
	}

	return cache, nil
}

// getShard returns the shard for a key (sharded mode only)
func (c *LRUCache[K, V]) getShard(key K) *shard[K, V] {
	index := hash(key, c.shardMask)
	return &c.shards[index]
}

// Get retrieves the value associated with the key and marks it as recently used.
//
// Time complexity: O(1)
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	if c.isSharded {
		shard := c.getShard(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()

		val, ok := shard.cache.get(key)
		if c.trackStats {
			if ok {
				c.hits.Add(1)
			} else {
				c.misses.Add(1)
			}
		}
		return val, ok
	}

	// Non-sharded
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	if node, ok := c.elements[key]; ok {
		c.list.MoveToFront(node)
		if c.trackStats {
			c.hits.Add(1)
		}
		return node.Value.(entry[K, V]).value, true
	}
	if c.trackStats {
		c.misses.Add(1)
	}
	return zero, false
}

// Put inserts or updates a key-value pair in the cache.
//
// Time complexity: O(1)
func (c *LRUCache[K, V]) Put(key K, value V) (evicted bool, evictedKey K, evictedValue V) {
	if c.isSharded {
		shard := c.getShard(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()

		return shard.cache.put(key, value)
	}

	// Non-sharded
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

// GetOrCompute retrieves a value or computes it if missing (atomic operation).
//
// This is perfect for ETL lookup patterns with fallback to external sources.
//
// Example:
//
//	user, cached := cache.GetOrCompute("user_123", func() User {
//	    return database.FetchUser("user_123")
//	})
func (c *LRUCache[K, V]) GetOrCompute(key K, compute func() V) (V, bool) {
	if c.isSharded {
		shard := c.getShard(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()

		if val, ok := shard.cache.get(key); ok {
			if c.trackStats {
				c.hits.Add(1)
			}
			return val, true
		}

		if c.trackStats {
			c.misses.Add(1)
		}
		value := compute()
		shard.cache.put(key, value)
		return value, false
	}

	// Non-sharded
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.elements[key]; ok {
		c.list.MoveToFront(node)
		if c.trackStats {
			c.hits.Add(1)
		}
		return node.Value.(entry[K, V]).value, true
	}

	if c.trackStats {
		c.misses.Add(1)
	}
	value := compute()

	// Add to cache
	if c.list.Len() >= c.capacity && c.list.Len() > 0 {
		oldest := c.list.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(entry[K, V])
			delete(c.elements, oldEntry.key)
			c.list.Remove(oldest)
		}
	}

	newEntry := entry[K, V]{key: key, value: value}
	node := c.list.PushFront(newEntry)
	c.elements[key] = node

	return value, false
}

// BatchPut inserts multiple key-value pairs efficiently.
//
// In sharded mode, this processes shards in parallel for maximum throughput.
// Perfect for ETL bulk loading.
//
// Example:
//
//	records := map[string]Record{
//	    "id1": record1,
//	    "id2": record2,
//	    // ... thousands more
//	}
//	cache.BatchPut(records)
func (c *LRUCache[K, V]) BatchPut(items map[K]V) {
	if c.isSharded {
		// Group items by shard
		type batch struct {
			items []struct {
				key   K
				value V
			}
		}
		batches := make([]batch, len(c.shards))

		for key, value := range items {
			index := hash(key, c.shardMask)
			batches[index].items = append(batches[index].items, struct {
				key   K
				value V
			}{key, value})
		}

		// Process shards in parallel
		var wg sync.WaitGroup
		for i, b := range batches {
			if len(b.items) == 0 {
				continue
			}
			wg.Add(1)
			go func(shardIndex int, items []struct {
				key   K
				value V
			}) {
				defer wg.Done()
				shard := &c.shards[shardIndex]
				shard.mu.Lock()
				defer shard.mu.Unlock()

				for _, item := range items {
					shard.cache.put(item.key, item.value)
				}
			}(i, b.items)
		}
		wg.Wait()
		return
	}

	// Non-sharded
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, value := range items {
		// Update existing
		if node, ok := c.elements[key]; ok {
			c.list.MoveToFront(node)
			node.Value = entry[K, V]{key: key, value: value}
			continue
		}

		// Evict if needed
		if c.list.Len() >= c.capacity {
			oldest := c.list.Back()
			if oldest != nil {
				oldEntry := oldest.Value.(entry[K, V])
				delete(c.elements, oldEntry.key)
				c.list.Remove(oldest)
			}
		}

		// Add new
		newEntry := entry[K, V]{key: key, value: value}
		node := c.list.PushFront(newEntry)
		c.elements[key] = node
	}
}

// BatchGet retrieves multiple values efficiently.
//
// In sharded mode, this queries shards in parallel.
//
// Example:
//
//	keys := []string{"id1", "id2", "id3"}
//	results := cache.BatchGet(keys)
//	for key, value := range results {
//	    process(key, value)
//	}
func (c *LRUCache[K, V]) BatchGet(keys []K) map[K]V {
	results := make(map[K]V)

	if c.isSharded {
		var mu sync.Mutex

		// Group keys by shard
		type batch struct {
			keys []K
		}
		batches := make([]batch, len(c.shards))

		for _, key := range keys {
			index := hash(key, c.shardMask)
			batches[index].keys = append(batches[index].keys, key)
		}

		// Process shards in parallel
		var wg sync.WaitGroup
		for i, b := range batches {
			if len(b.keys) == 0 {
				continue
			}
			wg.Add(1)
			go func(shardIndex int, keys []K) {
				defer wg.Done()
				shard := &c.shards[shardIndex]
				shard.mu.Lock()
				defer shard.mu.Unlock()

				for _, key := range keys {
					if val, ok := shard.cache.get(key); ok {
						mu.Lock()
						results[key] = val
						mu.Unlock()
						if c.trackStats {
							c.hits.Add(1)
						}
					} else {
						if c.trackStats {
							c.misses.Add(1)
						}
					}
				}
			}(i, b.keys)
		}
		wg.Wait()
		return results
	}

	// Non-sharded
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, key := range keys {
		if node, ok := c.elements[key]; ok {
			c.list.MoveToFront(node)
			results[key] = node.Value.(entry[K, V]).value
			if c.trackStats {
				c.hits.Add(1)
			}
		} else {
			if c.trackStats {
				c.misses.Add(1)
			}
		}
	}

	return results
}

// Remove deletes the entry associated with the key.
//
// Time complexity: O(1)
func (c *LRUCache[K, V]) Remove(key K) (V, bool) {
	if c.isSharded {
		shard := c.getShard(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()

		return shard.cache.remove(key)
	}

	// Non-sharded
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
// Time complexity: O(1)
func (c *LRUCache[K, V]) Contains(key K) bool {
	if c.isSharded {
		shard := c.getShard(key)
		shard.mu.RLock()
		defer shard.mu.RUnlock()

		return shard.cache.contains(key)
	}

	// Non-sharded
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.elements[key]
	return ok
}

// Peek retrieves the value without updating its recency.
//
// Time complexity: O(1)
func (c *LRUCache[K, V]) Peek(key K) (V, bool) {
	if c.isSharded {
		shard := c.getShard(key)
		shard.mu.RLock()
		defer shard.mu.RUnlock()

		return shard.cache.peek(key)
	}

	// Non-sharded
	c.mu.RLock()
	defer c.mu.RUnlock()

	var zero V
	if node, ok := c.elements[key]; ok {
		return node.Value.(entry[K, V]).value, true
	}
	return zero, false
}

// Keys returns all keys in the cache in arbitrary order.
//
// Time complexity: O(n)
func (c *LRUCache[K, V]) Keys() []K {
	if c.isSharded {
		var allKeys []K
		var mu sync.Mutex

		var wg sync.WaitGroup
		for i := range c.shards {
			wg.Add(1)
			go func(shardIndex int) {
				defer wg.Done()
				shard := &c.shards[shardIndex]
				shard.mu.RLock()
				defer shard.mu.RUnlock()

				shardKeys := make([]K, 0, len(shard.cache.elements))
				for k := range shard.cache.elements {
					shardKeys = append(shardKeys, k)
				}

				mu.Lock()
				allKeys = append(allKeys, shardKeys...)
				mu.Unlock()
			}(i)
		}
		wg.Wait()
		return allKeys
	}

	// Non-sharded
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
// Time complexity: O(1) non-sharded, O(shards) sharded
func (c *LRUCache[K, V]) Len() int {
	if c.isSharded {
		total := 0
		for i := range c.shards {
			c.shards[i].mu.RLock()
			total += c.shards[i].cache.len()
			c.shards[i].mu.RUnlock()
		}
		return total
	}

	// Non-sharded
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.list.Len()
}

// Cap returns the maximum capacity of the cache.
//
// Time complexity: O(1)
func (c *LRUCache[K, V]) Cap() int {
	if c.isSharded {
		total := 0
		for i := range c.shards {
			total += c.shards[i].cache.capacity
		}
		return total
	}

	// Non-sharded
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.capacity
}

// Oldest returns the least recently used item without removing it.
//
// Note: In sharded mode, returns the oldest across any shard (not global oldest).
//
// Time complexity: O(1) non-sharded, O(shards) sharded
func (c *LRUCache[K, V]) Oldest() (K, V, error) {
	if c.isSharded {
		var oldestKey K
		var oldestVal V
		found := false

		for i := range c.shards {
			c.shards[i].mu.RLock()
			if c.shards[i].cache.list.Len() > 0 {
				oldest := c.shards[i].cache.list.Back()
				e := oldest.Value.(entry[K, V])
				if !found {
					oldestKey = e.key
					oldestVal = e.value
					found = true
				}
			}
			c.shards[i].mu.RUnlock()
		}

		if !found {
			var zeroK K
			var zeroV V
			return zeroK, zeroV, ErrEmptyCache
		}
		return oldestKey, oldestVal, nil
	}

	// Non-sharded
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
// Note: In sharded mode, returns the newest across any shard (not global newest).
//
// Time complexity: O(1) non-sharded, O(shards) sharded
func (c *LRUCache[K, V]) Newest() (K, V, error) {
	if c.isSharded {
		var newestKey K
		var newestVal V
		found := false

		for i := range c.shards {
			c.shards[i].mu.RLock()
			if c.shards[i].cache.list.Len() > 0 {
				newest := c.shards[i].cache.list.Front()
				e := newest.Value.(entry[K, V])
				if !found {
					newestKey = e.key
					newestVal = e.value
					found = true
				}
			}
			c.shards[i].mu.RUnlock()
		}

		if !found {
			var zeroK K
			var zeroV V
			return zeroK, zeroV, ErrEmptyCache
		}
		return newestKey, newestVal, nil
	}

	// Non-sharded
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
// Time complexity: O(1) non-sharded, O(shards) sharded
func (c *LRUCache[K, V]) RemoveOldest() (K, V, error) {
	if c.isSharded {
		// Find shard with oldest item
		var oldestShard *shard[K, V]
		for i := range c.shards {
			c.shards[i].mu.RLock()
			if c.shards[i].cache.list.Len() > 0 {
				if oldestShard == nil {
					oldestShard = &c.shards[i]
				}
			}
			c.shards[i].mu.RUnlock()
		}

		if oldestShard == nil {
			var zeroK K
			var zeroV V
			return zeroK, zeroV, ErrEmptyCache
		}

		oldestShard.mu.Lock()
		defer oldestShard.mu.Unlock()

		if oldestShard.cache.list.Len() == 0 {
			var zeroK K
			var zeroV V
			return zeroK, zeroV, ErrEmptyCache
		}

		oldest := oldestShard.cache.list.Back()
		e := oldest.Value.(entry[K, V])
		delete(oldestShard.cache.elements, e.key)
		oldestShard.cache.list.Remove(oldest)

		return e.key, e.value, nil
	}

	// Non-sharded
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
// Time complexity: O(1) non-sharded, O(shards) sharded
func (c *LRUCache[K, V]) Clear() {
	if c.isSharded {
		var wg sync.WaitGroup
		for i := range c.shards {
			wg.Add(1)
			go func(shardIndex int) {
				defer wg.Done()
				shard := &c.shards[shardIndex]
				shard.mu.Lock()
				defer shard.mu.Unlock()
				shard.cache.clear()
			}(i)
		}
		wg.Wait()

		if c.trackStats {
			c.hits.Store(0)
			c.misses.Store(0)
		}
		return
	}

	// Non-sharded
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list.Init()
	c.elements = make(map[K]*list.Element, c.capacity)

	if c.trackStats {
		c.hits.Store(0)
		c.misses.Store(0)
	}
}

// Resize changes the capacity of the cache.
//
// Time complexity: O(k) where k is the number of items to evict
func (c *LRUCache[K, V]) Resize(newCapacity int) error {
	if newCapacity < 1 {
		return ErrInvalidCapacity
	}

	if c.isSharded {
		capacityPerShard := newCapacity / len(c.shards)
		if capacityPerShard < 1 {
			capacityPerShard = 1
		}

		for i := range c.shards {
			c.shards[i].mu.Lock()
			c.shards[i].cache.capacity = capacityPerShard

			// Evict if needed
			for c.shards[i].cache.list.Len() > capacityPerShard {
				oldest := c.shards[i].cache.list.Back()
				if oldest != nil {
					e := oldest.Value.(entry[K, V])
					delete(c.shards[i].cache.elements, e.key)
					c.shards[i].cache.list.Remove(oldest)
				}
			}
			c.shards[i].mu.Unlock()
		}
		return nil
	}

	// Non-sharded
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

// Stats returns cache performance statistics (only if TrackStats is enabled).
//
// Use this to monitor cache effectiveness in ETL pipelines.
func (c *LRUCache[K, V]) Stats() Stats {
	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses

	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	shardCount := 0
	if c.isSharded {
		shardCount = len(c.shards)
	}

	return Stats{
		Hits:     hits,
		Misses:   misses,
		HitRate:  hitRate,
		Size:     c.Len(),
		Capacity: c.Cap(),
		Shards:   shardCount,
	}
}

// ResetStats resets the hit/miss counters to zero.
func (c *LRUCache[K, V]) ResetStats() {
	c.hits.Store(0)
	c.misses.Store(0)
}

// String returns a string representation of the cache for debugging.
func (c *LRUCache[K, V]) String() string {
	if c.isSharded {
		stats := c.Stats()
		return fmt.Sprintf("LRUCache{sharded: true, shards: %d, capacity: %d, size: %d, hitRate: %.2f%%}",
			stats.Shards, stats.Capacity, stats.Size, stats.HitRate*100)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return fmt.Sprintf("LRUCache{sharded: false, capacity: %d, size: %d}", c.capacity, c.list.Len())
}
