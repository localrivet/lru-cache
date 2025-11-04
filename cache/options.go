package lru

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"sync"
)

// Options configures the behavior of an LRU cache.
type Options struct {
	// Shards enables sharding for massive concurrent throughput.
	// Must be a power of 2 (e.g., 16, 32, 64, 128).
	//
	// Performance guidance:
	//   - 0 (default): Single lock, good for <100K ops/sec
	//   - 16-32: Good for general high-concurrency workloads
	//   - 64-128: Insane performance for ETL (millions of ops/sec)
	//
	// Sharding reduces lock contention by partitioning the cache.
	// Each shard has its own lock, enabling near-linear scaling.
	Shards int

	// TrackStats enables hit/miss ratio tracking.
	// Adds ~1-2ns overhead per operation.
	// Useful for monitoring cache effectiveness in ETL pipelines.
	TrackStats bool
}

// DefaultOptions returns standard options (no sharding, no stats).
func DefaultOptions() Options {
	return Options{
		Shards:     0,
		TrackStats: false,
	}
}

// HighThroughputOptions returns options optimized for ETL workloads.
// Enables 64-way sharding and stats tracking for maximum performance.
func HighThroughputOptions() Options {
	return Options{
		Shards:     64,
		TrackStats: true,
	}
}

// Stats contains cache performance statistics.
type Stats struct {
	Hits     uint64  // Number of cache hits
	Misses   uint64  // Number of cache misses
	HitRate  float64 // Hit rate as percentage (0.0 to 1.0)
	Size     int     // Current number of items
	Capacity int     // Maximum capacity
	Shards   int     // Number of shards (0 = not sharded)
}

// shard represents an internal cache partition
type shard[K comparable, V any] struct {
	cache *simpleLRU[K, V]
	mu    sync.RWMutex
}

// simpleLRU is the non-sharded implementation
type simpleLRU[K comparable, V any] struct {
	capacity int
	list     *list.List
	elements map[K]*list.Element
}

func (s *simpleLRU[K, V]) get(key K) (V, bool) {
	var zero V
	if node, ok := s.elements[key]; ok {
		s.list.MoveToFront(node)
		return node.Value.(entry[K, V]).value, true
	}
	return zero, false
}

func (s *simpleLRU[K, V]) put(key K, value V) (evicted bool, evictedKey K, evictedValue V) {
	// Update existing
	if node, ok := s.elements[key]; ok {
		s.list.MoveToFront(node)
		node.Value = entry[K, V]{key: key, value: value}
		return false, evictedKey, evictedValue
	}

	// Evict if at capacity
	if s.list.Len() >= s.capacity {
		oldest := s.list.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(entry[K, V])
			delete(s.elements, oldEntry.key)
			s.list.Remove(oldest)
			evicted = true
			evictedKey = oldEntry.key
			evictedValue = oldEntry.value
		}
	}

	// Add new
	newEntry := entry[K, V]{key: key, value: value}
	node := s.list.PushFront(newEntry)
	s.elements[key] = node

	return evicted, evictedKey, evictedValue
}

func (s *simpleLRU[K, V]) remove(key K) (V, bool) {
	var zero V
	if node, ok := s.elements[key]; ok {
		val := node.Value.(entry[K, V]).value
		delete(s.elements, key)
		s.list.Remove(node)
		return val, true
	}
	return zero, false
}

func (s *simpleLRU[K, V]) peek(key K) (V, bool) {
	var zero V
	if node, ok := s.elements[key]; ok {
		return node.Value.(entry[K, V]).value, true
	}
	return zero, false
}

func (s *simpleLRU[K, V]) contains(key K) bool {
	_, ok := s.elements[key]
	return ok
}

func (s *simpleLRU[K, V]) len() int {
	return s.list.Len()
}

func (s *simpleLRU[K, V]) clear() {
	s.list.Init()
	s.elements = make(map[K]*list.Element, s.capacity)
}

// hash computes shard index for a key
func hash[K comparable](key K, mask uint32) uint32 {
	h := fnv.New32a()
	// Note: In production, you might want a more sophisticated hash
	// depending on your key types. This works well for most cases.
	fmt.Fprintf(h, "%v", key)
	return h.Sum32() & mask
}
