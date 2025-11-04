package lru

import (
	"fmt"
	"sync"
	"time"
)

// ProxyCache is an HTTP-aware LRU cache with TTL, size limits, and eviction callbacks.
// Designed for L4/L7 proxies, API gateways, and HTTP caching.
type ProxyCache[K comparable, V any] struct {
	cache           *LRUCache[K, *proxyEntry[V]]
	maxBytes        int64
	currentBytes    int64
	bytesMu         sync.RWMutex
	onEvict         func(key K, value V)
	sizeFunc        func(V) int64
	defaultTTL      time.Duration
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
}

// proxyEntry wraps a value with metadata
type proxyEntry[V any] struct {
	value      V
	size       int64
	expiresAt  time.Time
	insertedAt time.Time
}

// ProxyOptions configures a ProxyCache
type ProxyOptions struct {
	// Capacity is the maximum number of items (required)
	Capacity int

	// MaxBytes is the maximum total size in bytes (0 = unlimited)
	MaxBytes int64

	// DefaultTTL is the time-to-live for cached items (0 = no expiration)
	DefaultTTL time.Duration

	// CleanupInterval is how often to check for expired items (default: 1 minute)
	// Set to 0 to disable automatic cleanup
	CleanupInterval time.Duration

	// OnEvict is called when an item is evicted (optional)
	OnEvict func(key interface{}, value interface{})

	// SizeFunc calculates the size of a value in bytes (required if MaxBytes > 0)
	SizeFunc func(value interface{}) int64

	// Shards enables internal sharding for concurrency (0, 16, 32, 64, 128)
	Shards int

	// TrackStats enables hit/miss statistics
	TrackStats bool
}

// NewProxyCache creates a new HTTP-aware cache with TTL and size limits.
//
// Example:
//
//	cache, err := lru.NewProxyCache[string, []byte](lru.ProxyOptions{
//	    Capacity:        10000,
//	    MaxBytes:        100 * 1024 * 1024, // 100MB
//	    DefaultTTL:      5 * time.Minute,
//	    Shards:          64,
//	    TrackStats:      true,
//	    SizeFunc: func(v interface{}) int64 {
//	        return int64(len(v.([]byte)))
//	    },
//	    OnEvict: func(key, val interface{}) {
//	        log.Printf("Evicted: %v", key)
//	    },
//	})
func NewProxyCache[K comparable, V any](opts ProxyOptions) (*ProxyCache[K, V], error) {
	if opts.Capacity < 1 {
		return nil, ErrInvalidCapacity
	}

	if opts.MaxBytes > 0 && opts.SizeFunc == nil {
		return nil, fmt.Errorf("SizeFunc required when MaxBytes is set")
	}

	if opts.CleanupInterval == 0 && opts.DefaultTTL > 0 {
		opts.CleanupInterval = 1 * time.Minute
	}

	// Create underlying LRU cache
	var cache *LRUCache[K, *proxyEntry[V]]
	var err error

	if opts.Shards > 0 || opts.TrackStats {
		cache, err = NewWithOptions[K, *proxyEntry[V]](
			opts.Capacity,
			Options{
				Shards:     opts.Shards,
				TrackStats: opts.TrackStats,
			},
		)
	} else {
		cache, err = New[K, *proxyEntry[V]](opts.Capacity)
	}

	if err != nil {
		return nil, err
	}

	pc := &ProxyCache[K, V]{
		cache:           cache,
		maxBytes:        opts.MaxBytes,
		defaultTTL:      opts.DefaultTTL,
		cleanupInterval: opts.CleanupInterval,
		stopCleanup:     make(chan struct{}),
	}

	// Wrap SizeFunc with type assertion
	if opts.SizeFunc != nil {
		pc.sizeFunc = func(v V) int64 {
			return opts.SizeFunc(v)
		}
	}

	// Wrap OnEvict with type assertions
	if opts.OnEvict != nil {
		pc.onEvict = func(key K, value V) {
			opts.OnEvict(key, value)
		}
	}

	// Start background cleanup if TTL is enabled
	if opts.DefaultTTL > 0 && opts.CleanupInterval > 0 {
		go pc.cleanupLoop()
	}

	return pc, nil
}

// Put inserts or updates a value with the default TTL.
func (pc *ProxyCache[K, V]) Put(key K, value V) error {
	return pc.PutWithTTL(key, value, pc.defaultTTL)
}

// PutWithTTL inserts or updates a value with a custom TTL.
// Set ttl to 0 for no expiration.
func (pc *ProxyCache[K, V]) PutWithTTL(key K, value V, ttl time.Duration) error {
	size := int64(0)
	if pc.sizeFunc != nil {
		size = pc.sizeFunc(value)
	}

	// Check size limit
	if pc.maxBytes > 0 {
		pc.bytesMu.RLock()
		needsSpace := pc.currentBytes+size > pc.maxBytes
		pc.bytesMu.RUnlock()

		if needsSpace {
			// Evict until we have space
			for pc.currentBytes+size > pc.maxBytes {
				evictedKey, evictedEntry, err := pc.cache.RemoveOldest()
				if err != nil {
					break
				}
				pc.bytesMu.Lock()
				pc.currentBytes -= evictedEntry.size
				pc.bytesMu.Unlock()

				if pc.onEvict != nil {
					pc.onEvict(evictedKey, evictedEntry.value)
				}
			}
		}
	}

	entry := &proxyEntry[V]{
		value:      value,
		size:       size,
		insertedAt: time.Now(),
	}

	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}

	// Put in cache
	evicted, evictedKey, evictedEntry := pc.cache.Put(key, entry)

	// Update byte count
	if pc.sizeFunc != nil {
		pc.bytesMu.Lock()
		pc.currentBytes += size
		if evicted {
			pc.currentBytes -= evictedEntry.size
		}
		pc.bytesMu.Unlock()
	}

	// Call eviction callback
	if evicted && pc.onEvict != nil {
		pc.onEvict(evictedKey, evictedEntry.value)
	}

	return nil
}

// Get retrieves a value if it exists and hasn't expired.
func (pc *ProxyCache[K, V]) Get(key K) (V, bool) {
	var zero V

	entry, ok := pc.cache.Get(key)
	if !ok {
		return zero, false
	}

	// Check expiration
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		// Expired - remove it
		pc.Remove(key)
		return zero, false
	}

	return entry.value, true
}

// Peek retrieves a value without updating LRU order.
func (pc *ProxyCache[K, V]) Peek(key K) (V, bool) {
	var zero V

	entry, ok := pc.cache.Peek(key)
	if !ok {
		return zero, false
	}

	// Check expiration
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return zero, false
	}

	return entry.value, true
}

// GetWithInfo retrieves a value along with metadata.
type CacheEntryInfo struct {
	Age       time.Duration // How long the entry has been cached
	ExpiresIn time.Duration // Time until expiration (0 if no TTL)
	Size      int64         // Size in bytes
}

func (pc *ProxyCache[K, V]) GetWithInfo(key K) (V, CacheEntryInfo, bool) {
	var zero V
	var info CacheEntryInfo

	entry, ok := pc.cache.Get(key)
	if !ok {
		return zero, info, false
	}

	now := time.Now()

	// Check expiration
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		pc.Remove(key)
		return zero, info, false
	}

	info.Age = now.Sub(entry.insertedAt)
	if !entry.expiresAt.IsZero() {
		info.ExpiresIn = entry.expiresAt.Sub(now)
	}
	info.Size = entry.size

	return entry.value, info, true
}

// Remove deletes an entry from the cache.
func (pc *ProxyCache[K, V]) Remove(key K) (V, bool) {
	var zero V

	entry, ok := pc.cache.Remove(key)
	if !ok {
		return zero, false
	}

	// Update byte count
	if pc.sizeFunc != nil {
		pc.bytesMu.Lock()
		pc.currentBytes -= entry.size
		pc.bytesMu.Unlock()
	}

	// Call eviction callback
	if pc.onEvict != nil {
		pc.onEvict(key, entry.value)
	}

	return entry.value, true
}

// Contains checks if a key exists (doesn't check expiration).
func (pc *ProxyCache[K, V]) Contains(key K) bool {
	return pc.cache.Contains(key)
}

// Clear removes all entries.
func (pc *ProxyCache[K, V]) Clear() {
	// Get all keys first
	keys := pc.cache.Keys()

	// Remove each one to trigger callbacks
	for _, key := range keys {
		pc.Remove(key)
	}

	pc.bytesMu.Lock()
	pc.currentBytes = 0
	pc.bytesMu.Unlock()
}

// Len returns the number of items in the cache.
func (pc *ProxyCache[K, V]) Len() int {
	return pc.cache.Len()
}

// BytesUsed returns the total bytes currently cached.
func (pc *ProxyCache[K, V]) BytesUsed() int64 {
	pc.bytesMu.RLock()
	defer pc.bytesMu.RUnlock()
	return pc.currentBytes
}

// Stats returns cache statistics.
func (pc *ProxyCache[K, V]) Stats() ProxyCacheStats {
	baseStats := pc.cache.Stats()

	pc.bytesMu.RLock()
	bytesUsed := pc.currentBytes
	pc.bytesMu.RUnlock()

	return ProxyCacheStats{
		Hits:      baseStats.Hits,
		Misses:    baseStats.Misses,
		HitRate:   baseStats.HitRate,
		Size:      baseStats.Size,
		Capacity:  baseStats.Capacity,
		Shards:    baseStats.Shards,
		BytesUsed: bytesUsed,
		MaxBytes:  pc.maxBytes,
	}
}

// ProxyCacheStats contains proxy cache statistics.
type ProxyCacheStats struct {
	Hits      uint64
	Misses    uint64
	HitRate   float64
	Size      int
	Capacity  int
	Shards    int
	BytesUsed int64
	MaxBytes  int64
}

// cleanupLoop periodically removes expired entries.
func (pc *ProxyCache[K, V]) cleanupLoop() {
	ticker := time.NewTicker(pc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pc.removeExpired()
		case <-pc.stopCleanup:
			return
		}
	}
}

// removeExpired removes all expired entries.
func (pc *ProxyCache[K, V]) removeExpired() {
	now := time.Now()
	keys := pc.cache.Keys()

	for _, key := range keys {
		entry, ok := pc.cache.Peek(key)
		if !ok {
			continue
		}

		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			pc.Remove(key)
		}
	}
}

// Close stops the background cleanup goroutine.
func (pc *ProxyCache[K, V]) Close() {
	close(pc.stopCleanup)
}

// HTTPCacheOptions returns pre-configured options for HTTP response caching.
//
// Example:
//
//	cache, _ := lru.NewProxyCache[string, *http.Response](
//	    lru.HTTPCacheOptions(10000, 100*1024*1024), // 10K items, 100MB
//	)
func HTTPCacheOptions(capacity int, maxBytes int64) ProxyOptions {
	return ProxyOptions{
		Capacity:        capacity,
		MaxBytes:        maxBytes,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		Shards:          64,
		TrackStats:      true,
		SizeFunc: func(v interface{}) int64 {
			// Approximate HTTP response size
			// In real usage, you'd measure actual response body size
			return 1024 // Default 1KB per response
		},
	}
}
