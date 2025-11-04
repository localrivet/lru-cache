# ProxyCache - HTTP-Aware LRU Cache

High-performance caching for L4/L7 proxies, API gateways, and HTTP services.

---

## Features

✅ **TTL/Expiration** - Automatic expiry with background cleanup
✅ **Size-Based Eviction** - Limit by bytes, not just count
✅ **Eviction Callbacks** - Cleanup resources on evict
✅ **HTTP Metadata** - Cache age, expiration time, entry size
✅ **Sharding** - 64-128 way parallelism for high concurrency
✅ **Stats Tracking** - Hit/miss ratios, byte usage

---

## Quick Start

### Basic Usage

```go
cache, _ := lru.NewProxyCache[string, []byte](lru.ProxyOptions{
    Capacity:   10000,
    MaxBytes:   500 * 1024 * 1024, // 500MB
    DefaultTTL: 5 * time.Minute,
    Shards:     64,
    TrackStats: true,
})
defer cache.Close()

// Store with default TTL
cache.Put("/api/users", responseBody)

// Retrieve
if response, ok := cache.Get("/api/users"); ok {
    // Cache hit
    sendResponse(response)
}

// Custom TTL
cache.PutWithTTL("/hot-data", data, 30*time.Second)
```

### HTTP Proxy Example

```go
type CachedResponse struct {
    StatusCode int
    Headers    map[string]string
    Body       []byte
}

cache, _ := lru.NewProxyCache[string, *CachedResponse](lru.ProxyOptions{
    Capacity:        10000,
    MaxBytes:        500 * 1024 * 1024,
    DefaultTTL:      5 * time.Minute,
    Shards:          64,
    TrackStats:      true,
    CleanupInterval: 1 * time.Minute,
    SizeFunc: func(v interface{}) int64 {
        return int64(len(v.(*CachedResponse).Body))
    },
    OnEvict: func(key, val interface{}) {
        log.Printf("Evicted: %v", key)
    },
})

// In HTTP handler
func handleRequest(w http.ResponseWriter, r *http.Request) {
    cacheKey := r.URL.Path

    // Check cache
    if cached, ok := cache.Get(cacheKey); ok {
        w.WriteHeader(cached.StatusCode)
        w.Write(cached.Body)
        w.Header().Set("X-Cache", "HIT")
        return
    }

    // Cache miss - fetch from backend
    response := fetchFromBackend(r)
    cache.Put(cacheKey, response)

    w.WriteHeader(response.StatusCode)
    w.Write(response.Body)
    w.Header().Set("X-Cache", "MISS")
}
```

---

## Performance

| Operation | ns/op | Use Case |
|-----------|-------|----------|
| **Put** | 844 | Store response |
| **Get** | 187 | Retrieve cached response |
| **HTTP Response** | 236 | Full workflow |
| **WithTTL** | 842 | Expiring cache |
| **WithSizeLimit** | 778 | Memory-bounded cache |

**Throughput**: 5-9M ops/sec with sharding

---

## Common Patterns

### 1. API Gateway

```go
cache, _ := lru.NewProxyCache[string, []byte](lru.ProxyOptions{
    Capacity:   50000,
    MaxBytes:   1 * 1024 * 1024 * 1024, // 1GB
    DefaultTTL: 5 * time.Minute,
    Shards:     128,
    TrackStats: true,
    SizeFunc: func(v interface{}) int64 {
        return int64(len(v.([]byte)))
    },
})

// Generate cache key with Vary headers
cacheKey := fmt.Sprintf("%s:%s:%s",
    r.URL.Path,
    r.Header.Get("Accept"),
    r.Header.Get("Authorization"),
)

if resp, ok := cache.Get(cacheKey); ok {
    return resp
}
```

### 2. Rate Limiting

```go
type RateLimit struct {
    Requests  int
    ResetTime time.Time
}

rateLimits, _ := lru.NewProxyCache[string, *RateLimit](lru.ProxyOptions{
    Capacity:   100000,
    DefaultTTL: 1 * time.Minute,
    Shards:     64,
})

func checkRateLimit(clientIP string) bool {
    info, ok := rateLimits.Get(clientIP)
    if !ok {
        rateLimits.Put(clientIP, &RateLimit{Requests: 1})
        return true
    }

    if info.Requests >= 100 {
        return false // Rate limited
    }

    info.Requests++
    rateLimits.Put(clientIP, info)
    return true
}
```

### 3. DNS Cache

```go
dnsCache, _ := lru.NewProxyCache[string, []string](lru.ProxyOptions{
    Capacity:   10000,
    DefaultTTL: 5 * time.Minute,
    Shards:     32,
})

func resolveBackend(hostname string) []string {
    if ips, ok := dnsCache.Get(hostname); ok {
        return ips
    }

    ips := net.LookupHost(hostname)
    dnsCache.Put(hostname, ips)
    return ips
}
```

### 4. Session Store

```go
type Session struct {
    UserID string
    Data   map[string]interface{}
}

sessions, _ := lru.NewProxyCache[string, *Session](lru.ProxyOptions{
    Capacity:   100000,
    DefaultTTL: 30 * time.Minute,
    Shards:     64,
    OnEvict: func(key, val interface{}) {
        session := val.(*Session)
        log.Printf("Session expired: %s", session.UserID)
    },
})
```

### 5. L4 TCP Connection Metadata

```go
type ConnectionInfo struct {
    BackendAddr string
    CreatedAt   time.Time
}

connCache, _ := lru.NewProxyCache[string, *ConnectionInfo](lru.ProxyOptions{
    Capacity:   50000,
    DefaultTTL: 10 * time.Minute,
    Shards:     64,
    OnEvict: func(key, val interface{}) {
        closeConnection(val.(*ConnectionInfo).BackendAddr)
    },
})
```

---

## Options Reference

```go
type ProxyOptions struct {
    // Capacity is the maximum number of items (required)
    Capacity int

    // MaxBytes is the maximum total size in bytes (0 = unlimited)
    MaxBytes int64

    // DefaultTTL is the time-to-live for cached items (0 = no expiration)
    DefaultTTL time.Duration

    // CleanupInterval is how often to check for expired items
    // Default: 1 minute when TTL is set
    CleanupInterval time.Duration

    // OnEvict is called when an item is evicted (optional)
    OnEvict func(key interface{}, value interface{})

    // SizeFunc calculates the size of a value in bytes
    // Required if MaxBytes > 0
    SizeFunc func(value interface{}) int64

    // Shards enables internal sharding for concurrency
    // Options: 0 (none), 16, 32, 64, 128
    Shards int

    // TrackStats enables hit/miss statistics
    TrackStats bool
}
```

### Pre-configured Options

```go
// For HTTP response caching
cache, _ := lru.NewProxyCache[string, []byte](
    lru.HTTPCacheOptions(10000, 100*1024*1024), // 10K items, 100MB
)

// Equivalent to:
lru.ProxyOptions{
    Capacity:        10000,
    MaxBytes:        100 * 1024 * 1024,
    DefaultTTL:      5 * time.Minute,
    CleanupInterval: 1 * time.Minute,
    Shards:          64,
    TrackStats:      true,
    SizeFunc:        func(v interface{}) int64 { return 1024 },
}
```

---

## API Methods

### Core Operations

```go
// Put with default TTL
err := cache.Put(key, value)

// Put with custom TTL
err := cache.PutWithTTL(key, value, 30*time.Second)

// Get (auto-removes if expired)
value, ok := cache.Get(key)

// Peek (no LRU update, no expiration check)
value, ok := cache.Peek(key)

// Get with metadata
value, info, ok := cache.GetWithInfo(key)
// info.Age, info.ExpiresIn, info.Size

// Remove
value, ok := cache.Remove(key)

// Check existence
exists := cache.Contains(key)
```

### Management

```go
// Clear all items
cache.Clear()

// Get statistics
stats := cache.Stats()
fmt.Printf("Hit rate: %.2f%%\n", stats.HitRate*100)
fmt.Printf("Memory: %d/%d bytes\n", stats.BytesUsed, stats.MaxBytes)

// Get size info
count := cache.Len()
bytes := cache.BytesUsed()

// Stop background cleanup
cache.Close()
```

---

## Monitoring

### Track Cache Effectiveness

```go
cache, _ := lru.NewProxyCache[string, []byte](lru.ProxyOptions{
    Capacity:   10000,
    TrackStats: true, // Must enable
    Shards:     64,
})

// Periodically log stats
go func() {
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        stats := cache.Stats()
        log.Printf(`Cache Stats:
  Hit Rate: %.2f%%
  Hits: %d, Misses: %d
  Size: %d/%d items
  Memory: %d/%d bytes`,
            stats.HitRate*100,
            stats.Hits,
            stats.Misses,
            stats.Size,
            stats.Capacity,
            stats.BytesUsed,
            stats.MaxBytes,
        )
    }
}()
```

### Prometheus Integration

```go
import "github.com/prometheus/client_golang/prometheus"

var (
    cacheHits = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "proxy_cache_hits_total",
    })
    cacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "proxy_cache_misses_total",
    })
    cacheSize = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "proxy_cache_size_bytes",
    })
)

// Update from cache stats
stats := cache.Stats()
cacheHits.Add(float64(stats.Hits))
cacheMisses.Add(float64(stats.Misses))
cacheSize.Set(float64(stats.BytesUsed))
```

---

## Production Checklist

### 1. Size the Cache

```go
// Calculate based on expected load
avgResponseSize := 10 * 1024 // 10KB
requestsPerSec := 10000
cacheDuration := 5 * time.Minute

capacity := requestsPerSec * int(cacheDuration.Seconds())
maxBytes := int64(capacity) * avgResponseSize
```

### 2. Configure Appropriately

```go
cache, _ := lru.NewProxyCache[string, []byte](lru.ProxyOptions{
    Capacity:        capacity,
    MaxBytes:        maxBytes,
    DefaultTTL:      5 * time.Minute,
    CleanupInterval: 1 * time.Minute,
    Shards:          64, // 2-4x your CPU cores
    TrackStats:      true,
    SizeFunc: func(v interface{}) int64 {
        return int64(len(v.([]byte)))
    },
    OnEvict: func(key, val interface{}) {
        // Cleanup resources
        metrics.IncrementEvictions()
    },
})
defer cache.Close()
```

### 3. Monitor Performance

- Enable `TrackStats: true`
- Log stats periodically
- Export to metrics system (Prometheus, Datadog, etc.)
- Alert on low hit rates

### 4. Handle Edge Cases

```go
// Check for empty cache
if _, ok := cache.Get(key); !ok {
    // Handle cache miss
}

// Always close to stop background goroutine
defer cache.Close()

// Handle size func errors gracefully
SizeFunc: func(v interface{}) int64 {
    if data, ok := v.([]byte); ok {
        return int64(len(data))
    }
    return 1024 // Default estimate
}
```

---

## Use Cases

✅ **L7 HTTP Reverse Proxy** - Response caching with TTL
✅ **API Gateway** - High-throughput request caching
✅ **L4 TCP Proxy** - Connection metadata storage
✅ **Rate Limiting** - State tracking with expiration
✅ **DNS Cache** - Lookup result caching
✅ **Session Store** - TTL-based session management
✅ **Authentication** - Token cache with expiration
✅ **Load Balancer** - Backend health state

---

## Performance Tuning

### Shard Count

| Workload | Recommended Shards |
|----------|-------------------|
| Single-threaded | 0 (no sharding) |
| Low concurrency (2-4 threads) | 16 |
| Medium concurrency (5-10 threads) | 32 |
| High concurrency (10-50 threads) | 64 |
| Extreme concurrency (50+ threads) | 128 |

**Rule of thumb**: Use 2-4x your CPU core count.

### Memory Sizing

```go
// Prevent OOM with MaxBytes
MaxBytes: availableMemory * 0.7 // Use 70% of available memory

// Or size by expected entries
MaxBytes: capacity * avgItemSize
```

### TTL Configuration

```go
// Match HTTP Cache-Control headers
DefaultTTL: 5 * time.Minute

// Or per-request TTL
cache.PutWithTTL(key, value, parseCacheControl(headers))
```

---

## Limitations & Tradeoffs

### Memory Usage
- In-memory only (not persistent)
- Size grows up to `MaxBytes`
- Use `MaxBytes` to prevent OOM

### TTL Precision
- Background cleanup runs every `CleanupInterval`
- Expired items removed on access or cleanup
- Not millisecond-precise

### Sharding Tradeoffs
- More shards = better concurrency
- More shards = slightly more memory
- Optimal: 2-4x CPU cores

### LRU Granularity
- In sharded mode, LRU is per-shard
- `Oldest()` returns oldest from any shard, not global
- Usually fine for most use cases

---

## Summary

ProxyCache is a production-ready HTTP-aware cache for proxies:

✅ **Fast**: 5-9M ops/sec
✅ **HTTP-Aware**: TTL, size limits, callbacks
✅ **Scalable**: Sharding for high concurrency
✅ **Observable**: Built-in stats tracking
✅ **Flexible**: Works for L4, L7, rate limiting, sessions, etc.

**Ready to drop into your proxy and handle millions of requests! 🚀**
