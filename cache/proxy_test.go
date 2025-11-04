package lru

import (
	"fmt"
	"testing"
	"time"
)

func TestProxyCache_Basic(t *testing.T) {
	cache, err := NewProxyCache[string, []byte](ProxyOptions{
		Capacity: 100,
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Put and Get
	data := []byte("hello world")
	cache.Put("key1", data)

	if val, ok := cache.Get("key1"); !ok || string(val) != "hello world" {
		t.Error("Failed to get value")
	}

	// Non-existent key
	if _, ok := cache.Get("nonexistent"); ok {
		t.Error("Should not find nonexistent key")
	}
}

func TestProxyCache_TTL(t *testing.T) {
	cache, err := NewProxyCache[string, string](ProxyOptions{
		Capacity:        10,
		DefaultTTL:      100 * time.Millisecond,
		CleanupInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")

	// Should exist immediately
	if _, ok := cache.Get("key1"); !ok {
		t.Error("Key should exist")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should be expired
	if _, ok := cache.Get("key1"); ok {
		t.Error("Key should have expired")
	}
}

func TestProxyCache_CustomTTL(t *testing.T) {
	cache, err := NewProxyCache[string, string](ProxyOptions{
		Capacity:   10,
		DefaultTTL: 1 * time.Hour, // Long default
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Put with short custom TTL
	cache.PutWithTTL("shortlived", "value", 50*time.Millisecond)
	cache.Put("longlived", "value") // Uses default 1 hour

	time.Sleep(100 * time.Millisecond)

	// Short should be expired
	if _, ok := cache.Get("shortlived"); ok {
		t.Error("Short-lived key should have expired")
	}

	// Long should still exist
	if _, ok := cache.Get("longlived"); !ok {
		t.Error("Long-lived key should still exist")
	}
}

func TestProxyCache_SizeLimit(t *testing.T) {
	cache, err := NewProxyCache[int, []byte](ProxyOptions{
		Capacity: 100,
		MaxBytes: 100, // Only 100 bytes total
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add 50 bytes
	cache.Put(1, make([]byte, 50))

	// Add another 50 bytes (total 100)
	cache.Put(2, make([]byte, 50))

	// Add 50 more (should evict first entry)
	cache.Put(3, make([]byte, 50))

	// Key 1 should be evicted
	if _, ok := cache.Get(1); ok {
		t.Error("Key 1 should have been evicted due to size limit")
	}

	// Keys 2 and 3 should exist
	if _, ok := cache.Get(2); !ok {
		t.Error("Key 2 should exist")
	}
	if _, ok := cache.Get(3); !ok {
		t.Error("Key 3 should exist")
	}

	// Check byte usage
	if cache.BytesUsed() != 100 {
		t.Errorf("Expected 100 bytes used, got %d", cache.BytesUsed())
	}
}

func TestProxyCache_EvictionCallback(t *testing.T) {
	evicted := make(map[int]string)

	cache, err := NewProxyCache[int, string](ProxyOptions{
		Capacity: 2,
		OnEvict: func(key, val interface{}) {
			evicted[key.(int)] = val.(string)
		},
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three") // Should evict key 1

	// Check callback was called
	if val, ok := evicted[1]; !ok || val != "one" {
		t.Error("Eviction callback not called for key 1")
	}

	// Explicit remove should also trigger callback
	cache.Remove(2)
	if val, ok := evicted[2]; !ok || val != "two" {
		t.Error("Eviction callback not called on explicit remove")
	}
}

func TestProxyCache_GetWithInfo(t *testing.T) {
	cache, err := NewProxyCache[string, []byte](ProxyOptions{
		Capacity:   10,
		DefaultTTL: 1 * time.Second,
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	data := []byte("test data")
	cache.Put("key1", data)

	time.Sleep(100 * time.Millisecond)

	val, info, ok := cache.GetWithInfo("key1")
	if !ok {
		t.Fatal("Key should exist")
	}

	if string(val) != "test data" {
		t.Error("Wrong value returned")
	}

	if info.Age < 100*time.Millisecond {
		t.Error("Age should be at least 100ms")
	}

	if info.ExpiresIn > 1*time.Second {
		t.Error("ExpiresIn should be less than 1s")
	}

	if info.Size != int64(len(data)) {
		t.Errorf("Expected size %d, got %d", len(data), info.Size)
	}
}

func TestProxyCache_Peek(t *testing.T) {
	cache, err := NewProxyCache[int, string](ProxyOptions{
		Capacity: 2,
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	cache.Put(1, "one")
	cache.Put(2, "two")

	// Peek at 1 (shouldn't update LRU)
	if val, ok := cache.Peek(1); !ok || val != "one" {
		t.Error("Peek failed")
	}

	// Add 3 - should evict 1 since Peek didn't update LRU
	cache.Put(3, "three")

	if _, ok := cache.Get(1); ok {
		t.Error("Key 1 should have been evicted")
	}
}

func TestProxyCache_Stats(t *testing.T) {
	cache, err := NewProxyCache[int, []byte](ProxyOptions{
		Capacity:   100,
		MaxBytes:   1000,
		TrackStats: true,
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Generate some activity
	cache.Put(1, make([]byte, 100))
	cache.Put(2, make([]byte, 200))

	cache.Get(1) // hit
	cache.Get(1) // hit
	cache.Get(3) // miss

	stats := cache.Stats()

	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}

	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	if stats.BytesUsed != 300 {
		t.Errorf("Expected 300 bytes used, got %d", stats.BytesUsed)
	}

	if stats.MaxBytes != 1000 {
		t.Errorf("Expected max 1000 bytes, got %d", stats.MaxBytes)
	}
}

func TestProxyCache_Clear(t *testing.T) {
	evictions := 0

	cache, err := NewProxyCache[int, string](ProxyOptions{
		Capacity: 10,
		OnEvict: func(key, val interface{}) {
			evictions++
		},
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Cache should be empty, got %d items", cache.Len())
	}

	if evictions != 3 {
		t.Errorf("Expected 3 eviction callbacks, got %d", evictions)
	}
}

func TestProxyCache_NoTTL(t *testing.T) {
	cache, err := NewProxyCache[string, string](ProxyOptions{
		Capacity: 10,
		// No TTL set
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	cache.Put("key1", "value1")

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Should still exist (no TTL)
	if _, ok := cache.Get("key1"); !ok {
		t.Error("Key should exist (no TTL)")
	}
}

func TestProxyCache_Sharded(t *testing.T) {
	cache, err := NewProxyCache[int, int](ProxyOptions{
		Capacity: 1000,
		Shards:   64,
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Test basic operations with sharding
	for i := 0; i < 100; i++ {
		cache.Put(i, i*10)
	}

	for i := 0; i < 100; i++ {
		if val, ok := cache.Get(i); !ok || val != i*10 {
			t.Errorf("Sharded cache failed for key %d", i)
		}
	}

	stats := cache.Stats()
	if stats.Shards != 64 {
		t.Errorf("Expected 64 shards, got %d", stats.Shards)
	}
}

// Benchmarks

func BenchmarkProxyCache_Put(b *testing.B) {
	cache, _ := NewProxyCache[int, []byte](ProxyOptions{
		Capacity: 100000,
		Shards:   64,
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	defer cache.Close()

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(i, data)
	}
}

func BenchmarkProxyCache_Get(b *testing.B) {
	cache, _ := NewProxyCache[int, []byte](ProxyOptions{
		Capacity: 100000,
		Shards:   64,
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	defer cache.Close()

	data := make([]byte, 1024)
	for i := 0; i < 10000; i++ {
		cache.Put(i, data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 10000)
	}
}

func BenchmarkProxyCache_WithTTL(b *testing.B) {
	cache, _ := NewProxyCache[int, []byte](ProxyOptions{
		Capacity:   100000,
		Shards:     64,
		DefaultTTL: 5 * time.Minute,
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	defer cache.Close()

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(i, data)
	}
}

func BenchmarkProxyCache_WithSizeLimit(b *testing.B) {
	cache, _ := NewProxyCache[int, []byte](ProxyOptions{
		Capacity: 100000,
		MaxBytes: 100 * 1024 * 1024, // 100MB
		Shards:   64,
		SizeFunc: func(v interface{}) int64 {
			return int64(len(v.([]byte)))
		},
	})
	defer cache.Close()

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(i, data)
	}
}

func BenchmarkProxyCache_HTTPResponse(b *testing.B) {
	type HTTPResponse struct {
		StatusCode int
		Headers    map[string]string
		Body       []byte
	}

	cache, _ := NewProxyCache[string, *HTTPResponse](ProxyOptions{
		Capacity:   10000,
		MaxBytes:   100 * 1024 * 1024,
		DefaultTTL: 5 * time.Minute,
		Shards:     64,
		TrackStats: true,
		SizeFunc: func(v interface{}) int64 {
			resp := v.(*HTTPResponse)
			return int64(len(resp.Body) + 100) // body + headers approx
		},
	})
	defer cache.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			url := fmt.Sprintf("/api/resource/%d", i%1000)

			if _, ok := cache.Get(url); !ok {
				// Simulate cache miss - store response
				cache.Put(url, &HTTPResponse{
					StatusCode: 200,
					Headers:    map[string]string{"Content-Type": "application/json"},
					Body:       make([]byte, 1024),
				})
			}
			i++
		}
	})
}
