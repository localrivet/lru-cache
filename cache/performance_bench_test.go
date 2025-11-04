package lru

import (
	"fmt"
	"sync"
	"testing"
)

// Benchmark non-sharded vs sharded performance

func BenchmarkNonSharded_SingleThread_Put(b *testing.B) {
	cache, _ := New[int, int](100000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.Put(i, i*10)
	}
}

func BenchmarkSharded64_SingleThread_Put(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.Put(i, i*10)
	}
}

func BenchmarkNonSharded_Concurrent_Put(b *testing.B) {
	cache, _ := New[int, int](100000)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Put(i, i*10)
			i++
		}
	})
}

func BenchmarkSharded64_Concurrent_Put(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Put(i, i*10)
			i++
		}
	})
}

func BenchmarkNonSharded_Concurrent_Get(b *testing.B) {
	cache, _ := New[int, int](100000)
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Get(i % 10000)
			i++
		}
	})
}

func BenchmarkSharded64_Concurrent_Get(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Get(i % 10000)
			i++
		}
	})
}

func BenchmarkNonSharded_Concurrent_Mixed(b *testing.B) {
	cache, _ := New[int, int](100000)
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				cache.Put(i, i*10)
			} else {
				cache.Get(i % 10000)
			}
			i++
		}
	})
}

func BenchmarkSharded64_Concurrent_Mixed(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				cache.Put(i, i*10)
			} else {
				cache.Get(i % 10000)
			}
			i++
		}
	})
}

// Batch operations benchmarks

func BenchmarkNonSharded_BatchPut(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache, _ := New[int, int](100000)
		items := make(map[int]int, 1000)
		for j := 0; j < 1000; j++ {
			items[j] = j * 10
		}
		b.StartTimer()

		cache.BatchPut(items)
	}
}

func BenchmarkSharded64_BatchPut(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
		items := make(map[int]int, 1000)
		for j := 0; j < 1000; j++ {
			items[j] = j * 10
		}
		b.StartTimer()

		cache.BatchPut(items)
	}
}

func BenchmarkNonSharded_BatchGet(b *testing.B) {
	cache, _ := New[int, int](100000)
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}

	keys := make([]int, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.BatchGet(keys)
	}
}

func BenchmarkSharded64_BatchGet(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}

	keys := make([]int, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.BatchGet(keys)
	}
}

// GetOrCompute benchmark

func BenchmarkNonSharded_GetOrCompute_Hit(b *testing.B) {
	cache, _ := New[int, int](100000)
	cache.Put(1, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.GetOrCompute(1, func() int { return 10 })
	}
}

func BenchmarkSharded64_GetOrCompute_Hit(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
	cache.Put(1, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.GetOrCompute(1, func() int { return 10 })
	}
}

// High contention ETL simulation

func BenchmarkNonSharded_ETL_Simulation(b *testing.B) {
	cache, _ := New[int, int](100000)
	b.ResetTimer()

	// Simulate 50 workers doing ETL
	var wg sync.WaitGroup
	workers := 50
	opsPerWorker := b.N / workers

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := workerID*opsPerWorker + i
				cache.Put(key, key*10)

				// Simulate lookup
				if i%10 == 0 {
					cache.Get(key - 1)
				}
			}
		}(w)
	}
	wg.Wait()
}

func BenchmarkSharded64_ETL_Simulation(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64})
	b.ResetTimer()

	// Simulate 50 workers doing ETL
	var wg sync.WaitGroup
	workers := 50
	opsPerWorker := b.N / workers

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := workerID*opsPerWorker + i
				cache.Put(key, key*10)

				// Simulate lookup
				if i%10 == 0 {
					cache.Get(key - 1)
				}
			}
		}(w)
	}
	wg.Wait()
}

// Shard count comparison

func benchmarkShardCount(b *testing.B, shards int) {
	var cache *LRUCache[int, int]
	if shards == 0 {
		cache, _ = New[int, int](100000)
	} else {
		cache, _ = NewWithOptions[int, int](100000, Options{Shards: shards})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Put(i, i*10)
			i++
		}
	})
}

func BenchmarkShardCount_0(b *testing.B)   { benchmarkShardCount(b, 0) }
func BenchmarkShardCount_16(b *testing.B)  { benchmarkShardCount(b, 16) }
func BenchmarkShardCount_32(b *testing.B)  { benchmarkShardCount(b, 32) }
func BenchmarkShardCount_64(b *testing.B)  { benchmarkShardCount(b, 64) }
func BenchmarkShardCount_128(b *testing.B) { benchmarkShardCount(b, 128) }

// Memory overhead comparison

func BenchmarkMemoryOverhead(b *testing.B) {
	b.Run("NonSharded", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache, _ := New[int, int](10000)
			for j := 0; j < 10000; j++ {
				cache.Put(j, j*10)
			}
		}
	})

	b.Run("Sharded64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache, _ := NewWithOptions[int, int](10000, Options{Shards: 64})
			for j := 0; j < 10000; j++ {
				cache.Put(j, j*10)
			}
		}
	})
}

// Stats tracking overhead

func BenchmarkStatsOverhead_Without(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64, TrackStats: false})
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 10000)
	}
}

func BenchmarkStatsOverhead_With(b *testing.B) {
	cache, _ := NewWithOptions[int, int](100000, Options{Shards: 64, TrackStats: true})
	for i := 0; i < 10000; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 10000)
	}
}

// Real-world ETL patterns

func BenchmarkETLPattern_Deduplication(b *testing.B) {
	cache, _ := NewWithOptions[string, bool](1000000, HighThroughputOptions())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			recordID := fmt.Sprintf("record_%d", i%100000)

			// Check if seen
			if _, seen := cache.Get(recordID); !seen {
				// Mark as seen
				cache.Put(recordID, true)
			}
			i++
		}
	})
}

func BenchmarkETLPattern_LookupEnrichment(b *testing.B) {
	type Record struct {
		ID   int
		Name string
	}

	cache, _ := NewWithOptions[int, Record](100000, HighThroughputOptions())

	// Pre-populate some data
	for i := 0; i < 10000; i++ {
		cache.Put(i, Record{ID: i, Name: fmt.Sprintf("Record%d", i)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			userID := i % 10000

			// GetOrCompute pattern - lookup with fallback
			cache.GetOrCompute(userID, func() Record {
				// Simulate DB lookup
				return Record{ID: userID, Name: fmt.Sprintf("Record%d", userID)}
			})
			i++
		}
	})
}
