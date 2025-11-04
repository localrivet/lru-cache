package lru

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// BenchmarkPut benchmarks the Put operation
func BenchmarkPut(b *testing.B) {
	cache, _ := New[int, int](1000)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cache.Put(i, i*10)
	}
}

// BenchmarkGet benchmarks the Get operation
func BenchmarkGet(b *testing.B) {
	cache, _ := New[int, int](1000)

	// Prepopulate cache
	for i := 0; i < 1000; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 1000)
	}
}

// BenchmarkPutAndGet benchmarks mixed Put and Get operations
func BenchmarkPutAndGet(b *testing.B) {
	cache, _ := New[int, int](1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			cache.Put(i, i*10)
		} else {
			cache.Get(i % 1000)
		}
	}
}

// BenchmarkEviction benchmarks cache with constant evictions
func BenchmarkEviction(b *testing.B) {
	cache, _ := New[int, int](100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(i, i*10)
	}
}

// BenchmarkRemove benchmarks the Remove operation
func BenchmarkRemove(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			cache, _ := New[int, int](size)

			// Prepopulate
			for i := 0; i < size; i++ {
				cache.Put(i, i*10)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := i % size
				cache.Remove(key)
				cache.Put(key, key*10) // Re-add for next iteration
			}
		})
	}
}

// BenchmarkPeek benchmarks the Peek operation
func BenchmarkPeek(b *testing.B) {
	cache, _ := New[int, int](1000)

	for i := 0; i < 1000; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Peek(i % 1000)
	}
}

// BenchmarkContains benchmarks the Contains operation
func BenchmarkContains(b *testing.B) {
	cache, _ := New[int, int](1000)

	for i := 0; i < 1000; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Contains(i % 1000)
	}
}

// BenchmarkConcurrentReadWrite benchmarks concurrent operations
func BenchmarkConcurrentReadWrite(b *testing.B) {
	cache, _ := New[int, int](1000)

	// Prepopulate
	for i := 0; i < 1000; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			key := r.Intn(1000)
			if r.Intn(2) == 0 {
				cache.Get(key)
			} else {
				cache.Put(key, key*10)
			}
		}
	})
}

// BenchmarkConcurrentReads benchmarks concurrent read operations
func BenchmarkConcurrentReads(b *testing.B) {
	cache, _ := New[int, int](1000)

	for i := 0; i < 1000; i++ {
		cache.Put(i, i*10)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			cache.Get(r.Intn(1000))
		}
	})
}

// BenchmarkConcurrentWrites benchmarks concurrent write operations
func BenchmarkConcurrentWrites(b *testing.B) {
	cache, _ := New[int, int](1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.Put(i, i*10)
			i++
		}
	})
}

// BenchmarkKeys benchmarks the Keys operation
func BenchmarkKeys(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			cache, _ := New[int, int](size)

			for i := 0; i < size; i++ {
				cache.Put(i, i*10)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = cache.Keys()
			}
		})
	}
}

// BenchmarkClear benchmarks the Clear operation
func BenchmarkClear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache, _ := New[int, int](1000)
		for j := 0; j < 1000; j++ {
			cache.Put(j, j*10)
		}
		b.StartTimer()

		cache.Clear()
	}
}

// BenchmarkResize benchmarks the Resize operation
func BenchmarkResize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache, _ := New[int, int](1000)
		for j := 0; j < 1000; j++ {
			cache.Put(j, j*10)
		}
		b.StartTimer()

		cache.Resize(500)
	}
}

// BenchmarkOldestNewest benchmarks Oldest/Newest operations
func BenchmarkOldestNewest(b *testing.B) {
	cache, _ := New[int, int](1000)

	for i := 0; i < 1000; i++ {
		cache.Put(i, i*10)
	}

	b.Run("Oldest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache.Oldest()
		}
	})

	b.Run("Newest", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache.Newest()
		}
	})
}

// BenchmarkCacheSizes benchmarks different cache sizes
func BenchmarkCacheSizes(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			cache, _ := New[int, int](size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cache.Put(i, i*10)
				cache.Get(i % size)
			}
		})
	}
}

// BenchmarkStringKeys benchmarks using string keys
func BenchmarkStringKeys(b *testing.B) {
	cache, _ := New[string, int](1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		cache.Put(key, i)
		cache.Get(key)
	}
}

// BenchmarkStructKeys benchmarks using struct keys
func BenchmarkStructKeys(b *testing.B) {
	type Key struct {
		id   int
		name string
	}

	cache, _ := New[Key, int](1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Key{id: i % 1000, name: fmt.Sprintf("name%d", i%1000)}
		cache.Put(key, i)
		cache.Get(key)
	}
}

// BenchmarkHighContention simulates high contention scenario
func BenchmarkHighContention(b *testing.B) {
	cache, _ := New[int, int](100)

	// Small cache, many goroutines accessing same keys
	numGoroutines := 100
	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < b.N/numGoroutines; i++ {
				key := i % 10 // High contention on just 10 keys
				cache.Put(key, i)
				cache.Get(key)
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkLowContention simulates low contention scenario
func BenchmarkLowContention(b *testing.B) {
	cache, _ := New[int, int](10000)

	// Large cache, many goroutines accessing different keys
	numGoroutines := 100
	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			offset := id * 100
			for i := 0; i < b.N/numGoroutines; i++ {
				key := offset + (i % 100) // Each goroutine has its own key range
				cache.Put(key, i)
				cache.Get(key)
			}
		}(g)
	}
	wg.Wait()
}
