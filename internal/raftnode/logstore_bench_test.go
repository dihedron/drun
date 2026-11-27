package raftnode

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dihedron/drun/internal/config"
	"github.com/hashicorp/raft"
)

// FW-2: the Raft log stores compared (make bench-logstore)

func percentile(d []time.Duration, p float64) time.Duration {
	slices.Sort(d)
	return d[int(float64(len(d)-1)*p)]
}

func report(b *testing.B, lat []time.Duration, elapsed time.Duration) {
	b.ReportMetric(float64(len(lat))/elapsed.Seconds(), "entries/s")
	b.ReportMetric(float64(percentile(lat, 0.5).Microseconds()), "p50-µs")
	b.ReportMetric(float64(percentile(lat, 0.99).Microseconds()), "p99-µs")
}

var sizes = []int{256, 16 << 10, 1 << 20, 4 << 20}

// one entry per append, as raft writes a lone entry: each fsynced
func BenchmarkLogStoreAppend(b *testing.B) {
	for _, kind := range []string{config.LogStoreBolt, config.LogStoreWAL} {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/%dB", kind, size), func(b *testing.B) {
				st, err := openLogStore(kind, b.TempDir(), discard)
				if err != nil {
					b.Fatal(err)
				}
				defer st.Close() //nolint:errcheck // benchmark
				data := make([]byte, size)
				lat := make([]time.Duration, 0, b.N)
				b.SetBytes(int64(size))
				b.ResetTimer()
				began := time.Now()
				for i := range b.N {
					t0 := time.Now()
					if err := st.StoreLog(&raft.Log{Index: uint64(i) + 1, Term: 1, Type: raft.LogCommand, Data: data}); err != nil { //nolint:gosec // i >= 0
						b.Fatal(err)
					}
					lat = append(lat, time.Since(t0))
				}
				report(b, lat, time.Since(began))
			})
		}
	}
}

// commits through a 3-node cluster over mutual TLS
func BenchmarkClusterApply(b *testing.B) {
	for _, kind := range []string{config.LogStoreBolt, config.LogStoreWAL} {
		for _, size := range []int{1 << 10, 256 << 10} {
			for _, workers := range []int{1, 16} {
				b.Run(fmt.Sprintf("%s/%dB/x%d", kind, size, workers), func(b *testing.B) {
					benchCluster(b, newTestCluster(b, 3, 3), kind, size, workers)
				})
			}
		}
	}
}

func benchCluster(b *testing.B, c *testCluster, kind string, size, workers int) {
	b.Helper()
	c.logStore = kind
	for _, id := range c.ids {
		c.start(id)
	}
	// stopped here, not at the end of the benchmark: it runs several times
	defer func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	}()
	l := c.leader(10 * time.Second)
	data := make([]byte, size)
	var mu sync.Mutex
	lat := make([]time.Duration, 0, b.N)
	jobs := make(chan struct{})
	var wg sync.WaitGroup
	b.SetBytes(int64(size))
	b.ResetTimer()
	began := time.Now()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				t0 := time.Now()
				if err := l.Raft().Apply(data, 10*time.Second).Error(); err != nil {
					b.Error(err)
					return
				}
				d := time.Since(t0)
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		}()
	}
	for range b.N {
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()
	report(b, lat, time.Since(began))
}

// the raft directory after 1000 entries of 1 MiB, a snapshot and the
// compaction (256 trailing entries)
func BenchmarkLogStoreDisk(b *testing.B) {
	for _, kind := range []string{config.LogStoreBolt, config.LogStoreWAL} {
		b.Run(kind, func(b *testing.B) {
			for range b.N {
				c := newTestCluster(b, 1, 1)
				c.logStore = kind
				nd := c.start("node-0")
				c.leader(10 * time.Second)
				data := make([]byte, 1<<20)
				for range 1000 {
					if err := nd.Raft().Apply(data, 10*time.Second).Error(); err != nil {
						b.Fatal(err)
					}
				}
				peak := dirSize(filepath.Join(c.dirs["node-0"], "data", "raft"))
				if err := nd.Raft().Snapshot().Error(); err != nil {
					b.Fatal(err)
				}
				after := dirSize(filepath.Join(c.dirs["node-0"], "data", "raft"))
				c.stop("node-0")
				_ = os.RemoveAll(c.dirs["node-0"])
				b.ReportMetric(float64(peak)/(1<<20), "peak-MiB")
				b.ReportMetric(float64(after)/(1<<20), "compacted-MiB")
			}
		})
	}
}

// dirSize is the space the files of a directory take on disk, without
// the snapshots (the same for both stores)
func dirSize(dir string) int64 {
	var n int64
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Base(filepath.Dir(filepath.Dir(p))) == "snapshots" || filepath.Base(filepath.Dir(p)) == "snapshots" {
			return nil
		}
		if fi, err := d.Info(); err == nil {
			n += fi.Size()
		}
		return nil
	})
	return n
}
