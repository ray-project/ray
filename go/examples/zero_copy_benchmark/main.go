// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// zero_copy_benchmark measures the cross-process zero-copy Get performance of
// the Ray Go runtime against a real cluster (driver mode).
//
// It runs two loads for each object size (default 4MB, 64MB):
//   - Local Put->Get: api.Put then ref.Get within the driver process.
//   - Cross-process:  api.Put, then a remote echoBytes task returns the payload,
//     then driver ref.Get (the zero-copy view path).
//
// Output is a latency table (mean/median/p95/p99 + MB/s) and optional CSV.
//
// Usage:
//
//	bazel build //go/examples/zero_copy_benchmark:zero_copy_benchmark
//	cp bazel-bin/go/examples/userfuncs/plugin/userfuncs.so .
//	cp bazel-bin/go/examples/zero_copy_benchmark_/zero_copy_benchmark .
//	ray job submit --working-dir . -- ./zero_copy_benchmark --run-mode=driver
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/examples/userfuncs"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/local"
)

// ============================================================================
// Flags
// ============================================================================

var (
	runModeFlag    = flag.String("run-mode", "driver", "Run mode: driver (real cluster) or local")
	sizesFlag      = flag.String("sizes", "4MB,64MB", "Comma-separated object sizes, e.g. 4MB,64MB")
	iterationsFlag = flag.Int("iterations", 20, "Number of timed iterations per size")
	warmupFlag     = flag.Int("warmup", 2, "Number of warmup iterations per size")
	csvFlag        = flag.String("csv", "", "Optional CSV output path (empty = no CSV)")
	codeSearchPath = flag.String("code-search-path", "", "Path to userfuncs.so (default: ./userfuncs.so)")
)

// Stats holds the computed latency statistics for one load/size combination.
type Stats struct {
	Mode         string
	SizeBytes    int
	Iterations   int
	Mean         time.Duration
	Median       time.Duration
	P95          time.Duration
	P99          time.Duration
	ThroughputMB float64 // MB/s
}

// sizeFromStr parses "4MB"/"64MB"/"1KB" (and bare bytes) into a byte count.
func sizeFromStr(s string) (int, error) {
	s = strings.TrimSpace(s)
	mult := 1
	switch {
	case strings.HasSuffix(s, "MB"):
		mult = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		mult = 1024
		s = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "B"):
		mult = 1
		s = strings.TrimSuffix(s, "B")
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	return n * mult, nil
}

// genPayload returns a deterministic payload of the given size.
func genPayload(size int) []byte {
	p := make([]byte, size)
	for i := range p {
		p[i] = byte(i * 31)
	}
	return p
}

// verifyPayload asserts got matches payload byte-for-byte. It returns an error
// rather than tolerating mismatch, because a wrong payload invalidates the
// measurement.
func verifyPayload(got []byte, payload []byte) error {
	if len(got) != len(payload) {
		return fmt.Errorf("payload length mismatch: got %d, want %d", len(got), len(payload))
	}
	for i := range got {
		if got[i] != payload[i] {
			return fmt.Errorf("payload mismatch at byte %d: got %d, want %d", i, got[i], payload[i])
		}
	}
	return nil
}

// benchLocalPutGet measures api.Put + ref.Get within the driver process.
func benchLocalPutGet(size int, iterations, warmup int) (*Stats, error) {
	payload := genPayload(size)
	var latencies []time.Duration
	failures := 0

	for i := 0; i < warmup+iterations; i++ {
		start := time.Now()
		ref, err := api.Put(payload, nil)
		if err == nil {
			var got []byte
			got, err = ref.Get()
			elapsed := time.Since(start)
			if err == nil {
				err = verifyPayload(got, payload)
			}
			if err != nil {
				failures++
				fmt.Printf("  [local] iteration %d failed: %v\n", i, err)
				if i >= warmup && failures > iterations/2 {
					return nil, fmt.Errorf("local Put->Get failure rate too high (%d/%d)", failures, iterations)
				}
				continue
			}
			// Print per-iteration latency for observability.
			warmupMark := ""
			if i < warmup {
				warmupMark = " (warmup)"
			}
			fmt.Printf("  [local] iteration %d: %v%s\n", i, elapsed.Round(time.Microsecond), warmupMark)
			if i >= warmup {
				latencies = append(latencies, elapsed)
			}
		} else {
			failures++
			fmt.Printf("  [local] iteration %d failed: %v\n", i, err)
			if i >= warmup && failures > iterations/2 {
				return nil, fmt.Errorf("local Put->Get failure rate too high (%d/%d)", failures, iterations)
			}
		}
	}

	if len(latencies) == 0 {
		return nil, fmt.Errorf("local Put->Get: no successful iterations")
	}
	return computeStats("local", size, latencies), nil
}

// benchCrossProcess measures api.Put, a remote echoBytes round-trip, then
// driver ref.Get (the zero-copy view path).
func benchCrossProcess(size int, iterations, warmup int) (*Stats, error) {
	payload := genPayload(size)
	var latencies []time.Duration
	failures := 0

	for i := 0; i < warmup+iterations; i++ {
		start := time.Now()
		_, err := api.Put(payload, nil)
		if err == nil {
			var retRef *api.ObjectRef[[]byte]
			retRef, err = api.Remote[[]byte](userfuncs.GetEchoBytes()).Call(payload)
			if err == nil {
				var got []byte
				got, err = retRef.Get()
				elapsed := time.Since(start)
				if err == nil {
					err = verifyPayload(got, payload)
				}
				if err != nil {
					failures++
					fmt.Printf("  [cross] iteration %d failed: %v\n", i, err)
					if i >= warmup && failures > iterations/2 {
						return nil, fmt.Errorf("cross-process failure rate too high (%d/%d)", failures, iterations)
					}
					continue
				}
				// Print per-iteration latency for observability.
				warmupMark := ""
				if i < warmup {
					warmupMark = " (warmup)"
				}
				fmt.Printf("  [cross] iteration %d: %v%s\n", i, elapsed.Round(time.Microsecond), warmupMark)
				if i >= warmup {
					latencies = append(latencies, elapsed)
				}
			} else {
				failures++
				fmt.Printf("  [cross] iteration %d failed: %v\n", i, err)
				if i >= warmup && failures > iterations/2 {
					return nil, fmt.Errorf("cross-process failure rate too high (%d/%d)", failures, iterations)
				}
			}
		} else {
			failures++
			fmt.Printf("  [cross] iteration %d failed: %v\n", i, err)
			if i >= warmup && failures > iterations/2 {
				return nil, fmt.Errorf("cross-process failure rate too high (%d/%d)", failures, iterations)
			}
		}
	}

	if len(latencies) == 0 {
		return nil, fmt.Errorf("cross-process: no successful iterations")
	}
	return computeStats("cross", size, latencies), nil
}

// percentile returns the q-th percentile (q in (0,1]) of the sorted
// latencies, clamped to a valid index so single-sample runs never panic.
func percentile(sorted []time.Duration, q float64) time.Duration {
	idx := int(math.Ceil(float64(len(sorted))*q)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// computeStats computes mean/median/p95/p99 and throughput (MB/s) from the
// full round-trip latencies.
func computeStats(mode string, sizeBytes int, latencies []time.Duration) *Stats {
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean := sum / time.Duration(len(sorted))
	median := sorted[len(sorted)/2]
	p95 := percentile(sorted, 0.95)
	p99 := percentile(sorted, 0.99)
	throughput := float64(sizeBytes) / (1024 * 1024) / mean.Seconds()

	return &Stats{
		Mode:         mode,
		SizeBytes:    sizeBytes,
		Iterations:   len(sorted),
		Mean:         mean,
		Median:       median,
		P95:          p95,
		P99:          p99,
		ThroughputMB: throughput,
	}
}

// printTable prints a human-readable results table to stdout.
func printTable(stats []*Stats) {
	fmt.Printf("\n%-18s %-10s %-12s %-10s %-10s %-10s %-10s %-10s\n",
		"mode", "size", "iterations", "mean", "median", "p95", "p99", "MB/s")
	fmt.Println(strings.Repeat("-", 90))
	for _, s := range stats {
		fmt.Printf("%-18s %-10s %-12d %-10s %-10s %-10s %-10s %-10.1f\n",
			s.Mode, humanSize(s.SizeBytes), s.Iterations,
			s.Mean.Round(time.Microsecond), s.Median.Round(time.Microsecond),
			s.P95.Round(time.Microsecond), s.P99.Round(time.Microsecond),
			s.ThroughputMB)
	}
	fmt.Println()
}

// humanSize renders a byte count as "4MB" / "64MB".
func humanSize(b int) string {
	if b%(1024*1024) == 0 {
		return fmt.Sprintf("%dMB", b/(1024*1024))
	}
	if b%1024 == 0 {
		return fmt.Sprintf("%dKB", b/1024)
	}
	return fmt.Sprintf("%dB", b)
}

// writeCSV writes machine-readable results to path.
func writeCSV(path string, stats []*Stats) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()

	fmt.Fprintln(f, "mode,size_bytes,iterations,mean_ms,median_ms,p95_ms,p99_ms,throughput_mbps")
	for _, s := range stats {
		fmt.Fprintf(f, "%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.1f\n",
			s.Mode, s.SizeBytes, s.Iterations,
			float64(s.Mean)/float64(time.Millisecond),
			float64(s.Median)/float64(time.Millisecond),
			float64(s.P95)/float64(time.Millisecond),
			float64(s.P99)/float64(time.Millisecond),
			s.ThroughputMB)
	}
	return nil
}

// parseSizes parses the --sizes flag into a list of byte counts.
func parseSizes(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	sizes := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := sizeFromStr(p)
		if err != nil {
			return nil, err
		}
		sizes = append(sizes, n)
	}
	return sizes, nil
}

// runBenchmark runs both loads for each size and returns the aggregated stats.
func runBenchmark(sizes []int, iterations, warmup int) ([]*Stats, error) {
	var stats []*Stats
	for _, size := range sizes {
		fmt.Printf("\n=== Size %s (%d iterations, %d warmup) ===\n",
			humanSize(size), iterations, warmup)

		local, err := benchLocalPutGet(size, iterations, warmup)
		if err != nil {
			return nil, err
		}
		stats = append(stats, local)

		cross, err := benchCrossProcess(size, iterations, warmup)
		if err != nil {
			return nil, err
		}
		stats = append(stats, cross)
	}
	return stats, nil
}

// runDriver initializes Ray against the cluster and runs the benchmark.
func runDriver() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	sp := *codeSearchPath
	if sp == "" {
		sp = filepath.Join(cwd, "userfuncs.so")
	}

	// Import userfuncs for its init() side effect (registers echoBytes etc.).
	builder := options.NewJobConfigBuilder().WithCodeSearchPath(sp)
	if err := api.Instance().InitializeWithJobConfig(builder); err != nil {
		return fmt.Errorf("initialize ray: %w", err)
	}
	defer api.Shutdown()

	sizes, err := parseSizes(*sizesFlag)
	if err != nil {
		return err
	}
	if *iterationsFlag <= 0 {
		return fmt.Errorf("--iterations must be > 0")
	}

	stats, err := runBenchmark(sizes, *iterationsFlag, *warmupFlag)
	if err != nil {
		return err
	}

	printTable(stats)
	if *csvFlag != "" {
		if err := writeCSV(*csvFlag, stats); err != nil {
			return err
		}
		fmt.Printf("CSV written to %s\n", *csvFlag)
	}
	return nil
}

func main() {
	flag.Parse()

	switch *runModeFlag {
	case "driver":
		if err := runDriver(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
	case "local":
		// Local mode: initialize the in-process runtime (WorkerTypeLocal) so
		// api.Remote tasks execute against the local object store. The local
		// package (imported above) registers the WorkerTypeLocal factory and
		// the api local-mode initializer, which api.InitWithOptions routes
		// through instead of loading go_runtime.so.
		local.Enable()
		opts := &options.InitializeOptions{
			WorkerType: options.WorkerTypeLocal,
		}
		if err := api.InitWithOptions(opts); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: local init: %v\n", err)
			os.Exit(1)
		}
		defer api.Shutdown()

		sizes, err := parseSizes(*sizesFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		if *iterationsFlag <= 0 {
			fmt.Fprintln(os.Stderr, "ERROR: --iterations must be > 0")
			os.Exit(1)
		}
		stats, err := runBenchmark(sizes, *iterationsFlag, *warmupFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		printTable(stats)
	default:
		fmt.Fprintf(os.Stderr, "ERROR: unknown --run-mode %q\n", *runModeFlag)
		os.Exit(1)
	}
}
