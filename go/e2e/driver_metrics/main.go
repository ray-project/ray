// Copyright 2026 The Ray Authors.
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

// driver_metrics is a Ray Go driver that verifies the user Metrics API end to
// end on a real cluster (and the local-mode not-supported behavior). It
// exercises the surfaces where the Go API previously trailed Java and C++:
//
//	User Metrics API: the user Metrics API (Count / Gauge / Sum / Histogram)
//	    was completely missing. This driver exercises the go/pkg/runtime/metric
//	    package over the cgo MetricBackend (metric_ops bridge -> stats::Metric)
//	    in cluster mode: tag key registration, the four metric types with tags,
//	    Record with aligned tag values, Unregister, and rejection of a stale
//	    handle after unregister.
//
// Cluster mode (--run-mode=driver, ray job submit) covers:
//   - RegisterTagKey registers a reusable tag key
//   - NewCount / NewGauge / NewSum / NewHistogram register through the C++
//     bridge and Record reports values into the core worker stats
//   - Unregister releases the metric, and a subsequent Record on the same
//     handle is rejected
//
// Local mode (--run-mode=local) covers the not-supported behavior: the metric
// backend is not registered in local mode, so registration and Record report a
// clear not-supported error instead of panicking.
//
// Build & submit as a Ray job:
//
//	bazel build //go/e2e/driver_metrics
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/driver_metrics_/driver_metrics job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./driver_metrics
//	./driver_metrics --run-mode=local
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/local"
	"github.com/ray-project/ray/go/pkg/runtime/metric"
)

var runModeFlag = flag.String("run-mode", "driver", "Run mode: driver or local")

// metricNamePrefix returns a unique metric-name prefix per run so repeated
// driver executions never collide on opencensus measure names.
func metricNamePrefix(suffix string) string {
	return fmt.Sprintf("ray_e2e_%s_%d", suffix, time.Now().UnixNano())
}

// testMetricLifecycle verifies tag key registration, the four metric types,
// Record with aligned tag values, Unregister, and stale-handle rejection in
// cluster mode.
func testMetricLifecycle() error {
	// Tag key registration.
	if err := metric.RegisterTagKey("e2e_tag"); err != nil {
		return fmt.Errorf("RegisterTagKey failed: %w", err)
	}
	fmt.Println("  RegisterTagKey(e2e_tag) -> nil ✓")

	// Count.
	count := metric.NewCount(metricNamePrefix("count"), "e2e count metric", "count", []string{"e2e_tag"})
	if err := count.Record(map[string]string{"e2e_tag": "v1"}, 3.0); err != nil {
		return fmt.Errorf("Count.Record failed: %w", err)
	}
	fmt.Println("  Count registered + Record(3.0) -> nil ✓")

	// Gauge.
	gauge := metric.NewGauge(metricNamePrefix("gauge"), "e2e gauge metric", "1", []string{"e2e_tag"})
	if err := gauge.Record(map[string]string{"e2e_tag": "v1"}, 42.5); err != nil {
		return fmt.Errorf("Gauge.Record failed: %w", err)
	}
	fmt.Println("  Gauge registered + Record(42.5) -> nil ✓")

	// Sum.
	sum := metric.NewSum(metricNamePrefix("sum"), "e2e sum metric", "1", []string{"e2e_tag"})
	if err := sum.Record(map[string]string{"e2e_tag": "v1"}, 7.0); err != nil {
		return fmt.Errorf("Sum.Record failed: %w", err)
	}
	fmt.Println("  Sum registered + Record(7.0) -> nil ✓")

	// Histogram with explicit bucket boundaries.
	hist := metric.NewHistogram(metricNamePrefix("hist"), "e2e histogram metric", "ms",
		[]float64{1, 5, 10, 50}, []string{"e2e_tag"})
	if err := hist.Record(map[string]string{"e2e_tag": "v1"}, 3.0); err != nil {
		return fmt.Errorf("Histogram.Record failed: %w", err)
	}
	fmt.Println("  Histogram registered + Record(3.0) -> nil ✓")

	// Unregister releases the C++ stats object; a stale handle must then
	// be rejected (not a use-after-free).
	if err := hist.Unregister(); err != nil {
		return fmt.Errorf("Histogram.Unregister failed: %w", err)
	}
	fmt.Println("  Histogram.Unregister -> nil ✓")
	if err := hist.Record(map[string]string{"e2e_tag": "v1"}, 9.0); err == nil {
		return fmt.Errorf("Record on unregistered histogram should error")
	}
	fmt.Println("  Record on unregistered histogram rejected ✓")

	if err := count.Unregister(); err != nil {
		return fmt.Errorf("Count.Unregister failed: %w", err)
	}
	if err := gauge.Unregister(); err != nil {
		return fmt.Errorf("Gauge.Unregister failed: %w", err)
	}
	if err := sum.Unregister(); err != nil {
		return fmt.Errorf("Sum.Unregister failed: %w", err)
	}
	fmt.Println("  Count/Gauge/Sum.Unregister -> nil ✓")

	return nil
}

// testLocalMetricNotSupported verifies that in local mode the metric backend
// is not registered, so metric operations report a clear not-supported error
// instead of panicking.
func testLocalMetricNotSupported() error {
	if err := metric.RegisterTagKey("e2e_tag"); err == nil {
		return fmt.Errorf("local RegisterTagKey should error (backend not registered)")
	} else {
		fmt.Printf("  local RegisterTagKey -> error: %v ✓\n", err)
	}

	count := metric.NewCount(metricNamePrefix("local_count"), "local count", "count", nil)
	if err := count.Record(map[string]string{}, 1.0); err == nil {
		return fmt.Errorf("local Count.Record should error (backend not registered)")
	} else {
		fmt.Printf("  local Count.Record -> error: %v ✓\n", err)
	}
	if err := count.Unregister(); err == nil {
		return fmt.Errorf("local Count.Unregister should error (handle not registered)")
	} else {
		fmt.Printf("  local Count.Unregister -> error: %v ✓\n", err)
	}
	return nil
}

func runDriver() {
	// driver_metrics does not submit remote tasks or actors, so no userfuncs.so
	// code search path is required. Initialize with an empty job config so the
	// native runtime (and its metric backend) is set up.
	if err := api.Instance().InitializeWithJobConfig(options.NewJobConfigBuilder()); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestMetricLifecycle ===")
	if err := testMetricLifecycle(); err != nil {
		log.Fatalf("TestMetricLifecycle FAILED: %v", err)
	}
	fmt.Println("=== TestMetricLifecycle PASSED ===\n")

	fmt.Println("All metrics cluster checks passed.")
}

func runLocal() {
	local.Enable()
	if err := api.InitLocal(); err != nil {
		log.Fatalf("local init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestLocalMetricNotSupported ===")
	if err := testLocalMetricNotSupported(); err != nil {
		log.Fatalf("TestLocalMetricNotSupported FAILED: %v", err)
	}
	fmt.Println("=== TestLocalMetricNotSupported PASSED ===\n")

	fmt.Println("All metrics local-mode checks passed.")
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	switch *runModeFlag {
	case "driver":
		runDriver()
	case "local":
		runLocal()
	default:
		log.Fatalf("unknown run-mode %q (want driver or local)", *runModeFlag)
	}
}
