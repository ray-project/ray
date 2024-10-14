// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build go1.17
// +build go1.17

package collectors

import "github.com/prometheus/client_golang/prometheus"

//nolint:staticcheck // Ignore SA1019 until v2.
type goOptions = prometheus.GoCollectorOptions
type goOption func(o *goOptions)

type GoCollectionOption uint32

const (
	// GoRuntimeMemStatsCollection represents the metrics represented by runtime.MemStats structure such as
	// go_memstats_alloc_bytes
	// go_memstats_alloc_bytes_total
	// go_memstats_sys_bytes
	// go_memstats_lookups_total
	// go_memstats_mallocs_total
	// go_memstats_frees_total
	// go_memstats_heap_alloc_bytes
	// go_memstats_heap_sys_bytes
	// go_memstats_heap_idle_bytes
	// go_memstats_heap_inuse_bytes
	// go_memstats_heap_released_bytes
	// go_memstats_heap_objects
	// go_memstats_stack_inuse_bytes
	// go_memstats_stack_sys_bytes
	// go_memstats_mspan_inuse_bytes
	// go_memstats_mspan_sys_bytes
	// go_memstats_mcache_inuse_bytes
	// go_memstats_mcache_sys_bytes
	// go_memstats_buck_hash_sys_bytes
	// go_memstats_gc_sys_bytes
	// go_memstats_other_sys_bytes
	// go_memstats_next_gc_bytes
	// so the metrics known from pre client_golang v1.12.0, except skipped go_memstats_gc_cpu_fraction (see
	// https://github.com/prometheus/client_golang/issues/842#issuecomment-861812034 for explanation.
	//
	// NOTE that this mode represents runtime.MemStats statistics, but they are
	// actually implemented using new runtime/metrics package.
	// Deprecated: Use GoRuntimeMetricsCollection instead going forward.
	GoRuntimeMemStatsCollection GoCollectionOption = 1 << iota
	// GoRuntimeMetricsCollection is the new set of metrics represented by runtime/metrics package and follows
	// consistent naming. The exposed metric set depends on Go version, but it is controlled against
	// unexpected cardinality. This set has overlapping information with GoRuntimeMemStatsCollection, just with
	// new names. GoRuntimeMetricsCollection is what is recommended for using going forward.
	GoRuntimeMetricsCollection
)

// WithGoCollections allows enabling different collections for Go collector on top of base metrics
// like go_goroutines, go_threads, go_gc_duration_seconds, go_memstats_last_gc_time_seconds, go_info.
//
// Check GoRuntimeMemStatsCollection and GoRuntimeMetricsCollection for more details. You can use none,
// one or more collections at once. For example:
// WithGoCollections(GoRuntimeMemStatsCollection | GoRuntimeMetricsCollection) means both GoRuntimeMemStatsCollection
// metrics and GoRuntimeMetricsCollection will be exposed.
//
// The current default is GoRuntimeMemStatsCollection, so the compatibility mode with
// client_golang pre v1.12 (move to runtime/metrics).
func WithGoCollections(flags GoCollectionOption) goOption {
	return func(o *goOptions) {
		o.EnabledCollections = uint32(flags)
	}
}

// NewGoCollector returns a collector that exports metrics about the current Go
// process using debug.GCStats using runtime/metrics.
func NewGoCollector(opts ...goOption) prometheus.Collector {
	//nolint:staticcheck // Ignore SA1019 until v2.
	promPkgOpts := make([]func(o *prometheus.GoCollectorOptions), len(opts))
	for i, opt := range opts {
		promPkgOpts[i] = opt
	}
	//nolint:staticcheck // Ignore SA1019 until v2.
	return prometheus.NewGoCollector(promPkgOpts...)
}
