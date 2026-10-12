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

// Package cgo: cgo-backed metric backend bridging to C++ stats::Metric.
package cgo

/*
#include <stdlib.h>
#include <stdint.h>

// The prototypes below mirror src/ray/core_worker/lib/go/metric_ops.h. They are
// declared directly (instead of #include-ing the header) to avoid pulling the
// C++ stats/metric_interface include graph into the cgo compile; the symbols
// are resolved at link time against the metric_ops library.
//
// Return convention (from metric_ops.h): register functions return an opaque
// int64_t handle, or -1 on failure with *error set (caller frees with free()).
// RegisterTagKey / record / unregister return 1 on success, 0 on failure with
// *error set. All error strings are caller-freed with free().
int ray_metric_register_tag_key(const char* tag_key, char** error);

int64_t ray_metric_register_count(const char* name,
                                  const char* description,
                                  const char* unit,
                                  const char* const* tag_keys,
                                  int tag_key_count,
                                  char** error);
int64_t ray_metric_register_gauge(const char* name,
                                  const char* description,
                                  const char* unit,
                                  const char* const* tag_keys,
                                  int tag_key_count,
                                  char** error);
int64_t ray_metric_register_sum(const char* name,
                                const char* description,
                                const char* unit,
                                const char* const* tag_keys,
                                int tag_key_count,
                                char** error);
int64_t ray_metric_register_histogram(const char* name,
                                      const char* description,
                                      const char* unit,
                                      const double* boundaries,
                                      int boundary_count,
                                      const char* const* tag_keys,
                                      int tag_key_count,
                                      char** error);

int ray_metric_record(int64_t handle,
                      double value,
                      const char* const* tag_keys,
                      const char* const* tag_values,
                      int tag_count,
                      char** error);

int ray_metric_unregister(int64_t handle, char** error);
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/runtime/metric"
)

// RegisterDefaultBackend installs the cgo metric backend as the active backend.
// It must be called after the C++ core worker (and its stats module) has been
// initialized, and only in native (cluster) mode; local mode intentionally
// leaves the backend unregistered so metric operations report a clear
// not-supported error.
func RegisterDefaultBackend() {
	metric.SetBackend(&CgoMetricBackend{})
}

// CgoMetricBackend registers and records metrics through the C++ bridge.
// Its method set mirrors the metric.Backend interface consumed by
// go/pkg/runtime/metric.
type CgoMetricBackend struct{}

// RegisterTagKey registers a tag key that can be used on metrics.
func (b *CgoMetricBackend) RegisterTagKey(tagKey string) error {
	cKey := C.CString(tagKey)
	defer C.free(unsafe.Pointer(cKey))
	var errMsg *C.char
	if C.ray_metric_register_tag_key(cKey, &errMsg) == 0 {
		return cError(errMsg)
	}
	return nil
}

// RegisterCount registers a counter metric and returns its handle.
func (b *CgoMetricBackend) RegisterCount(name, desc, unit string, tagKeys []string) (int64, error) {
	return b.register(func(cName, cDesc, cUnit *C.char, cKeys **C.char, keyCount C.int, err **C.char) C.int64_t {
		return C.ray_metric_register_count(cName, cDesc, cUnit, cKeys, keyCount, err)
	}, name, desc, unit, tagKeys)
}

// RegisterGauge registers a gauge metric and returns its handle.
func (b *CgoMetricBackend) RegisterGauge(name, desc, unit string, tagKeys []string) (int64, error) {
	return b.register(func(cName, cDesc, cUnit *C.char, cKeys **C.char, keyCount C.int, err **C.char) C.int64_t {
		return C.ray_metric_register_gauge(cName, cDesc, cUnit, cKeys, keyCount, err)
	}, name, desc, unit, tagKeys)
}

// RegisterSum registers a sum metric and returns its handle.
func (b *CgoMetricBackend) RegisterSum(name, desc, unit string, tagKeys []string) (int64, error) {
	return b.register(func(cName, cDesc, cUnit *C.char, cKeys **C.char, keyCount C.int, err **C.char) C.int64_t {
		return C.ray_metric_register_sum(cName, cDesc, cUnit, cKeys, keyCount, err)
	}, name, desc, unit, tagKeys)
}

// RegisterHistogram registers a histogram metric with the given bucket
// boundaries and returns its handle.
func (b *CgoMetricBackend) RegisterHistogram(name, desc, unit string, boundaries []float64, tagKeys []string) (int64, error) {
	cName := C.CString(name)
	cDesc := C.CString(desc)
	cUnit := C.CString(unit)
	defer C.free(unsafe.Pointer(cName))
	defer C.free(unsafe.Pointer(cDesc))
	defer C.free(unsafe.Pointer(cUnit))

	cKeys, freeKeys := CStringSlice(tagKeys)
	defer freeKeys()

	cBounds := doubleSliceToC(boundaries)
	defer C.free(unsafe.Pointer(cBounds))

	var errMsg *C.char
	handle := C.ray_metric_register_histogram(cName, cDesc, cUnit, cBounds, C.int(len(boundaries)), cStringArrayPtr(cKeys), C.int(len(tagKeys)), &errMsg)
	if handle < 0 {
		return 0, cError(errMsg)
	}
	return int64(handle), nil
}

// registerFunc is the common signature of the count/gauge/sum register
// functions. The closures passed by callers invoke the C boundary directly,
// since cgo exposes C functions as unsafe.Pointer values rather than typed
// function values.
type registerFunc func(*C.char, *C.char, *C.char, **C.char, C.int, **C.char) C.int64_t

// register forwards a non-histogram metric registration to the C++ boundary.
func (b *CgoMetricBackend) register(fn registerFunc, name, desc, unit string, tagKeys []string) (int64, error) {
	cName := C.CString(name)
	cDesc := C.CString(desc)
	cUnit := C.CString(unit)
	defer C.free(unsafe.Pointer(cName))
	defer C.free(unsafe.Pointer(cDesc))
	defer C.free(unsafe.Pointer(cUnit))

	cKeys, freeKeys := CStringSlice(tagKeys)
	defer freeKeys()

	var errMsg *C.char
	handle := fn(cName, cDesc, cUnit, cStringArrayPtr(cKeys), C.int(len(tagKeys)), &errMsg)
	if handle < 0 {
		return 0, cError(errMsg)
	}
	return int64(handle), nil
}

// Record reports a value for a registered metric handle.
func (b *CgoMetricBackend) Record(handle int64, value float64, tagKeys, tagValues []string) error {
	// The C++ boundary builds the tag map from the parallel key/value arrays and
	// stops at tag_count (= len(tagKeys)). Passing fewer values than keys would
	// make it read past the value array (heap out-of-bounds), so reject the
	// mismatch before crossing the boundary.
	if len(tagKeys) != len(tagValues) {
		return fmt.Errorf("tag key/value count mismatch: %d vs %d", len(tagKeys), len(tagValues))
	}
	cKeys, freeKeys := CStringSlice(tagKeys)
	defer freeKeys()
	cValues, freeValues := CStringSlice(tagValues)
	defer freeValues()

	var errMsg *C.char
	if C.ray_metric_record(C.int64_t(handle), C.double(value), cStringArrayPtr(cKeys), cStringArrayPtr(cValues), C.int(len(tagKeys)), &errMsg) == 0 {
		return cError(errMsg)
	}
	return nil
}

// Unregister releases a registered metric handle.
func (b *CgoMetricBackend) Unregister(handle int64) error {
	var errMsg *C.char
	if C.ray_metric_unregister(C.int64_t(handle), &errMsg) == 0 {
		return cError(errMsg)
	}
	return nil
}

// cStringArrayPtr converts a Go slice of C strings to a **C.char pointer for
// the C boundary, returning nil for an empty slice. The slice is allocated by
// CStringSlice, which also provides the matching cleanup function.
func cStringArrayPtr(arr []*C.char) **C.char {
	if len(arr) == 0 {
		return nil
	}
	return (**C.char)(unsafe.Pointer(&arr[0]))
}

// doubleSliceToC allocates a C array of doubles.
func doubleSliceToC(ds []float64) *C.double {
	n := len(ds)
	if n == 0 {
		return nil
	}
	ptr := C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(C.double(0))))
	arr := (*[1 << 30]C.double)(ptr)
	for i, d := range ds {
		arr[i] = C.double(d)
	}
	return (*C.double)(ptr)
}
