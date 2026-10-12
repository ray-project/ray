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

//go:build cgo
// +build cgo

package cgo

/*
#include <stdlib.h>
*/
import "C"

import (
	"testing"
	"unsafe"
)

func TestCStringSliceAndDoubleSliceToC(t *testing.T) {
	ss := []string{"a", "b"}
	cs, free := CStringSlice(ss)
	defer free()
	if C.GoString(cs[0]) != "a" || C.GoString(cs[1]) != "b" {
		t.Fatal("string slice roundtrip failed")
	}
	ptr := cStringArrayPtr(cs)
	arr := (*[1 << 20]*C.char)(unsafe.Pointer(ptr))
	if C.GoString(arr[0]) != "a" || C.GoString(arr[1]) != "b" {
		t.Fatal("cStringArrayPtr roundtrip failed")
	}

	ds := []float64{0.5, 1.5}
	cd := doubleSliceToC(ds)
	defer C.free(unsafe.Pointer(cd))
	cdArr := (*[1 << 20]C.double)(unsafe.Pointer(cd))
	if float64(cdArr[0]) != 0.5 || float64(cdArr[1]) != 1.5 {
		t.Fatal("double slice roundtrip failed")
	}
}

func TestCStringSliceEmptyReturnsNil(t *testing.T) {
	cs, free := CStringSlice(nil)
	defer free()
	if cStringArrayPtr(cs) != nil {
		t.Fatal("cStringArrayPtr(nil slice) should return nil")
	}
	cs2, free2 := CStringSlice([]string{})
	defer free2()
	if cStringArrayPtr(cs2) != nil {
		t.Fatal("cStringArrayPtr(empty slice) should return nil")
	}

	if cd := doubleSliceToC(nil); cd != nil {
		t.Fatal("doubleSliceToC(nil) should return nil")
	}
	if cd := doubleSliceToC([]float64{}); cd != nil {
		t.Fatal("doubleSliceToC(empty) should return nil")
	}
}

func TestCgoMetricBackend_RecordTagCountMismatch(t *testing.T) {
	b := &CgoMetricBackend{}
	// Fewer tag values than keys must be rejected before crossing the C++
	// boundary, which would otherwise build tags from len(tagKeys) and read past
	// the shorter value array.
	if err := b.Record(1, 1.0, []string{"k1", "k2"}, []string{"v1"}); err == nil {
		t.Fatal("expected error for tag key/value count mismatch")
	}
	// Equal counts pass the Go-side guard and reach the C++ boundary; an unknown
	// handle is rejected there safely (no dereference of freed memory).
	if err := b.Record(-1, 1.0, []string{"k1"}, []string{"v1"}); err == nil {
		t.Fatal("expected error for unknown metric handle")
	}
}

func TestCgoMetricBackend_RegisterValidationErrors(t *testing.T) {
	b := &CgoMetricBackend{}
	// Empty tag key is rejected by the C++ boundary before any stats
	// interaction, so these are safe to exercise without a real core worker.
	if err := b.RegisterTagKey(""); err == nil {
		t.Fatal("expected error for empty tag key")
	}
	// Invalid (empty) metric names are rejected by IsValidMetricName before any
	// stats::Metric object is constructed.
	if _, err := b.RegisterCount("", "", "", nil); err == nil {
		t.Fatal("expected error for invalid count metric name")
	}
	if _, err := b.RegisterGauge("", "", "", nil); err == nil {
		t.Fatal("expected error for invalid gauge metric name")
	}
	if _, err := b.RegisterSum("", "", "", nil); err == nil {
		t.Fatal("expected error for invalid sum metric name")
	}
	// A valid name with empty boundaries is rejected before construction.
	if _, err := b.RegisterHistogram("h", "desc", "unit", nil, nil); err == nil {
		t.Fatal("expected error for histogram without boundaries")
	}
	// Unregistering an unknown handle is rejected safely.
	if err := b.Unregister(-1); err == nil {
		t.Fatal("expected error for unknown metric handle")
	}
}
