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

package object

import (
	"testing"
	"unsafe"
)

// TestCloseOnlyReturnsPooledData verifies that Close() returns pool-marked Data
// via PutBuffer and leaves non-pooled Data (msgpack output, bytes.Clone,
// C.GoBytes, caller-owned arrays) to the GC. Returning non-pooled buffers to
// the tiered pool would violate its 64-byte alignment / size-class accounting.
func TestCloseOnlyReturnsPooledData(t *testing.T) {
	// Non-pooled Data: Close() must nil it out without pushing it into the pool.
	np := NewNativeRayObject([]byte("hello"), []byte("RAW"))
	if np.DataFromPool() {
		t.Fatalf("non-pooled object must not report DataFromPool")
	}
	if err := np.Close(); err != nil {
		t.Fatalf("Close() failed for non-pooled object: %v", err)
	}
	if np.Data != nil {
		t.Fatalf("Close() should nil out Data, got %d bytes", len(np.Data))
	}

	// Pooled Data (GetBuffer + MarkDataFromPool): Close() returns it to the pool.
	pooled := NewNativeRayObject(nil, nil)
	buf := GetBuffer(128)
	pooled.Data = append(buf, []byte("world")...)
	pooled.MarkDataFromPool()
	if !pooled.DataFromPool() {
		t.Fatalf("DataFromPool should be true after MarkDataFromPool")
	}
	if err := pooled.Close(); err != nil {
		t.Fatalf("Close() failed for pooled object: %v", err)
	}
	if pooled.Data != nil {
		t.Fatalf("Close() should nil out Data, got %d bytes", len(pooled.Data))
	}
}

// TestDataViewPreferredByDataBytesAndReleasedByClose pins the zero-copy
// contract on NativeRayObject: DataBytes() serves the view's bytes whenever a
// view is attached (Data may be nil for plasma-backed objects), Close() drops
// the backing buffer handle exactly once, and Close() is idempotent so a
// second call cannot double-free the handle.
func TestDataViewPreferredByDataBytesAndReleasedByClose(t *testing.T) {
	// Without a view, DataBytes() must return Data unchanged (the copied path).
	plain := NewNativeRayObject([]byte("copied"), []byte("RAW"))
	if got := string(plain.DataBytes()); got != "copied" {
		t.Fatalf("DataBytes() = %q, want %q", got, "copied")
	}

	backing := []byte("plasma-backed")
	var releases []uint64
	view := &PlasmaBufferView{
		ptr:    unsafe.Pointer(&backing[0]),
		size:   len(backing),
		handle: 7,
		releaseFunc: func(h uint64) {
			releases = append(releases, h)
		},
	}

	obj := &NativeRayObject{DataView: view}
	if got := string(obj.DataBytes()); got != string(backing) {
		t.Fatalf("DataBytes() = %q, want %q", got, backing)
	}

	if err := obj.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
	if obj.DataView != nil {
		t.Fatalf("Close() should clear the view")
	}
	if len(releases) != 1 || releases[0] != 7 {
		t.Fatalf("Close() should release the handle exactly once, got %v", releases)
	}
	if got := obj.DataBytes(); got != nil {
		t.Fatalf("DataBytes() after Close() = %q, want nil", got)
	}

	// A second Close must not release the handle again.
	if err := obj.Close(); err != nil {
		t.Fatalf("second Close() failed: %v", err)
	}
	if len(releases) != 1 {
		t.Fatalf("double Close() released the handle %d times, want 1", len(releases))
	}
}

// TestDataFromPoolNilReceiver guards nil receivers on the new accessor.
func TestDataFromPoolNilReceiver(t *testing.T) {
	var n *NativeRayObject
	if n.DataFromPool() {
		t.Fatalf("nil receiver DataFromPool should be false")
	}
	if err := n.Close(); err != nil {
		t.Fatalf("nil receiver Close should be a no-op, got: %v", err)
	}
}

// TestReleasePoolOwnershipPreventsPoolReturn verifies that calling
// ReleasePoolOwnership on a pool-marked object makes Close() leave the buffer
// to the GC instead of returning it to the pool. Returning a shared buffer to
// the pool would violate the pool's alignment/size-class accounting and allow
// pool reuse to invalidate references shared with the local mode object store.
func TestReleasePoolOwnershipPreventsPoolReturn(t *testing.T) {
	obj := NewNativeRayObject(nil, nil)
	buf := GetBuffer(128)
	obj.Data = append(buf, []byte("world")...)
	obj.MarkDataFromPool()
	if !obj.DataFromPool() {
		t.Fatalf("DataFromPool should be true after MarkDataFromPool")
	}

	obj.ReleasePoolOwnership()
	if obj.DataFromPool() {
		t.Fatalf("DataFromPool should be false after ReleasePoolOwnership")
	}

	// Close() must not push Data into the pool (it is no longer pool-owned).
	if err := obj.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
	if obj.Data != nil {
		t.Fatalf("Close() should nil out Data, got %d bytes", len(obj.Data))
	}
}

// TestReleasePoolOwnershipNilReceiver guards nil receivers on the new method.
func TestReleasePoolOwnershipNilReceiver(t *testing.T) {
	var n *NativeRayObject
	// Must be a no-op, not a panic.
	n.ReleasePoolOwnership()
}
