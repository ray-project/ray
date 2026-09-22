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

import "testing"

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
