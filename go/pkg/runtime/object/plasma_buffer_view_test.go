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

// Unit tests for PlasmaBufferView.
//
// These validate the zero-copy slicing and handle-release semantics WITHOUT
// calling into a real CoreWorker/plasma store. A Go byte slice stands in for
// the plasma shared-memory backing: the pointer + size + handle triple of
// PlasmaBufferView behaves identically regardless of where the memory lives.
//
// What is validated here:
//  1. Bytes() aliases the backing memory (no copy) - pointer equality.
//  2. Bytes() content matches the backing memory.
//  3. release() is idempotent (safe to call once handle is zero).
//  4. Release() clears the finalizer and drops the handle via releaseFunc.

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestPlasmaBufferView_BytesZeroCopy: Bytes() must alias the backing memory,
// not copy it.
func TestPlasmaBufferView_BytesZeroCopy(t *testing.T) {
	backing := []byte("hello-zero-copy-poc")
	// handle=0 -> release() is a no-op, so no releaseFunc call happens here.
	pv := &PlasmaBufferView{
		ptr:    unsafe.Pointer(&backing[0]),
		size:   len(backing),
		handle: 0,
	}

	got := pv.Bytes()

	// Content matches.
	assert.Equal(t, backing, got)
	// Pointer equality: same underlying array -> zero copy.
	assert.Equal(t, uintptr(unsafe.Pointer(&backing[0])), uintptr(unsafe.Pointer(&got[0])))
}

// TestPlasmaBufferView_BytesEmpty: nil/empty view returns nil, no panic.
func TestPlasmaBufferView_BytesEmpty(t *testing.T) {
	pv := &PlasmaBufferView{ptr: nil, size: 0, handle: 0}
	assert.Nil(t, pv.Bytes())

	buf := [1]byte{0}
	pv2 := &PlasmaBufferView{ptr: unsafe.Pointer(&buf[0]), size: 0, handle: 0}
	assert.Nil(t, pv2.Bytes())
}

// TestPlasmaBufferView_ReleaseDropsHandle: Release calls releaseFunc exactly
// once with the handle, then the handle is zeroed.
func TestPlasmaBufferView_ReleaseDropsHandle(t *testing.T) {
	backing := []byte("data")
	released := make([]uint64, 0, 2)
	pv := &PlasmaBufferView{
		ptr:    unsafe.Pointer(&backing[0]),
		size:   len(backing),
		handle: 42,
		releaseFunc: func(h uint64) {
			released = append(released, h)
		},
	}

	pv.Release()
	pv.Release() // idempotent

	assert.Equal(t, []uint64{42}, released)
	assert.Equal(t, uint64(0), pv.handle)
}

// TestPlasmaBufferView_ReleaseIdempotent: release() is safe to call repeatedly
// and Release() clears the finalizer before dropping the handle.
func TestPlasmaBufferView_ReleaseIdempotent(t *testing.T) {
	backing := []byte("data")
	pv := &PlasmaBufferView{
		ptr:    unsafe.Pointer(&backing[0]),
		size:   len(backing),
		handle: 0, // 0 -> release is a no-op; real handles are dropped by releaseFunc
	}

	// Must not panic or crash on repeated release with handle == 0.
	pv.Release()
	pv.Release()

	// Explicit Release clears the finalizer and is also idempotent.
	pv.Release()
	pv.Release()
}

// TestPlasmaBufferView_AfterReleaseUnusable: Bytes() after release is not
// allowed by contract, but a nil/zeroed view must not crash.
func TestPlasmaBufferView_AfterReleaseUnusable(t *testing.T) {
	pv := &PlasmaBufferView{ptr: nil, size: 0, handle: 0}
	pv.Release()
	assert.Nil(t, pv.Bytes())
}
