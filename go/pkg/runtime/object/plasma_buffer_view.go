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
	"runtime"
	"sync/atomic"
	"unsafe"
)

// PlasmaBufferView is a zero-copy view over an object's data buffer.
//
// It holds a raw pointer into the backing memory (a plasma shared-memory
// buffer in the native object store, a LocalMemoryBuffer elsewhere) together
// with an opaque handle that keeps that memory alive. The handle is owned and
// released by an injected release function (provided by the CGO layer that
// constructed the view); the Go GC also runs the release via a finalizer.
//
// This mirrors Python's Buffer.make(shared_ptr) reference-counting semantics:
// the view keeps the backing memory alive until Release is called, and the
// bytes are accessed with no copy.
type PlasmaBufferView struct {
	// ptr points to the first byte of the backing memory.
	ptr unsafe.Pointer
	// size is the number of valid bytes at ptr.
	size int
	// handle is the opaque ownership token handed to releaseFunc. A value of 0
	// means the view owns nothing (e.g. it was constructed over Go memory).
	handle uint64
	// releaseFunc drops the ownership token; nil when there is nothing to
	// release (Go-backed view).
	releaseFunc func(handle uint64)
	// released guards against double release from explicit Release + finalizer.
	released atomic.Bool
}

// NewPlasmaBufferView creates a zero-copy view. ptr/size describe the backing
// memory; handle/releaseFunc are the ownership token and its release callback.
// When releaseFunc is nil, Release is a no-op (safe for Go-backed memory).
// A finalizer is only registered when the view actually owns a handle, so
// handle-less views (e.g. over Go memory) do not pay GC finalizer overhead.
func NewPlasmaBufferView(ptr unsafe.Pointer, size int, handle uint64, releaseFunc func(uint64)) *PlasmaBufferView {
	v := &PlasmaBufferView{
		ptr:         ptr,
		size:        size,
		handle:      handle,
		releaseFunc: releaseFunc,
	}
	if handle != 0 && releaseFunc != nil {
		runtime.SetFinalizer(v, (*PlasmaBufferView).Release)
	}
	return v
}

// Bytes returns a zero-copy slice over the backing memory. The slice aliases
// the underlying buffer; the caller must keep the view alive while using it.
func (v *PlasmaBufferView) Bytes() []byte {
	if v == nil || v.ptr == nil || v.size == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(v.ptr), v.size)
}

// Size returns the number of bytes in the view.
func (v *PlasmaBufferView) Size() int {
	if v == nil {
		return 0
	}
	return v.size
}

// Release drops the ownership token, allowing the backing memory to be
// reclaimed once no other references remain. It is safe to call explicitly and
// is run by the Go GC via the finalizer. Release is idempotent; after Release
// the view is unusable and Bytes() returns nil.
func (v *PlasmaBufferView) Release() {
	if v == nil {
		return
	}
	runtime.SetFinalizer(v, nil)
	if v.released.CompareAndSwap(false, true) {
		if v.releaseFunc != nil && v.handle != 0 {
			v.releaseFunc(v.handle)
		}
	}
	// Invalidate the view so any later Bytes() call returns nil. Note this does
	// NOT protect slices already handed out via Bytes(): callers must not use
	// the view (or its slices) after Release.
	v.handle = 0
	v.ptr = nil
	v.size = 0
}
