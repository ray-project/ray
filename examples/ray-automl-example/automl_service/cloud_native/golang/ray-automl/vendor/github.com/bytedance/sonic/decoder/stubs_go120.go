// +build go1.20

/*
 * Copyright 2021 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package decoder

import (
    `unsafe`
    `reflect`

    _ `github.com/chenzhuoyu/base64x`

    `github.com/bytedance/sonic/internal/rt`
)

//go:linkname _subr__b64decode github.com/chenzhuoyu/base64x._subr__b64decode
var _subr__b64decode uintptr

// runtime.maxElementSize
const _max_map_element_size uintptr = 128

func mapfast(vt reflect.Type) bool {
    return vt.Elem().Size() <= _max_map_element_size
}

//go:nosplit
//go:linkname throw runtime.throw
//goland:noinspection GoUnusedParameter
func throw(s string)

//go:linkname convT64 runtime.convT64
//goland:noinspection GoUnusedParameter
func convT64(v uint64) unsafe.Pointer

//go:linkname convTslice runtime.convTslice
//goland:noinspection GoUnusedParameter
func convTslice(v []byte) unsafe.Pointer

//go:linkname convTstring runtime.convTstring
//goland:noinspection GoUnusedParameter
func convTstring(v string) unsafe.Pointer

//go:noescape
//go:linkname memequal runtime.memequal
//goland:noinspection GoUnusedParameter
func memequal(a unsafe.Pointer, b unsafe.Pointer, size uintptr) bool

//go:noescape
//go:linkname memmove runtime.memmove
//goland:noinspection GoUnusedParameter
func memmove(to unsafe.Pointer, from unsafe.Pointer, n uintptr)

//go:linkname mallocgc runtime.mallocgc
//goland:noinspection GoUnusedParameter
func mallocgc(size uintptr, typ *rt.GoType, needzero bool) unsafe.Pointer

//go:linkname makeslice runtime.makeslice
//goland:noinspection GoUnusedParameter
func makeslice(et *rt.GoType, len int, cap int) unsafe.Pointer

//go:noescape
//go:linkname growslice reflect.growslice
//goland:noinspection GoUnusedParameter
func growslice(et *rt.GoType, old rt.GoSlice, cap int) rt.GoSlice

//go:linkname makemap_small runtime.makemap_small
func makemap_small() unsafe.Pointer

//go:linkname mapassign runtime.mapassign
//goland:noinspection GoUnusedParameter
func mapassign(t *rt.GoType, h unsafe.Pointer, k unsafe.Pointer) unsafe.Pointer

//go:linkname mapassign_fast32 runtime.mapassign_fast32
//goland:noinspection GoUnusedParameter
func mapassign_fast32(t *rt.GoType, h unsafe.Pointer, k uint32) unsafe.Pointer

//go:linkname mapassign_fast64 runtime.mapassign_fast64
//goland:noinspection GoUnusedParameter
func mapassign_fast64(t *rt.GoType, h unsafe.Pointer, k uint64) unsafe.Pointer

//go:linkname mapassign_fast64ptr runtime.mapassign_fast64ptr
//goland:noinspection GoUnusedParameter
func mapassign_fast64ptr(t *rt.GoType, h unsafe.Pointer, k unsafe.Pointer) unsafe.Pointer

//go:linkname mapassign_faststr runtime.mapassign_faststr
//goland:noinspection GoUnusedParameter
func mapassign_faststr(t *rt.GoType, h unsafe.Pointer, s string) unsafe.Pointer

//go:nosplit
//go:linkname memclrHasPointers runtime.memclrHasPointers
//goland:noinspection GoUnusedParameter
func memclrHasPointers(ptr unsafe.Pointer, n uintptr)

//go:noescape
//go:linkname memclrNoHeapPointers runtime.memclrNoHeapPointers
//goland:noinspection GoUnusedParameter
func memclrNoHeapPointers(ptr unsafe.Pointer, n uintptr)