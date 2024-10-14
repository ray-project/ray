// +build !go1.20

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

package ast

import (
    `unsafe`
    `unicode/utf8`

    `github.com/bytedance/sonic/internal/rt`
)

//go:noescape
//go:linkname memmove runtime.memmove
//goland:noinspection GoUnusedParameter
func memmove(to unsafe.Pointer, from unsafe.Pointer, n uintptr)

//go:linkname unsafe_NewArray reflect.unsafe_NewArray
//goland:noinspection GoUnusedParameter
func unsafe_NewArray(typ *rt.GoType, n int) unsafe.Pointer

//go:linkname growslice runtime.growslice
//goland:noinspection GoUnusedParameter
func growslice(et *rt.GoType, old rt.GoSlice, cap int) rt.GoSlice

//go:nosplit
func mem2ptr(s []byte) unsafe.Pointer {
    return (*rt.GoSlice)(unsafe.Pointer(&s)).Ptr
}

var (
    //go:linkname safeSet encoding/json.safeSet
    safeSet [utf8.RuneSelf]bool

    //go:linkname hex encoding/json.hex
    hex string
)

//go:linkname unquoteBytes encoding/json.unquoteBytes
func unquoteBytes(s []byte) (t []byte, ok bool)