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

package loader

import (
    `reflect`
    `sync`
    `unsafe`

    `github.com/bytedance/sonic/internal/rt`
)

//go:linkname lastmoduledatap runtime.lastmoduledatap
//goland:noinspection GoUnusedGlobalVariable
var lastmoduledatap *_ModuleData

//go:linkname moduledataverify1 runtime.moduledataverify1
func moduledataverify1(_ *_ModuleData)

// PCDATA and FUNCDATA table indexes.
//
// See funcdata.h and $GROOT/src/cmd/internal/objabi/funcdata.go.
const (
    _FUNCDATA_ArgsPointerMaps = 0
    _FUNCDATA_LocalsPointerMaps = 1
)

type funcInfo struct {
    *_Func
    datap *_ModuleData
}

//go:linkname findfunc runtime.findfunc
func findfunc(pc uintptr) funcInfo

//go:linkname funcdata runtime.funcdata
func funcdata(f funcInfo, i uint8) unsafe.Pointer

var (
    modLock sync.Mutex
    modList []*_ModuleData
)

var emptyByte byte

func encodeVariant(v int) []byte {
    var u int
    var r []byte

    /* split every 7 bits */
    for v > 127 {
        u = v & 0x7f
        v = v >> 7
        r = append(r, byte(u) | 0x80)
    }

    /* check for last one */
    if v == 0 {
        return r
    }

    /* add the last one */
    r = append(r, byte(v))
    return r
}

func registerModule(mod *_ModuleData) {
    modLock.Lock()
    modList = append(modList, mod)
    lastmoduledatap.next = mod
    lastmoduledatap = mod
    modLock.Unlock()
}

func stackMap(f interface{}) (args uintptr, locals uintptr) {
    fv := reflect.ValueOf(f)
    if fv.Kind() != reflect.Func {
        panic("f must be reflect.Func kind!")
    }
    fi := findfunc(fv.Pointer())
    return uintptr(funcdata(fi, uint8(_FUNCDATA_ArgsPointerMaps))), uintptr(funcdata(fi, uint8(_FUNCDATA_LocalsPointerMaps)))
}

var moduleCache = struct{
    m map[*_ModuleData][]byte
    l sync.Mutex
}{
    m : make(map[*_ModuleData][]byte),
}

func cacheStackmap(argPtrs []bool, localPtrs []bool, mod *_ModuleData) (argptrs uintptr, localptrs uintptr) {
    as := rt.StackMapBuilder{}
    for _, b := range argPtrs {
        as.AddField(b)
    }
    ab, _ := as.Build().MarshalBinary()
    ls := rt.StackMapBuilder{}
    for _, b := range localPtrs {
        ls.AddField(b)
    }
    lb, _ := ls.Build().MarshalBinary()
    cache := make([]byte, len(ab) + len(lb))
    copy(cache, ab)
    copy(cache[len(ab):], lb)
    moduleCache.l.Lock()
    moduleCache.m[mod] = cache
    moduleCache.l.Unlock()
    return uintptr(rt.IndexByte(cache, 0)), uintptr(rt.IndexByte(cache, len(ab)))

}