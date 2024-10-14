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

package encoder

import (
	"encoding"
	"reflect"
	"sync"
	"unsafe"

	"github.com/bytedance/sonic/internal/native"
	"github.com/bytedance/sonic/internal/rt"
)

type _MapPair struct {
    k string  // when the map key is integer, k is pointed to m
    v unsafe.Pointer
    m [32]byte
}

type _MapIterator struct {
    it rt.GoMapIterator     // must be the first field
    kv rt.GoSlice           // slice of _MapPair
    ki int
}

var (
    iteratorPool = sync.Pool{}
    iteratorPair = rt.UnpackType(reflect.TypeOf(_MapPair{}))
)

func init() {
    if unsafe.Offsetof(_MapIterator{}.it) != 0 {
        panic("_MapIterator.it is not the first field")
    }
}


func newIterator() *_MapIterator {
    if v := iteratorPool.Get(); v == nil {
        return new(_MapIterator)
    } else {
        return resetIterator(v.(*_MapIterator))
    }
}

func resetIterator(p *_MapIterator) *_MapIterator {
    p.ki = 0
    p.it = rt.GoMapIterator{}
    p.kv.Len = 0
    return p
}

func (self *_MapIterator) at(i int) *_MapPair {
    return (*_MapPair)(unsafe.Pointer(uintptr(self.kv.Ptr) + uintptr(i) * unsafe.Sizeof(_MapPair{})))
}

func (self *_MapIterator) add() (p *_MapPair) {
    p = self.at(self.kv.Len)
    self.kv.Len++
    return
}

func (self *_MapIterator) data() (p []_MapPair) {
    *(*rt.GoSlice)(unsafe.Pointer(&p)) = self.kv
    return
}

func (self *_MapIterator) append(t *rt.GoType, k unsafe.Pointer, v unsafe.Pointer) (err error) {
    p := self.add()
    p.v = v

    /* check for strings */
    if tk := t.Kind(); tk != reflect.String {
        return self.appendGeneric(p, t, tk, k)
    }

    /* fast path for strings */
    p.k = *(*string)(k)
    return nil
}

func (self *_MapIterator) appendGeneric(p *_MapPair, t *rt.GoType, v reflect.Kind, k unsafe.Pointer) error {
    switch v {
        case reflect.Int       : p.k = rt.Mem2Str(p.m[:native.I64toa(&p.m[0], int64(*(*int)(k)))])      ; return nil
        case reflect.Int8      : p.k = rt.Mem2Str(p.m[:native.I64toa(&p.m[0], int64(*(*int8)(k)))])     ; return nil
        case reflect.Int16     : p.k = rt.Mem2Str(p.m[:native.I64toa(&p.m[0], int64(*(*int16)(k)))])    ; return nil
        case reflect.Int32     : p.k = rt.Mem2Str(p.m[:native.I64toa(&p.m[0], int64(*(*int32)(k)))])    ; return nil
        case reflect.Int64     : p.k = rt.Mem2Str(p.m[:native.I64toa(&p.m[0], *(*int64)(k))])           ; return nil
        case reflect.Uint      : p.k = rt.Mem2Str(p.m[:native.U64toa(&p.m[0], uint64(*(*uint)(k)))])    ; return nil
        case reflect.Uint8     : p.k = rt.Mem2Str(p.m[:native.U64toa(&p.m[0], uint64(*(*uint8)(k)))])   ; return nil
        case reflect.Uint16    : p.k = rt.Mem2Str(p.m[:native.U64toa(&p.m[0], uint64(*(*uint16)(k)))])  ; return nil
        case reflect.Uint32    : p.k = rt.Mem2Str(p.m[:native.U64toa(&p.m[0], uint64(*(*uint32)(k)))])  ; return nil
        case reflect.Uint64    : p.k = rt.Mem2Str(p.m[:native.U64toa(&p.m[0], *(*uint64)(k))])          ; return nil
        case reflect.Uintptr   : p.k = rt.Mem2Str(p.m[:native.U64toa(&p.m[0], uint64(*(*uintptr)(k)))]) ; return nil
        case reflect.Interface : return self.appendInterface(p, t, k)
        case reflect.Struct, reflect.Ptr : return self.appendConcrete(p, t, k)
        default                : panic("unexpected map key type")
    }
}

func (self *_MapIterator) appendConcrete(p *_MapPair, t *rt.GoType, k unsafe.Pointer) (err error) {
    // compiler has already checked that the type implements the encoding.MarshalText interface
    if !t.Indirect() {
        k = *(*unsafe.Pointer)(k)
    }
    eface := rt.GoEface{Value: k, Type: t}.Pack()
    out, err := eface.(encoding.TextMarshaler).MarshalText()
    if err != nil {
        return err
    }
    p.k = rt.Mem2Str(out)
    return
}

func (self *_MapIterator) appendInterface(p *_MapPair, t *rt.GoType, k unsafe.Pointer) (err error) {
    if len(rt.IfaceType(t).Methods) == 0 {
        panic("unexpected map key type")
    } else if p.k, err = asText(k); err == nil {
        return nil
    } else {
        return
    }
}

func iteratorStop(p *_MapIterator) {
    iteratorPool.Put(p)
}

func iteratorNext(p *_MapIterator) {
    i := p.ki
    t := &p.it

    /* check for unordered iteration */
    if i < 0 {
        mapiternext(t)
        return
    }

    /* check for end of iteration */
    if p.ki >= p.kv.Len {
        t.K = nil
        t.V = nil
        return
    }

    /* update the key-value pair, and increase the pointer */
    t.K = unsafe.Pointer(&p.at(p.ki).k)
    t.V = p.at(p.ki).v
    p.ki++
}

func iteratorStart(t *rt.GoMapType, m *rt.GoMap, fv uint64) (*_MapIterator, error) {
    it := newIterator()
    mapiterinit(t, m, &it.it)

    /* check for key-sorting, empty map don't need sorting */
    if m.Count == 0 || (fv & uint64(SortMapKeys)) == 0 {
        it.ki = -1
        return it, nil
    }

    /* pre-allocate space if needed */
    if m.Count > it.kv.Cap {
        it.kv = growslice(iteratorPair, it.kv, m.Count)
    }

    /* dump all the key-value pairs */
    for ; it.it.K != nil; mapiternext(&it.it) {
        if err := it.append(t.Key, it.it.K, it.it.V); err != nil {
            iteratorStop(it)
            return nil, err
        }
    }

    /* sort the keys, map with only 1 item don't need sorting */
    if it.ki = 1; m.Count > 1 {
        radixQsort(it.data(), 0, maxDepth(it.kv.Len))
    }

    /* load the first pair into iterator */
    it.it.V = it.at(0).v
    it.it.K = unsafe.Pointer(&it.at(0).k)
    return it, nil
}
