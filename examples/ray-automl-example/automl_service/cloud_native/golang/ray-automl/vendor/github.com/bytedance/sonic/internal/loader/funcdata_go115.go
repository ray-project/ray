// +build go1.15,!go1.16

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
    `unsafe`

    `github.com/bytedance/sonic/internal/rt`
)

type _Func struct {
    entry       uintptr // start pc
    nameoff     int32   // function name
    args        int32   // in/out args size
    deferreturn uint32  // offset of start of a deferreturn call instruction from entry, if any.
    pcsp        int32
    pcfile      int32
    pcln        int32
    npcdata     int32
    funcID      uint8   // set for certain special runtime functions
    _           [2]int8 // unused
    nfuncdata   uint8   // must be last
    argptrs     uintptr
    localptrs   uintptr
}

type _FuncTab struct {
    entry   uintptr
    funcoff uintptr
}

type _BitVector struct {
    n        int32 // # of bits
    bytedata *uint8
}

type _PtabEntry struct {
    name int32
    typ  int32
}

type _TextSection struct {
    vaddr    uintptr // prelinked section vaddr
    length   uintptr // section length
    baseaddr uintptr // relocated section address
}

type _ModuleData struct {
    pclntable             []byte
    ftab                  []_FuncTab
    filetab               []uint32
    findfunctab           *_FindFuncBucket
    minpc, maxpc          uintptr
    text, etext           uintptr
    noptrdata, enoptrdata uintptr
    data, edata           uintptr
    bss, ebss             uintptr
    noptrbss, enoptrbss   uintptr
    end, gcdata, gcbss    uintptr
    types, etypes         uintptr
    textsectmap           []_TextSection
    typelinks             []int32 // offsets from types
    itablinks             []*rt.GoItab
    ptab                  []_PtabEntry
    pluginpath            string
    pkghashes             []byte
    modulename            string
    modulehashes          []byte
    hasmain               uint8 // 1 if module contains the main function, 0 otherwise
    gcdatamask, gcbssmask _BitVector
    typemap               map[int32]*rt.GoType // offset to *_rtype in previous module
    bad                   bool                 // module failed to load and should be ignored
    next                  *_ModuleData
}

type _FindFuncBucket struct {
    idx        uint32
    subbuckets [16]byte
}

var findFuncTab = &_FindFuncBucket {
    idx: 1,
}

func registerFunction(name string, pc uintptr, textSize uintptr, fp int, args int, size uintptr, argPtrs []bool, localPtrs []bool) {
    mod := new(_ModuleData)
    minpc := pc
    maxpc := pc + size

    /* build the PC & line table */
    pclnt := []byte {
        0xfb, 0xff, 0xff, 0xff,     // magic   : 0xfffffffb
        0,                          // pad1    : 0
        0,                          // pad2    : 0
        1,                          // minLC   : 1
        4 << (^uintptr(0) >> 63),   // ptrSize : 4 << (^uintptr(0) >> 63)
    }

    // cache arg and local stackmap
    argptrs, localptrs := cacheStackmap(argPtrs, localPtrs, mod)

    /* add the function name */
    noff := len(pclnt)
    pclnt = append(append(pclnt, name...), 0)

    /* add PCDATA */
    pcsp := len(pclnt)
    pclnt = append(pclnt, encodeVariant((fp + 1) << 1)...)
    pclnt = append(pclnt, encodeVariant(int(size))...)

    /* function entry */
    fnv := _Func {
        entry     : pc,
        nameoff   : int32(noff),
        args      : int32(args),
        pcsp      : int32(pcsp),
        nfuncdata : 2,
        argptrs   : uintptr(argptrs),
        localptrs : uintptr(localptrs),
    }

    /* align the func to 8 bytes */
    if p := len(pclnt) % 8; p != 0 {
        pclnt = append(pclnt, make([]byte, 8 - p)...)
    }

    /* add the function descriptor */
    foff := len(pclnt)
    pclnt = append(pclnt, (*(*[unsafe.Sizeof(_Func{})]byte)(unsafe.Pointer(&fnv)))[:]...)

    /* function table */
    tab := []_FuncTab {
        {entry: pc, funcoff: uintptr(foff)},
        {entry: pc, funcoff: uintptr(foff)},
        {entry: maxpc},
    }

    /* module data */
    *mod = _ModuleData {
        pclntable   : pclnt,
        ftab        : tab,
        findfunctab : findFuncTab,
        minpc       : minpc,
        maxpc       : maxpc,
        modulename  : name,
        gcdata: uintptr(unsafe.Pointer(&emptyByte)),
        gcbss: uintptr(unsafe.Pointer(&emptyByte)),
    }

    /* verify and register the new module */
    moduledataverify1(mod)
    registerModule(mod)
}
