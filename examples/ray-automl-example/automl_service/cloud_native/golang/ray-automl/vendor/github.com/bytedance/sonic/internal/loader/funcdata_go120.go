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

package loader

import (
    `unsafe`

    `github.com/bytedance/sonic/internal/rt`
)

// A FuncFlag holds bits about a function.
// This list must match the list in cmd/internal/objabi/funcid.go.
type funcFlag uint8

type _Func struct {
    entryOff    uint32 // start pc
    nameoff     int32   // function name
    args        int32   // in/out args size
    deferreturn uint32  // offset of start of a deferreturn call instruction from entry, if any.
    pcsp        uint32
    pcfile      uint32
    pcln        uint32
    npcdata     uint32
    cuOffset    uint32  // runtime.cutab offset of this function's CU
    funcID      uint8   // set for certain special runtime functions
    flag        funcFlag
    _           [1]byte // pad
    nfuncdata   uint8   // must be last
    argptrs     uint32
    localptrs   uint32
}

type _FuncTab struct {
    entry   uint32
    funcoff uint32
}

type _PCHeader struct {
    magic          uint32  // 0xFFFFFFF0
    pad1, pad2     uint8   // 0,0
    minLC          uint8   // min instruction size
    ptrSize        uint8   // size of a ptr in bytes
    nfunc          int     // number of functions in the module
    nfiles         uint    // number of entries in the file tab
    textStart      uintptr // base for function entry PC offsets in this module, equal to moduledata.text
    funcnameOffset uintptr // offset to the funcnametab variable from pcHeader
    cuOffset       uintptr // offset to the cutab variable from pcHeader
    filetabOffset  uintptr // offset to the filetab variable from pcHeader
    pctabOffset    uintptr // offset to the pctab variable from pcHeader
    pclnOffset     uintptr // offset to the pclntab variable from pcHeader
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
    pcHeader              *_PCHeader
    funcnametab           []byte
    cutab                 []uint32
    filetab               []byte
    pctab                 []byte
    pclntable             []byte
    ftab                  []_FuncTab
    findfunctab           *_FindFuncBucket
    minpc, maxpc          uintptr
    text, etext           uintptr
    noptrdata, enoptrdata uintptr
    data, edata           uintptr
    bss, ebss             uintptr
    noptrbss, enoptrbss   uintptr
    end, gcdata, gcbss    uintptr
    types, etypes         uintptr
    rodata                uintptr
    gofunc                uintptr 
    textsectmap           []_TextSection
    typelinks             []int32
    itablinks             []unsafe.Pointer
    ptab                  []_PtabEntry
    pluginpath            string
    pkghashes             []struct{}
    modulename            string
    modulehashes          []struct{}
    hasmain               uint8
    gcdatamask, gcbssmask _BitVector
    typemap               map[int32]unsafe.Pointer
    bad                   bool
    next                  *_ModuleData
}


type _FindFuncBucket struct {
    idx        uint32
    subbuckets [16]byte
}



func makePCtab(fp int) []byte {
    return append([]byte{0}, encodeVariant((fp + 1) << 1)...)
}

func registerFunction(name string, pc uintptr, textSize uintptr, fp int, args int, size uintptr, argPtrs []bool, localPtrs []bool) {
    mod := new(_ModuleData)

    minpc := pc
    maxpc := pc + size

    findFuncTab := make([]_FindFuncBucket, textSize/4096 + 1)

    modHeader := &_PCHeader {
        magic   : 0xfffffff0,
        minLC   : 1,
        nfunc   : 1,
        ptrSize : 4 << (^uintptr(0) >> 63),
        textStart: minpc,
    }

    // cache arg and local stackmap
    argptrs, localptrs := cacheStackmap(argPtrs, localPtrs, mod)

    base := argptrs
    if argptrs > localptrs {
        base = localptrs
    }

    /* function entry */
    lnt := []_Func {{
        entryOff  : 0,
        nameoff   : 1,
        args      : int32(args),
        pcsp      : 1,
        nfuncdata : 2,
        argptrs: uint32(argptrs - base),
        localptrs: uint32(localptrs - base),
    }}
    nlnt := len(lnt)*int(unsafe.Sizeof(_Func{}))
    plnt := unsafe.Pointer(&lnt[0])

    /* function table */
    ftab := []_FuncTab {
        {entry   : 0, funcoff : 16},    
        {entry   : uint32(size)},
    }
    nftab := len(ftab)*int(unsafe.Sizeof(_FuncTab{}))
    pftab := unsafe.Pointer(&ftab[0])

    pclntab := make([]byte, 0, nftab + nlnt)
    pclntab = append(pclntab, rt.BytesFrom(pftab, nftab, nftab)...)
    pclntab = append(pclntab, rt.BytesFrom(plnt, nlnt, nlnt)...)

    /* module data */
    *mod = _ModuleData {
        pcHeader    : modHeader,
        funcnametab : append(append([]byte{0}, name...), 0),
        pctab       : append(makePCtab(fp), encodeVariant(int(size))...),
        pclntable   : pclntab,
        ftab        : ftab,
        text        : minpc,
        etext       : pc + textSize,
        findfunctab : &findFuncTab[0],
        minpc       : minpc,
        maxpc       : maxpc,
        modulename  : name,
        gcdata: uintptr(unsafe.Pointer(&emptyByte)),
        gcbss: uintptr(unsafe.Pointer(&emptyByte)),
        gofunc: base,
    }

    /* verify and register the new module */
    moduledataverify1(mod)
    registerModule(mod)
}