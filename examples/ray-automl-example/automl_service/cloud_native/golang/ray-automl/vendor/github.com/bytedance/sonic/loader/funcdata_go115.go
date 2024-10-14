// go:build go1.15 && !go1.18
// +build go1.15,!go1.18

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
    `encoding`
    `os`
    `unsafe`

    `github.com/bytedance/sonic/internal/rt`
)

const (
    _Magic uint32 = 0xfffffff0
)

type pcHeader struct {
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

type moduledata struct {
    pcHeader     *pcHeader
    funcnametab  []byte
    cutab        []uint32
    filetab      []byte
    pctab        []byte
    pclntable    []byte
    ftab         []funcTab
    findfunctab  uintptr
    minpc, maxpc uintptr // first func address, last func address + last func size

    text, etext           uintptr // start/end of text, (etext-text) must be greater than MIN_FUNC
    noptrdata, enoptrdata uintptr
    data, edata           uintptr
    bss, ebss             uintptr
    noptrbss, enoptrbss   uintptr
    end, gcdata, gcbss    uintptr
    types, etypes         uintptr
    rodata                uintptr
    gofunc                uintptr // go.func.* is actual funcinfo object in image

    textsectmap []textSection // see runtime/symtab.go: textAddr()
    typelinks   []int32 // offsets from types
    itablinks   []*rt.GoItab

    ptab []ptabEntry

    pluginpath string
    pkghashes  []modulehash

    modulename   string
    modulehashes []modulehash

    hasmain uint8 // 1 if module contains the main function, 0 otherwise

    gcdatamask, gcbssmask bitVector

    typemap map[int32]*rt.GoType // offset to *_rtype in previous module

    bad bool // module failed to load and should be ignored

    next *moduledata
}

type _func struct {
    entryOff uint32 // start pc, as offset from moduledata.text/pcHeader.textStart
    nameOff  int32  // function name, as index into moduledata.funcnametab.

    args        int32  // in/out args size
    deferreturn uint32 // offset of start of a deferreturn call instruction from entry, if any.

    pcsp      uint32 
    pcfile    uint32
    pcln      uint32
    npcdata   uint32
    cuOffset  uint32 // runtime.cutab offset of this function's CU
    funcID    uint8  // set for certain special runtime functions
    flag      uint8
    _         [1]byte // pad
    nfuncdata uint8   // 
    
    // The end of the struct is followed immediately by two variable-length
    // arrays that reference the pcdata and funcdata locations for this
    // function.

    // pcdata contains the offset into moduledata.pctab for the start of
    // that index's table. e.g.,
    // &moduledata.pctab[_func.pcdata[_PCDATA_UnsafePoint]] is the start of
    // the unsafe point table.
    //
    // An offset of 0 indicates that there is no table.
    //
    // pcdata [npcdata]uint32

    // funcdata contains the offset past moduledata.gofunc which contains a
    // pointer to that index's funcdata. e.g.,
    // *(moduledata.gofunc +  _func.funcdata[_FUNCDATA_ArgsPointerMaps]) is
    // the argument pointer map.
    //
    // An offset of ^uint32(0) indicates that there is no entry.
    //
    // funcdata [nfuncdata]uint32
}

type funcTab struct {
    entry   uint32
    funcoff uint32
}

type bitVector struct {
    n        int32 // # of bits
    bytedata *uint8
}

type ptabEntry struct {
    name int32
    typ  int32
}

type textSection struct {
    vaddr    uintptr // prelinked section vaddr
    end      uintptr // vaddr + section length
    baseaddr uintptr // relocated section address
}

type modulehash struct {
    modulename   string
    linktimehash string
    runtimehash  *string
}

// findfuncbucket is an array of these structures.
// Each bucket represents 4096 bytes of the text segment.
// Each subbucket represents 256 bytes of the text segment.
// To find a function given a pc, locate the bucket and subbucket for
// that pc. Add together the idx and subbucket value to obtain a
// function index. Then scan the functab array starting at that
// index to find the target function.
// This table uses 20 bytes for every 4096 bytes of code, or ~0.5% overhead.
type findfuncbucket struct {
    idx        uint32
    _SUBBUCKETS [16]byte
}

// func name table format: 
//   nameOff[0] -> namePartA namePartB namePartC \x00 
//   nameOff[1] -> namePartA namePartB namePartC \x00
//  ...
func makeFuncnameTab(funcs []Func) (tab []byte, offs []int32) {
    offs = make([]int32, len(funcs))
    offset := 0

    for i, f := range funcs {
        offs[i] = int32(offset)

        a, b, c := funcNameParts(f.Name)
        tab = append(tab, a...)
        tab = append(tab, b...)
        tab = append(tab, c...)
        tab = append(tab, 0)
        offset += len(a) + len(b) + len(c) + 1
    }

    return
}

type compilationUnit struct {
    fileNames []string
}

// CU table format:
//  cuOffsets[0] -> filetabOffset[0] filetabOffset[1] ... filetabOffset[len(CUs[0].fileNames)-1]
//  cuOffsets[1] -> filetabOffset[len(CUs[0].fileNames)] ... filetabOffset[len(CUs[0].fileNames) + len(CUs[1].fileNames)-1]
//  ...
//
// file name table format:
//  filetabOffset[0] -> CUs[0].fileNames[0] \x00
//  ...
//  filetabOffset[len(CUs[0]-1)] -> CUs[0].fileNames[len(CUs[0].fileNames)-1] \x00
//  ...
//  filetabOffset[SUM(CUs,fileNames)-1] -> CUs[len(CU)-1].fileNames[len(CUs[len(CU)-1].fileNames)-1] \x00
func makeFilenametab(cus []compilationUnit) (cutab []uint32, filetab []byte, cuOffsets []uint32) {
    cuOffsets = make([]uint32, len(cus))
    cuOffset := 0
    fileOffset := 0

    for i, cu := range cus {
        cuOffsets[i] = uint32(cuOffset)

        for _, name := range cu.fileNames {
            cutab = append(cutab, uint32(fileOffset))

            fileOffset += len(name) + 1
            filetab = append(filetab, name...)
            filetab = append(filetab, 0)
        }

        cuOffset += len(cu.fileNames)
    }

    return
}

func writeFuncdata(out *[]byte, funcs []Func) (fstart int, funcdataOffs [][]uint32) {
    fstart = len(*out)
    *out = append(*out, byte(0))
    offs := uint32(1)

    funcdataOffs = make([][]uint32, len(funcs))
    for i, f := range funcs {

        var writer = func(fd encoding.BinaryMarshaler) {
            var ab []byte
            var err error
            if fd != nil {
                ab, err = fd.MarshalBinary()
                if err != nil {
                    panic(err)
                }
                funcdataOffs[i] = append(funcdataOffs[i], offs)
            } else {
                ab = []byte{0}
                funcdataOffs[i] = append(funcdataOffs[i], _INVALID_FUNCDATA_OFFSET)
            }
            *out = append(*out, ab...)
            offs += uint32(len(ab))
        }

        writer(f.ArgsPointerMaps)
        writer(f.LocalsPointerMaps)
        writer(f.StackObjects)
        writer(f.InlTree)
        writer(f.OpenCodedDeferInfo)
        writer(f.ArgInfo)
        writer(f.ArgLiveInfo)
        writer(f.WrapInfo)
    }
    return 
}

func makeFtab(funcs []_func, lastFuncSize uint32) (ftab []funcTab) {
    // Allocate space for the pc->func table. This structure consists of a pc offset
    // and an offset to the func structure. After that, we have a single pc
    // value that marks the end of the last function in the binary.
    var size int64 = int64(len(funcs)*2*4 + 4)
    var startLocations = make([]uint32, len(funcs))
    for i, f := range funcs {
        size = rnd(size, int64(_PtrSize))
        //writePCToFunc
        startLocations[i] = uint32(size)
        size += int64(uint8(_FUNC_SIZE)+f.nfuncdata*4+uint8(f.npcdata)*4)
    }

    ftab = make([]funcTab, 0, len(funcs)+1)

    // write a map of pc->func info offsets 
    for i, f := range funcs {
        ftab = append(ftab, funcTab{uint32(f.entryOff), uint32(startLocations[i])})
    }

    // Final entry of table is just end pc offset.
    lastFunc := funcs[len(funcs)-1]
    ftab = append(ftab, funcTab{uint32(lastFunc.entryOff + lastFuncSize), 0})

    return
}

// Pcln table format: [...]funcTab + [...]_Func
func makePclntable(funcs []_func, lastFuncSize uint32, pcdataOffs [][]uint32, funcdataOffs [][]uint32) (pclntab []byte) {
    // Allocate space for the pc->func table. This structure consists of a pc offset
    // and an offset to the func structure. After that, we have a single pc
    // value that marks the end of the last function in the binary.
    var size int64 = int64(len(funcs)*2*4 + 4)
    var startLocations = make([]uint32, len(funcs))
    for i := range funcs {
        size = rnd(size, int64(_PtrSize))
        //writePCToFunc
        startLocations[i] = uint32(size)
        size += int64(int(_FUNC_SIZE)+len(funcdataOffs[i])*4+len(pcdataOffs[i])*4)
    }

    pclntab = make([]byte, size, size)

    // write a map of pc->func info offsets 
    offs := 0
    for i, f := range funcs {
        byteOrder.PutUint32(pclntab[offs:offs+4], uint32(f.entryOff))
        byteOrder.PutUint32(pclntab[offs+4:offs+8], uint32(startLocations[i]))
        offs += 8
    }
    // Final entry of table is just end pc offset.
    lastFunc := funcs[len(funcs)-1]
    byteOrder.PutUint32(pclntab[offs:offs+4], uint32(lastFunc.entryOff+lastFuncSize))

    // write func info table
    for i, f := range funcs {
        off := startLocations[i]

        // write _func structure to pclntab
        fb := rt.BytesFrom(unsafe.Pointer(&f), int(_FUNC_SIZE), int(_FUNC_SIZE))
        copy(pclntab[off:off+uint32(_FUNC_SIZE)], fb)
        off += uint32(_FUNC_SIZE)

        // NOTICE: _func.pcdata always starts from PcUnsafePoint, which is index 3
        for j := 3; j < len(pcdataOffs[i]); j++ {
            byteOrder.PutUint32(pclntab[off:off+4], uint32(pcdataOffs[i][j]))
            off += 4
        }

        // funcdata refs as offsets from gofunc
        for _, funcdata := range funcdataOffs[i] {
            byteOrder.PutUint32(pclntab[off:off+4], uint32(funcdata))
            off += 4
        }

    }

    return
}

// findfunc table used to map pc to belonging func, 
// returns the index in the func table.
//
// All text section are divided into buckets sized _BUCKETSIZE(4K):
//   every bucket is divided into _SUBBUCKETS sized _SUB_BUCKETSIZE(64),
//   and it has a base idx to plus the offset stored in jth subbucket.
// see findfunc() in runtime/symtab.go
func writeFindfunctab(out *[]byte, ftab []funcTab) (start int) {
    start = len(*out)

    max := ftab[len(ftab)-1].entry
    min := ftab[0].entry
    nbuckets := (max - min + _BUCKETSIZE - 1) / _BUCKETSIZE
    n := (max - min + _SUB_BUCKETSIZE - 1) / _SUB_BUCKETSIZE

    tab := make([]findfuncbucket, 0, nbuckets)
    var s, e = 0, 0
    for i := 0; i<int(nbuckets); i++ {
        var pc = min + uint32((i+1)*_BUCKETSIZE)
        // find the end func of the bucket
        for ; e < len(ftab)-1 && ftab[e+1].entry <= pc; e++ {}
        // store the start func of the bucket
        var fb = findfuncbucket{idx: uint32(s)}

        for j := 0; j<_SUBBUCKETS && (i*_SUBBUCKETS+j)<int(n); j++ {
            pc = min + uint32(i*_BUCKETSIZE) + uint32((j+1)*_SUB_BUCKETSIZE)
            var ss = s
            // find the end func of the subbucket
            for ; ss < len(ftab)-1 && ftab[ss+1].entry <= pc; ss++ {}
            // store the start func of the subbucket
            fb._SUBBUCKETS[j] = byte(uint32(s) - fb.idx)
            s = ss
        }
        s = e
        tab = append(tab, fb)
    }

    // write findfuncbucket
    if len(tab) > 0 {
        size := int(unsafe.Sizeof(findfuncbucket{}))*len(tab)
        *out = append(*out, rt.BytesFrom(unsafe.Pointer(&tab[0]), size, size)...)
    }
    return 
}

func makeModuledata(name string, filenames []string, funcs []Func, text []byte) (mod *moduledata) {
    mod = new(moduledata)
    mod.modulename = name

    // make filename table
    cu := make([]string, 0, len(filenames))
    for _, f := range filenames {
        cu = append(cu, f)
    }
    cutab, filetab, cuOffs := makeFilenametab([]compilationUnit{{cu}})
    mod.cutab = cutab
    mod.filetab = filetab

    // make funcname table
    funcnametab, nameOffs := makeFuncnameTab(funcs)
    mod.funcnametab = funcnametab

    // make pcdata table
    // NOTICE: _func only use offset to index pcdata, thus no need mmap() pcdata 
    pctab, pcdataOffs, _funcs := makePctab(funcs, cuOffs, nameOffs)
    mod.pctab = pctab

    // write func data
    // NOTICE: _func use mod.gofunc+offset to directly point funcdata, thus need cache funcdata
    // TODO: estimate accurate capacity
    cache := make([]byte, 0, len(funcs)*int(_PtrSize)) 
    fstart, funcdataOffs := writeFuncdata(&cache, funcs)

    // make pc->func (binary search) func table
    lastFuncsize := funcs[len(funcs)-1].TextSize
    ftab := makeFtab(_funcs, lastFuncsize)
    mod.ftab = ftab

    // write pc->func (modmap) findfunc table
    ffstart := writeFindfunctab(&cache, ftab)

    // make pclnt table
    pclntab := makePclntable(_funcs, lastFuncsize, pcdataOffs, funcdataOffs)
    mod.pclntable = pclntab

    // mmap() text and funcdata segements
    p := os.Getpagesize()
    size := int(rnd(int64(len(text)), int64(p)))
    addr := mmap(size)
    // copy the machine code
    s := rt.BytesFrom(unsafe.Pointer(addr), len(text), size)
    copy(s, text)
    // make it executable
    mprotect(addr, size)

    // assign addresses
    mod.text = addr
    mod.etext = addr + uintptr(size)
    mod.minpc = addr
    mod.maxpc = addr + uintptr(len(text))

    // cache funcdata and findfuncbucket
    moduleCache.Lock()
    moduleCache.m[mod] = cache
    moduleCache.Unlock()
    mod.gofunc = uintptr(unsafe.Pointer(&cache[fstart]))
    mod.findfunctab = uintptr(unsafe.Pointer(&cache[ffstart]))

    // make pc header
    mod.pcHeader = &pcHeader {
        magic   : _Magic,
        minLC   : _MinLC,
        ptrSize : _PtrSize,
        nfunc   : len(funcs),
        nfiles: uint(len(cu)),
        textStart: mod.text,
        funcnameOffset: getOffsetOf(moduledata{}, "funcnametab"),
        cuOffset: getOffsetOf(moduledata{}, "cutab"),
        filetabOffset: getOffsetOf(moduledata{}, "filetab"),
        pctabOffset: getOffsetOf(moduledata{}, "pctab"),
        pclnOffset: getOffsetOf(moduledata{}, "pclntable"),
    }

    // sepecial case: gcdata and gcbss must by non-empty
    mod.gcdata = uintptr(unsafe.Pointer(&emptyByte))
    mod.gcbss = uintptr(unsafe.Pointer(&emptyByte))

    return
}

// makePctab generates pcdelta->valuedelta tables for functions,
// and returns the table and the entry offset of every kind pcdata in the table.
func makePctab(funcs []Func, cuOffset []uint32, nameOffset []int32) (pctab []byte, pcdataOffs [][]uint32, _funcs []_func) {
    _funcs = make([]_func, len(funcs))

    // Pctab offsets of 0 are considered invalid in the runtime. We respect
    // that by just padding a single byte at the beginning of runtime.pctab,
    // that way no real offsets can be zero.
    pctab = make([]byte, 1, 12*len(funcs)+1)
    pcdataOffs = make([][]uint32, len(funcs))

    for i, f := range funcs {
        _f := &_funcs[i]

        var writer = func(pc *Pcdata) {
            var ab []byte
            var err error
            if pc != nil {
                ab, err = pc.MarshalBinary()
                if err != nil {
                    panic(err)
                }
                pcdataOffs[i] = append(pcdataOffs[i], uint32(len(pctab)))
            } else {
                ab = []byte{0}
                pcdataOffs[i] = append(pcdataOffs[i], _PCDATA_INVALID_OFFSET)
            }
            pctab = append(pctab, ab...)
        }

        if f.Pcsp != nil {
            _f.pcsp = uint32(len(pctab))
        }
        writer(f.Pcsp)
        if f.Pcfile != nil {
            _f.pcfile = uint32(len(pctab))
        }
        writer(f.Pcfile)
        if f.Pcline != nil {
            _f.pcln = uint32(len(pctab))
        }
        writer(f.Pcline)
        writer(f.PcUnsafePoint)
        writer(f.PcStackMapIndex)
        writer(f.PcInlTreeIndex)
        writer(f.PcArgLiveIndex)
        
        _f.entryOff = f.EntryOff
        _f.nameOff = nameOffset[i]
        _f.args = f.ArgsSize
        _f.deferreturn = f.DeferReturn
        // NOTICE: _func.pcdata is always as [PCDATA_UnsafePoint(0) : PCDATA_ArgLiveIndex(3)]
        _f.npcdata = uint32(_N_PCDATA)
        _f.cuOffset = cuOffset[i]
        _f.funcID = f.ID
        _f.flag = f.Flag
        _f.nfuncdata = uint8(_N_FUNCDATA)
    }

    return
}

func registerFunction(name string, pc uintptr, textSize uintptr, fp int, args int, size uintptr, argptrs uintptr, localptrs uintptr) {} 