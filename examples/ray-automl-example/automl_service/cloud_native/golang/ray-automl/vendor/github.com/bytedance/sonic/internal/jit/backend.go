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

package jit

import (
    `fmt`
    `sync`
    _ `unsafe`

    `github.com/twitchyliquid64/golang-asm/asm/arch`
    `github.com/twitchyliquid64/golang-asm/obj`
    `github.com/twitchyliquid64/golang-asm/objabi`
)

type Backend struct {
    Ctxt *obj.Link
    Arch *arch.Arch
    Head *obj.Prog
    Tail *obj.Prog
    Prog []*obj.Prog
}

var (
    _progPool sync.Pool
)

//go:nosplit
//go:linkname throw runtime.throw
func throw(_ string)

func newProg() *obj.Prog {
    if val := _progPool.Get(); val == nil {
        return new(obj.Prog)
    } else {
        return remProg(val.(*obj.Prog))
    }
}

func remProg(p *obj.Prog) *obj.Prog {
    *p = obj.Prog{}
    return p
}

func newBackend(name string) (ret *Backend) {
    ret      = new(Backend)
    ret.Arch = arch.Set(name)
    ret.Ctxt = newLinkContext(ret.Arch.LinkArch)
    ret.Arch.Init(ret.Ctxt)
    return
}

func newLinkContext(arch *obj.LinkArch) (ret *obj.Link) {
    ret          = obj.Linknew(arch)
    ret.Headtype = objabi.Hlinux
    ret.DiagFunc = diagLinkContext
    return
}

func diagLinkContext(str string, args ...interface{}) {
    throw(fmt.Sprintf(str, args...))
}

func (self *Backend) New() (ret *obj.Prog) {
    ret = newProg()
    ret.Ctxt = self.Ctxt
    self.Prog = append(self.Prog, ret)
    return
}

func (self *Backend) Append(p *obj.Prog) {
    if self.Head == nil {
        self.Head = p
        self.Tail = p
    } else {
        self.Tail.Link = p
        self.Tail = p
    }
}

func (self *Backend) Release() {
    self.Arch = nil
    self.Ctxt = nil

    /* return all the progs into pool */
    for _, p := range self.Prog {
        _progPool.Put(p)
    }

    /* clear all the references */
    self.Head = nil
    self.Tail = nil
    self.Prog = nil
}

func (self *Backend) Assemble() []byte {
    var sym obj.LSym
    var fnv obj.FuncInfo

    /* construct the function */
    sym.Func = &fnv
    fnv.Text = self.Head

    /* call the assembler */
    self.Arch.Assemble(self.Ctxt, &sym, self.New)
    return sym.P
}
