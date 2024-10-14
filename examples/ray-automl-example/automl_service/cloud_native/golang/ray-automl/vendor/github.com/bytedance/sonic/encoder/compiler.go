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
    `fmt`
    `reflect`
    `strconv`
    `strings`
    `unsafe`

    `github.com/bytedance/sonic/internal/resolver`
    `github.com/bytedance/sonic/internal/rt`
    `github.com/bytedance/sonic/option`
)

type _Op uint8

const (
    _OP_null _Op = iota + 1
    _OP_empty_arr
    _OP_empty_obj
    _OP_bool
    _OP_i8
    _OP_i16
    _OP_i32
    _OP_i64
    _OP_u8
    _OP_u16
    _OP_u32
    _OP_u64
    _OP_f32
    _OP_f64
    _OP_str
    _OP_bin
    _OP_quote
    _OP_number
    _OP_eface
    _OP_iface
    _OP_byte
    _OP_text
    _OP_deref
    _OP_index
    _OP_load
    _OP_save
    _OP_drop
    _OP_drop_2
    _OP_recurse
    _OP_is_nil
    _OP_is_nil_p1
    _OP_is_zero_1
    _OP_is_zero_2
    _OP_is_zero_4
    _OP_is_zero_8
    _OP_is_zero_map
    _OP_goto
    _OP_map_iter
    _OP_map_stop
    _OP_map_check_key
    _OP_map_write_key
    _OP_map_value_next
    _OP_slice_len
    _OP_slice_next
    _OP_marshal
    _OP_marshal_p
    _OP_marshal_text
    _OP_marshal_text_p
    _OP_cond_set
    _OP_cond_testc
)

const (
    _INT_SIZE = 32 << (^uint(0) >> 63)
    _PTR_SIZE = 32 << (^uintptr(0) >> 63)
    _PTR_BYTE = unsafe.Sizeof(uintptr(0))
)

const (
    _MAX_ILBUF = 100000     // cutoff at 100k of IL instructions
    _MAX_FIELDS = 50        // cutoff at 50 fields struct
)

var _OpNames = [256]string {
    _OP_null           : "null",
    _OP_empty_arr      : "empty_arr",
    _OP_empty_obj      : "empty_obj",
    _OP_bool           : "bool",
    _OP_i8             : "i8",
    _OP_i16            : "i16",
    _OP_i32            : "i32",
    _OP_i64            : "i64",
    _OP_u8             : "u8",
    _OP_u16            : "u16",
    _OP_u32            : "u32",
    _OP_u64            : "u64",
    _OP_f32            : "f32",
    _OP_f64            : "f64",
    _OP_str            : "str",
    _OP_bin            : "bin",
    _OP_quote          : "quote",
    _OP_number         : "number",
    _OP_eface          : "eface",
    _OP_iface          : "iface",
    _OP_byte           : "byte",
    _OP_text           : "text",
    _OP_deref          : "deref",
    _OP_index          : "index",
    _OP_load           : "load",
    _OP_save           : "save",
    _OP_drop           : "drop",
    _OP_drop_2         : "drop_2",
    _OP_recurse        : "recurse",
    _OP_is_nil         : "is_nil",
    _OP_is_nil_p1      : "is_nil_p1",
    _OP_is_zero_1      : "is_zero_1",
    _OP_is_zero_2      : "is_zero_2",
    _OP_is_zero_4      : "is_zero_4",
    _OP_is_zero_8      : "is_zero_8",
    _OP_is_zero_map    : "is_zero_map",
    _OP_goto           : "goto",
    _OP_map_iter       : "map_iter",
    _OP_map_stop       : "map_stop",
    _OP_map_check_key  : "map_check_key",
    _OP_map_write_key  : "map_write_key",
    _OP_map_value_next : "map_value_next",
    _OP_slice_len      : "slice_len",
    _OP_slice_next     : "slice_next",
    _OP_marshal        : "marshal",
    _OP_marshal_p      : "marshal_p",
    _OP_marshal_text   : "marshal_text",
    _OP_marshal_text_p : "marshal_text_p",
    _OP_cond_set       : "cond_set",
    _OP_cond_testc     : "cond_testc",
}

func (self _Op) String() string {
    if ret := _OpNames[self]; ret != "" {
        return ret
    } else {
        return "<invalid>"
    }
}

func _OP_int() _Op {
    switch _INT_SIZE {
        case 32: return _OP_i32
        case 64: return _OP_i64
        default: panic("unsupported int size")
    }
}

func _OP_uint() _Op {
    switch _INT_SIZE {
        case 32: return _OP_u32
        case 64: return _OP_u64
        default: panic("unsupported uint size")
    }
}

func _OP_uintptr() _Op {
    switch _PTR_SIZE {
        case 32: return _OP_u32
        case 64: return _OP_u64
        default: panic("unsupported pointer size")
    }
}

func _OP_is_zero_ints() _Op {
    switch _INT_SIZE {
        case 32: return _OP_is_zero_4
        case 64: return _OP_is_zero_8
        default: panic("unsupported integer size")
    }
}

type _Instr struct {
    u uint64            // union {op: 8, _: 8, vi: 48}, vi maybe int or len(str)
    p unsafe.Pointer    // maybe GoString.Ptr, or *GoType
}

func packOp(op _Op) uint64 {
    return uint64(op) << 56
}

func newInsOp(op _Op) _Instr {
    return _Instr{u: packOp(op)}
}

func newInsVi(op _Op, vi int) _Instr {
    return _Instr{u: packOp(op) | rt.PackInt(vi)}
}

func newInsVs(op _Op, vs string) _Instr {
    return _Instr {
        u: packOp(op) | rt.PackInt(len(vs)),
        p: (*rt.GoString)(unsafe.Pointer(&vs)).Ptr,
    }
}

func newInsVt(op _Op, vt reflect.Type) _Instr {
    return _Instr {
        u: packOp(op),
        p: unsafe.Pointer(rt.UnpackType(vt)),
    }
}

func newInsVp(op _Op, vt reflect.Type, pv bool) _Instr {
    i := 0
    if pv {
        i = 1
    }
    return _Instr {
        u: packOp(op) | rt.PackInt(i),
        p: unsafe.Pointer(rt.UnpackType(vt)),
    }
}

func (self _Instr) op() _Op {
    return _Op(self.u >> 56)
}

func (self _Instr) vi() int {
    return rt.UnpackInt(self.u)
}

func (self _Instr) vf() uint8 {
    return (*rt.GoType)(self.p).KindFlags
}

func (self _Instr) vs() (v string) {
    (*rt.GoString)(unsafe.Pointer(&v)).Ptr = self.p
    (*rt.GoString)(unsafe.Pointer(&v)).Len = self.vi()
    return
}

func (self _Instr) vk() reflect.Kind {
    return (*rt.GoType)(self.p).Kind()
}

func (self _Instr) vt() reflect.Type {
    return (*rt.GoType)(self.p).Pack()
}

func (self _Instr) vp() (vt reflect.Type, pv bool) {
    return (*rt.GoType)(self.p).Pack(), rt.UnpackInt(self.u) == 1
}

func (self _Instr) i64() int64 {
    return int64(self.vi())
}

func (self _Instr) vlen() int {
    return int((*rt.GoType)(self.p).Size)
}

func (self _Instr) isBranch() bool {
    switch self.op() {
        case _OP_goto          : fallthrough
        case _OP_is_nil        : fallthrough
        case _OP_is_nil_p1     : fallthrough
        case _OP_is_zero_1     : fallthrough
        case _OP_is_zero_2     : fallthrough
        case _OP_is_zero_4     : fallthrough
        case _OP_is_zero_8     : fallthrough
        case _OP_map_check_key : fallthrough
        case _OP_map_write_key : fallthrough
        case _OP_slice_next    : fallthrough
        case _OP_cond_testc    : return true
        default                : return false
    }
}

func (self _Instr) disassemble() string {
    switch self.op() {
        case _OP_byte           : return fmt.Sprintf("%-18s%s", self.op().String(), strconv.QuoteRune(rune(self.vi())))
        case _OP_text           : return fmt.Sprintf("%-18s%s", self.op().String(), strconv.Quote(self.vs()))
        case _OP_index          : return fmt.Sprintf("%-18s%d", self.op().String(), self.vi())
        case _OP_recurse        : fallthrough
        case _OP_map_iter       : fallthrough
        case _OP_marshal        : fallthrough
        case _OP_marshal_p      : fallthrough
        case _OP_marshal_text   : fallthrough
        case _OP_marshal_text_p : return fmt.Sprintf("%-18s%s", self.op().String(), self.vt())
        case _OP_goto           : fallthrough
        case _OP_is_nil         : fallthrough
        case _OP_is_nil_p1      : fallthrough
        case _OP_is_zero_1      : fallthrough
        case _OP_is_zero_2      : fallthrough
        case _OP_is_zero_4      : fallthrough
        case _OP_is_zero_8      : fallthrough
        case _OP_is_zero_map    : fallthrough
        case _OP_cond_testc     : fallthrough
        case _OP_map_check_key  : fallthrough
        case _OP_map_write_key  : return fmt.Sprintf("%-18sL_%d", self.op().String(), self.vi())
        case _OP_slice_next     : return fmt.Sprintf("%-18sL_%d, %s", self.op().String(), self.vi(), self.vt())
        default                 : return self.op().String()
    }
}

type (
	_Program []_Instr
)

func (self _Program) pc() int {
    return len(self)
}

func (self _Program) tag(n int) {
    if n >= _MaxStack {
        panic("type nesting too deep")
    }
}

func (self _Program) pin(i int) {
    v := &self[i]
    v.u &= 0xffff000000000000
    v.u |= rt.PackInt(self.pc())
}

func (self _Program) rel(v []int) {
    for _, i := range v {
        self.pin(i)
    }
}

func (self *_Program) add(op _Op) {
    *self = append(*self, newInsOp(op))
}

func (self *_Program) key(op _Op) {
    *self = append(*self,
        newInsVi(_OP_byte, '"'),
        newInsOp(op),
        newInsVi(_OP_byte, '"'),
    )
}

func (self *_Program) int(op _Op, vi int) {
    *self = append(*self, newInsVi(op, vi))
}

func (self *_Program) str(op _Op, vs string) {
    *self = append(*self, newInsVs(op, vs))
}

func (self *_Program) rtt(op _Op, vt reflect.Type) {
    *self = append(*self, newInsVt(op, vt))
}

func (self *_Program) vp(op _Op, vt reflect.Type, pv bool) {
    *self = append(*self, newInsVp(op, vt, pv))
}

func (self _Program) disassemble() string {
    nb  := len(self)
    tab := make([]bool, nb + 1)
    ret := make([]string, 0, nb + 1)

    /* prescan to get all the labels */
    for _, ins := range self {
        if ins.isBranch() {
            tab[ins.vi()] = true
        }
    }

    /* disassemble each instruction */
    for i, ins := range self {
        if !tab[i] {
            ret = append(ret, "\t" + ins.disassemble())
        } else {
            ret = append(ret, fmt.Sprintf("L_%d:\n\t%s", i, ins.disassemble()))
        }
    }

    /* add the last label, if needed */
    if tab[nb] {
        ret = append(ret, fmt.Sprintf("L_%d:", nb))
    }

    /* add an "end" indicator, and join all the strings */
    return strings.Join(append(ret, "\tend"), "\n")
}

type _Compiler struct {
    opts option.CompileOptions
    pv   bool
    tab  map[reflect.Type]bool
    rec  map[reflect.Type]uint8
}

func newCompiler() *_Compiler {
    return &_Compiler {
        opts: option.DefaultCompileOptions(),
        tab: map[reflect.Type]bool{},
        rec: map[reflect.Type]uint8{},
    }
}

func (self *_Compiler) apply(opts option.CompileOptions) *_Compiler {
    self.opts = opts
    if self.opts.RecursiveDepth > 0 {
        self.rec = map[reflect.Type]uint8{}
    }
    return self
}

func (self *_Compiler) rescue(ep *error) {
    if val := recover(); val != nil {
        if err, ok := val.(error); ok {
            *ep = err
        } else {
            panic(val)
        }
    }
}

func (self *_Compiler) compile(vt reflect.Type, pv bool) (ret _Program, err error) {
    defer self.rescue(&err)
    self.compileOne(&ret, 0, vt, pv)
    return
}

func (self *_Compiler) compileOne(p *_Program, sp int, vt reflect.Type, pv bool) {
    if self.tab[vt] {
        p.vp(_OP_recurse, vt, pv)
    } else {
        self.compileRec(p, sp, vt, pv)
    }
}

func (self *_Compiler) compileRec(p *_Program, sp int, vt reflect.Type, pv bool) {
    pr := self.pv
    pt := reflect.PtrTo(vt)

    /* check for addressable `json.Marshaler` with pointer receiver */
    if pv && pt.Implements(jsonMarshalerType) {
        p.rtt(_OP_marshal_p, pt)
        return
    }

    /* check for `json.Marshaler` */
    if vt.Implements(jsonMarshalerType) {
        self.compileMarshaler(p, _OP_marshal, vt, jsonMarshalerType)
        return
    }

    /* check for addressable `encoding.TextMarshaler` with pointer receiver */
    if pv && pt.Implements(encodingTextMarshalerType) {
        p.rtt(_OP_marshal_text_p, pt)
        return
    }

    /* check for `encoding.TextMarshaler` */
    if vt.Implements(encodingTextMarshalerType) {
        self.compileMarshaler(p, _OP_marshal_text, vt, encodingTextMarshalerType)
        return
    }

    /* enter the recursion, and compile the type */
    self.pv = pv
    self.tab[vt] = true
    self.compileOps(p, sp, vt)

    /* exit the recursion */
    self.pv = pr
    delete(self.tab, vt)
}

func (self *_Compiler) compileOps(p *_Program, sp int, vt reflect.Type) {
    switch vt.Kind() {
        case reflect.Bool      : p.add(_OP_bool)
        case reflect.Int       : p.add(_OP_int())
        case reflect.Int8      : p.add(_OP_i8)
        case reflect.Int16     : p.add(_OP_i16)
        case reflect.Int32     : p.add(_OP_i32)
        case reflect.Int64     : p.add(_OP_i64)
        case reflect.Uint      : p.add(_OP_uint())
        case reflect.Uint8     : p.add(_OP_u8)
        case reflect.Uint16    : p.add(_OP_u16)
        case reflect.Uint32    : p.add(_OP_u32)
        case reflect.Uint64    : p.add(_OP_u64)
        case reflect.Uintptr   : p.add(_OP_uintptr())
        case reflect.Float32   : p.add(_OP_f32)
        case reflect.Float64   : p.add(_OP_f64)
        case reflect.String    : self.compileString    (p, vt)
        case reflect.Array     : self.compileArray     (p, sp, vt.Elem(), vt.Len())
        case reflect.Interface : self.compileInterface (p, vt)
        case reflect.Map       : self.compileMap       (p, sp, vt)
        case reflect.Ptr       : self.compilePtr       (p, sp, vt.Elem())
        case reflect.Slice     : self.compileSlice     (p, sp, vt.Elem())
        case reflect.Struct    : self.compileStruct    (p, sp, vt)
        default                : panic                 (error_type(vt))
    }
}

func (self *_Compiler) compileNil(p *_Program, sp int, vt reflect.Type, nil_op _Op, fn func(*_Program, int, reflect.Type)) {
    x := p.pc()
    p.add(_OP_is_nil)
    fn(p, sp, vt)
    e := p.pc()
    p.add(_OP_goto)
    p.pin(x)
    p.add(nil_op)
    p.pin(e)
}

func (self *_Compiler) compilePtr(p *_Program, sp int, vt reflect.Type) {
    self.compileNil(p, sp, vt, _OP_null, self.compilePtrBody)
}

func (self *_Compiler) compilePtrBody(p *_Program, sp int, vt reflect.Type) {
    p.tag(sp)
    p.add(_OP_save)
    p.add(_OP_deref)
    self.compileOne(p, sp + 1, vt, true)
    p.add(_OP_drop)
}

func (self *_Compiler) compileMap(p *_Program, sp int, vt reflect.Type) {
    self.compileNil(p, sp, vt, _OP_empty_obj, self.compileMapBody)
}

func (self *_Compiler) compileMapBody(p *_Program, sp int, vt reflect.Type) {
    p.tag(sp + 1)
    p.int(_OP_byte, '{')
    p.add(_OP_save)
    p.rtt(_OP_map_iter, vt)
    p.add(_OP_save)
    i := p.pc()
    p.add(_OP_map_check_key)
    u := p.pc()
    p.add(_OP_map_write_key)
    self.compileMapBodyKey(p, vt.Key())
    p.pin(u)
    p.int(_OP_byte, ':')
    p.add(_OP_map_value_next)
    self.compileOne(p, sp + 2, vt.Elem(), false)
    j := p.pc()
    p.add(_OP_map_check_key)
    p.int(_OP_byte, ',')
    v := p.pc()
    p.add(_OP_map_write_key)
    self.compileMapBodyKey(p, vt.Key())
    p.pin(v)
    p.int(_OP_byte, ':')
    p.add(_OP_map_value_next)
    self.compileOne(p, sp + 2, vt.Elem(), false)
    p.int(_OP_goto, j)
    p.pin(i)
    p.pin(j)
    p.add(_OP_map_stop)
    p.add(_OP_drop_2)
    p.int(_OP_byte, '}')
}

func (self *_Compiler) compileMapBodyKey(p *_Program, vk reflect.Type) {
    if !vk.Implements(encodingTextMarshalerType) {
        self.compileMapBodyTextKey(p, vk)
    } else {
        self.compileMapBodyUtextKey(p, vk)
    }
}

func (self *_Compiler) compileMapBodyTextKey(p *_Program, vk reflect.Type) {
    switch vk.Kind() {
        case reflect.Invalid : panic("map key is nil")
        case reflect.Bool    : p.key(_OP_bool)
        case reflect.Int     : p.key(_OP_int())
        case reflect.Int8    : p.key(_OP_i8)
        case reflect.Int16   : p.key(_OP_i16)
        case reflect.Int32   : p.key(_OP_i32)
        case reflect.Int64   : p.key(_OP_i64)
        case reflect.Uint    : p.key(_OP_uint())
        case reflect.Uint8   : p.key(_OP_u8)
        case reflect.Uint16  : p.key(_OP_u16)
        case reflect.Uint32  : p.key(_OP_u32)
        case reflect.Uint64  : p.key(_OP_u64)
        case reflect.Uintptr : p.key(_OP_uintptr())
        case reflect.Float32 : p.key(_OP_f32)
        case reflect.Float64 : p.key(_OP_f64)
        case reflect.String  : self.compileString(p, vk)
        default              : panic(error_type(vk))
    }
}

func (self *_Compiler) compileMapBodyUtextKey(p *_Program, vk reflect.Type) {
    if vk.Kind() != reflect.Ptr {
        p.rtt(_OP_marshal_text, vk)
    } else {
        self.compileMapBodyUtextPtr(p, vk)
    }
}

func (self *_Compiler) compileMapBodyUtextPtr(p *_Program, vk reflect.Type) {
    i := p.pc()
    p.add(_OP_is_nil)
    p.rtt(_OP_marshal_text, vk)
    j := p.pc()
    p.add(_OP_goto)
    p.pin(i)
    p.str(_OP_text, "\"\"")
    p.pin(j)
}

func (self *_Compiler) compileSlice(p *_Program, sp int, vt reflect.Type) {
    self.compileNil(p, sp, vt, _OP_empty_arr, self.compileSliceBody)
}

func (self *_Compiler) compileSliceBody(p *_Program, sp int, vt reflect.Type) {
    if isSimpleByte(vt) {
        p.add(_OP_bin)
    } else {
        self.compileSliceArray(p, sp, vt)
    }
}

func (self *_Compiler) compileSliceArray(p *_Program, sp int, vt reflect.Type) {
    p.tag(sp)
    p.int(_OP_byte, '[')
    p.add(_OP_save)
    p.add(_OP_slice_len)
    i := p.pc()
    p.rtt(_OP_slice_next, vt)
    self.compileOne(p, sp + 1, vt, true)
    j := p.pc()
    p.rtt(_OP_slice_next, vt)
    p.int(_OP_byte, ',')
    self.compileOne(p, sp + 1, vt, true)
    p.int(_OP_goto, j)
    p.pin(i)
    p.pin(j)
    p.add(_OP_drop)
    p.int(_OP_byte, ']')
}

func (self *_Compiler) compileArray(p *_Program, sp int, vt reflect.Type, nb int) {
    p.tag(sp)
    p.int(_OP_byte, '[')
    p.add(_OP_save)

    /* first item */
    if nb != 0 {
        self.compileOne(p, sp + 1, vt, self.pv)
        p.add(_OP_load)
    }

    /* remaining items */
    for i := 1; i < nb; i++ {
        p.int(_OP_byte, ',')
        p.int(_OP_index, i * int(vt.Size()))
        self.compileOne(p, sp + 1, vt, self.pv)
        p.add(_OP_load)
    }

    /* end of array */
    p.add(_OP_drop)
    p.int(_OP_byte, ']')
}

func (self *_Compiler) compileString(p *_Program, vt reflect.Type) {
    if vt != jsonNumberType {
        p.add(_OP_str)
    } else {
        p.add(_OP_number)
    }
}

func (self *_Compiler) compileStruct(p *_Program, sp int, vt reflect.Type) {
    if sp >= self.opts.MaxInlineDepth || p.pc() >= _MAX_ILBUF || (sp > 0 && vt.NumField() >= _MAX_FIELDS) {
        p.vp(_OP_recurse, vt, self.pv)
        if self.opts.RecursiveDepth > 0 {
            if self.pv {
                self.rec[vt] = 1
            } else {
                self.rec[vt] = 0
            }
        }
    } else {
        self.compileStructBody(p, sp, vt)
    }
}

func (self *_Compiler) compileStructBody(p *_Program, sp int, vt reflect.Type) {
    p.tag(sp)
    p.int(_OP_byte, '{')
    p.add(_OP_save)
    p.add(_OP_cond_set)

    /* compile each field */
    for _, fv := range resolver.ResolveStruct(vt) {
        var s []int
        var o resolver.Offset

        /* "omitempty" for arrays */
        if fv.Type.Kind() == reflect.Array {
            if fv.Type.Len() == 0 && (fv.Opts & resolver.F_omitempty) != 0 {
                continue
            }
        }

        /* index to the field */
        for _, o = range fv.Path {
            if p.int(_OP_index, int(o.Size)); o.Kind == resolver.F_deref {
                s = append(s, p.pc())
                p.add(_OP_is_nil)
                p.add(_OP_deref)
            }
        }

        /* check for "omitempty" option */
        if fv.Type.Kind() != reflect.Struct && fv.Type.Kind() != reflect.Array && (fv.Opts & resolver.F_omitempty) != 0 {
            s = append(s, p.pc())
            self.compileStructFieldZero(p, fv.Type)
        }

        /* add the comma if not the first element */
        i := p.pc()
        p.add(_OP_cond_testc)
        p.int(_OP_byte, ',')
        p.pin(i)

        /* compile the key and value */
        ft := fv.Type
        p.str(_OP_text, Quote(fv.Name) + ":")

        /* check for "stringnize" option */
        if (fv.Opts & resolver.F_stringize) == 0 {
            self.compileOne(p, sp + 1, ft, self.pv)
        } else {
            self.compileStructFieldStr(p, sp + 1, ft)
        }

        /* patch the skipping jumps and reload the struct pointer */
        p.rel(s)
        p.add(_OP_load)
    }

    /* end of object */
    p.add(_OP_drop)
    p.int(_OP_byte, '}')
}

func (self *_Compiler) compileStructFieldStr(p *_Program, sp int, vt reflect.Type) {
    pc := -1
    ft := vt
    sv := false

    /* dereference the pointer if needed */
    if ft.Kind() == reflect.Ptr {
        ft = ft.Elem()
    }

    /* check if it can be stringized */
    switch ft.Kind() {
        case reflect.Bool    : sv = true
        case reflect.Int     : sv = true
        case reflect.Int8    : sv = true
        case reflect.Int16   : sv = true
        case reflect.Int32   : sv = true
        case reflect.Int64   : sv = true
        case reflect.Uint    : sv = true
        case reflect.Uint8   : sv = true
        case reflect.Uint16  : sv = true
        case reflect.Uint32  : sv = true
        case reflect.Uint64  : sv = true
        case reflect.Uintptr : sv = true
        case reflect.Float32 : sv = true
        case reflect.Float64 : sv = true
        case reflect.String  : sv = true
    }

    /* if it's not, ignore the "string" and follow the regular path */
    if !sv {
        self.compileOne(p, sp, vt, self.pv)
        return
    }

    /* dereference the pointer */
    if vt.Kind() == reflect.Ptr {
        pc = p.pc()
        vt = vt.Elem()
        p.add(_OP_is_nil)
        p.add(_OP_deref)
    }

    /* special case of a double-quoted string */
    if ft != jsonNumberType && ft.Kind() == reflect.String {
        p.add(_OP_quote)
    } else {
        self.compileStructFieldQuoted(p, sp, vt)
    }

    /* the "null" case of the pointer */
    if pc != -1 {
        e := p.pc()
        p.add(_OP_goto)
        p.pin(pc)
        p.add(_OP_null)
        p.pin(e)
    }
}

func (self *_Compiler) compileStructFieldZero(p *_Program, vt reflect.Type) {
    switch vt.Kind() {
        case reflect.Bool      : p.add(_OP_is_zero_1)
        case reflect.Int       : p.add(_OP_is_zero_ints())
        case reflect.Int8      : p.add(_OP_is_zero_1)
        case reflect.Int16     : p.add(_OP_is_zero_2)
        case reflect.Int32     : p.add(_OP_is_zero_4)
        case reflect.Int64     : p.add(_OP_is_zero_8)
        case reflect.Uint      : p.add(_OP_is_zero_ints())
        case reflect.Uint8     : p.add(_OP_is_zero_1)
        case reflect.Uint16    : p.add(_OP_is_zero_2)
        case reflect.Uint32    : p.add(_OP_is_zero_4)
        case reflect.Uint64    : p.add(_OP_is_zero_8)
        case reflect.Uintptr   : p.add(_OP_is_nil)
        case reflect.Float32   : p.add(_OP_is_zero_4)
        case reflect.Float64   : p.add(_OP_is_zero_8)
        case reflect.String    : p.add(_OP_is_nil_p1)
        case reflect.Interface : p.add(_OP_is_nil_p1)
        case reflect.Map       : p.add(_OP_is_zero_map)
        case reflect.Ptr       : p.add(_OP_is_nil)
        case reflect.Slice     : p.add(_OP_is_nil_p1)
        default                : panic(error_type(vt))
    }
}

func (self *_Compiler) compileStructFieldQuoted(p *_Program, sp int, vt reflect.Type) {
    p.int(_OP_byte, '"')
    self.compileOne(p, sp, vt, self.pv)
    p.int(_OP_byte, '"')
}

func (self *_Compiler) compileInterface(p *_Program, vt reflect.Type) {
    x := p.pc()
    p.add(_OP_is_nil_p1)

    /* iface and efaces are different */
    if vt.NumMethod() == 0 {
        p.add(_OP_eface)
    } else {
        p.add(_OP_iface)
    }

    /* the "null" value */
    e := p.pc()
    p.add(_OP_goto)
    p.pin(x)
    p.add(_OP_null)
    p.pin(e)
}

func (self *_Compiler) compileMarshaler(p *_Program, op _Op, vt reflect.Type, mt reflect.Type) {
    pc := p.pc()
    vk := vt.Kind()

    /* direct receiver */
    if vk != reflect.Ptr {
        p.rtt(op, vt)
        return
    }

    /* value receiver with a pointer type, check for nil before calling the marshaler */
    p.add(_OP_is_nil)
    p.rtt(op, vt)
    i := p.pc()
    p.add(_OP_goto)
    p.pin(pc)
    p.add(_OP_null)
    p.pin(i)
}
