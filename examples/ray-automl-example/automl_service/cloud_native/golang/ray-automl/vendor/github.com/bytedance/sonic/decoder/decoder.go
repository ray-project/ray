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
    `encoding/json`
    `reflect`
    `runtime`

    `github.com/bytedance/sonic/internal/native`
    `github.com/bytedance/sonic/internal/native/types`
    `github.com/bytedance/sonic/internal/rt`
    `github.com/bytedance/sonic/option`
    `github.com/bytedance/sonic/utf8`
)

const (
    _F_use_int64 = iota
    _F_use_number
    _F_disable_urc
    _F_disable_unknown
    _F_copy_string
    _F_validate_string

    _F_allow_control = 31
)

type Options uint64

const (
    OptionUseInt64         Options = 1 << _F_use_int64
    OptionUseNumber        Options = 1 << _F_use_number
    OptionUseUnicodeErrors Options = 1 << _F_disable_urc
    OptionDisableUnknown   Options = 1 << _F_disable_unknown
    OptionCopyString       Options = 1 << _F_copy_string
    OptionValidateString   Options = 1 << _F_validate_string
)

func (self *Decoder) SetOptions(opts Options) {
    if (opts & OptionUseNumber != 0) && (opts & OptionUseInt64 != 0) {
        panic("can't set OptionUseInt64 and OptionUseNumber both!")
    }
    self.f = uint64(opts)
}


// Decoder is the decoder context object
type Decoder struct {
    i int
    f uint64
    s string
}

// NewDecoder creates a new decoder instance.
func NewDecoder(s string) *Decoder {
    return &Decoder{s: s}
}

// Pos returns the current decoding position.
func (self *Decoder) Pos() int {
    return self.i
}

func (self *Decoder) Reset(s string) {
    self.s = s
    self.i = 0
    // self.f = 0
}

func (self *Decoder) CheckTrailings() error {
    pos := self.i
    buf := self.s
    /* skip all the trailing spaces */
    if pos != len(buf) {
        for pos < len(buf) && (types.SPACE_MASK & (1 << buf[pos])) != 0 {
            pos++
        }
    }

    /* then it must be at EOF */
    if pos == len(buf) {
        return nil
    }

    /* junk after JSON value */
    return SyntaxError {
        Src  : buf,
        Pos  : pos,
        Code : types.ERR_INVALID_CHAR,
    }
}


// Decode parses the JSON-encoded data from current position and stores the result
// in the value pointed to by val.
func (self *Decoder) Decode(val interface{}) error {
    /* validate json if needed */
    if (self.f & (1 << _F_validate_string)) != 0  && !utf8.ValidateString(self.s){
        dbuf := utf8.CorrectWith(nil, rt.Str2Mem(self.s), "\ufffd")
        self.s = rt.Mem2Str(dbuf)
    }

    vv := rt.UnpackEface(val)
    vp := vv.Value

    /* check for nil type */
    if vv.Type == nil {
        return &json.InvalidUnmarshalError{}
    }

    /* must be a non-nil pointer */
    if vp == nil || vv.Type.Kind() != reflect.Ptr {
        return &json.InvalidUnmarshalError{Type: vv.Type.Pack()}
    }

    /* create a new stack, and call the decoder */
    sb, etp := newStack(), rt.PtrElem(vv.Type)
    nb, err := decodeTypedPointer(self.s, self.i, etp, vp, sb, self.f)
    /* return the stack back */
    self.i = nb
    freeStack(sb)

    /* avoid GC ahead */
    runtime.KeepAlive(vv)
    return err
}

// UseInt64 indicates the Decoder to unmarshal an integer into an interface{} as an
// int64 instead of as a float64.
func (self *Decoder) UseInt64() {
    self.f  |= 1 << _F_use_int64
    self.f &^= 1 << _F_use_number
}

// UseNumber indicates the Decoder to unmarshal a number into an interface{} as a
// json.Number instead of as a float64.
func (self *Decoder) UseNumber() {
    self.f &^= 1 << _F_use_int64
    self.f  |= 1 << _F_use_number
}

// UseUnicodeErrors indicates the Decoder to return an error when encounter invalid
// UTF-8 escape sequences.
func (self *Decoder) UseUnicodeErrors() {
    self.f |= 1 << _F_disable_urc
}

// DisallowUnknownFields indicates the Decoder to return an error when the destination
// is a struct and the input contains object keys which do not match any
// non-ignored, exported fields in the destination.
func (self *Decoder) DisallowUnknownFields() {
    self.f |= 1 << _F_disable_unknown
}

// CopyString indicates the Decoder to decode string values by copying instead of referring.
func (self *Decoder) CopyString() {
    self.f |= 1 << _F_copy_string
}

// ValidateString causes the Decoder to validate string values when decoding string value 
// in JSON. Validation is that, returning error when unescaped control chars(0x00-0x1f) or
// invalid UTF-8 chars in the string value of JSON.
func (self *Decoder) ValidateString() {
    self.f |= 1 << _F_validate_string
}

// Pretouch compiles vt ahead-of-time to avoid JIT compilation on-the-fly, in
// order to reduce the first-hit latency.
//
// Opts are the compile options, for example, "option.WithCompileRecursiveDepth" is
// a compile option to set the depth of recursive compile for the nested struct type.
func Pretouch(vt reflect.Type, opts ...option.CompileOption) error {
    cfg := option.DefaultCompileOptions()
    for _, opt := range opts {
        opt(&cfg)
    }
    return pretouchRec(map[reflect.Type]bool{vt:true}, cfg)
}

func pretouchType(_vt reflect.Type, opts option.CompileOptions) (map[reflect.Type]bool, error) {
    /* compile function */
    compiler := newCompiler().apply(opts)
    decoder := func(vt *rt.GoType, _ ...interface{}) (interface{}, error) {
        if pp, err := compiler.compile(_vt); err != nil {
            return nil, err
        } else {
            as := newAssembler(pp)
            as.name = _vt.String()
            return as.Load(), nil
        }
    }

    /* find or compile */
    vt := rt.UnpackType(_vt)
    if val := programCache.Get(vt); val != nil {
        return nil, nil
    } else if _, err := programCache.Compute(vt, decoder); err == nil {
        return compiler.rec, nil
    } else {
        return nil, err
    }
}

func pretouchRec(vtm map[reflect.Type]bool, opts option.CompileOptions) error {
    if opts.RecursiveDepth < 0 || len(vtm) == 0 {
        return nil
    }
    next := make(map[reflect.Type]bool)
    for vt := range(vtm) {
        sub, err := pretouchType(vt, opts)
        if err != nil {
            return err
        }
        for svt := range(sub) {
            next[svt] = true
        }
    }
    opts.RecursiveDepth -= 1
    return pretouchRec(next, opts)
}

// Skip skips only one json value, and returns first non-blank character position and its ending position if it is valid.
// Otherwise, returns negative error code using start and invalid character position using end
func Skip(data []byte) (start int, end int) {
    s := rt.Mem2Str(data)
    p := 0
    m := types.NewStateMachine()
    ret := native.SkipOne(&s, &p, m, uint64(0))
    types.FreeStateMachine(m) 
    return ret, p
}