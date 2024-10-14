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
    `bytes`
    `encoding/json`
    `reflect`
    `runtime`

    `github.com/bytedance/sonic/internal/native`
    `github.com/bytedance/sonic/internal/native/types`
    `github.com/bytedance/sonic/internal/rt`
    `github.com/bytedance/sonic/utf8`
    `github.com/bytedance/sonic/option`
)

// Options is a set of encoding options.
type Options uint64

const (
    bitSortMapKeys          = iota
    bitEscapeHTML          
    bitCompactMarshaler
    bitNoQuoteTextMarshaler
    bitNoNullSliceOrMap
    bitValidateString

    // used for recursive compile
    bitPointerValue = 63
)

const (
    // SortMapKeys indicates that the keys of a map needs to be sorted 
    // before serializing into JSON.
    // WARNING: This hurts performance A LOT, USE WITH CARE.
    SortMapKeys          Options = 1 << bitSortMapKeys

    // EscapeHTML indicates encoder to escape all HTML characters 
    // after serializing into JSON (see https://pkg.go.dev/encoding/json#HTMLEscape).
    // WARNING: This hurts performance A LOT, USE WITH CARE.
    EscapeHTML           Options = 1 << bitEscapeHTML

    // CompactMarshaler indicates that the output JSON from json.Marshaler 
    // is always compact and needs no validation 
    CompactMarshaler     Options = 1 << bitCompactMarshaler

    // NoQuoteTextMarshaler indicates that the output text from encoding.TextMarshaler 
    // is always escaped string and needs no quoting
    NoQuoteTextMarshaler Options = 1 << bitNoQuoteTextMarshaler

    // NoNullSliceOrMap indicates all empty Array or Object are encoded as '[]' or '{}',
    // instead of 'null'
    NoNullSliceOrMap     Options = 1 << bitNoNullSliceOrMap

    // ValidateString indicates that encoder should validate the input string
    // before encoding it into JSON.
    ValidateString       Options = 1 << bitValidateString
  
    // CompatibleWithStd is used to be compatible with std encoder.
    CompatibleWithStd Options = SortMapKeys | EscapeHTML | CompactMarshaler
)

// Encoder represents a specific set of encoder configurations.
type Encoder struct {
    Opts Options
    prefix string
    indent string
}

// Encode returns the JSON encoding of v.
func (self *Encoder) Encode(v interface{}) ([]byte, error) {
    if self.indent != "" || self.prefix != "" { 
        return EncodeIndented(v, self.prefix, self.indent, self.Opts)
    }
    return Encode(v, self.Opts)
}

// SortKeys enables the SortMapKeys option.
func (self *Encoder) SortKeys() *Encoder {
    self.Opts |= SortMapKeys
    return self
}

// SetEscapeHTML specifies if option EscapeHTML opens
func (self *Encoder) SetEscapeHTML(f bool) {
    if f {
        self.Opts |= EscapeHTML
    } else {
        self.Opts &= ^EscapeHTML
    }
}

// SetValidateString specifies if option ValidateString opens
func (self *Encoder) SetValidateString(f bool) {
    if f {
        self.Opts |= ValidateString
    } else {
        self.Opts &= ^ValidateString
    }
}

// SetCompactMarshaler specifies if option CompactMarshaler opens
func (self *Encoder) SetCompactMarshaler(f bool) {
    if f {
        self.Opts |= CompactMarshaler
    } else {
        self.Opts &= ^CompactMarshaler
    }
}

// SetNoQuoteTextMarshaler specifies if option NoQuoteTextMarshaler opens
func (self *Encoder) SetNoQuoteTextMarshaler(f bool) {
    if f {
        self.Opts |= NoQuoteTextMarshaler
    } else {
        self.Opts &= ^NoQuoteTextMarshaler
    }
}

// SetIndent instructs the encoder to format each subsequent encoded
// value as if indented by the package-level function EncodeIndent().
// Calling SetIndent("", "") disables indentation.
func (enc *Encoder) SetIndent(prefix, indent string) {
    enc.prefix = prefix
    enc.indent = indent
}

// Quote returns the JSON-quoted version of s.
func Quote(s string) string {
    var n int
    var p []byte

    /* check for empty string */
    if s == "" {
        return `""`
    }

    /* allocate space for result */
    n = len(s) + 2
    p = make([]byte, 0, n)

    /* call the encoder */
    _ = encodeString(&p, s)
    return rt.Mem2Str(p)
}

// Encode returns the JSON encoding of val, encoded with opts.
func Encode(val interface{}, opts Options) ([]byte, error) {
    buf := newBytes()
    err := EncodeInto(&buf, val, opts)

    /* check for errors */
    if err != nil {
        freeBytes(buf)
        return nil, err
    }

    if opts & EscapeHTML != 0 || opts & ValidateString != 0  {
        return buf, nil
    }

    /* make a copy of the result */
    ret := make([]byte, len(buf))
    copy(ret, buf)

    freeBytes(buf)
    /* return the buffer into pool */
    return ret, nil
}

// EncodeInto is like Encode but uses a user-supplied buffer instead of allocating
// a new one.
func EncodeInto(buf *[]byte, val interface{}, opts Options) error {
    stk := newStack()
    efv := rt.UnpackEface(val)
    err := encodeTypedPointer(buf, efv.Type, &efv.Value, stk, uint64(opts))

    /* return the stack into pool */
    if err != nil {
        resetStack(stk)
    }
    freeStack(stk)

    /* EscapeHTML needs to allocate a new buffer*/
    if opts & EscapeHTML != 0 {
        dest := HTMLEscape(nil, *buf)
        freeBytes(*buf) // free origin used buffer
        *buf = dest
    }

    if opts & ValidateString != 0 && !utf8.Validate(*buf) {
        dest := utf8.CorrectWith(nil, *buf, `\ufffd`)
        freeBytes(*buf) // free origin used buffer
        *buf = dest
    }

    /* avoid GC ahead */
    runtime.KeepAlive(buf)
    runtime.KeepAlive(efv)
    return err
}

var typeByte = rt.UnpackType(reflect.TypeOf(byte(0)))

// HTMLEscape appends to dst the JSON-encoded src with <, >, &, U+2028 and U+2029
// characters inside string literals changed to \u003c, \u003e, \u0026, \u2028, \u2029
// so that the JSON will be safe to embed inside HTML <script> tags.
// For historical reasons, web browsers don't honor standard HTML
// escaping within <script> tags, so an alternative JSON encoding must
// be used.
func HTMLEscape(dst []byte, src []byte) []byte {
    return htmlEscape(dst, src)
}

// EncodeIndented is like Encode but applies Indent to format the output.
// Each JSON element in the output will begin on a new line beginning with prefix
// followed by one or more copies of indent according to the indentation nesting.
func EncodeIndented(val interface{}, prefix string, indent string, opts Options) ([]byte, error) {
    var err error
    var out []byte
    var buf *bytes.Buffer

    /* encode into the buffer */
    out = newBytes()
    err = EncodeInto(&out, val, opts)

    /* check for errors */
    if err != nil {
        freeBytes(out)
        return nil, err
    }

    /* indent the JSON */
    buf = newBuffer()
    err = json.Indent(buf, out, prefix, indent)

    /* check for errors */
    if err != nil {
        freeBytes(out)
        freeBuffer(buf)
        return nil, err
    }

    /* copy to the result buffer */
    ret := make([]byte, buf.Len())
    copy(ret, buf.Bytes())

    /* return the buffers into pool */
    freeBytes(out)
    freeBuffer(buf)
    return ret, nil
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
        break
    }
    return pretouchRec(map[reflect.Type]uint8{vt: 0}, cfg)
}

// Valid validates json and returns first non-blank character position,
// if it is only one valid json value.
// Otherwise returns invalid character position using start.
//
// Note: it does not check for the invalid UTF-8 characters.
func Valid(data []byte) (ok bool, start int) {
    n := len(data)
    if n == 0 {
        return false, -1
    }
    s := rt.Mem2Str(data)
    p := 0
    m := types.NewStateMachine()
    ret := native.ValidateOne(&s, &p, m)
    types.FreeStateMachine(m)

    if ret < 0 {
        return false, p-1
    }

    /* check for trailing spaces */
    for ;p < n; p++ {
        if (types.SPACE_MASK & (1 << data[p])) == 0 {
            return false, p
        }
    }

    return true, ret
}