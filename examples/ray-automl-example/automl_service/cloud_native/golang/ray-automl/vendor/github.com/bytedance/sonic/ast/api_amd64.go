// +build amd64,go1.15,!go1.21


package ast

import (
    `runtime`
    `unsafe`

    `github.com/bytedance/sonic/encoder`
    `github.com/bytedance/sonic/internal/native`
    `github.com/bytedance/sonic/internal/native/types`
    `github.com/bytedance/sonic/internal/rt`
    uq `github.com/bytedance/sonic/unquote`
    `github.com/chenzhuoyu/base64x`
)

var typeByte = rt.UnpackEface(byte(0)).Type

func quote(buf *[]byte, val string) {
    *buf = append(*buf, '"')
    if len(val) == 0 {
        *buf = append(*buf, '"')
    }

    sp := rt.IndexChar(val, 0)
    nb := len(val)
    b := (*rt.GoSlice)(unsafe.Pointer(buf))

    // input buffer
    for nb > 0 {
        // output buffer
        dp := unsafe.Pointer(uintptr(b.Ptr) + uintptr(b.Len))
        dn := b.Cap - b.Len
        // call native.Quote, dn is byte count it outputs
        ret := native.Quote(sp, nb, dp, &dn, 0)
        // update *buf length
        b.Len += dn

        // no need more output
        if ret >= 0 {
            break
        }

        // double buf size
        *b = growslice(typeByte, *b, b.Cap*2)
        // ret is the complement of consumed input
        ret = ^ret
        // update input buffer
        nb -= ret
        sp = unsafe.Pointer(uintptr(sp) + uintptr(ret))
    }

    runtime.KeepAlive(buf)
    runtime.KeepAlive(sp)
    *buf = append(*buf, '"')
}

func unquote(src string) (string, types.ParsingError) {
    return uq.String(src)
}

func decodeBase64(src string) ([]byte, error) {
    return base64x.StdEncoding.DecodeString(src)
}

func encodeBase64(src []byte) string {
    return base64x.StdEncoding.EncodeToString(src)
}

func (self *Parser) decodeValue() (val types.JsonState) {
    sv := (*rt.GoString)(unsafe.Pointer(&self.s))
    self.p = native.Value(sv.Ptr, sv.Len, self.p, &val, 0)
    return
}

func (self *Parser) skip() (int, types.ParsingError) {
    fsm := types.NewStateMachine()
    start := native.SkipOne(&self.s, &self.p, fsm, 0)
    types.FreeStateMachine(fsm)

    if start < 0 {
        return self.p, types.ParsingError(-start)
    }
    return start, 0
}

func (self *Node) encodeInterface(buf *[]byte) error {
    //WARN: NOT compatible with json.Encoder
    return encoder.EncodeInto(buf, self.packAny(), 0)
}

func (self *Parser) skipFast() (int, types.ParsingError) {
    start := native.SkipOneFast(&self.s, &self.p)
    if start < 0 {
        return self.p, types.ParsingError(-start)
    }
    return start, 0
}

func (self *Parser) getByPath(path ...interface{}) (int, types.ParsingError) {
    start := native.GetByPath(&self.s, &self.p, &path)
    runtime.KeepAlive(path)
    if start < 0 {
        return self.p, types.ParsingError(-start)
    }
    return start, 0
}


func (self *Searcher) GetByPath(path ...interface{}) (Node, error) {
    var err types.ParsingError
    var start int

    self.parser.p = 0
    start, err = self.parser.getByPath(path...)
    if err != 0 {
        return Node{}, self.parser.syntaxError(err)
    }

    t := switchRawType(self.parser.s[start])
    if t == _V_NONE {
        return Node{}, self.parser.ExportError(err)
    }
    return newRawNode(self.parser.s[start:self.parser.p], t), nil
}