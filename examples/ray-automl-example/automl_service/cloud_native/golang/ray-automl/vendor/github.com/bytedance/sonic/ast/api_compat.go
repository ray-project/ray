// +build !amd64 go1.21

package ast

import (
    `encoding/base64`
    `encoding/json`
    `fmt`

    `github.com/bytedance/sonic/internal/native/types`
    `github.com/bytedance/sonic/internal/rt`
)

func quote(buf *[]byte, val string) {
    quoteString(buf, val)
}

func unquote(src string) (string, types.ParsingError) {
    sp := rt.IndexChar(src, -1)
    out, ok := unquoteBytes(rt.BytesFrom(sp, len(src)+2, len(src)+2))
    if !ok {
        return "", types.ERR_INVALID_ESCAPE
    }
    return rt.Mem2Str(out), 0
}



func decodeBase64(src string) ([]byte, error) {
    return base64.StdEncoding.DecodeString(src)
}

func encodeBase64(src []byte) string {
    return base64.StdEncoding.EncodeToString(src)
}

func (self *Parser) decodeValue() (val types.JsonState) {
    e, v := decodeValue(self.s, self.p)
    if e < 0 {
        return v
    }
    self.p = e
    return v
}

func (self *Parser) skip() (int, types.ParsingError) {
    e, s := skipValue(self.s, self.p)
    if e < 0 {
        return self.p, types.ParsingError(-e)
    }
    self.p = e
    return s, 0
}

func (self *Parser) skipFast() (int, types.ParsingError) {
    return self.skip()
}

func (self *Node) encodeInterface(buf *[]byte) error {
    out, err := json.Marshal(self.packAny())
    if err != nil {
        return err
    }
    *buf = append(*buf, out...)
    return nil
}

func (self *Searcher) GetByPath(path ...interface{}) (Node, error) {
    self.parser.p = 0

    var err types.ParsingError
    for _, p := range path {
        switch p := p.(type) {
        case int:
            if err = self.parser.searchIndex(p); err != 0 {
                return Node{}, self.parser.ExportError(err)
            }
        case string:
            if err = self.parser.searchKey(p); err != 0 {
                return Node{}, self.parser.ExportError(err)
            }
        default:
            panic("path must be either int or string")
        }
    }

    var start = self.parser.p
    if start, err = self.parser.skip(); err != 0 {
        return Node{}, self.parser.ExportError(err)
    }
    ns := len(self.parser.s)
    if self.parser.p > ns || start >= ns || start>=self.parser.p {
        return Node{}, fmt.Errorf("skip %d char out of json boundary", start)
    }

    t := switchRawType(self.parser.s[start])
    if t == _V_NONE {
        return Node{}, self.parser.ExportError(err)
    }

    return newRawNode(self.parser.s[start:self.parser.p], t), nil
}