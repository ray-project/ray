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

package ast

import (
    `fmt`
    `github.com/bytedance/sonic/internal/native/types`
    `github.com/bytedance/sonic/internal/rt`
)

const _DEFAULT_NODE_CAP int = 16

const (
    _ERR_NOT_FOUND      types.ParsingError = 33
    _ERR_UNSUPPORT_TYPE types.ParsingError = 34
)

var (
    ErrNotExist error = newError(_ERR_NOT_FOUND, "value not exists")
    ErrUnsupportType error = newError(_ERR_UNSUPPORT_TYPE, "unsupported type")
)

type Parser struct {
    p           int
    s           string
    noLazy      bool
    skipValue   bool
}

/** Parser Private Methods **/

func (self *Parser) delim() types.ParsingError {
    n := len(self.s)
    p := self.lspace(self.p)

    /* check for EOF */
    if p >= n {
        return types.ERR_EOF
    }

    /* check for the delimtier */
    if self.s[p] != ':' {
        return types.ERR_INVALID_CHAR
    }

    /* update the read pointer */
    self.p = p + 1
    return 0
}

func (self *Parser) object() types.ParsingError {
    n := len(self.s)
    p := self.lspace(self.p)

    /* check for EOF */
    if p >= n {
        return types.ERR_EOF
    }

    /* check for the delimtier */
    if self.s[p] != '{' {
        return types.ERR_INVALID_CHAR
    }

    /* update the read pointer */
    self.p = p + 1
    return 0
}

func (self *Parser) array() types.ParsingError {
    n := len(self.s)
    p := self.lspace(self.p)

    /* check for EOF */
    if p >= n {
        return types.ERR_EOF
    }

    /* check for the delimtier */
    if self.s[p] != '[' {
        return types.ERR_INVALID_CHAR
    }

    /* update the read pointer */
    self.p = p + 1
    return 0
}

func (self *Parser) lspace(sp int) int {
    ns := len(self.s)
    for ; sp<ns && isSpace(self.s[sp]); sp+=1 {}

    return sp
}

func (self *Parser) decodeArray(ret []Node) (Node, types.ParsingError) {
    sp := self.p
    ns := len(self.s)

    /* check for EOF */
    if self.p = self.lspace(sp); self.p >= ns {
        return Node{}, types.ERR_EOF
    }

    /* check for empty array */
    if self.s[self.p] == ']' {
        self.p++
        return emptyArrayNode, 0
    }

    /* allocate array space and parse every element */
    for {
        var val Node
        var err types.ParsingError

        if self.skipValue {
            /* skip the value */
            var start int
            if start, err = self.skipFast(); err != 0 {
                return Node{}, err
            }
            if self.p > ns {
                return Node{}, types.ERR_EOF
            }
            t := switchRawType(self.s[start])
            if t == _V_NONE {
                return Node{}, types.ERR_INVALID_CHAR
            }
            val = newRawNode(self.s[start:self.p], t)
        }else{
            /* decode the value */
            if val, err = self.Parse(); err != 0 {
                return Node{}, err
            }
        }

        /* add the value to result */
        ret = append(ret, val)
        self.p = self.lspace(self.p)

        /* check for EOF */
        if self.p >= ns {
            return Node{}, types.ERR_EOF
        }

        /* check for the next character */
        switch self.s[self.p] {
            case ',' : self.p++
            case ']' : self.p++; return NewArray(ret), 0
        default:
            if val.isLazy() {
                return newLazyArray(self, ret), 0
            }
            return Node{}, types.ERR_INVALID_CHAR
        }
    }
}

func (self *Parser) decodeObject(ret []Pair) (Node, types.ParsingError) {
    sp := self.p
    ns := len(self.s)

    /* check for EOF */
    if self.p = self.lspace(sp); self.p >= ns {
        return Node{}, types.ERR_EOF
    }

    /* check for empty object */
    if self.s[self.p] == '}' {
        self.p++
        return emptyObjectNode, 0
    }

    /* decode each pair */
    for {
        var val Node
        var njs types.JsonState
        var err types.ParsingError

        /* decode the key */
        if njs = self.decodeValue(); njs.Vt != types.V_STRING {
            return Node{}, types.ERR_INVALID_CHAR
        }

        /* extract the key */
        idx := self.p - 1
        key := self.s[njs.Iv:idx]

        /* check for escape sequence */
        if njs.Ep != -1 {
            if key, err = unquote(key); err != 0 {
                return Node{}, err
            }
        }

        /* expect a ':' delimiter */
        if err = self.delim(); err != 0 {
            return Node{}, err
        }

        
        if self.skipValue {
            /* skip the value */
            var start int
            if start, err = self.skipFast(); err != 0 {
                return Node{}, err
            }
            if self.p > ns {
                return Node{}, types.ERR_EOF
            }
            t := switchRawType(self.s[start])
            if t == _V_NONE {
                return Node{}, types.ERR_INVALID_CHAR
            }
            val = newRawNode(self.s[start:self.p], t)
        } else {
            /* decode the value */
            if val, err = self.Parse(); err != 0 {
                return Node{}, err
            }
        }

        /* add the value to result */
        ret = append(ret, Pair{Key: key, Value: val})
        self.p = self.lspace(self.p)

        /* check for EOF */
        if self.p >= ns {
            return Node{}, types.ERR_EOF
        }

        /* check for the next character */
        switch self.s[self.p] {
            case ',' : self.p++
            case '}' : self.p++; return NewObject(ret), 0
        default:
            if val.isLazy() {
                return newLazyObject(self, ret), 0
            }
            return Node{}, types.ERR_INVALID_CHAR
        }
    }
}

func (self *Parser) decodeString(iv int64, ep int) (Node, types.ParsingError) {
    p := self.p - 1
    s := self.s[iv:p]

    /* fast path: no escape sequence */
    if ep == -1 {
        return NewString(s), 0
    }

    /* unquote the string */
    out, err := unquote(s)

    /* check for errors */
    if err != 0 {
        return Node{}, err
    } else {
        return newBytes(rt.Str2Mem(out)), 0
    }
}

/** Parser Interface **/

func (self *Parser) Pos() int {
    return self.p
}

func (self *Parser) Parse() (Node, types.ParsingError) {
    switch val := self.decodeValue(); val.Vt {
        case types.V_EOF     : return Node{}, types.ERR_EOF
        case types.V_NULL    : return nullNode, 0
        case types.V_TRUE    : return trueNode, 0
        case types.V_FALSE   : return falseNode, 0
        case types.V_STRING  : return self.decodeString(val.Iv, val.Ep)
        case types.V_ARRAY:
            if self.noLazy {
                return self.decodeArray(make([]Node, 0, _DEFAULT_NODE_CAP))
            }
            return newLazyArray(self, make([]Node, 0, _DEFAULT_NODE_CAP)), 0
        case types.V_OBJECT:
            if self.noLazy {
                return self.decodeObject(make([]Pair, 0, _DEFAULT_NODE_CAP))
            }
            return newLazyObject(self, make([]Pair, 0, _DEFAULT_NODE_CAP)), 0
        case types.V_DOUBLE  : return NewNumber(self.s[val.Ep:self.p]), 0
        case types.V_INTEGER : return NewNumber(self.s[val.Ep:self.p]), 0
        default              : return Node{}, types.ParsingError(-val.Vt)
    }
}

func (self *Parser) searchKey(match string) types.ParsingError {
    ns := len(self.s)
    if err := self.object(); err != 0 {
        return err
    }

    /* check for EOF */
    if self.p = self.lspace(self.p); self.p >= ns {
        return types.ERR_EOF
    }

    /* check for empty object */
    if self.s[self.p] == '}' {
        self.p++
        return _ERR_NOT_FOUND
    }

    var njs types.JsonState
    var err types.ParsingError
    /* decode each pair */
    for {

        /* decode the key */
        if njs = self.decodeValue(); njs.Vt != types.V_STRING {
            return types.ERR_INVALID_CHAR
        }

        /* extract the key */
        idx := self.p - 1
        key := self.s[njs.Iv:idx]

        /* check for escape sequence */
        if njs.Ep != -1 {
            if key, err = unquote(key); err != 0 {
                return err
            }
        }

        /* expect a ':' delimiter */
        if err = self.delim(); err != 0 {
            return err
        }

        /* skip value */
        if key != match {
            if _, err = self.skip(); err != 0 {
                return err
            }
        } else {
            return 0
        }

        /* check for EOF */
        self.p = self.lspace(self.p)
        if self.p >= ns {
            return types.ERR_EOF
        }

        /* check for the next character */
        switch self.s[self.p] {
        case ',':
            self.p++
        case '}':
            self.p++
            return _ERR_NOT_FOUND
        default:
            return types.ERR_INVALID_CHAR
        }
    }
}

func (self *Parser) searchIndex(idx int) types.ParsingError {
    ns := len(self.s)
    if err := self.array(); err != 0 {
        return err
    }

    /* check for EOF */
    if self.p = self.lspace(self.p); self.p >= ns {
        return types.ERR_EOF
    }

    /* check for empty array */
    if self.s[self.p] == ']' {
        self.p++
        return _ERR_NOT_FOUND
    }

    var err types.ParsingError
    /* allocate array space and parse every element */
    for i := 0; i < idx; i++ {

        /* decode the value */
        if _, err = self.skip(); err != 0 {
            return err
        }

        /* check for EOF */
        self.p = self.lspace(self.p)
        if self.p >= ns {
            return types.ERR_EOF
        }

        /* check for the next character */
        switch self.s[self.p] {
        case ',':
            self.p++
        case ']':
            self.p++
            return _ERR_NOT_FOUND
        default:
            return types.ERR_INVALID_CHAR
        }
    }

    return 0
}

func (self *Node) skipNextNode() *Node {
    if !self.isLazy() {
        return nil
    }

    parser, stack := self.getParserAndArrayStack()
    ret := stack.v
    sp := parser.p
    ns := len(parser.s)

    /* check for EOF */
    if parser.p = parser.lspace(sp); parser.p >= ns {
        return newSyntaxError(parser.syntaxError(types.ERR_EOF))
    }

    /* check for empty array */
    if parser.s[parser.p] == ']' {
        parser.p++
        self.setArray(ret)
        return nil
    }

    var val Node
    /* skip the value */
    if start, err := parser.skipFast(); err != 0 {
        return newSyntaxError(parser.syntaxError(err))
    } else {
        t := switchRawType(parser.s[start])
        if t == _V_NONE {
            return newSyntaxError(parser.syntaxError(types.ERR_INVALID_CHAR))
        }
        val = newRawNode(parser.s[start:parser.p], t)
    }

    /* add the value to result */
    ret = append(ret, val)
    parser.p = parser.lspace(parser.p)

    /* check for EOF */
    if parser.p >= ns {
        return newSyntaxError(parser.syntaxError(types.ERR_EOF))
    }

    /* check for the next character */
    switch parser.s[parser.p] {
    case ',':
        parser.p++
        self.setLazyArray(parser, ret)
        return &ret[len(ret)-1]
    case ']':
        parser.p++
        self.setArray(ret)
        return &ret[len(ret)-1]
    default:
        return newSyntaxError(parser.syntaxError(types.ERR_INVALID_CHAR))
    }
}

func (self *Node) skipNextPair() (*Pair) {
    if !self.isLazy() {
        return nil
    }

    parser, stack := self.getParserAndObjectStack()
    ret := stack.v
    sp := parser.p
    ns := len(parser.s)

    /* check for EOF */
    if parser.p = parser.lspace(sp); parser.p >= ns {
        return &Pair{"", *newSyntaxError(parser.syntaxError(types.ERR_EOF))}
    }

    /* check for empty object */
    if parser.s[parser.p] == '}' {
        parser.p++
        self.setObject(ret)
        return nil
    }

    /* decode one pair */
    var val Node
    var njs types.JsonState
    var err types.ParsingError

    /* decode the key */
    if njs = parser.decodeValue(); njs.Vt != types.V_STRING {
        return &Pair{"", *newSyntaxError(parser.syntaxError(types.ERR_INVALID_CHAR))}
    }

    /* extract the key */
    idx := parser.p - 1
    key := parser.s[njs.Iv:idx]

    /* check for escape sequence */
    if njs.Ep != -1 {
        if key, err = unquote(key); err != 0 {
            return &Pair{key, *newSyntaxError(parser.syntaxError(err))}
        }
    }

    /* expect a ':' delimiter */
    if err = parser.delim(); err != 0 {
        return &Pair{key, *newSyntaxError(parser.syntaxError(err))}
    }

    /* skip the value */
    if start, err := parser.skipFast(); err != 0 {
        return &Pair{key, *newSyntaxError(parser.syntaxError(err))}
    } else {
        t := switchRawType(parser.s[start])
        if t == _V_NONE {
            return &Pair{key, *newSyntaxError(parser.syntaxError(types.ERR_INVALID_CHAR))}
        }
        val = newRawNode(parser.s[start:parser.p], t)
    }

    /* add the value to result */
    ret = append(ret, Pair{Key: key, Value: val})
    parser.p = parser.lspace(parser.p)

    /* check for EOF */
    if parser.p >= ns {
        return &Pair{key, *newSyntaxError(parser.syntaxError(types.ERR_EOF))}
    }

    /* check for the next character */
    switch parser.s[parser.p] {
    case ',':
        parser.p++
        self.setLazyObject(parser, ret)
        return &ret[len(ret)-1]
    case '}':
        parser.p++
        self.setObject(ret)
        return &ret[len(ret)-1]
    default:
        return &Pair{key, *newSyntaxError(parser.syntaxError(types.ERR_INVALID_CHAR))}
    }
}


/** Parser Factory **/

// Loads parse all json into interface{}
func Loads(src string) (int, interface{}, error) {
    ps := &Parser{s: src}
    np, err := ps.Parse()

    /* check for errors */
    if err != 0 {
        return 0, nil, ps.ExportError(err)
    } else {
        x, err := np.Interface()
        if err != nil {
            return 0, nil, err
        }
        return ps.Pos(), x, nil
    }
}

// LoadsUseNumber parse all json into interface{}, with numeric nodes casted to json.Number
func LoadsUseNumber(src string) (int, interface{}, error) {
    ps := &Parser{s: src}
    np, err := ps.Parse()

    /* check for errors */
    if err != 0 {
        return 0, nil, err
    } else {
        x, err := np.InterfaceUseNumber()
        if err != nil {
            return 0, nil, err
        }
        return ps.Pos(), x, nil
    }
}

func NewParser(src string) *Parser {
    return &Parser{s: src}
}

// ExportError converts types.ParsingError to std Error
func (self *Parser) ExportError(err types.ParsingError) error {
    if err == _ERR_NOT_FOUND {
        return ErrNotExist
    }
    return fmt.Errorf("%q", SyntaxError{
        Pos : self.p,
        Src : self.s,
        Code: err,
    }.Description())
}