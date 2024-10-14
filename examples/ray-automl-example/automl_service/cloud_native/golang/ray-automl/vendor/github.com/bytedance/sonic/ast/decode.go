package ast

import (
    `encoding/base64`
    `runtime`
    `strconv`
    `unsafe`

    `github.com/bytedance/sonic/internal/native/types`
    `github.com/bytedance/sonic/internal/rt`
)

const _blankCharsMask = (1 << ' ') | (1 << '\t') | (1 << '\r') | (1 << '\n')

const (
    bytesNull   = "null"
    bytesTrue   = "true"
    bytesFalse  = "false"
    bytesObject = "{}"
    bytesArray  = "[]"
)

func isSpace(c byte) bool {
    return (int(1<<c) & _blankCharsMask) != 0
}

func skipBlank(src string, pos int) int {
    se := uintptr(rt.IndexChar(src, len(src)))
    sp := uintptr(rt.IndexChar(src, pos))

    for sp < se {
        if !isSpace(*(*byte)(unsafe.Pointer(sp))) {
            break
        }
        sp += 1
    }
    if sp >= se {
        return -int(types.ERR_EOF)
    }
    runtime.KeepAlive(src)
    return int(sp - uintptr(rt.IndexChar(src, 0)))
}

func decodeNull(src string, pos int) (ret int) {
    ret = pos + 4
    if ret > len(src) {
        return -int(types.ERR_EOF)
    }
    if src[pos:ret] == bytesNull {
        return ret
    } else {
        return -int(types.ERR_INVALID_CHAR)
    }
}

func decodeTrue(src string, pos int) (ret int) {
    ret = pos + 4
    if ret > len(src) {
        return -int(types.ERR_EOF)
    }
    if src[pos:ret] == bytesTrue {
        return ret
    } else {
        return -int(types.ERR_INVALID_CHAR)
    }

}

func decodeFalse(src string, pos int) (ret int) {
    ret = pos + 5
    if ret > len(src) {
        return -int(types.ERR_EOF)
    }
    if src[pos:ret] == bytesFalse {
        return ret
    }
    return -int(types.ERR_INVALID_CHAR)
}

func decodeString(src string, pos int) (ret int, v string) {
    ret, ep := skipString(src, pos)
    if ep == -1 {
        (*rt.GoString)(unsafe.Pointer(&v)).Ptr = rt.IndexChar(src, pos+1)
        (*rt.GoString)(unsafe.Pointer(&v)).Len = ret - pos - 2
        return ret, v
    }

    vv, ok := unquoteBytes(rt.Str2Mem(src[pos:ret]))
    if !ok {
        return -int(types.ERR_INVALID_CHAR), ""
    }

    runtime.KeepAlive(src)
    return ret, rt.Mem2Str(vv)
}

func decodeBinary(src string, pos int) (ret int, v []byte) {
    var vv string
    ret, vv = decodeString(src, pos)
    if ret < 0 {
        return ret, nil
    }
    var err error
    v, err = base64.StdEncoding.DecodeString(vv)
    if err != nil {
        return -int(types.ERR_INVALID_CHAR), nil
    }
    return ret, v
}

func isDigit(c byte) bool {
    return c >= '0' && c <= '9'
}

func decodeInt64(src string, pos int) (ret int, v int64, err error) {
    sp := uintptr(rt.IndexChar(src, pos))
    ss := uintptr(sp)
    se := uintptr(rt.IndexChar(src, len(src)))
    if uintptr(sp) >= se {
        return -int(types.ERR_EOF), 0, nil
    }

    if c := *(*byte)(unsafe.Pointer(sp)); c == '-' {
        sp += 1
    }
    if sp == se {
        return -int(types.ERR_EOF), 0, nil
    }

    for ; sp < se; sp += uintptr(1) {
        if !isDigit(*(*byte)(unsafe.Pointer(sp))) {
            break
        }
    }

    if sp < se {
        if c := *(*byte)(unsafe.Pointer(sp)); c == '.' || c == 'e' || c == 'E' {
            return -int(types.ERR_INVALID_NUMBER_FMT), 0, nil
        }
    }

    var vv string
    ret = int(uintptr(sp) - uintptr((*rt.GoString)(unsafe.Pointer(&src)).Ptr))
    (*rt.GoString)(unsafe.Pointer(&vv)).Ptr = unsafe.Pointer(ss)
    (*rt.GoString)(unsafe.Pointer(&vv)).Len = ret - pos

    v, err = strconv.ParseInt(vv, 10, 64)
    if err != nil {
        //NOTICE: allow overflow here
        if err.(*strconv.NumError).Err == strconv.ErrRange {
            return ret, 0, err
        }
        return -int(types.ERR_INVALID_CHAR), 0, err
    }

    runtime.KeepAlive(src)
    return ret, v, nil
}

func isNumberChars(c byte) bool {
    return (c >= '0' && c <= '9') || c == '+' || c == '-' || c == 'e' || c == 'E' || c == '.'
}

func decodeFloat64(src string, pos int) (ret int, v float64, err error) {
    sp := uintptr(rt.IndexChar(src, pos))
    ss := uintptr(sp)
    se := uintptr(rt.IndexChar(src, len(src)))
    if uintptr(sp) >= se {
        return -int(types.ERR_EOF), 0, nil
    }

    if c := *(*byte)(unsafe.Pointer(sp)); c == '-' {
        sp += 1
    }
    if sp == se {
        return -int(types.ERR_EOF), 0, nil
    }

    for ; sp < se; sp += uintptr(1) {
        if !isNumberChars(*(*byte)(unsafe.Pointer(sp))) {
            break
        }
    }

    var vv string
    ret = int(uintptr(sp) - uintptr((*rt.GoString)(unsafe.Pointer(&src)).Ptr))
    (*rt.GoString)(unsafe.Pointer(&vv)).Ptr = unsafe.Pointer(ss)
    (*rt.GoString)(unsafe.Pointer(&vv)).Len = ret - pos

    v, err = strconv.ParseFloat(vv, 64)
    if err != nil {
        //NOTICE: allow overflow here
        if err.(*strconv.NumError).Err == strconv.ErrRange {
            return ret, 0, err
        }
        return -int(types.ERR_INVALID_CHAR), 0, err
    }

    runtime.KeepAlive(src)
    return ret, v, nil
}

func decodeValue(src string, pos int) (ret int, v types.JsonState) {
    pos = skipBlank(src, pos)
    if pos < 0 {
        return pos, types.JsonState{Vt: types.ValueType(pos)}
    }
    switch c := src[pos]; c {
    case 'n':
        ret = decodeNull(src, pos)
        if ret < 0 {
            return ret, types.JsonState{Vt: types.ValueType(ret)}
        }
        return ret, types.JsonState{Vt: types.V_NULL}
    case '"':
        var ep int
        ret, ep = skipString(src, pos)
        if ret < 0 {
            return ret, types.JsonState{Vt: types.ValueType(ret)}
        }
        return ret, types.JsonState{Vt: types.V_STRING, Iv: int64(pos + 1), Ep: ep}
    case '{':
        return pos + 1, types.JsonState{Vt: types.V_OBJECT}
    case '[':
        return pos + 1, types.JsonState{Vt: types.V_ARRAY}
    case 't':
        ret = decodeTrue(src, pos)
        if ret < 0 {
            return ret, types.JsonState{Vt: types.ValueType(ret)}
        }
        return ret, types.JsonState{Vt: types.V_TRUE}
    case 'f':
        ret = decodeFalse(src, pos)
        if ret < 0 {
            return ret, types.JsonState{Vt: types.ValueType(ret)}
        }
        return ret, types.JsonState{Vt: types.V_FALSE}
    case '-', '+', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
        var iv int64
        ret, iv, _ = decodeInt64(src, pos)
        if ret >= 0 {
            return ret, types.JsonState{Vt: types.V_INTEGER, Iv: iv, Ep: pos}
        } else if ret != -int(types.ERR_INVALID_NUMBER_FMT) {
            return ret, types.JsonState{Vt: types.ValueType(ret)}
        }
        var fv float64
        ret, fv, _ = decodeFloat64(src, pos)
        if ret >= 0 {
            return ret, types.JsonState{Vt: types.V_DOUBLE, Dv: fv, Ep: pos}
        } else {
            return ret, types.JsonState{Vt: types.ValueType(ret)}
        }
    default:
        return -int(types.ERR_INVALID_CHAR), types.JsonState{Vt:-types.ValueType(types.ERR_INVALID_CHAR)}
    }
}

func skipNumber(src string, pos int) (ret int) {
    sp := uintptr(rt.IndexChar(src, pos))
    se := uintptr(rt.IndexChar(src, len(src)))
    if uintptr(sp) >= se {
        return -int(types.ERR_EOF)
    }

    if c := *(*byte)(unsafe.Pointer(sp)); c == '-' {
        sp += 1
    }
    ss := sp

    var pointer bool
    var exponent bool
    var lastIsDigit bool
    var nextNeedDigit = true

    for ; sp < se; sp += uintptr(1) {
        c := *(*byte)(unsafe.Pointer(sp))
        if isDigit(c) {
            lastIsDigit = true
            nextNeedDigit = false
            continue
        } else if nextNeedDigit {
            return -int(types.ERR_INVALID_CHAR)
        } else if c == '.' {
            if !lastIsDigit || pointer || sp == ss {
                return -int(types.ERR_INVALID_CHAR)
            }
            pointer = true
            lastIsDigit = false
            nextNeedDigit = true
            continue
        } else if c == 'e' || c == 'E' {
            if !lastIsDigit || exponent {
                return -int(types.ERR_INVALID_CHAR)
            }
            if sp == se-1 {
                return -int(types.ERR_EOF)
            }
            exponent = true
            lastIsDigit = false
            nextNeedDigit = false
            continue
        } else if c == '-' || c == '+' {
            if prev := *(*byte)(unsafe.Pointer(sp - 1)); prev != 'e' && prev != 'E' {
                return -int(types.ERR_INVALID_CHAR)
            }
            lastIsDigit = false
            nextNeedDigit = true
            continue
        } else {
            break
        }
    }

    if nextNeedDigit {
        return -int(types.ERR_EOF)
    }

    runtime.KeepAlive(src)
    return int(uintptr(sp) - uintptr((*rt.GoString)(unsafe.Pointer(&src)).Ptr))
}

func skipString(src string, pos int) (ret int, ep int) {
    if pos+1 >= len(src) {
        return -int(types.ERR_EOF), -1
    }

    sp := uintptr(rt.IndexChar(src, pos))
    se := uintptr(rt.IndexChar(src, len(src)))

    if *(*byte)(unsafe.Pointer(sp)) != '"' {
        return -int(types.ERR_INVALID_CHAR), -1
    }
    sp += 1

    ep = -1
    for sp < se {
        c := *(*byte)(unsafe.Pointer(sp))
        if c == '\\' {
            if ep == -1 {
                ep = int(uintptr(sp) - uintptr((*rt.GoString)(unsafe.Pointer(&src)).Ptr))
            }
            sp += 2
            continue
        }
        sp += 1
        if c == '"' {
            break
        }
    }

    if sp > se {
        return -int(types.ERR_EOF), -1
    }

    runtime.KeepAlive(src)
    return int(uintptr(sp) - uintptr((*rt.GoString)(unsafe.Pointer(&src)).Ptr)), ep
}

func skipPair(src string, pos int, lchar byte, rchar byte) (ret int) {
    if pos+1 >= len(src) {
        return -int(types.ERR_EOF)
    }

    sp := uintptr(rt.IndexChar(src, pos))
    se := uintptr(rt.IndexChar(src, len(src)))

    if *(*byte)(unsafe.Pointer(sp)) != lchar {
        return -int(types.ERR_INVALID_CHAR)
    }

    sp += 1
    nbrace := 1
    inquote := false

    for sp < se {
        c := *(*byte)(unsafe.Pointer(sp))
        if c == '\\' {
            sp += 2
            continue
        } else if c == '"' {
            inquote = !inquote
        } else if c == lchar {
            if !inquote {
                nbrace += 1
            }
        } else if c == rchar {
            if !inquote {
                nbrace -= 1
                if nbrace == 0 {
                    sp += 1
                    break
                }
            }
        }
        sp += 1
    }

    if nbrace != 0 {
        return -int(types.ERR_INVALID_CHAR)
    }

    runtime.KeepAlive(src)
    return int(uintptr(sp) - uintptr((*rt.GoString)(unsafe.Pointer(&src)).Ptr))
}

func skipValue(src string, pos int) (ret int, start int) {
    pos = skipBlank(src, pos)
    if pos < 0 {
        return pos, -1
    }
    switch c := src[pos]; c {
    case 'n':
        ret = decodeNull(src, pos)
    case '"':
        ret, _ = skipString(src, pos)
    case '{':
        ret = skipPair(src, pos, '{', '}')
    case '[':
        ret = skipPair(src, pos, '[', ']')
    case 't':
        ret = decodeTrue(src, pos)
    case 'f':
        ret = decodeFalse(src, pos)
    case '-', '+', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
        ret = skipNumber(src, pos)
    default:
        ret = -int(types.ERR_INVALID_CHAR)
    }
    return ret, pos
}
