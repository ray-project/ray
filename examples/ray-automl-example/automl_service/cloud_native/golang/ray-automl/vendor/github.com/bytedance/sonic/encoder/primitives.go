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
    `encoding`
    `encoding/json`
    `unsafe`

    `github.com/bytedance/sonic/internal/jit`
    `github.com/bytedance/sonic/internal/native`
    `github.com/bytedance/sonic/internal/rt`
)

/** Encoder Primitives **/

func encodeNil(rb *[]byte) error {
    *rb = append(*rb, 'n', 'u', 'l', 'l')
    return nil
}

func encodeString(buf *[]byte, val string) error {
    var sidx int
    var pbuf *rt.GoSlice
    var pstr *rt.GoString

    /* opening quote */
    *buf = append(*buf, '"')
    pbuf = (*rt.GoSlice)(unsafe.Pointer(buf))
    pstr = (*rt.GoString)(unsafe.Pointer(&val))

    /* encode with native library */
    for sidx < pstr.Len {
        sn := pstr.Len - sidx
        dn := pbuf.Cap - pbuf.Len
        sp := padd(pstr.Ptr, sidx)
        dp := padd(pbuf.Ptr, pbuf.Len)
        nb := native.Quote(sp, sn, dp, &dn, 0)

        /* check for errors */
        if pbuf.Len += dn; nb >= 0 {
            break
        }

        /* not enough space, grow the slice and try again */
        sidx += ^nb
        *pbuf = growslice(rt.UnpackType(byteType), *pbuf, pbuf.Cap * 2)
    }

    /* closing quote */
    *buf = append(*buf, '"')
    return nil
}

func encodeTypedPointer(buf *[]byte, vt *rt.GoType, vp *unsafe.Pointer, sb *_Stack, fv uint64) error {
    if vt == nil {
        return encodeNil(buf)
    } else if fn, err := findOrCompile(vt, (fv&(1<<bitPointerValue)) != 0); err != nil {
        return err
    } else if vt.Indirect() {
        rt.MoreStack(_FP_size + native.MaxFrameSize)
        rt.StopProf()
        err := fn(buf, *vp, sb, fv)
        rt.StartProf()
        return err
    } else {
        rt.MoreStack(_FP_size + native.MaxFrameSize)
        rt.StopProf()
        err := fn(buf, unsafe.Pointer(vp), sb, fv)
        rt.StartProf()
        return err
    }
}

func encodeJsonMarshaler(buf *[]byte, val json.Marshaler, opt Options) error {
    if ret, err := val.MarshalJSON(); err != nil {
        return err
    } else {
        if opt & CompactMarshaler != 0 {
            return compact(buf, ret)
        }
        if ok, s := Valid(ret); !ok {
            return error_marshaler(ret, s)
        }
        *buf = append(*buf, ret...)
        return nil
    }
}

func encodeTextMarshaler(buf *[]byte, val encoding.TextMarshaler, opt Options) error {
    if ret, err := val.MarshalText(); err != nil {
        return err
    } else {
        if opt & NoQuoteTextMarshaler != 0 {
            *buf = append(*buf, ret...)
            return nil
        }
        return encodeString(buf, rt.Mem2Str(ret) )
    }
}

func htmlEscape(dst []byte, src []byte) []byte {
    var sidx int

    dst  = append(dst, src[:0]...) // avoid check nil dst
    sbuf := (*rt.GoSlice)(unsafe.Pointer(&src))
    dbuf := (*rt.GoSlice)(unsafe.Pointer(&dst))

    /* grow dst if it is shorter */
    if cap(dst) - len(dst) < len(src) + native.BufPaddingSize {
        cap :=  len(src) * 3 / 2 + native.BufPaddingSize
        *dbuf = growslice(typeByte, *dbuf, cap)
    }

    for sidx < sbuf.Len {
        sp := padd(sbuf.Ptr, sidx)
        dp := padd(dbuf.Ptr, dbuf.Len)

        sn := sbuf.Len - sidx
        dn := dbuf.Cap - dbuf.Len
        nb := native.HTMLEscape(sp, sn, dp, &dn)

        /* check for errors */
        if dbuf.Len += dn; nb >= 0 {
            break
        }

        /* not enough space, grow the slice and try again */
        sidx += ^nb
        *dbuf = growslice(typeByte, *dbuf, dbuf.Cap * 2)
    }
    return dst
}

var (
    argPtrs   = []bool { true, true, true, false }
    localPtrs = []bool{}
)

var (
    _F_assertI2I = jit.Func(assertI2I)
)

func asText(v unsafe.Pointer) (string, error) {
    text := assertI2I(_T_encoding_TextMarshaler, *(*rt.GoIface)(v))
    r, e := (*(*encoding.TextMarshaler)(unsafe.Pointer(&text))).MarshalText()
    return rt.Mem2Str(r), e
}

func asJson(v unsafe.Pointer) (string, error) {
    text := assertI2I(_T_json_Marshaler, *(*rt.GoIface)(v))
    r, e := (*(*json.Marshaler)(unsafe.Pointer(&text))).MarshalJSON()
    return rt.Mem2Str(r), e
}