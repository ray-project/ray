// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

/*
Msgpack-c implementation powers the c, c++, python, ruby, etc libraries.
We need to maintain compatibility with it and how it encodes integer values
without caring about the type.

For compatibility with behaviour of msgpack-c reference implementation:
  - Go intX (>0) and uintX
       IS ENCODED AS
    msgpack +ve fixnum, unsigned
  - Go intX (<0)
       IS ENCODED AS
    msgpack -ve fixnum, signed
*/

package codec

import (
	"fmt"
	"io"
	"math"
	"net/rpc"
	"reflect"
	"time"
	"unicode/utf8"
)

const (
	mpPosFixNumMin byte = 0x00
	mpPosFixNumMax byte = 0x7f
	mpFixMapMin    byte = 0x80
	mpFixMapMax    byte = 0x8f
	mpFixArrayMin  byte = 0x90
	mpFixArrayMax  byte = 0x9f
	mpFixStrMin    byte = 0xa0
	mpFixStrMax    byte = 0xbf
	mpNil          byte = 0xc0
	_              byte = 0xc1
	mpFalse        byte = 0xc2
	mpTrue         byte = 0xc3
	mpFloat        byte = 0xca
	mpDouble       byte = 0xcb
	mpUint8        byte = 0xcc
	mpUint16       byte = 0xcd
	mpUint32       byte = 0xce
	mpUint64       byte = 0xcf
	mpInt8         byte = 0xd0
	mpInt16        byte = 0xd1
	mpInt32        byte = 0xd2
	mpInt64        byte = 0xd3

	// extensions below
	mpBin8     byte = 0xc4
	mpBin16    byte = 0xc5
	mpBin32    byte = 0xc6
	mpExt8     byte = 0xc7
	mpExt16    byte = 0xc8
	mpExt32    byte = 0xc9
	mpFixExt1  byte = 0xd4
	mpFixExt2  byte = 0xd5
	mpFixExt4  byte = 0xd6
	mpFixExt8  byte = 0xd7
	mpFixExt16 byte = 0xd8

	mpStr8  byte = 0xd9 // new
	mpStr16 byte = 0xda
	mpStr32 byte = 0xdb

	mpArray16 byte = 0xdc
	mpArray32 byte = 0xdd

	mpMap16 byte = 0xde
	mpMap32 byte = 0xdf

	mpNegFixNumMin byte = 0xe0
	mpNegFixNumMax byte = 0xff
)

var mpTimeExtTag int8 = -1
var mpTimeExtTagU = uint8(mpTimeExtTag)

var mpdescNames = map[byte]string{
	mpNil:    "nil",
	mpFalse:  "false",
	mpTrue:   "true",
	mpFloat:  "float",
	mpDouble: "float",
	mpUint8:  "uuint",
	mpUint16: "uint",
	mpUint32: "uint",
	mpUint64: "uint",
	mpInt8:   "int",
	mpInt16:  "int",
	mpInt32:  "int",
	mpInt64:  "int",

	mpStr8:  "string|bytes",
	mpStr16: "string|bytes",
	mpStr32: "string|bytes",

	mpBin8:  "bytes",
	mpBin16: "bytes",
	mpBin32: "bytes",

	mpArray16: "array",
	mpArray32: "array",

	mpMap16: "map",
	mpMap32: "map",
}

func mpdesc(bd byte) (s string) {
	s = mpdescNames[bd]
	if s == "" {
		switch {
		case bd >= mpPosFixNumMin && bd <= mpPosFixNumMax,
			bd >= mpNegFixNumMin && bd <= mpNegFixNumMax:
			s = "int"
		case bd >= mpFixStrMin && bd <= mpFixStrMax:
			s = "string|bytes"
		case bd >= mpFixArrayMin && bd <= mpFixArrayMax:
			s = "array"
		case bd >= mpFixMapMin && bd <= mpFixMapMax:
			s = "map"
		case bd >= mpFixExt1 && bd <= mpFixExt16,
			bd >= mpExt8 && bd <= mpExt32:
			s = "ext"
		default:
			s = "unknown"
		}
	}
	return
}

// MsgpackSpecRpcMultiArgs is a special type which signifies to the MsgpackSpecRpcCodec
// that the backend RPC service takes multiple arguments, which have been arranged
// in sequence in the slice.
//
// The Codec then passes it AS-IS to the rpc service (without wrapping it in an
// array of 1 element).
type MsgpackSpecRpcMultiArgs []interface{}

// A MsgpackContainer type specifies the different types of msgpackContainers.
type msgpackContainerType struct {
	fixCutoff, bFixMin, b8, b16, b32 byte
	// hasFixMin, has8, has8Always bool
}

var (
	msgpackContainerRawLegacy = msgpackContainerType{
		32, mpFixStrMin, 0, mpStr16, mpStr32,
	}
	msgpackContainerStr = msgpackContainerType{
		32, mpFixStrMin, mpStr8, mpStr16, mpStr32, // true, true, false,
	}
	msgpackContainerBin = msgpackContainerType{
		0, 0, mpBin8, mpBin16, mpBin32, // false, true, true,
	}
	msgpackContainerList = msgpackContainerType{
		16, mpFixArrayMin, 0, mpArray16, mpArray32, // true, false, false,
	}
	msgpackContainerMap = msgpackContainerType{
		16, mpFixMapMin, 0, mpMap16, mpMap32, // true, false, false,
	}
)

//---------------------------------------------

type msgpackEncDriver struct {
	noBuiltInTypes
	encDriverNoopContainerWriter
	encDriverNoState
	h *MsgpackHandle
	// x [8]byte
	e Encoder
}

func (e *msgpackEncDriver) encoder() *Encoder {
	return &e.e
}

func (e *msgpackEncDriver) EncodeNil() {
	e.e.encWr.writen1(mpNil)
}

func (e *msgpackEncDriver) EncodeInt(i int64) {
	if e.h.PositiveIntUnsigned && i >= 0 {
		e.EncodeUint(uint64(i))
	} else if i > math.MaxInt8 {
		if i <= math.MaxInt16 {
			e.e.encWr.writen1(mpInt16)
			bigen.writeUint16(e.e.w(), uint16(i))
		} else if i <= math.MaxInt32 {
			e.e.encWr.writen1(mpInt32)
			bigen.writeUint32(e.e.w(), uint32(i))
		} else {
			e.e.encWr.writen1(mpInt64)
			bigen.writeUint64(e.e.w(), uint64(i))
		}
	} else if i >= -32 {
		if e.h.NoFixedNum {
			e.e.encWr.writen2(mpInt8, byte(i))
		} else {
			e.e.encWr.writen1(byte(i))
		}
	} else if i >= math.MinInt8 {
		e.e.encWr.writen2(mpInt8, byte(i))
	} else if i >= math.MinInt16 {
		e.e.encWr.writen1(mpInt16)
		bigen.writeUint16(e.e.w(), uint16(i))
	} else if i >= math.MinInt32 {
		e.e.encWr.writen1(mpInt32)
		bigen.writeUint32(e.e.w(), uint32(i))
	} else {
		e.e.encWr.writen1(mpInt64)
		bigen.writeUint64(e.e.w(), uint64(i))
	}
}

func (e *msgpackEncDriver) EncodeUint(i uint64) {
	if i <= math.MaxInt8 {
		if e.h.NoFixedNum {
			e.e.encWr.writen2(mpUint8, byte(i))
		} else {
			e.e.encWr.writen1(byte(i))
		}
	} else if i <= math.MaxUint8 {
		e.e.encWr.writen2(mpUint8, byte(i))
	} else if i <= math.MaxUint16 {
		e.e.encWr.writen1(mpUint16)
		bigen.writeUint16(e.e.w(), uint16(i))
	} else if i <= math.MaxUint32 {
		e.e.encWr.writen1(mpUint32)
		bigen.writeUint32(e.e.w(), uint32(i))
	} else {
		e.e.encWr.writen1(mpUint64)
		bigen.writeUint64(e.e.w(), uint64(i))
	}
}

func (e *msgpackEncDriver) EncodeBool(b bool) {
	if b {
		e.e.encWr.writen1(mpTrue)
	} else {
		e.e.encWr.writen1(mpFalse)
	}
}

func (e *msgpackEncDriver) EncodeFloat32(f float32) {
	e.e.encWr.writen1(mpFloat)
	bigen.writeUint32(e.e.w(), math.Float32bits(f))
}

func (e *msgpackEncDriver) EncodeFloat64(f float64) {
	e.e.encWr.writen1(mpDouble)
	bigen.writeUint64(e.e.w(), math.Float64bits(f))
}

func (e *msgpackEncDriver) EncodeTime(t time.Time) {
	if t.IsZero() {
		e.EncodeNil()
		return
	}
	t = t.UTC()
	sec, nsec := t.Unix(), uint64(t.Nanosecond())
	var data64 uint64
	var l = 4
	if sec >= 0 && sec>>34 == 0 {
		data64 = (nsec << 34) | uint64(sec)
		if data64&0xffffffff00000000 != 0 {
			l = 8
		}
	} else {
		l = 12
	}
	if e.h.WriteExt {
		e.encodeExtPreamble(mpTimeExtTagU, l)
	} else {
		e.writeContainerLen(msgpackContainerRawLegacy, l)
	}
	switch l {
	case 4:
		bigen.writeUint32(e.e.w(), uint32(data64))
	case 8:
		bigen.writeUint64(e.e.w(), data64)
	case 12:
		bigen.writeUint32(e.e.w(), uint32(nsec))
		bigen.writeUint64(e.e.w(), uint64(sec))
	}
}

func (e *msgpackEncDriver) EncodeExt(v interface{}, basetype reflect.Type, xtag uint64, ext Ext) {
	var bs0, bs []byte
	if ext == SelfExt {
		bs0 = e.e.blist.get(1024)
		bs = bs0
		e.e.sideEncode(v, basetype, &bs)
	} else {
		bs = ext.WriteExt(v)
	}
	if bs == nil {
		e.EncodeNil()
		goto END
	}
	if e.h.WriteExt {
		e.encodeExtPreamble(uint8(xtag), len(bs))
		e.e.encWr.writeb(bs)
	} else {
		e.EncodeStringBytesRaw(bs)
	}
END:
	if ext == SelfExt {
		e.e.blist.put(bs)
		if !byteSliceSameData(bs0, bs) {
			e.e.blist.put(bs0)
		}
	}
}

func (e *msgpackEncDriver) EncodeRawExt(re *RawExt) {
	e.encodeExtPreamble(uint8(re.Tag), len(re.Data))
	e.e.encWr.writeb(re.Data)
}

func (e *msgpackEncDriver) encodeExtPreamble(xtag byte, l int) {
	if l == 1 {
		e.e.encWr.writen2(mpFixExt1, xtag)
	} else if l == 2 {
		e.e.encWr.writen2(mpFixExt2, xtag)
	} else if l == 4 {
		e.e.encWr.writen2(mpFixExt4, xtag)
	} else if l == 8 {
		e.e.encWr.writen2(mpFixExt8, xtag)
	} else if l == 16 {
		e.e.encWr.writen2(mpFixExt16, xtag)
	} else if l < 256 {
		e.e.encWr.writen2(mpExt8, byte(l))
		e.e.encWr.writen1(xtag)
	} else if l < 65536 {
		e.e.encWr.writen1(mpExt16)
		bigen.writeUint16(e.e.w(), uint16(l))
		e.e.encWr.writen1(xtag)
	} else {
		e.e.encWr.writen1(mpExt32)
		bigen.writeUint32(e.e.w(), uint32(l))
		e.e.encWr.writen1(xtag)
	}
}

func (e *msgpackEncDriver) WriteArrayStart(length int) {
	e.writeContainerLen(msgpackContainerList, length)
}

func (e *msgpackEncDriver) WriteMapStart(length int) {
	e.writeContainerLen(msgpackContainerMap, length)
}

func (e *msgpackEncDriver) EncodeString(s string) {
	var ct msgpackContainerType
	if e.h.WriteExt {
		if e.h.StringToRaw {
			ct = msgpackContainerBin
		} else {
			ct = msgpackContainerStr
		}
	} else {
		ct = msgpackContainerRawLegacy
	}
	e.writeContainerLen(ct, len(s))
	if len(s) > 0 {
		e.e.encWr.writestr(s)
	}
}

func (e *msgpackEncDriver) EncodeStringBytesRaw(bs []byte) {
	if bs == nil {
		e.EncodeNil()
		return
	}
	if e.h.WriteExt {
		e.writeContainerLen(msgpackContainerBin, len(bs))
	} else {
		e.writeContainerLen(msgpackContainerRawLegacy, len(bs))
	}
	if len(bs) > 0 {
		e.e.encWr.writeb(bs)
	}
}

func (e *msgpackEncDriver) writeContainerLen(ct msgpackContainerType, l int) {
	if ct.fixCutoff > 0 && l < int(ct.fixCutoff) {
		e.e.encWr.writen1(ct.bFixMin | byte(l))
	} else if ct.b8 > 0 && l < 256 {
		e.e.encWr.writen2(ct.b8, uint8(l))
	} else if l < 65536 {
		e.e.encWr.writen1(ct.b16)
		bigen.writeUint16(e.e.w(), uint16(l))
	} else {
		e.e.encWr.writen1(ct.b32)
		bigen.writeUint32(e.e.w(), uint32(l))
	}
}

//---------------------------------------------

type msgpackDecDriver struct {
	decDriverNoopContainerReader
	decDriverNoopNumberHelper
	h *MsgpackHandle
	bdAndBdread
	_ bool
	noBuiltInTypes
	d Decoder
}

func (d *msgpackDecDriver) decoder() *Decoder {
	return &d.d
}

// Note: This returns either a primitive (int, bool, etc) for non-containers,
// or a containerType, or a specific type denoting nil or extension.
// It is called when a nil interface{} is passed, leaving it up to the DecDriver
// to introspect the stream and decide how best to decode.
// It deciphers the value by looking at the stream first.
func (d *msgpackDecDriver) DecodeNaked() {
	if !d.bdRead {
		d.readNextBd()
	}
	bd := d.bd
	n := d.d.naked()
	var decodeFurther bool

	switch bd {
	case mpNil:
		n.v = valueTypeNil
		d.bdRead = false
	case mpFalse:
		n.v = valueTypeBool
		n.b = false
	case mpTrue:
		n.v = valueTypeBool
		n.b = true

	case mpFloat:
		n.v = valueTypeFloat
		n.f = float64(math.Float32frombits(bigen.Uint32(d.d.decRd.readn4())))
	case mpDouble:
		n.v = valueTypeFloat
		n.f = math.Float64frombits(bigen.Uint64(d.d.decRd.readn8()))

	case mpUint8:
		n.v = valueTypeUint
		n.u = uint64(d.d.decRd.readn1())
	case mpUint16:
		n.v = valueTypeUint
		n.u = uint64(bigen.Uint16(d.d.decRd.readn2()))
	case mpUint32:
		n.v = valueTypeUint
		n.u = uint64(bigen.Uint32(d.d.decRd.readn4()))
	case mpUint64:
		n.v = valueTypeUint
		n.u = uint64(bigen.Uint64(d.d.decRd.readn8()))

	case mpInt8:
		n.v = valueTypeInt
		n.i = int64(int8(d.d.decRd.readn1()))
	case mpInt16:
		n.v = valueTypeInt
		n.i = int64(int16(bigen.Uint16(d.d.decRd.readn2())))
	case mpInt32:
		n.v = valueTypeInt
		n.i = int64(int32(bigen.Uint32(d.d.decRd.readn4())))
	case mpInt64:
		n.v = valueTypeInt
		n.i = int64(int64(bigen.Uint64(d.d.decRd.readn8())))

	default:
		switch {
		case bd >= mpPosFixNumMin && bd <= mpPosFixNumMax:
			// positive fixnum (always signed)
			n.v = valueTypeInt
			n.i = int64(int8(bd))
		case bd >= mpNegFixNumMin && bd <= mpNegFixNumMax:
			// negative fixnum
			n.v = valueTypeInt
			n.i = int64(int8(bd))
		case bd == mpStr8, bd == mpStr16, bd == mpStr32, bd >= mpFixStrMin && bd <= mpFixStrMax:
			d.d.fauxUnionReadRawBytes(d.h.WriteExt)
			// if d.h.WriteExt || d.h.RawToString {
			// 	n.v = valueTypeString
			// 	n.s = d.d.stringZC(d.DecodeStringAsBytes())
			// } else {
			// 	n.v = valueTypeBytes
			// 	n.l = d.DecodeBytes([]byte{})
			// }
		case bd == mpBin8, bd == mpBin16, bd == mpBin32:
			d.d.fauxUnionReadRawBytes(false)
		case bd == mpArray16, bd == mpArray32, bd >= mpFixArrayMin && bd <= mpFixArrayMax:
			n.v = valueTypeArray
			decodeFurther = true
		case bd == mpMap16, bd == mpMap32, bd >= mpFixMapMin && bd <= mpFixMapMax:
			n.v = valueTypeMap
			decodeFurther = true
		case bd >= mpFixExt1 && bd <= mpFixExt16, bd >= mpExt8 && bd <= mpExt32:
			n.v = valueTypeExt
			clen := d.readExtLen()
			n.u = uint64(d.d.decRd.readn1())
			if n.u == uint64(mpTimeExtTagU) {
				n.v = valueTypeTime
				n.t = d.decodeTime(clen)
			} else if d.d.bytes {
				n.l = d.d.decRd.rb.readx(uint(clen))
			} else {
				n.l = decByteSlice(d.d.r(), clen, d.d.h.MaxInitLen, d.d.b[:])
			}
		default:
			d.d.errorf("cannot infer value: %s: Ox%x/%d/%s", msgBadDesc, bd, bd, mpdesc(bd))
		}
	}
	if !decodeFurther {
		d.bdRead = false
	}
	if n.v == valueTypeUint && d.h.SignedInteger {
		n.v = valueTypeInt
		n.i = int64(n.u)
	}
}

func (d *msgpackDecDriver) nextValueBytes(v0 []byte) (v []byte) {
	if !d.bdRead {
		d.readNextBd()
	}
	v = v0
	var h = decNextValueBytesHelper{d: &d.d}
	var cursor = d.d.rb.c - 1
	h.append1(&v, d.bd)
	v = d.nextValueBytesBdReadR(v)
	d.bdRead = false
	h.bytesRdV(&v, cursor)
	return
}

func (d *msgpackDecDriver) nextValueBytesR(v0 []byte) (v []byte) {
	d.readNextBd()
	v = v0
	var h = decNextValueBytesHelper{d: &d.d}
	h.append1(&v, d.bd)
	return d.nextValueBytesBdReadR(v)
}

func (d *msgpackDecDriver) nextValueBytesBdReadR(v0 []byte) (v []byte) {
	v = v0
	var h = decNextValueBytesHelper{d: &d.d}

	bd := d.bd

	var clen uint

	switch bd {
	case mpNil, mpFalse, mpTrue: // pass
	case mpUint8, mpInt8:
		h.append1(&v, d.d.decRd.readn1())
	case mpUint16, mpInt16:
		h.appendN(&v, d.d.decRd.readx(2)...)
	case mpFloat, mpUint32, mpInt32:
		h.appendN(&v, d.d.decRd.readx(4)...)
	case mpDouble, mpUint64, mpInt64:
		h.appendN(&v, d.d.decRd.readx(8)...)
	case mpStr8, mpBin8:
		clen = uint(d.d.decRd.readn1())
		h.append1(&v, byte(clen))
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case mpStr16, mpBin16:
		x := d.d.decRd.readn2()
		h.appendN(&v, x[:]...)
		clen = uint(bigen.Uint16(x))
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case mpStr32, mpBin32:
		x := d.d.decRd.readn4()
		h.appendN(&v, x[:]...)
		clen = uint(bigen.Uint32(x))
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case mpFixExt1:
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.append1(&v, d.d.decRd.readn1())
	case mpFixExt2:
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(2)...)
	case mpFixExt4:
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(4)...)
	case mpFixExt8:
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(8)...)
	case mpFixExt16:
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(16)...)
	case mpExt8:
		clen = uint(d.d.decRd.readn1())
		h.append1(&v, uint8(clen))
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case mpExt16:
		x := d.d.decRd.readn2()
		clen = uint(bigen.Uint16(x))
		h.appendN(&v, x[:]...)
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case mpExt32:
		x := d.d.decRd.readn4()
		clen = uint(bigen.Uint32(x))
		h.appendN(&v, x[:]...)
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case mpArray16:
		x := d.d.decRd.readn2()
		clen = uint(bigen.Uint16(x))
		h.appendN(&v, x[:]...)
		for i := uint(0); i < clen; i++ {
			v = d.nextValueBytesR(v)
		}
	case mpArray32:
		x := d.d.decRd.readn4()
		clen = uint(bigen.Uint32(x))
		h.appendN(&v, x[:]...)
		for i := uint(0); i < clen; i++ {
			v = d.nextValueBytesR(v)
		}
	case mpMap16:
		x := d.d.decRd.readn2()
		clen = uint(bigen.Uint16(x))
		h.appendN(&v, x[:]...)
		for i := uint(0); i < clen; i++ {
			v = d.nextValueBytesR(v)
			v = d.nextValueBytesR(v)
		}
	case mpMap32:
		x := d.d.decRd.readn4()
		clen = uint(bigen.Uint32(x))
		h.appendN(&v, x[:]...)
		for i := uint(0); i < clen; i++ {
			v = d.nextValueBytesR(v)
			v = d.nextValueBytesR(v)
		}
	default:
		switch {
		case bd >= mpPosFixNumMin && bd <= mpPosFixNumMax: // pass
		case bd >= mpNegFixNumMin && bd <= mpNegFixNumMax: // pass
		case bd >= mpFixStrMin && bd <= mpFixStrMax:
			clen = uint(mpFixStrMin ^ bd)
			h.appendN(&v, d.d.decRd.readx(clen)...)
		case bd >= mpFixArrayMin && bd <= mpFixArrayMax:
			clen = uint(mpFixArrayMin ^ bd)
			for i := uint(0); i < clen; i++ {
				v = d.nextValueBytesR(v)
			}
		case bd >= mpFixMapMin && bd <= mpFixMapMax:
			clen = uint(mpFixMapMin ^ bd)
			for i := uint(0); i < clen; i++ {
				v = d.nextValueBytesR(v)
				v = d.nextValueBytesR(v)
			}
		default:
			d.d.errorf("nextValueBytes: cannot infer value: %s: Ox%x/%d/%s", msgBadDesc, bd, bd, mpdesc(bd))
		}
	}
	return
}

func (d *msgpackDecDriver) decFloat4Int32() (f float32) {
	fbits := bigen.Uint32(d.d.decRd.readn4())
	f = math.Float32frombits(fbits)
	if !noFrac32(fbits) {
		d.d.errorf("assigning integer value from float32 with a fraction: %v", f)
	}
	return
}

func (d *msgpackDecDriver) decFloat4Int64() (f float64) {
	fbits := bigen.Uint64(d.d.decRd.readn8())
	f = math.Float64frombits(fbits)
	if !noFrac64(fbits) {
		d.d.errorf("assigning integer value from float64 with a fraction: %v", f)
	}
	return
}

// int can be decoded from msgpack type: intXXX or uintXXX
func (d *msgpackDecDriver) DecodeInt64() (i int64) {
	if d.advanceNil() {
		return
	}
	switch d.bd {
	case mpUint8:
		i = int64(uint64(d.d.decRd.readn1()))
	case mpUint16:
		i = int64(uint64(bigen.Uint16(d.d.decRd.readn2())))
	case mpUint32:
		i = int64(uint64(bigen.Uint32(d.d.decRd.readn4())))
	case mpUint64:
		i = int64(bigen.Uint64(d.d.decRd.readn8()))
	case mpInt8:
		i = int64(int8(d.d.decRd.readn1()))
	case mpInt16:
		i = int64(int16(bigen.Uint16(d.d.decRd.readn2())))
	case mpInt32:
		i = int64(int32(bigen.Uint32(d.d.decRd.readn4())))
	case mpInt64:
		i = int64(bigen.Uint64(d.d.decRd.readn8()))
	case mpFloat:
		i = int64(d.decFloat4Int32())
	case mpDouble:
		i = int64(d.decFloat4Int64())
	default:
		switch {
		case d.bd >= mpPosFixNumMin && d.bd <= mpPosFixNumMax:
			i = int64(int8(d.bd))
		case d.bd >= mpNegFixNumMin && d.bd <= mpNegFixNumMax:
			i = int64(int8(d.bd))
		default:
			d.d.errorf("cannot decode signed integer: %s: %x/%s", msgBadDesc, d.bd, mpdesc(d.bd))
		}
	}
	d.bdRead = false
	return
}

// uint can be decoded from msgpack type: intXXX or uintXXX
func (d *msgpackDecDriver) DecodeUint64() (ui uint64) {
	if d.advanceNil() {
		return
	}
	switch d.bd {
	case mpUint8:
		ui = uint64(d.d.decRd.readn1())
	case mpUint16:
		ui = uint64(bigen.Uint16(d.d.decRd.readn2()))
	case mpUint32:
		ui = uint64(bigen.Uint32(d.d.decRd.readn4()))
	case mpUint64:
		ui = bigen.Uint64(d.d.decRd.readn8())
	case mpInt8:
		if i := int64(int8(d.d.decRd.readn1())); i >= 0 {
			ui = uint64(i)
		} else {
			d.d.errorf("assigning negative signed value: %v, to unsigned type", i)
		}
	case mpInt16:
		if i := int64(int16(bigen.Uint16(d.d.decRd.readn2()))); i >= 0 {
			ui = uint64(i)
		} else {
			d.d.errorf("assigning negative signed value: %v, to unsigned type", i)
		}
	case mpInt32:
		if i := int64(int32(bigen.Uint32(d.d.decRd.readn4()))); i >= 0 {
			ui = uint64(i)
		} else {
			d.d.errorf("assigning negative signed value: %v, to unsigned type", i)
		}
	case mpInt64:
		if i := int64(bigen.Uint64(d.d.decRd.readn8())); i >= 0 {
			ui = uint64(i)
		} else {
			d.d.errorf("assigning negative signed value: %v, to unsigned type", i)
		}
	case mpFloat:
		if f := d.decFloat4Int32(); f >= 0 {
			ui = uint64(f)
		} else {
			d.d.errorf("assigning negative float value: %v, to unsigned type", f)
		}
	case mpDouble:
		if f := d.decFloat4Int64(); f >= 0 {
			ui = uint64(f)
		} else {
			d.d.errorf("assigning negative float value: %v, to unsigned type", f)
		}
	default:
		switch {
		case d.bd >= mpPosFixNumMin && d.bd <= mpPosFixNumMax:
			ui = uint64(d.bd)
		case d.bd >= mpNegFixNumMin && d.bd <= mpNegFixNumMax:
			d.d.errorf("assigning negative signed value: %v, to unsigned type", int(d.bd))
		default:
			d.d.errorf("cannot decode unsigned integer: %s: %x/%s", msgBadDesc, d.bd, mpdesc(d.bd))
		}
	}
	d.bdRead = false
	return
}

// float can either be decoded from msgpack type: float, double or intX
func (d *msgpackDecDriver) DecodeFloat64() (f float64) {
	if d.advanceNil() {
		return
	}
	if d.bd == mpFloat {
		f = float64(math.Float32frombits(bigen.Uint32(d.d.decRd.readn4())))
	} else if d.bd == mpDouble {
		f = math.Float64frombits(bigen.Uint64(d.d.decRd.readn8()))
	} else {
		f = float64(d.DecodeInt64())
	}
	d.bdRead = false
	return
}

// bool can be decoded from bool, fixnum 0 or 1.
func (d *msgpackDecDriver) DecodeBool() (b bool) {
	if d.advanceNil() {
		return
	}
	if d.bd == mpFalse || d.bd == 0 {
		// b = false
	} else if d.bd == mpTrue || d.bd == 1 {
		b = true
	} else {
		d.d.errorf("cannot decode bool: %s: %x/%s", msgBadDesc, d.bd, mpdesc(d.bd))
	}
	d.bdRead = false
	return
}

func (d *msgpackDecDriver) DecodeBytes(bs []byte) (bsOut []byte) {
	d.d.decByteState = decByteStateNone
	if d.advanceNil() {
		return
	}

	bd := d.bd
	var clen int
	if bd == mpBin8 || bd == mpBin16 || bd == mpBin32 {
		clen = d.readContainerLen(msgpackContainerBin) // binary
	} else if bd == mpStr8 || bd == mpStr16 || bd == mpStr32 ||
		(bd >= mpFixStrMin && bd <= mpFixStrMax) {
		clen = d.readContainerLen(msgpackContainerStr) // string/raw
	} else if bd == mpArray16 || bd == mpArray32 ||
		(bd >= mpFixArrayMin && bd <= mpFixArrayMax) {
		// check if an "array" of uint8's
		if bs == nil {
			d.d.decByteState = decByteStateReuseBuf
			bs = d.d.b[:]
		}
		// bsOut, _ = fastpathTV.DecSliceUint8V(bs, true, d.d)
		slen := d.ReadArrayStart()
		var changed bool
		if bs, changed = usableByteSlice(bs, slen); changed {
			d.d.decByteState = decByteStateNone
		}
		for i := 0; i < len(bs); i++ {
			bs[i] = uint8(chkOvf.UintV(d.DecodeUint64(), 8))
		}
		for i := len(bs); i < slen; i++ {
			bs = append(bs, uint8(chkOvf.UintV(d.DecodeUint64(), 8)))
		}
		return bs
	} else {
		d.d.errorf("invalid byte descriptor for decoding bytes, got: 0x%x", d.bd)
	}

	d.bdRead = false
	if d.d.zerocopy() {
		d.d.decByteState = decByteStateZerocopy
		return d.d.decRd.rb.readx(uint(clen))
	}
	if bs == nil {
		d.d.decByteState = decByteStateReuseBuf
		bs = d.d.b[:]
	}
	return decByteSlice(d.d.r(), clen, d.h.MaxInitLen, bs)
}

func (d *msgpackDecDriver) DecodeStringAsBytes() (s []byte) {
	s = d.DecodeBytes(nil)
	if d.h.ValidateUnicode && !utf8.Valid(s) {
		d.d.errorf("DecodeStringAsBytes: invalid UTF-8: %s", s)
	}
	return
}

func (d *msgpackDecDriver) descBd() string {
	return sprintf("%v (%s)", d.bd, mpdesc(d.bd))
}

func (d *msgpackDecDriver) readNextBd() {
	d.bd = d.d.decRd.readn1()
	d.bdRead = true
}

func (d *msgpackDecDriver) advanceNil() (null bool) {
	if !d.bdRead {
		d.readNextBd()
	}
	if d.bd == mpNil {
		d.bdRead = false
		return true // null = true
	}
	return
}

func (d *msgpackDecDriver) TryNil() (v bool) {
	return d.advanceNil()
}

func (d *msgpackDecDriver) ContainerType() (vt valueType) {
	if !d.bdRead {
		d.readNextBd()
	}
	bd := d.bd
	if bd == mpNil {
		d.bdRead = false
		return valueTypeNil
	} else if bd == mpBin8 || bd == mpBin16 || bd == mpBin32 {
		return valueTypeBytes
	} else if bd == mpStr8 || bd == mpStr16 || bd == mpStr32 ||
		(bd >= mpFixStrMin && bd <= mpFixStrMax) {
		if d.h.WriteExt || d.h.RawToString { // UTF-8 string (new spec)
			return valueTypeString
		}
		return valueTypeBytes // raw (old spec)
	} else if bd == mpArray16 || bd == mpArray32 || (bd >= mpFixArrayMin && bd <= mpFixArrayMax) {
		return valueTypeArray
	} else if bd == mpMap16 || bd == mpMap32 || (bd >= mpFixMapMin && bd <= mpFixMapMax) {
		return valueTypeMap
	}
	return valueTypeUnset
}

func (d *msgpackDecDriver) readContainerLen(ct msgpackContainerType) (clen int) {
	bd := d.bd
	if bd == ct.b8 {
		clen = int(d.d.decRd.readn1())
	} else if bd == ct.b16 {
		clen = int(bigen.Uint16(d.d.decRd.readn2()))
	} else if bd == ct.b32 {
		clen = int(bigen.Uint32(d.d.decRd.readn4()))
	} else if (ct.bFixMin & bd) == ct.bFixMin {
		clen = int(ct.bFixMin ^ bd)
	} else {
		d.d.errorf("cannot read container length: %s: hex: %x, decimal: %d", msgBadDesc, bd, bd)
	}
	d.bdRead = false
	return
}

func (d *msgpackDecDriver) ReadMapStart() int {
	if d.advanceNil() {
		return containerLenNil
	}
	return d.readContainerLen(msgpackContainerMap)
}

func (d *msgpackDecDriver) ReadArrayStart() int {
	if d.advanceNil() {
		return containerLenNil
	}
	return d.readContainerLen(msgpackContainerList)
}

func (d *msgpackDecDriver) readExtLen() (clen int) {
	switch d.bd {
	case mpFixExt1:
		clen = 1
	case mpFixExt2:
		clen = 2
	case mpFixExt4:
		clen = 4
	case mpFixExt8:
		clen = 8
	case mpFixExt16:
		clen = 16
	case mpExt8:
		clen = int(d.d.decRd.readn1())
	case mpExt16:
		clen = int(bigen.Uint16(d.d.decRd.readn2()))
	case mpExt32:
		clen = int(bigen.Uint32(d.d.decRd.readn4()))
	default:
		d.d.errorf("decoding ext bytes: found unexpected byte: %x", d.bd)
	}
	return
}

func (d *msgpackDecDriver) DecodeTime() (t time.Time) {
	// decode time from string bytes or ext
	if d.advanceNil() {
		return
	}
	bd := d.bd
	var clen int
	if bd == mpBin8 || bd == mpBin16 || bd == mpBin32 {
		clen = d.readContainerLen(msgpackContainerBin) // binary
	} else if bd == mpStr8 || bd == mpStr16 || bd == mpStr32 ||
		(bd >= mpFixStrMin && bd <= mpFixStrMax) {
		clen = d.readContainerLen(msgpackContainerStr) // string/raw
	} else {
		// expect to see mpFixExt4,-1 OR mpFixExt8,-1 OR mpExt8,12,-1
		d.bdRead = false
		b2 := d.d.decRd.readn1()
		if d.bd == mpFixExt4 && b2 == mpTimeExtTagU {
			clen = 4
		} else if d.bd == mpFixExt8 && b2 == mpTimeExtTagU {
			clen = 8
		} else if d.bd == mpExt8 && b2 == 12 && d.d.decRd.readn1() == mpTimeExtTagU {
			clen = 12
		} else {
			d.d.errorf("invalid stream for decoding time as extension: got 0x%x, 0x%x", d.bd, b2)
		}
	}
	return d.decodeTime(clen)
}

func (d *msgpackDecDriver) decodeTime(clen int) (t time.Time) {
	d.bdRead = false
	switch clen {
	case 4:
		t = time.Unix(int64(bigen.Uint32(d.d.decRd.readn4())), 0).UTC()
	case 8:
		tv := bigen.Uint64(d.d.decRd.readn8())
		t = time.Unix(int64(tv&0x00000003ffffffff), int64(tv>>34)).UTC()
	case 12:
		nsec := bigen.Uint32(d.d.decRd.readn4())
		sec := bigen.Uint64(d.d.decRd.readn8())
		t = time.Unix(int64(sec), int64(nsec)).UTC()
	default:
		d.d.errorf("invalid length of bytes for decoding time - expecting 4 or 8 or 12, got %d", clen)
	}
	return
}

func (d *msgpackDecDriver) DecodeExt(rv interface{}, basetype reflect.Type, xtag uint64, ext Ext) {
	if xtag > 0xff {
		d.d.errorf("ext: tag must be <= 0xff; got: %v", xtag)
	}
	if d.advanceNil() {
		return
	}
	xbs, realxtag1, zerocopy := d.decodeExtV(ext != nil, uint8(xtag))
	realxtag := uint64(realxtag1)
	if ext == nil {
		re := rv.(*RawExt)
		re.Tag = realxtag
		re.setData(xbs, zerocopy)
	} else if ext == SelfExt {
		d.d.sideDecode(rv, basetype, xbs)
	} else {
		ext.ReadExt(rv, xbs)
	}
}

func (d *msgpackDecDriver) decodeExtV(verifyTag bool, tag byte) (xbs []byte, xtag byte, zerocopy bool) {
	xbd := d.bd
	if xbd == mpBin8 || xbd == mpBin16 || xbd == mpBin32 {
		xbs = d.DecodeBytes(nil)
	} else if xbd == mpStr8 || xbd == mpStr16 || xbd == mpStr32 ||
		(xbd >= mpFixStrMin && xbd <= mpFixStrMax) {
		xbs = d.DecodeStringAsBytes()
	} else {
		clen := d.readExtLen()
		xtag = d.d.decRd.readn1()
		if verifyTag && xtag != tag {
			d.d.errorf("wrong extension tag - got %b, expecting %v", xtag, tag)
		}
		if d.d.bytes {
			xbs = d.d.decRd.rb.readx(uint(clen))
			zerocopy = true
		} else {
			xbs = decByteSlice(d.d.r(), clen, d.d.h.MaxInitLen, d.d.b[:])
		}
	}
	d.bdRead = false
	return
}

//--------------------------------------------------

// MsgpackHandle is a Handle for the Msgpack Schema-Free Encoding Format.
type MsgpackHandle struct {
	binaryEncodingType
	BasicHandle

	// NoFixedNum says to output all signed integers as 2-bytes, never as 1-byte fixednum.
	NoFixedNum bool

	// WriteExt controls whether the new spec is honored.
	//
	// With WriteExt=true, we can encode configured extensions with extension tags
	// and encode string/[]byte/extensions in a way compatible with the new spec
	// but incompatible with the old spec.
	//
	// For compatibility with the old spec, set WriteExt=false.
	//
	// With WriteExt=false:
	//    configured extensions are serialized as raw bytes (not msgpack extensions).
	//    reserved byte descriptors like Str8 and those enabling the new msgpack Binary type
	//    are not encoded.
	WriteExt bool

	// PositiveIntUnsigned says to encode positive integers as unsigned.
	PositiveIntUnsigned bool
}

// Name returns the name of the handle: msgpack
func (h *MsgpackHandle) Name() string { return "msgpack" }

func (h *MsgpackHandle) desc(bd byte) string { return mpdesc(bd) }

func (h *MsgpackHandle) newEncDriver() encDriver {
	var e = &msgpackEncDriver{h: h}
	e.e.e = e
	e.e.init(h)
	e.reset()
	return e
}

func (h *MsgpackHandle) newDecDriver() decDriver {
	d := &msgpackDecDriver{h: h}
	d.d.d = d
	d.d.init(h)
	d.reset()
	return d
}

//--------------------------------------------------

type msgpackSpecRpcCodec struct {
	rpcCodec
}

// /////////////// Spec RPC Codec ///////////////////
func (c *msgpackSpecRpcCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	// WriteRequest can write to both a Go service, and other services that do
	// not abide by the 1 argument rule of a Go service.
	// We discriminate based on if the body is a MsgpackSpecRpcMultiArgs
	var bodyArr []interface{}
	if m, ok := body.(MsgpackSpecRpcMultiArgs); ok {
		bodyArr = ([]interface{})(m)
	} else {
		bodyArr = []interface{}{body}
	}
	r2 := []interface{}{0, uint32(r.Seq), r.ServiceMethod, bodyArr}
	return c.write(r2)
}

func (c *msgpackSpecRpcCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	var moe interface{}
	if r.Error != "" {
		moe = r.Error
	}
	if moe != nil && body != nil {
		body = nil
	}
	r2 := []interface{}{1, uint32(r.Seq), moe, body}
	return c.write(r2)
}

func (c *msgpackSpecRpcCodec) ReadResponseHeader(r *rpc.Response) error {
	return c.parseCustomHeader(1, &r.Seq, &r.Error)
}

func (c *msgpackSpecRpcCodec) ReadRequestHeader(r *rpc.Request) error {
	return c.parseCustomHeader(0, &r.Seq, &r.ServiceMethod)
}

func (c *msgpackSpecRpcCodec) ReadRequestBody(body interface{}) error {
	if body == nil { // read and discard
		return c.read(nil)
	}
	bodyArr := []interface{}{body}
	return c.read(&bodyArr)
}

func (c *msgpackSpecRpcCodec) parseCustomHeader(expectTypeByte byte, msgid *uint64, methodOrError *string) (err error) {
	if cls := c.cls.load(); cls.closed {
		return io.EOF
	}

	// We read the response header by hand
	// so that the body can be decoded on its own from the stream at a later time.

	const fia byte = 0x94 //four item array descriptor value

	var ba [1]byte
	var n int
	for {
		n, err = c.r.Read(ba[:])
		if err != nil {
			return
		}
		if n == 1 {
			break
		}
	}

	var b = ba[0]
	if b != fia {
		err = fmt.Errorf("not array - %s %x/%s", msgBadDesc, b, mpdesc(b))
	} else {
		err = c.read(&b)
		if err == nil {
			if b != expectTypeByte {
				err = fmt.Errorf("%s - expecting %v but got %x/%s", msgBadDesc, expectTypeByte, b, mpdesc(b))
			} else {
				err = c.read(msgid)
				if err == nil {
					err = c.read(methodOrError)
				}
			}
		}
	}
	return
}

//--------------------------------------------------

// msgpackSpecRpc is the implementation of Rpc that uses custom communication protocol
// as defined in the msgpack spec at https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md
type msgpackSpecRpc struct{}

// MsgpackSpecRpc implements Rpc using the communication protocol defined in
// the msgpack spec at https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md .
//
// See GoRpc documentation, for information on buffering for better performance.
var MsgpackSpecRpc msgpackSpecRpc

func (x msgpackSpecRpc) ServerCodec(conn io.ReadWriteCloser, h Handle) rpc.ServerCodec {
	return &msgpackSpecRpcCodec{newRPCCodec(conn, h)}
}

func (x msgpackSpecRpc) ClientCodec(conn io.ReadWriteCloser, h Handle) rpc.ClientCodec {
	return &msgpackSpecRpcCodec{newRPCCodec(conn, h)}
}

var _ decDriver = (*msgpackDecDriver)(nil)
var _ encDriver = (*msgpackEncDriver)(nil)
