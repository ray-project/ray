// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

import (
	"math"
	"reflect"
	"time"
	"unicode/utf8"
)

// major
const (
	cborMajorUint byte = iota
	cborMajorNegInt
	cborMajorBytes
	cborMajorString
	cborMajorArray
	cborMajorMap
	cborMajorTag
	cborMajorSimpleOrFloat
)

// simple
const (
	cborBdFalse byte = 0xf4 + iota
	cborBdTrue
	cborBdNil
	cborBdUndefined
	cborBdExt
	cborBdFloat16
	cborBdFloat32
	cborBdFloat64
)

// indefinite
const (
	cborBdIndefiniteBytes  byte = 0x5f
	cborBdIndefiniteString byte = 0x7f
	cborBdIndefiniteArray  byte = 0x9f
	cborBdIndefiniteMap    byte = 0xbf
	cborBdBreak            byte = 0xff
)

// These define some in-stream descriptors for
// manual encoding e.g. when doing explicit indefinite-length
const (
	CborStreamBytes  byte = 0x5f
	CborStreamString byte = 0x7f
	CborStreamArray  byte = 0x9f
	CborStreamMap    byte = 0xbf
	CborStreamBreak  byte = 0xff
)

// base values
const (
	cborBaseUint   byte = 0x00
	cborBaseNegInt byte = 0x20
	cborBaseBytes  byte = 0x40
	cborBaseString byte = 0x60
	cborBaseArray  byte = 0x80
	cborBaseMap    byte = 0xa0
	cborBaseTag    byte = 0xc0
	cborBaseSimple byte = 0xe0
)

// const (
// 	cborSelfDesrTag  byte = 0xd9
// 	cborSelfDesrTag2 byte = 0xd9
// 	cborSelfDesrTag3 byte = 0xf7
// )

var (
	cbordescSimpleNames = map[byte]string{
		cborBdNil:     "nil",
		cborBdFalse:   "false",
		cborBdTrue:    "true",
		cborBdFloat16: "float",
		cborBdFloat32: "float",
		cborBdFloat64: "float",
		cborBdBreak:   "break",
	}
	cbordescIndefNames = map[byte]string{
		cborBdIndefiniteBytes:  "bytes*",
		cborBdIndefiniteString: "string*",
		cborBdIndefiniteArray:  "array*",
		cborBdIndefiniteMap:    "map*",
	}
	cbordescMajorNames = map[byte]string{
		cborMajorUint:          "(u)int",
		cborMajorNegInt:        "int",
		cborMajorBytes:         "bytes",
		cborMajorString:        "string",
		cborMajorArray:         "array",
		cborMajorMap:           "map",
		cborMajorTag:           "tag",
		cborMajorSimpleOrFloat: "simple",
	}
)

func cbordesc(bd byte) (s string) {
	bm := bd >> 5
	if bm == cborMajorSimpleOrFloat {
		s = cbordescSimpleNames[bd]
	} else {
		s = cbordescMajorNames[bm]
		if s == "" {
			s = cbordescIndefNames[bd]
		}
	}
	if s == "" {
		s = "unknown"
	}
	return
}

// -------------------

type cborEncDriver struct {
	noBuiltInTypes
	encDriverNoState
	encDriverNoopContainerWriter
	h *CborHandle

	e Encoder
}

func (e *cborEncDriver) encoder() *Encoder {
	return &e.e
}

func (e *cborEncDriver) EncodeNil() {
	e.e.encWr.writen1(cborBdNil)
}

func (e *cborEncDriver) EncodeBool(b bool) {
	if b {
		e.e.encWr.writen1(cborBdTrue)
	} else {
		e.e.encWr.writen1(cborBdFalse)
	}
}

func (e *cborEncDriver) EncodeFloat32(f float32) {
	b := math.Float32bits(f)
	if e.h.OptimumSize {
		if h := floatToHalfFloatBits(b); halfFloatToFloatBits(h) == b {
			e.e.encWr.writen1(cborBdFloat16)
			bigen.writeUint16(e.e.w(), h)
			return
		}
	}
	e.e.encWr.writen1(cborBdFloat32)
	bigen.writeUint32(e.e.w(), b)
}

func (e *cborEncDriver) EncodeFloat64(f float64) {
	if e.h.OptimumSize {
		if f32 := float32(f); float64(f32) == f {
			e.EncodeFloat32(f32)
			return
		}
	}
	e.e.encWr.writen1(cborBdFloat64)
	bigen.writeUint64(e.e.w(), math.Float64bits(f))
}

func (e *cborEncDriver) encUint(v uint64, bd byte) {
	if v <= 0x17 {
		e.e.encWr.writen1(byte(v) + bd)
	} else if v <= math.MaxUint8 {
		e.e.encWr.writen2(bd+0x18, uint8(v))
	} else if v <= math.MaxUint16 {
		e.e.encWr.writen1(bd + 0x19)
		bigen.writeUint16(e.e.w(), uint16(v))
	} else if v <= math.MaxUint32 {
		e.e.encWr.writen1(bd + 0x1a)
		bigen.writeUint32(e.e.w(), uint32(v))
	} else { // if v <= math.MaxUint64 {
		e.e.encWr.writen1(bd + 0x1b)
		bigen.writeUint64(e.e.w(), v)
	}
}

func (e *cborEncDriver) EncodeInt(v int64) {
	if v < 0 {
		e.encUint(uint64(-1-v), cborBaseNegInt)
	} else {
		e.encUint(uint64(v), cborBaseUint)
	}
}

func (e *cborEncDriver) EncodeUint(v uint64) {
	e.encUint(v, cborBaseUint)
}

func (e *cborEncDriver) encLen(bd byte, length int) {
	e.encUint(uint64(length), bd)
}

func (e *cborEncDriver) EncodeTime(t time.Time) {
	if t.IsZero() {
		e.EncodeNil()
	} else if e.h.TimeRFC3339 {
		e.encUint(0, cborBaseTag)
		e.encStringBytesS(cborBaseString, t.Format(time.RFC3339Nano))
	} else {
		e.encUint(1, cborBaseTag)
		t = t.UTC().Round(time.Microsecond)
		sec, nsec := t.Unix(), uint64(t.Nanosecond())
		if nsec == 0 {
			e.EncodeInt(sec)
		} else {
			e.EncodeFloat64(float64(sec) + float64(nsec)/1e9)
		}
	}
}

func (e *cborEncDriver) EncodeExt(rv interface{}, basetype reflect.Type, xtag uint64, ext Ext) {
	e.encUint(uint64(xtag), cborBaseTag)
	if ext == SelfExt {
		e.e.encodeValue(baseRV(rv), e.h.fnNoExt(basetype))
	} else if v := ext.ConvertExt(rv); v == nil {
		e.EncodeNil()
	} else {
		e.e.encode(v)
	}
}

func (e *cborEncDriver) EncodeRawExt(re *RawExt) {
	e.encUint(uint64(re.Tag), cborBaseTag)
	// only encodes re.Value (never re.Data)
	if re.Value != nil {
		e.e.encode(re.Value)
	} else {
		e.EncodeNil()
	}
}

func (e *cborEncDriver) WriteArrayStart(length int) {
	if e.h.IndefiniteLength {
		e.e.encWr.writen1(cborBdIndefiniteArray)
	} else {
		e.encLen(cborBaseArray, length)
	}
}

func (e *cborEncDriver) WriteMapStart(length int) {
	if e.h.IndefiniteLength {
		e.e.encWr.writen1(cborBdIndefiniteMap)
	} else {
		e.encLen(cborBaseMap, length)
	}
}

func (e *cborEncDriver) WriteMapEnd() {
	if e.h.IndefiniteLength {
		e.e.encWr.writen1(cborBdBreak)
	}
}

func (e *cborEncDriver) WriteArrayEnd() {
	if e.h.IndefiniteLength {
		e.e.encWr.writen1(cborBdBreak)
	}
}

func (e *cborEncDriver) EncodeString(v string) {
	bb := cborBaseString
	if e.h.StringToRaw {
		bb = cborBaseBytes
	}
	e.encStringBytesS(bb, v)
}

func (e *cborEncDriver) EncodeStringBytesRaw(v []byte) {
	if v == nil {
		e.EncodeNil()
	} else {
		e.encStringBytesS(cborBaseBytes, stringView(v))
	}
}

func (e *cborEncDriver) encStringBytesS(bb byte, v string) {
	if e.h.IndefiniteLength {
		if bb == cborBaseBytes {
			e.e.encWr.writen1(cborBdIndefiniteBytes)
		} else {
			e.e.encWr.writen1(cborBdIndefiniteString)
		}
		var vlen uint = uint(len(v))
		blen := vlen / 4
		if blen == 0 {
			blen = 64
		} else if blen > 1024 {
			blen = 1024
		}
		for i := uint(0); i < vlen; {
			var v2 string
			i2 := i + blen
			if i2 >= i && i2 < vlen {
				v2 = v[i:i2]
			} else {
				v2 = v[i:]
			}
			e.encLen(bb, len(v2))
			e.e.encWr.writestr(v2)
			i = i2
		}
		e.e.encWr.writen1(cborBdBreak)
	} else {
		e.encLen(bb, len(v))
		e.e.encWr.writestr(v)
	}
}

// ----------------------

type cborDecDriver struct {
	decDriverNoopContainerReader
	decDriverNoopNumberHelper
	h *CborHandle
	bdAndBdread
	st bool // skip tags
	_  bool // found nil
	noBuiltInTypes
	d Decoder
}

func (d *cborDecDriver) decoder() *Decoder {
	return &d.d
}

func (d *cborDecDriver) descBd() string {
	return sprintf("%v (%s)", d.bd, cbordesc(d.bd))
}

func (d *cborDecDriver) readNextBd() {
	d.bd = d.d.decRd.readn1()
	d.bdRead = true
}

func (d *cborDecDriver) advanceNil() (null bool) {
	if !d.bdRead {
		d.readNextBd()
	}
	if d.bd == cborBdNil || d.bd == cborBdUndefined {
		d.bdRead = false
		return true // null = true
	}
	return
}

func (d *cborDecDriver) TryNil() bool {
	return d.advanceNil()
}

// skipTags is called to skip any tags in the stream.
//
// Since any value can be tagged, then we should call skipTags
// before any value is decoded.
//
// By definition, skipTags should not be called before
// checking for break, or nil or undefined.
func (d *cborDecDriver) skipTags() {
	for d.bd>>5 == cborMajorTag {
		d.decUint()
		d.bd = d.d.decRd.readn1()
	}
}

func (d *cborDecDriver) ContainerType() (vt valueType) {
	if !d.bdRead {
		d.readNextBd()
	}
	if d.st {
		d.skipTags()
	}
	if d.bd == cborBdNil {
		d.bdRead = false // always consume nil after seeing it in container type
		return valueTypeNil
	}
	major := d.bd >> 5
	if major == cborMajorBytes {
		return valueTypeBytes
	} else if major == cborMajorString {
		return valueTypeString
	} else if major == cborMajorArray {
		return valueTypeArray
	} else if major == cborMajorMap {
		return valueTypeMap
	}
	return valueTypeUnset
}

func (d *cborDecDriver) CheckBreak() (v bool) {
	if !d.bdRead {
		d.readNextBd()
	}
	if d.bd == cborBdBreak {
		d.bdRead = false
		v = true
	}
	return
}

func (d *cborDecDriver) decUint() (ui uint64) {
	v := d.bd & 0x1f
	if v <= 0x17 {
		ui = uint64(v)
	} else if v == 0x18 {
		ui = uint64(d.d.decRd.readn1())
	} else if v == 0x19 {
		ui = uint64(bigen.Uint16(d.d.decRd.readn2()))
	} else if v == 0x1a {
		ui = uint64(bigen.Uint32(d.d.decRd.readn4()))
	} else if v == 0x1b {
		ui = uint64(bigen.Uint64(d.d.decRd.readn8()))
	} else {
		d.d.errorf("invalid descriptor decoding uint: %x/%s", d.bd, cbordesc(d.bd))
	}
	return
}

func (d *cborDecDriver) decLen() int {
	return int(d.decUint())
}

func (d *cborDecDriver) decAppendIndefiniteBytes(bs []byte) []byte {
	d.bdRead = false
	for !d.CheckBreak() {
		if major := d.bd >> 5; major != cborMajorBytes && major != cborMajorString {
			d.d.errorf("invalid indefinite string/bytes %x (%s); got major %v, expected %v or %v",
				d.bd, cbordesc(d.bd), major, cborMajorBytes, cborMajorString)
		}
		n := uint(d.decLen())
		oldLen := uint(len(bs))
		newLen := oldLen + n
		if newLen > uint(cap(bs)) {
			bs2 := make([]byte, newLen, 2*uint(cap(bs))+n)
			copy(bs2, bs)
			bs = bs2
		} else {
			bs = bs[:newLen]
		}
		d.d.decRd.readb(bs[oldLen:newLen])
		d.bdRead = false
	}
	d.bdRead = false
	return bs
}

func (d *cborDecDriver) decFloat() (f float64, ok bool) {
	ok = true
	switch d.bd {
	case cborBdFloat16:
		f = float64(math.Float32frombits(halfFloatToFloatBits(bigen.Uint16(d.d.decRd.readn2()))))
	case cborBdFloat32:
		f = float64(math.Float32frombits(bigen.Uint32(d.d.decRd.readn4())))
	case cborBdFloat64:
		f = math.Float64frombits(bigen.Uint64(d.d.decRd.readn8()))
	default:
		ok = false
	}
	return
}

func (d *cborDecDriver) decInteger() (ui uint64, neg, ok bool) {
	ok = true
	switch d.bd >> 5 {
	case cborMajorUint:
		ui = d.decUint()
	case cborMajorNegInt:
		ui = d.decUint()
		neg = true
	default:
		ok = false
	}
	return
}

func (d *cborDecDriver) DecodeInt64() (i int64) {
	if d.advanceNil() {
		return
	}
	if d.st {
		d.skipTags()
	}
	i = decNegintPosintFloatNumberHelper{&d.d}.int64(d.decInteger())
	d.bdRead = false
	return
}

func (d *cborDecDriver) DecodeUint64() (ui uint64) {
	if d.advanceNil() {
		return
	}
	if d.st {
		d.skipTags()
	}
	ui = decNegintPosintFloatNumberHelper{&d.d}.uint64(d.decInteger())
	d.bdRead = false
	return
}

func (d *cborDecDriver) DecodeFloat64() (f float64) {
	if d.advanceNil() {
		return
	}
	if d.st {
		d.skipTags()
	}
	f = decNegintPosintFloatNumberHelper{&d.d}.float64(d.decFloat())
	d.bdRead = false
	return
}

// bool can be decoded from bool only (single byte).
func (d *cborDecDriver) DecodeBool() (b bool) {
	if d.advanceNil() {
		return
	}
	if d.st {
		d.skipTags()
	}
	if d.bd == cborBdTrue {
		b = true
	} else if d.bd == cborBdFalse {
	} else {
		d.d.errorf("not bool - %s %x/%s", msgBadDesc, d.bd, cbordesc(d.bd))
	}
	d.bdRead = false
	return
}

func (d *cborDecDriver) ReadMapStart() (length int) {
	if d.advanceNil() {
		return containerLenNil
	}
	if d.st {
		d.skipTags()
	}
	d.bdRead = false
	if d.bd == cborBdIndefiniteMap {
		return containerLenUnknown
	}
	if d.bd>>5 != cborMajorMap {
		d.d.errorf("error reading map; got major type: %x, expected %x/%s", d.bd>>5, cborMajorMap, cbordesc(d.bd))
	}
	return d.decLen()
}

func (d *cborDecDriver) ReadArrayStart() (length int) {
	if d.advanceNil() {
		return containerLenNil
	}
	if d.st {
		d.skipTags()
	}
	d.bdRead = false
	if d.bd == cborBdIndefiniteArray {
		return containerLenUnknown
	}
	if d.bd>>5 != cborMajorArray {
		d.d.errorf("invalid array; got major type: %x, expect: %x/%s", d.bd>>5, cborMajorArray, cbordesc(d.bd))
	}
	return d.decLen()
}

func (d *cborDecDriver) DecodeBytes(bs []byte) (bsOut []byte) {
	d.d.decByteState = decByteStateNone
	if d.advanceNil() {
		return
	}
	if d.st {
		d.skipTags()
	}
	if d.bd == cborBdIndefiniteBytes || d.bd == cborBdIndefiniteString {
		d.bdRead = false
		if bs == nil {
			d.d.decByteState = decByteStateReuseBuf
			return d.decAppendIndefiniteBytes(d.d.b[:0])
		}
		return d.decAppendIndefiniteBytes(bs[:0])
	}
	if d.bd == cborBdIndefiniteArray {
		d.bdRead = false
		if bs == nil {
			d.d.decByteState = decByteStateReuseBuf
			bs = d.d.b[:0]
		} else {
			bs = bs[:0]
		}
		for !d.CheckBreak() {
			bs = append(bs, uint8(chkOvf.UintV(d.DecodeUint64(), 8)))
		}
		return bs
	}
	if d.bd>>5 == cborMajorArray {
		d.bdRead = false
		if bs == nil {
			d.d.decByteState = decByteStateReuseBuf
			bs = d.d.b[:]
		}
		slen := d.decLen()
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
	}
	clen := d.decLen()
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

func (d *cborDecDriver) DecodeStringAsBytes() (s []byte) {
	s = d.DecodeBytes(nil)
	if d.h.ValidateUnicode && !utf8.Valid(s) {
		d.d.errorf("DecodeStringAsBytes: invalid UTF-8: %s", s)
	}
	return
}

func (d *cborDecDriver) DecodeTime() (t time.Time) {
	if d.advanceNil() {
		return
	}
	if d.bd>>5 != cborMajorTag {
		d.d.errorf("error reading tag; expected major type: %x, got: %x", cborMajorTag, d.bd>>5)
	}
	xtag := d.decUint()
	d.bdRead = false
	return d.decodeTime(xtag)
}

func (d *cborDecDriver) decodeTime(xtag uint64) (t time.Time) {
	switch xtag {
	case 0:
		var err error
		t, err = time.Parse(time.RFC3339, stringView(d.DecodeStringAsBytes()))
		d.d.onerror(err)
	case 1:
		f1, f2 := math.Modf(d.DecodeFloat64())
		t = time.Unix(int64(f1), int64(f2*1e9))
	default:
		d.d.errorf("invalid tag for time.Time - expecting 0 or 1, got 0x%x", xtag)
	}
	t = t.UTC().Round(time.Microsecond)
	return
}

func (d *cborDecDriver) DecodeExt(rv interface{}, basetype reflect.Type, xtag uint64, ext Ext) {
	if d.advanceNil() {
		return
	}
	if d.bd>>5 != cborMajorTag {
		d.d.errorf("error reading tag; expected major type: %x, got: %x", cborMajorTag, d.bd>>5)
	}
	realxtag := d.decUint()
	d.bdRead = false
	if ext == nil {
		re := rv.(*RawExt)
		re.Tag = realxtag
		d.d.decode(&re.Value)
	} else if xtag != realxtag {
		d.d.errorf("Wrong extension tag. Got %b. Expecting: %v", realxtag, xtag)
	} else if ext == SelfExt {
		d.d.decodeValue(baseRV(rv), d.h.fnNoExt(basetype))
	} else {
		d.d.interfaceExtConvertAndDecode(rv, ext)
	}
	d.bdRead = false
}

func (d *cborDecDriver) DecodeNaked() {
	if !d.bdRead {
		d.readNextBd()
	}

	n := d.d.naked()
	var decodeFurther bool

	switch d.bd >> 5 {
	case cborMajorUint:
		if d.h.SignedInteger {
			n.v = valueTypeInt
			n.i = d.DecodeInt64()
		} else {
			n.v = valueTypeUint
			n.u = d.DecodeUint64()
		}
	case cborMajorNegInt:
		n.v = valueTypeInt
		n.i = d.DecodeInt64()
	case cborMajorBytes:
		d.d.fauxUnionReadRawBytes(false)
	case cborMajorString:
		n.v = valueTypeString
		n.s = d.d.stringZC(d.DecodeStringAsBytes())
	case cborMajorArray:
		n.v = valueTypeArray
		decodeFurther = true
	case cborMajorMap:
		n.v = valueTypeMap
		decodeFurther = true
	case cborMajorTag:
		n.v = valueTypeExt
		n.u = d.decUint()
		n.l = nil
		if n.u == 0 || n.u == 1 {
			d.bdRead = false
			n.v = valueTypeTime
			n.t = d.decodeTime(n.u)
		} else if d.st && d.h.getExtForTag(n.u) == nil {
			// d.skipTags() // no need to call this - tags already skipped
			d.bdRead = false
			d.DecodeNaked()
			return // return when done (as true recursive function)
		}
	case cborMajorSimpleOrFloat:
		switch d.bd {
		case cborBdNil, cborBdUndefined:
			n.v = valueTypeNil
		case cborBdFalse:
			n.v = valueTypeBool
			n.b = false
		case cborBdTrue:
			n.v = valueTypeBool
			n.b = true
		case cborBdFloat16, cborBdFloat32, cborBdFloat64:
			n.v = valueTypeFloat
			n.f = d.DecodeFloat64()
		default:
			d.d.errorf("decodeNaked: Unrecognized d.bd: 0x%x", d.bd)
		}
	default: // should never happen
		d.d.errorf("decodeNaked: Unrecognized d.bd: 0x%x", d.bd)
	}
	if !decodeFurther {
		d.bdRead = false
	}
}

func (d *cborDecDriver) uintBytes() (v []byte, ui uint64) {
	// this is only used by nextValueBytes, so it's ok to
	// use readx and bigenstd here.
	switch vv := d.bd & 0x1f; vv {
	case 0x18:
		v = d.d.decRd.readx(1)
		ui = uint64(v[0])
	case 0x19:
		v = d.d.decRd.readx(2)
		ui = uint64(bigenstd.Uint16(v))
	case 0x1a:
		v = d.d.decRd.readx(4)
		ui = uint64(bigenstd.Uint32(v))
	case 0x1b:
		v = d.d.decRd.readx(8)
		ui = uint64(bigenstd.Uint64(v))
	default:
		if vv > 0x1b {
			d.d.errorf("invalid descriptor decoding uint: %x/%s", d.bd, cbordesc(d.bd))
		}
		ui = uint64(vv)
	}
	return
}

func (d *cborDecDriver) nextValueBytes(v0 []byte) (v []byte) {
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

func (d *cborDecDriver) nextValueBytesR(v0 []byte) (v []byte) {
	d.readNextBd()
	v = v0
	var h = decNextValueBytesHelper{d: &d.d}
	h.append1(&v, d.bd)
	return d.nextValueBytesBdReadR(v)
}

func (d *cborDecDriver) nextValueBytesBdReadR(v0 []byte) (v []byte) {
	v = v0
	var h = decNextValueBytesHelper{d: &d.d}

	var bs []byte
	var ui uint64

	switch d.bd >> 5 {
	case cborMajorUint, cborMajorNegInt:
		bs, _ = d.uintBytes()
		h.appendN(&v, bs...)
	case cborMajorString, cborMajorBytes:
		if d.bd == cborBdIndefiniteBytes || d.bd == cborBdIndefiniteString {
			for {
				d.readNextBd()
				h.append1(&v, d.bd)
				if d.bd == cborBdBreak {
					break
				}
				bs, ui = d.uintBytes()
				h.appendN(&v, bs...)
				h.appendN(&v, d.d.decRd.readx(uint(ui))...)
			}
		} else {
			bs, ui = d.uintBytes()
			h.appendN(&v, bs...)
			h.appendN(&v, d.d.decRd.readx(uint(ui))...)
		}
	case cborMajorArray:
		if d.bd == cborBdIndefiniteArray {
			for {
				d.readNextBd()
				h.append1(&v, d.bd)
				if d.bd == cborBdBreak {
					break
				}
				v = d.nextValueBytesBdReadR(v)
			}
		} else {
			bs, ui = d.uintBytes()
			h.appendN(&v, bs...)
			for i := uint64(0); i < ui; i++ {
				v = d.nextValueBytesR(v)
			}
		}
	case cborMajorMap:
		if d.bd == cborBdIndefiniteMap {
			for {
				d.readNextBd()
				h.append1(&v, d.bd)
				if d.bd == cborBdBreak {
					break
				}
				v = d.nextValueBytesBdReadR(v)
				v = d.nextValueBytesR(v)
			}
		} else {
			bs, ui = d.uintBytes()
			h.appendN(&v, bs...)
			for i := uint64(0); i < ui; i++ {
				v = d.nextValueBytesR(v)
				v = d.nextValueBytesR(v)
			}
		}
	case cborMajorTag:
		bs, _ = d.uintBytes()
		h.appendN(&v, bs...)
		v = d.nextValueBytesR(v)
	case cborMajorSimpleOrFloat:
		switch d.bd {
		case cborBdNil, cborBdUndefined, cborBdFalse, cborBdTrue: // pass
		case cborBdFloat16:
			h.appendN(&v, d.d.decRd.readx(2)...)
		case cborBdFloat32:
			h.appendN(&v, d.d.decRd.readx(4)...)
		case cborBdFloat64:
			h.appendN(&v, d.d.decRd.readx(8)...)
		default:
			d.d.errorf("nextValueBytes: Unrecognized d.bd: 0x%x", d.bd)
		}
	default: // should never happen
		d.d.errorf("nextValueBytes: Unrecognized d.bd: 0x%x", d.bd)
	}
	return
}

// -------------------------

// CborHandle is a Handle for the CBOR encoding format,
// defined at http://tools.ietf.org/html/rfc7049 and documented further at http://cbor.io .
//
// CBOR is comprehensively supported, including support for:
//   - indefinite-length arrays/maps/bytes/strings
//   - (extension) tags in range 0..0xffff (0 .. 65535)
//   - half, single and double-precision floats
//   - all numbers (1, 2, 4 and 8-byte signed and unsigned integers)
//   - nil, true, false, ...
//   - arrays and maps, bytes and text strings
//
// None of the optional extensions (with tags) defined in the spec are supported out-of-the-box.
// Users can implement them as needed (using SetExt), including spec-documented ones:
//   - timestamp, BigNum, BigFloat, Decimals,
//   - Encoded Text (e.g. URL, regexp, base64, MIME Message), etc.
type CborHandle struct {
	binaryEncodingType
	// noElemSeparators
	BasicHandle

	// IndefiniteLength=true, means that we encode using indefinitelength
	IndefiniteLength bool

	// TimeRFC3339 says to encode time.Time using RFC3339 format.
	// If unset, we encode time.Time using seconds past epoch.
	TimeRFC3339 bool

	// SkipUnexpectedTags says to skip over any tags for which extensions are
	// not defined. This is in keeping with the cbor spec on "Optional Tagging of Items".
	//
	// Furthermore, this allows the skipping over of the Self Describing Tag 0xd9d9f7.
	SkipUnexpectedTags bool
}

// Name returns the name of the handle: cbor
func (h *CborHandle) Name() string { return "cbor" }

func (h *CborHandle) desc(bd byte) string { return cbordesc(bd) }

func (h *CborHandle) newEncDriver() encDriver {
	var e = &cborEncDriver{h: h}
	e.e.e = e
	e.e.init(h)
	e.reset()
	return e
}

func (h *CborHandle) newDecDriver() decDriver {
	d := &cborDecDriver{h: h, st: h.SkipUnexpectedTags}
	d.d.d = d
	d.d.cbor = true
	d.d.init(h)
	d.reset()
	return d
}

func (d *cborDecDriver) reset() {
	d.bdAndBdread.reset()
	d.st = d.h.SkipUnexpectedTags
}

var _ decDriver = (*cborDecDriver)(nil)
var _ encDriver = (*cborEncDriver)(nil)
