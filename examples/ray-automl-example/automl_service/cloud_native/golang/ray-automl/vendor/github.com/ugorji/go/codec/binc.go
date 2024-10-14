// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

import (
	"math"
	"reflect"
	"time"
	"unicode/utf8"
)

// Symbol management:
// - symbols are stored in a symbol map during encoding and decoding.
// - the symbols persist until the (En|De)coder ResetXXX method is called.

const bincDoPrune = true

// vd as low 4 bits (there are 16 slots)
const (
	bincVdSpecial byte = iota
	bincVdPosInt
	bincVdNegInt
	bincVdFloat

	bincVdString
	bincVdByteArray
	bincVdArray
	bincVdMap

	bincVdTimestamp
	bincVdSmallInt
	_ // bincVdUnicodeOther
	bincVdSymbol

	_               // bincVdDecimal
	_               // open slot
	_               // open slot
	bincVdCustomExt = 0x0f
)

const (
	bincSpNil byte = iota
	bincSpFalse
	bincSpTrue
	bincSpNan
	bincSpPosInf
	bincSpNegInf
	bincSpZeroFloat
	bincSpZero
	bincSpNegOne
)

const (
	_ byte = iota // bincFlBin16
	bincFlBin32
	_ // bincFlBin32e
	bincFlBin64
	_ // bincFlBin64e
	// others not currently supported
)

const bincBdNil = 0 // bincVdSpecial<<4 | bincSpNil // staticcheck barfs on this (SA4016)

var (
	bincdescSpecialVsNames = map[byte]string{
		bincSpNil:       "nil",
		bincSpFalse:     "false",
		bincSpTrue:      "true",
		bincSpNan:       "float",
		bincSpPosInf:    "float",
		bincSpNegInf:    "float",
		bincSpZeroFloat: "float",
		bincSpZero:      "uint",
		bincSpNegOne:    "int",
	}
	bincdescVdNames = map[byte]string{
		bincVdSpecial:   "special",
		bincVdSmallInt:  "uint",
		bincVdPosInt:    "uint",
		bincVdFloat:     "float",
		bincVdSymbol:    "string",
		bincVdString:    "string",
		bincVdByteArray: "bytes",
		bincVdTimestamp: "time",
		bincVdCustomExt: "ext",
		bincVdArray:     "array",
		bincVdMap:       "map",
	}
)

func bincdescbd(bd byte) (s string) {
	return bincdesc(bd>>4, bd&0x0f)
}

func bincdesc(vd, vs byte) (s string) {
	if vd == bincVdSpecial {
		s = bincdescSpecialVsNames[vs]
	} else {
		s = bincdescVdNames[vd]
	}
	if s == "" {
		s = "unknown"
	}
	return
}

type bincEncState struct {
	m map[string]uint16 // symbols
}

func (e bincEncState) captureState() interface{}   { return e.m }
func (e *bincEncState) resetState()                { e.m = nil }
func (e *bincEncState) reset()                     { e.resetState() }
func (e *bincEncState) restoreState(v interface{}) { e.m = v.(map[string]uint16) }

type bincEncDriver struct {
	noBuiltInTypes
	encDriverNoopContainerWriter
	h *BincHandle
	bincEncState

	e Encoder
}

func (e *bincEncDriver) encoder() *Encoder {
	return &e.e
}

func (e *bincEncDriver) EncodeNil() {
	e.e.encWr.writen1(bincBdNil)
}

func (e *bincEncDriver) EncodeTime(t time.Time) {
	if t.IsZero() {
		e.EncodeNil()
	} else {
		bs := bincEncodeTime(t)
		e.e.encWr.writen1(bincVdTimestamp<<4 | uint8(len(bs)))
		e.e.encWr.writeb(bs)
	}
}

func (e *bincEncDriver) EncodeBool(b bool) {
	if b {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpTrue)
	} else {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpFalse)
	}
}

func (e *bincEncDriver) encSpFloat(f float64) (done bool) {
	if f == 0 {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpZeroFloat)
	} else if math.IsNaN(float64(f)) {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpNan)
	} else if math.IsInf(float64(f), +1) {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpPosInf)
	} else if math.IsInf(float64(f), -1) {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpNegInf)
	} else {
		return
	}
	return true
}

func (e *bincEncDriver) EncodeFloat32(f float32) {
	if !e.encSpFloat(float64(f)) {
		e.e.encWr.writen1(bincVdFloat<<4 | bincFlBin32)
		bigen.writeUint32(e.e.w(), math.Float32bits(f))
	}
}

func (e *bincEncDriver) EncodeFloat64(f float64) {
	if e.encSpFloat(f) {
		return
	}
	b := bigen.PutUint64(math.Float64bits(f))
	if bincDoPrune {
		i := 7
		for ; i >= 0 && (b[i] == 0); i-- {
		}
		i++
		if i <= 6 {
			e.e.encWr.writen1(bincVdFloat<<4 | 0x8 | bincFlBin64)
			e.e.encWr.writen1(byte(i))
			e.e.encWr.writeb(b[:i])
			return
		}
	}
	e.e.encWr.writen1(bincVdFloat<<4 | bincFlBin64)
	e.e.encWr.writen8(b)
}

func (e *bincEncDriver) encIntegerPrune32(bd byte, pos bool, v uint64) {
	b := bigen.PutUint32(uint32(v))
	if bincDoPrune {
		i := byte(pruneSignExt(b[:], pos))
		e.e.encWr.writen1(bd | 3 - i)
		e.e.encWr.writeb(b[i:])
	} else {
		e.e.encWr.writen1(bd | 3)
		e.e.encWr.writen4(b)
	}
}

func (e *bincEncDriver) encIntegerPrune64(bd byte, pos bool, v uint64) {
	b := bigen.PutUint64(v)
	if bincDoPrune {
		i := byte(pruneSignExt(b[:], pos))
		e.e.encWr.writen1(bd | 7 - i)
		e.e.encWr.writeb(b[i:])
	} else {
		e.e.encWr.writen1(bd | 7)
		e.e.encWr.writen8(b)
	}
}

func (e *bincEncDriver) EncodeInt(v int64) {
	if v >= 0 {
		e.encUint(bincVdPosInt<<4, true, uint64(v))
	} else if v == -1 {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpNegOne)
	} else {
		e.encUint(bincVdNegInt<<4, false, uint64(-v))
	}
}

func (e *bincEncDriver) EncodeUint(v uint64) {
	e.encUint(bincVdPosInt<<4, true, v)
}

func (e *bincEncDriver) encUint(bd byte, pos bool, v uint64) {
	if v == 0 {
		e.e.encWr.writen1(bincVdSpecial<<4 | bincSpZero)
	} else if pos && v >= 1 && v <= 16 {
		e.e.encWr.writen1(bincVdSmallInt<<4 | byte(v-1))
	} else if v <= math.MaxUint8 {
		e.e.encWr.writen2(bd|0x0, byte(v))
	} else if v <= math.MaxUint16 {
		e.e.encWr.writen1(bd | 0x01)
		bigen.writeUint16(e.e.w(), uint16(v))
	} else if v <= math.MaxUint32 {
		e.encIntegerPrune32(bd, pos, v)
	} else {
		e.encIntegerPrune64(bd, pos, v)
	}
}

func (e *bincEncDriver) EncodeExt(v interface{}, basetype reflect.Type, xtag uint64, ext Ext) {
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
	e.encodeExtPreamble(uint8(xtag), len(bs))
	e.e.encWr.writeb(bs)
END:
	if ext == SelfExt {
		e.e.blist.put(bs)
		if !byteSliceSameData(bs0, bs) {
			e.e.blist.put(bs0)
		}
	}
}

func (e *bincEncDriver) EncodeRawExt(re *RawExt) {
	e.encodeExtPreamble(uint8(re.Tag), len(re.Data))
	e.e.encWr.writeb(re.Data)
}

func (e *bincEncDriver) encodeExtPreamble(xtag byte, length int) {
	e.encLen(bincVdCustomExt<<4, uint64(length))
	e.e.encWr.writen1(xtag)
}

func (e *bincEncDriver) WriteArrayStart(length int) {
	e.encLen(bincVdArray<<4, uint64(length))
}

func (e *bincEncDriver) WriteMapStart(length int) {
	e.encLen(bincVdMap<<4, uint64(length))
}

func (e *bincEncDriver) EncodeSymbol(v string) {
	//symbols only offer benefit when string length > 1.
	//This is because strings with length 1 take only 2 bytes to store
	//(bd with embedded length, and single byte for string val).

	l := len(v)
	if l == 0 {
		e.encBytesLen(cUTF8, 0)
		return
	} else if l == 1 {
		e.encBytesLen(cUTF8, 1)
		e.e.encWr.writen1(v[0])
		return
	}
	if e.m == nil {
		e.m = make(map[string]uint16, 16)
	}
	ui, ok := e.m[v]
	if ok {
		if ui <= math.MaxUint8 {
			e.e.encWr.writen2(bincVdSymbol<<4, byte(ui))
		} else {
			e.e.encWr.writen1(bincVdSymbol<<4 | 0x8)
			bigen.writeUint16(e.e.w(), ui)
		}
	} else {
		e.e.seq++
		ui = e.e.seq
		e.m[v] = ui
		var lenprec uint8
		if l <= math.MaxUint8 {
			// lenprec = 0
		} else if l <= math.MaxUint16 {
			lenprec = 1
		} else if int64(l) <= math.MaxUint32 {
			lenprec = 2
		} else {
			lenprec = 3
		}
		if ui <= math.MaxUint8 {
			e.e.encWr.writen2(bincVdSymbol<<4|0x0|0x4|lenprec, byte(ui))
		} else {
			e.e.encWr.writen1(bincVdSymbol<<4 | 0x8 | 0x4 | lenprec)
			bigen.writeUint16(e.e.w(), ui)
		}
		if lenprec == 0 {
			e.e.encWr.writen1(byte(l))
		} else if lenprec == 1 {
			bigen.writeUint16(e.e.w(), uint16(l))
		} else if lenprec == 2 {
			bigen.writeUint32(e.e.w(), uint32(l))
		} else {
			bigen.writeUint64(e.e.w(), uint64(l))
		}
		e.e.encWr.writestr(v)
	}
}

func (e *bincEncDriver) EncodeString(v string) {
	if e.h.StringToRaw {
		e.encLen(bincVdByteArray<<4, uint64(len(v)))
		if len(v) > 0 {
			e.e.encWr.writestr(v)
		}
		return
	}
	e.EncodeStringEnc(cUTF8, v)
}

func (e *bincEncDriver) EncodeStringEnc(c charEncoding, v string) {
	if e.e.c == containerMapKey && c == cUTF8 && (e.h.AsSymbols == 1) {
		e.EncodeSymbol(v)
		return
	}
	e.encLen(bincVdString<<4, uint64(len(v)))
	if len(v) > 0 {
		e.e.encWr.writestr(v)
	}
}

func (e *bincEncDriver) EncodeStringBytesRaw(v []byte) {
	if v == nil {
		e.EncodeNil()
		return
	}
	e.encLen(bincVdByteArray<<4, uint64(len(v)))
	if len(v) > 0 {
		e.e.encWr.writeb(v)
	}
}

func (e *bincEncDriver) encBytesLen(c charEncoding, length uint64) {
	// MARKER: we currently only support UTF-8 (string) and RAW (bytearray).
	// We should consider supporting bincUnicodeOther.

	if c == cRAW {
		e.encLen(bincVdByteArray<<4, length)
	} else {
		e.encLen(bincVdString<<4, length)
	}
}

func (e *bincEncDriver) encLen(bd byte, l uint64) {
	if l < 12 {
		e.e.encWr.writen1(bd | uint8(l+4))
	} else {
		e.encLenNumber(bd, l)
	}
}

func (e *bincEncDriver) encLenNumber(bd byte, v uint64) {
	if v <= math.MaxUint8 {
		e.e.encWr.writen2(bd, byte(v))
	} else if v <= math.MaxUint16 {
		e.e.encWr.writen1(bd | 0x01)
		bigen.writeUint16(e.e.w(), uint16(v))
	} else if v <= math.MaxUint32 {
		e.e.encWr.writen1(bd | 0x02)
		bigen.writeUint32(e.e.w(), uint32(v))
	} else {
		e.e.encWr.writen1(bd | 0x03)
		bigen.writeUint64(e.e.w(), uint64(v))
	}
}

//------------------------------------

type bincDecState struct {
	bdRead bool
	bd     byte
	vd     byte
	vs     byte

	_ bool
	// MARKER: consider using binary search here instead of a map (ie bincDecSymbol)
	s map[uint16][]byte
}

func (x bincDecState) captureState() interface{}   { return x }
func (x *bincDecState) resetState()                { *x = bincDecState{} }
func (x *bincDecState) reset()                     { x.resetState() }
func (x *bincDecState) restoreState(v interface{}) { *x = v.(bincDecState) }

type bincDecDriver struct {
	decDriverNoopContainerReader
	decDriverNoopNumberHelper
	noBuiltInTypes

	h *BincHandle

	bincDecState
	d Decoder
}

func (d *bincDecDriver) decoder() *Decoder {
	return &d.d
}

func (d *bincDecDriver) descBd() string {
	return sprintf("%v (%s)", d.bd, bincdescbd(d.bd))
}

func (d *bincDecDriver) readNextBd() {
	d.bd = d.d.decRd.readn1()
	d.vd = d.bd >> 4
	d.vs = d.bd & 0x0f
	d.bdRead = true
}

func (d *bincDecDriver) advanceNil() (null bool) {
	if !d.bdRead {
		d.readNextBd()
	}
	if d.bd == bincBdNil {
		d.bdRead = false
		return true // null = true
	}
	return
}

func (d *bincDecDriver) TryNil() bool {
	return d.advanceNil()
}

func (d *bincDecDriver) ContainerType() (vt valueType) {
	if !d.bdRead {
		d.readNextBd()
	}
	if d.bd == bincBdNil {
		d.bdRead = false
		return valueTypeNil
	} else if d.vd == bincVdByteArray {
		return valueTypeBytes
	} else if d.vd == bincVdString {
		return valueTypeString
	} else if d.vd == bincVdArray {
		return valueTypeArray
	} else if d.vd == bincVdMap {
		return valueTypeMap
	}
	return valueTypeUnset
}

func (d *bincDecDriver) DecodeTime() (t time.Time) {
	if d.advanceNil() {
		return
	}
	if d.vd != bincVdTimestamp {
		d.d.errorf("cannot decode time - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	t, err := bincDecodeTime(d.d.decRd.readx(uint(d.vs)))
	halt.onerror(err)
	d.bdRead = false
	return
}

func (d *bincDecDriver) decFloatPruned(maxlen uint8) {
	l := d.d.decRd.readn1()
	if l > maxlen {
		d.d.errorf("cannot read float - at most %v bytes used to represent float - received %v bytes", maxlen, l)
	}
	for i := l; i < maxlen; i++ {
		d.d.b[i] = 0
	}
	d.d.decRd.readb(d.d.b[0:l])
}

func (d *bincDecDriver) decFloatPre32() (b [4]byte) {
	if d.vs&0x8 == 0 {
		b = d.d.decRd.readn4()
	} else {
		d.decFloatPruned(4)
		copy(b[:], d.d.b[:])
	}
	return
}

func (d *bincDecDriver) decFloatPre64() (b [8]byte) {
	if d.vs&0x8 == 0 {
		b = d.d.decRd.readn8()
	} else {
		d.decFloatPruned(8)
		copy(b[:], d.d.b[:])
	}
	return
}

func (d *bincDecDriver) decFloatVal() (f float64) {
	switch d.vs & 0x7 {
	case bincFlBin32:
		f = float64(math.Float32frombits(bigen.Uint32(d.decFloatPre32())))
	case bincFlBin64:
		f = math.Float64frombits(bigen.Uint64(d.decFloatPre64()))
	default:
		// ok = false
		d.d.errorf("read float supports only float32/64 - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	return
}

func (d *bincDecDriver) decUint() (v uint64) {
	switch d.vs {
	case 0:
		v = uint64(d.d.decRd.readn1())
	case 1:
		v = uint64(bigen.Uint16(d.d.decRd.readn2()))
	case 2:
		b3 := d.d.decRd.readn3()
		var b [4]byte
		copy(b[1:], b3[:])
		v = uint64(bigen.Uint32(b))
	case 3:
		v = uint64(bigen.Uint32(d.d.decRd.readn4()))
	case 4, 5, 6:
		var b [8]byte
		lim := 7 - d.vs
		bs := d.d.b[lim:8]
		d.d.decRd.readb(bs)
		copy(b[lim:], bs)
		v = bigen.Uint64(b)
	case 7:
		v = bigen.Uint64(d.d.decRd.readn8())
	default:
		d.d.errorf("unsigned integers with greater than 64 bits of precision not supported: d.vs: %v %x", d.vs, d.vs)
	}
	return
}

func (d *bincDecDriver) uintBytes() (bs []byte) {
	switch d.vs {
	case 0:
		bs = d.d.b[:1]
		bs[0] = d.d.decRd.readn1()
	case 1:
		bs = d.d.b[:2]
		d.d.decRd.readb(bs)
	case 2:
		bs = d.d.b[:3]
		d.d.decRd.readb(bs)
	case 3:
		bs = d.d.b[:4]
		d.d.decRd.readb(bs)
	case 4, 5, 6:
		lim := 7 - d.vs
		bs = d.d.b[lim:8]
		d.d.decRd.readb(bs)
	case 7:
		bs = d.d.b[:8]
		d.d.decRd.readb(bs)
	default:
		d.d.errorf("unsigned integers with greater than 64 bits of precision not supported: d.vs: %v %x", d.vs, d.vs)
	}
	return
}

func (d *bincDecDriver) decInteger() (ui uint64, neg, ok bool) {
	ok = true
	vd, vs := d.vd, d.vs
	if vd == bincVdPosInt {
		ui = d.decUint()
	} else if vd == bincVdNegInt {
		ui = d.decUint()
		neg = true
	} else if vd == bincVdSmallInt {
		ui = uint64(d.vs) + 1
	} else if vd == bincVdSpecial {
		if vs == bincSpZero {
			// i = 0
		} else if vs == bincSpNegOne {
			neg = true
			ui = 1
		} else {
			ok = false
			// d.d.errorf("integer decode has invalid special value %x-%x/%s", d.vd, d.vs, bincdesc(d.vd, d.vs))
		}
	} else {
		ok = false
		// d.d.errorf("integer can only be decoded from int/uint. d.bd: 0x%x, d.vd: 0x%x", d.bd, d.vd)
	}
	return
}

func (d *bincDecDriver) decFloat() (f float64, ok bool) {
	ok = true
	vd, vs := d.vd, d.vs
	if vd == bincVdSpecial {
		if vs == bincSpNan {
			f = math.NaN()
		} else if vs == bincSpPosInf {
			f = math.Inf(1)
		} else if vs == bincSpZeroFloat || vs == bincSpZero {

		} else if vs == bincSpNegInf {
			f = math.Inf(-1)
		} else {
			ok = false
			// d.d.errorf("float - invalid special value %x-%x/%s", d.vd, d.vs, bincdesc(d.vd, d.vs))
		}
	} else if vd == bincVdFloat {
		f = d.decFloatVal()
	} else {
		ok = false
	}
	return
}

func (d *bincDecDriver) DecodeInt64() (i int64) {
	if d.advanceNil() {
		return
	}
	i = decNegintPosintFloatNumberHelper{&d.d}.int64(d.decInteger())
	d.bdRead = false
	return
}

func (d *bincDecDriver) DecodeUint64() (ui uint64) {
	if d.advanceNil() {
		return
	}
	ui = decNegintPosintFloatNumberHelper{&d.d}.uint64(d.decInteger())
	d.bdRead = false
	return
}

func (d *bincDecDriver) DecodeFloat64() (f float64) {
	if d.advanceNil() {
		return
	}
	f = decNegintPosintFloatNumberHelper{&d.d}.float64(d.decFloat())
	d.bdRead = false
	return
}

func (d *bincDecDriver) DecodeBool() (b bool) {
	if d.advanceNil() {
		return
	}
	if d.bd == (bincVdSpecial | bincSpFalse) {
		// b = false
	} else if d.bd == (bincVdSpecial | bincSpTrue) {
		b = true
	} else {
		d.d.errorf("bool - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	d.bdRead = false
	return
}

func (d *bincDecDriver) ReadMapStart() (length int) {
	if d.advanceNil() {
		return containerLenNil
	}
	if d.vd != bincVdMap {
		d.d.errorf("map - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	length = d.decLen()
	d.bdRead = false
	return
}

func (d *bincDecDriver) ReadArrayStart() (length int) {
	if d.advanceNil() {
		return containerLenNil
	}
	if d.vd != bincVdArray {
		d.d.errorf("array - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	length = d.decLen()
	d.bdRead = false
	return
}

func (d *bincDecDriver) decLen() int {
	if d.vs > 3 {
		return int(d.vs - 4)
	}
	return int(d.decLenNumber())
}

func (d *bincDecDriver) decLenNumber() (v uint64) {
	if x := d.vs; x == 0 {
		v = uint64(d.d.decRd.readn1())
	} else if x == 1 {
		v = uint64(bigen.Uint16(d.d.decRd.readn2()))
	} else if x == 2 {
		v = uint64(bigen.Uint32(d.d.decRd.readn4()))
	} else {
		v = bigen.Uint64(d.d.decRd.readn8())
	}
	return
}

// func (d *bincDecDriver) decStringBytes(bs []byte, zerocopy bool) (bs2 []byte) {
func (d *bincDecDriver) DecodeStringAsBytes() (bs2 []byte) {
	d.d.decByteState = decByteStateNone
	if d.advanceNil() {
		return
	}
	var slen = -1
	switch d.vd {
	case bincVdString, bincVdByteArray:
		slen = d.decLen()
		if d.d.bytes {
			d.d.decByteState = decByteStateZerocopy
			bs2 = d.d.decRd.rb.readx(uint(slen))
		} else {
			d.d.decByteState = decByteStateReuseBuf
			bs2 = decByteSlice(d.d.r(), slen, d.d.h.MaxInitLen, d.d.b[:])
		}
	case bincVdSymbol:
		// zerocopy doesn't apply for symbols,
		// as the values must be stored in a table for later use.
		var symbol uint16
		vs := d.vs
		if vs&0x8 == 0 {
			symbol = uint16(d.d.decRd.readn1())
		} else {
			symbol = uint16(bigen.Uint16(d.d.decRd.readn2()))
		}
		if d.s == nil {
			d.s = make(map[uint16][]byte, 16)
		}

		if vs&0x4 == 0 {
			bs2 = d.s[symbol]
		} else {
			switch vs & 0x3 {
			case 0:
				slen = int(d.d.decRd.readn1())
			case 1:
				slen = int(bigen.Uint16(d.d.decRd.readn2()))
			case 2:
				slen = int(bigen.Uint32(d.d.decRd.readn4()))
			case 3:
				slen = int(bigen.Uint64(d.d.decRd.readn8()))
			}
			// As we are using symbols, do not store any part of
			// the parameter bs in the map, as it might be a shared buffer.
			bs2 = decByteSlice(d.d.r(), slen, d.d.h.MaxInitLen, nil)
			d.s[symbol] = bs2
		}
	default:
		d.d.errorf("string/bytes - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}

	if d.h.ValidateUnicode && !utf8.Valid(bs2) {
		d.d.errorf("DecodeStringAsBytes: invalid UTF-8: %s", bs2)
	}

	d.bdRead = false
	return
}

func (d *bincDecDriver) DecodeBytes(bs []byte) (bsOut []byte) {
	d.d.decByteState = decByteStateNone
	if d.advanceNil() {
		return
	}
	if d.vd == bincVdArray {
		if bs == nil {
			bs = d.d.b[:]
			d.d.decByteState = decByteStateReuseBuf
		}
		slen := d.ReadArrayStart()
		var changed bool
		if bs, changed = usableByteSlice(bs, slen); changed {
			d.d.decByteState = decByteStateNone
		}
		for i := 0; i < slen; i++ {
			bs[i] = uint8(chkOvf.UintV(d.DecodeUint64(), 8))
		}
		for i := len(bs); i < slen; i++ {
			bs = append(bs, uint8(chkOvf.UintV(d.DecodeUint64(), 8)))
		}
		return bs
	}
	var clen int
	if d.vd == bincVdString || d.vd == bincVdByteArray {
		clen = d.decLen()
	} else {
		d.d.errorf("bytes - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	d.bdRead = false
	if d.d.zerocopy() {
		d.d.decByteState = decByteStateZerocopy
		return d.d.decRd.rb.readx(uint(clen))
	}
	if bs == nil {
		bs = d.d.b[:]
		d.d.decByteState = decByteStateReuseBuf
	}
	return decByteSlice(d.d.r(), clen, d.d.h.MaxInitLen, bs)
}

func (d *bincDecDriver) DecodeExt(rv interface{}, basetype reflect.Type, xtag uint64, ext Ext) {
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

func (d *bincDecDriver) decodeExtV(verifyTag bool, tag byte) (xbs []byte, xtag byte, zerocopy bool) {
	if d.vd == bincVdCustomExt {
		l := d.decLen()
		xtag = d.d.decRd.readn1()
		if verifyTag && xtag != tag {
			d.d.errorf("wrong extension tag - got %b, expecting: %v", xtag, tag)
		}
		if d.d.bytes {
			xbs = d.d.decRd.rb.readx(uint(l))
			zerocopy = true
		} else {
			xbs = decByteSlice(d.d.r(), l, d.d.h.MaxInitLen, d.d.b[:])
		}
	} else if d.vd == bincVdByteArray {
		xbs = d.DecodeBytes(nil)
	} else {
		d.d.errorf("ext expects extensions or byte array - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	d.bdRead = false
	return
}

func (d *bincDecDriver) DecodeNaked() {
	if !d.bdRead {
		d.readNextBd()
	}

	n := d.d.naked()
	var decodeFurther bool

	switch d.vd {
	case bincVdSpecial:
		switch d.vs {
		case bincSpNil:
			n.v = valueTypeNil
		case bincSpFalse:
			n.v = valueTypeBool
			n.b = false
		case bincSpTrue:
			n.v = valueTypeBool
			n.b = true
		case bincSpNan:
			n.v = valueTypeFloat
			n.f = math.NaN()
		case bincSpPosInf:
			n.v = valueTypeFloat
			n.f = math.Inf(1)
		case bincSpNegInf:
			n.v = valueTypeFloat
			n.f = math.Inf(-1)
		case bincSpZeroFloat:
			n.v = valueTypeFloat
			n.f = float64(0)
		case bincSpZero:
			n.v = valueTypeUint
			n.u = uint64(0) // int8(0)
		case bincSpNegOne:
			n.v = valueTypeInt
			n.i = int64(-1) // int8(-1)
		default:
			d.d.errorf("cannot infer value - unrecognized special value %x-%x/%s", d.vd, d.vs, bincdesc(d.vd, d.vs))
		}
	case bincVdSmallInt:
		n.v = valueTypeUint
		n.u = uint64(int8(d.vs)) + 1 // int8(d.vs) + 1
	case bincVdPosInt:
		n.v = valueTypeUint
		n.u = d.decUint()
	case bincVdNegInt:
		n.v = valueTypeInt
		n.i = -(int64(d.decUint()))
	case bincVdFloat:
		n.v = valueTypeFloat
		n.f = d.decFloatVal()
	case bincVdString:
		n.v = valueTypeString
		n.s = d.d.stringZC(d.DecodeStringAsBytes())
	case bincVdByteArray:
		d.d.fauxUnionReadRawBytes(false)
	case bincVdSymbol:
		n.v = valueTypeSymbol
		n.s = d.d.stringZC(d.DecodeStringAsBytes())
	case bincVdTimestamp:
		n.v = valueTypeTime
		tt, err := bincDecodeTime(d.d.decRd.readx(uint(d.vs)))
		halt.onerror(err)
		n.t = tt
	case bincVdCustomExt:
		n.v = valueTypeExt
		l := d.decLen()
		n.u = uint64(d.d.decRd.readn1())
		if d.d.bytes {
			n.l = d.d.decRd.rb.readx(uint(l))
		} else {
			n.l = decByteSlice(d.d.r(), l, d.d.h.MaxInitLen, d.d.b[:])
		}
	case bincVdArray:
		n.v = valueTypeArray
		decodeFurther = true
	case bincVdMap:
		n.v = valueTypeMap
		decodeFurther = true
	default:
		d.d.errorf("cannot infer value - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}

	if !decodeFurther {
		d.bdRead = false
	}
	if n.v == valueTypeUint && d.h.SignedInteger {
		n.v = valueTypeInt
		n.i = int64(n.u)
	}
}

func (d *bincDecDriver) nextValueBytes(v0 []byte) (v []byte) {
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

func (d *bincDecDriver) nextValueBytesR(v0 []byte) (v []byte) {
	d.readNextBd()
	v = v0
	var h = decNextValueBytesHelper{d: &d.d}
	h.append1(&v, d.bd)
	return d.nextValueBytesBdReadR(v)
}

func (d *bincDecDriver) nextValueBytesBdReadR(v0 []byte) (v []byte) {
	v = v0
	var h = decNextValueBytesHelper{d: &d.d}

	fnLen := func(vs byte) uint {
		switch vs {
		case 0:
			x := d.d.decRd.readn1()
			h.append1(&v, x)
			return uint(x)
		case 1:
			x := d.d.decRd.readn2()
			h.appendN(&v, x[:]...)
			return uint(bigen.Uint16(x))
		case 2:
			x := d.d.decRd.readn4()
			h.appendN(&v, x[:]...)
			return uint(bigen.Uint32(x))
		case 3:
			x := d.d.decRd.readn8()
			h.appendN(&v, x[:]...)
			return uint(bigen.Uint64(x))
		default:
			return uint(vs - 4)
		}
	}

	var clen uint

	switch d.vd {
	case bincVdSpecial:
		switch d.vs {
		case bincSpNil, bincSpFalse, bincSpTrue, bincSpNan, bincSpPosInf: // pass
		case bincSpNegInf, bincSpZeroFloat, bincSpZero, bincSpNegOne: // pass
		default:
			d.d.errorf("cannot infer value - unrecognized special value %x-%x/%s", d.vd, d.vs, bincdesc(d.vd, d.vs))
		}
	case bincVdSmallInt: // pass
	case bincVdPosInt, bincVdNegInt:
		bs := d.uintBytes()
		h.appendN(&v, bs...)
	case bincVdFloat:
		fn := func(xlen byte) {
			if d.vs&0x8 != 0 {
				xlen = d.d.decRd.readn1()
				h.append1(&v, xlen)
				if xlen > 8 {
					d.d.errorf("cannot read float - at most 8 bytes used to represent float - received %v bytes", xlen)
				}
			}
			d.d.decRd.readb(d.d.b[:xlen])
			h.appendN(&v, d.d.b[:xlen]...)
		}
		switch d.vs & 0x7 {
		case bincFlBin32:
			fn(4)
		case bincFlBin64:
			fn(8)
		default:
			d.d.errorf("read float supports only float32/64 - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
		}
	case bincVdString, bincVdByteArray:
		clen = fnLen(d.vs)
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case bincVdSymbol:
		if d.vs&0x8 == 0 {
			h.append1(&v, d.d.decRd.readn1())
		} else {
			h.appendN(&v, d.d.decRd.rb.readx(2)...)
		}
		if d.vs&0x4 != 0 {
			clen = fnLen(d.vs & 0x3)
			h.appendN(&v, d.d.decRd.readx(clen)...)
		}
	case bincVdTimestamp:
		h.appendN(&v, d.d.decRd.readx(uint(d.vs))...)
	case bincVdCustomExt:
		clen = fnLen(d.vs)
		h.append1(&v, d.d.decRd.readn1()) // tag
		h.appendN(&v, d.d.decRd.readx(clen)...)
	case bincVdArray:
		clen = fnLen(d.vs)
		for i := uint(0); i < clen; i++ {
			v = d.nextValueBytesR(v)
		}
	case bincVdMap:
		clen = fnLen(d.vs)
		for i := uint(0); i < clen; i++ {
			v = d.nextValueBytesR(v)
			v = d.nextValueBytesR(v)
		}
	default:
		d.d.errorf("cannot infer value - %s %x-%x/%s", msgBadDesc, d.vd, d.vs, bincdesc(d.vd, d.vs))
	}
	return
}

//------------------------------------

// BincHandle is a Handle for the Binc Schema-Free Encoding Format
// defined at https://github.com/ugorji/binc .
//
// BincHandle currently supports all Binc features with the following EXCEPTIONS:
//   - only integers up to 64 bits of precision are supported.
//     big integers are unsupported.
//   - Only IEEE 754 binary32 and binary64 floats are supported (ie Go float32 and float64 types).
//     extended precision and decimal IEEE 754 floats are unsupported.
//   - Only UTF-8 strings supported.
//     Unicode_Other Binc types (UTF16, UTF32) are currently unsupported.
//
// Note that these EXCEPTIONS are temporary and full support is possible and may happen soon.
type BincHandle struct {
	BasicHandle
	binaryEncodingType
	// noElemSeparators

	// AsSymbols defines what should be encoded as symbols.
	//
	// Encoding as symbols can reduce the encoded size significantly.
	//
	// However, during decoding, each string to be encoded as a symbol must
	// be checked to see if it has been seen before. Consequently, encoding time
	// will increase if using symbols, because string comparisons has a clear cost.
	//
	// Values:
	// - 0: default: library uses best judgement
	// - 1: use symbols
	// - 2: do not use symbols
	AsSymbols uint8

	// AsSymbols: may later on introduce more options ...
	// - m: map keys
	// - s: struct fields
	// - n: none
	// - a: all: same as m, s, ...

	// _ [7]uint64 // padding (cache-aligned)
}

// Name returns the name of the handle: binc
func (h *BincHandle) Name() string { return "binc" }

func (h *BincHandle) desc(bd byte) string { return bincdesc(bd>>4, bd&0x0f) }

func (h *BincHandle) newEncDriver() encDriver {
	var e = &bincEncDriver{h: h}
	e.e.e = e
	e.e.init(h)
	e.reset()
	return e
}

func (h *BincHandle) newDecDriver() decDriver {
	d := &bincDecDriver{h: h}
	d.d.d = d
	d.d.init(h)
	d.reset()
	return d
}

// var timeDigits = [...]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}

// EncodeTime encodes a time.Time as a []byte, including
// information on the instant in time and UTC offset.
//
// Format Description
//
//	A timestamp is composed of 3 components:
//
//	- secs: signed integer representing seconds since unix epoch
//	- nsces: unsigned integer representing fractional seconds as a
//	  nanosecond offset within secs, in the range 0 <= nsecs < 1e9
//	- tz: signed integer representing timezone offset in minutes east of UTC,
//	  and a dst (daylight savings time) flag
//
//	When encoding a timestamp, the first byte is the descriptor, which
//	defines which components are encoded and how many bytes are used to
//	encode secs and nsecs components. *If secs/nsecs is 0 or tz is UTC, it
//	is not encoded in the byte array explicitly*.
//
//	    Descriptor 8 bits are of the form `A B C DDD EE`:
//	        A:   Is secs component encoded? 1 = true
//	        B:   Is nsecs component encoded? 1 = true
//	        C:   Is tz component encoded? 1 = true
//	        DDD: Number of extra bytes for secs (range 0-7).
//	             If A = 1, secs encoded in DDD+1 bytes.
//	                 If A = 0, secs is not encoded, and is assumed to be 0.
//	                 If A = 1, then we need at least 1 byte to encode secs.
//	                 DDD says the number of extra bytes beyond that 1.
//	                 E.g. if DDD=0, then secs is represented in 1 byte.
//	                      if DDD=2, then secs is represented in 3 bytes.
//	        EE:  Number of extra bytes for nsecs (range 0-3).
//	             If B = 1, nsecs encoded in EE+1 bytes (similar to secs/DDD above)
//
//	Following the descriptor bytes, subsequent bytes are:
//
//	    secs component encoded in `DDD + 1` bytes (if A == 1)
//	    nsecs component encoded in `EE + 1` bytes (if B == 1)
//	    tz component encoded in 2 bytes (if C == 1)
//
//	secs and nsecs components are integers encoded in a BigEndian
//	2-complement encoding format.
//
//	tz component is encoded as 2 bytes (16 bits). Most significant bit 15 to
//	Least significant bit 0 are described below:
//
//	    Timezone offset has a range of -12:00 to +14:00 (ie -720 to +840 minutes).
//	    Bit 15 = have\_dst: set to 1 if we set the dst flag.
//	    Bit 14 = dst\_on: set to 1 if dst is in effect at the time, or 0 if not.
//	    Bits 13..0 = timezone offset in minutes. It is a signed integer in Big Endian format.
func bincEncodeTime(t time.Time) []byte {
	// t := rv2i(rv).(time.Time)
	tsecs, tnsecs := t.Unix(), t.Nanosecond()
	var (
		bd byte
		bs [16]byte
		i  int = 1
	)
	l := t.Location()
	if l == time.UTC {
		l = nil
	}
	if tsecs != 0 {
		bd = bd | 0x80
		btmp := bigen.PutUint64(uint64(tsecs))
		f := pruneSignExt(btmp[:], tsecs >= 0)
		bd = bd | (byte(7-f) << 2)
		copy(bs[i:], btmp[f:])
		i = i + (8 - f)
	}
	if tnsecs != 0 {
		bd = bd | 0x40
		btmp := bigen.PutUint32(uint32(tnsecs))
		f := pruneSignExt(btmp[:4], true)
		bd = bd | byte(3-f)
		copy(bs[i:], btmp[f:4])
		i = i + (4 - f)
	}
	if l != nil {
		bd = bd | 0x20
		// Note that Go Libs do not give access to dst flag.
		_, zoneOffset := t.Zone()
		// zoneName, zoneOffset := t.Zone()
		zoneOffset /= 60
		z := uint16(zoneOffset)
		btmp := bigen.PutUint16(z)
		// clear dst flags
		bs[i] = btmp[0] & 0x3f
		bs[i+1] = btmp[1]
		i = i + 2
	}
	bs[0] = bd
	return bs[0:i]
}

// bincDecodeTime decodes a []byte into a time.Time.
func bincDecodeTime(bs []byte) (tt time.Time, err error) {
	bd := bs[0]
	var (
		tsec  int64
		tnsec uint32
		tz    uint16
		i     byte = 1
		i2    byte
		n     byte
	)
	if bd&(1<<7) != 0 {
		var btmp [8]byte
		n = ((bd >> 2) & 0x7) + 1
		i2 = i + n
		copy(btmp[8-n:], bs[i:i2])
		// if first bit of bs[i] is set, then fill btmp[0..8-n] with 0xff (ie sign extend it)
		if bs[i]&(1<<7) != 0 {
			copy(btmp[0:8-n], bsAll0xff)
		}
		i = i2
		tsec = int64(bigen.Uint64(btmp))
	}
	if bd&(1<<6) != 0 {
		var btmp [4]byte
		n = (bd & 0x3) + 1
		i2 = i + n
		copy(btmp[4-n:], bs[i:i2])
		i = i2
		tnsec = bigen.Uint32(btmp)
	}
	if bd&(1<<5) == 0 {
		tt = time.Unix(tsec, int64(tnsec)).UTC()
		return
	}
	// In stdlib time.Parse, when a date is parsed without a zone name, it uses "" as zone name.
	// However, we need name here, so it can be shown when time is printf.d.
	// Zone name is in form: UTC-08:00.
	// Note that Go Libs do not give access to dst flag, so we ignore dst bits

	tz = bigen.Uint16([2]byte{bs[i], bs[i+1]})
	// sign extend sign bit into top 2 MSB (which were dst bits):
	if tz&(1<<13) == 0 { // positive
		tz = tz & 0x3fff //clear 2 MSBs: dst bits
	} else { // negative
		tz = tz | 0xc000 //set 2 MSBs: dst bits
	}
	tzint := int16(tz)
	if tzint == 0 {
		tt = time.Unix(tsec, int64(tnsec)).UTC()
	} else {
		// For Go Time, do not use a descriptive timezone.
		// It's unnecessary, and makes it harder to do a reflect.DeepEqual.
		// The Offset already tells what the offset should be, if not on UTC and unknown zone name.
		// var zoneName = timeLocUTCName(tzint)
		tt = time.Unix(tsec, int64(tnsec)).In(time.FixedZone("", int(tzint)*60))
	}
	return
}

var _ decDriver = (*bincDecDriver)(nil)
var _ encDriver = (*bincEncDriver)(nil)
