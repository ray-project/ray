// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

import (
	"encoding"
	"errors"
	"io"
	"math"
	"reflect"
	"strconv"
	"time"
)

const msgBadDesc = "unrecognized descriptor byte"

const (
	decDefMaxDepth         = 1024                // maximum depth
	decDefChanCap          = 64                  // should be large, as cap cannot be expanded
	decScratchByteArrayLen = (8 + 2 + 2 + 1) * 8 // around cacheLineSize ie ~64, depending on Decoder size

	// MARKER: massage decScratchByteArrayLen to ensure xxxDecDriver structs fit within cacheLine*N

	// decFailNonEmptyIntf configures whether we error
	// when decoding naked into a non-empty interface.
	//
	// Typically, we cannot decode non-nil stream value into
	// nil interface with methods (e.g. io.Reader).
	// However, in some scenarios, this should be allowed:
	//   - MapType
	//   - SliceType
	//   - Extensions
	//
	// Consequently, we should relax this. Put it behind a const flag for now.
	decFailNonEmptyIntf = false

	// decUseTransient says that we should not use the transient optimization.
	//
	// There's potential for GC corruption or memory overwrites if transient isn't
	// used carefully, so this flag helps turn it off quickly if needed.
	//
	// Use it everywhere needed so we can completely remove unused code blocks.
	decUseTransient = true
)

var (
	errNeedMapOrArrayDecodeToStruct = errors.New("only encoded map or array can decode into struct")
	errCannotDecodeIntoNil          = errors.New("cannot decode into nil")

	errExpandSliceCannotChange = errors.New("expand slice: cannot change")

	errDecoderNotInitialized = errors.New("Decoder not initialized")

	errDecUnreadByteNothingToRead   = errors.New("cannot unread - nothing has been read")
	errDecUnreadByteLastByteNotRead = errors.New("cannot unread - last byte has not been read")
	errDecUnreadByteUnknown         = errors.New("cannot unread - reason unknown")
	errMaxDepthExceeded             = errors.New("maximum decoding depth exceeded")
)

// decByteState tracks where the []byte returned by the last call
// to DecodeBytes or DecodeStringAsByte came from
type decByteState uint8

const (
	decByteStateNone     decByteState = iota
	decByteStateZerocopy              // view into []byte that we are decoding from
	decByteStateReuseBuf              // view into transient buffer used internally by decDriver
	// decByteStateNewAlloc
)

type decNotDecodeableReason uint8

const (
	decNotDecodeableReasonUnknown decNotDecodeableReason = iota
	decNotDecodeableReasonBadKind
	decNotDecodeableReasonNonAddrValue
	decNotDecodeableReasonNilReference
)

type decDriver interface {
	// this will check if the next token is a break.
	CheckBreak() bool

	// TryNil tries to decode as nil.
	// If a nil is in the stream, it consumes it and returns true.
	//
	// Note: if TryNil returns true, that must be handled.
	TryNil() bool

	// ContainerType returns one of: Bytes, String, Nil, Slice or Map.
	//
	// Return unSet if not known.
	//
	// Note: Implementations MUST fully consume sentinel container types, specifically Nil.
	ContainerType() (vt valueType)

	// DecodeNaked will decode primitives (number, bool, string, []byte) and RawExt.
	// For maps and arrays, it will not do the decoding in-band, but will signal
	// the decoder, so that is done later, by setting the fauxUnion.valueType field.
	//
	// Note: Numbers are decoded as int64, uint64, float64 only (no smaller sized number types).
	// for extensions, DecodeNaked must read the tag and the []byte if it exists.
	// if the []byte is not read, then kInterfaceNaked will treat it as a Handle
	// that stores the subsequent value in-band, and complete reading the RawExt.
	//
	// extensions should also use readx to decode them, for efficiency.
	// kInterface will extract the detached byte slice if it has to pass it outside its realm.
	DecodeNaked()

	DecodeInt64() (i int64)
	DecodeUint64() (ui uint64)

	DecodeFloat64() (f float64)
	DecodeBool() (b bool)

	// DecodeStringAsBytes returns the bytes representing a string.
	// It will return a view into scratch buffer or input []byte (if applicable).
	//
	// Note: This can also decode symbols, if supported.
	//
	// Users should consume it right away and not store it for later use.
	DecodeStringAsBytes() (v []byte)

	// DecodeBytes returns the bytes representing a binary value.
	// It will return a view into scratch buffer or input []byte (if applicable).
	//
	// All implementations must honor the contract below:
	//    if ZeroCopy and applicable, return a view into input []byte we are decoding from
	//    else if in == nil,          return a view into scratch buffer
	//    else                        append decoded value to in[:0] and return that
	//                                (this can be simulated by passing []byte{} as in parameter)
	//
	// Implementations must also update Decoder.decByteState on each call to
	// DecodeBytes or DecodeStringAsBytes. Some callers may check that and work appropriately.
	//
	// Note: DecodeBytes may decode past the length of the passed byte slice, up to the cap.
	// Consequently, it is ok to pass a zero-len slice to DecodeBytes, as the returned
	// byte slice will have the appropriate length.
	DecodeBytes(in []byte) (out []byte)
	// DecodeBytes(bs []byte, isstring, zerocopy bool) (bsOut []byte)

	// DecodeExt will decode into a *RawExt or into an extension.
	DecodeExt(v interface{}, basetype reflect.Type, xtag uint64, ext Ext)
	// decodeExt(verifyTag bool, tag byte) (xtag byte, xbs []byte)

	DecodeTime() (t time.Time)

	// ReadArrayStart will return the length of the array.
	// If the format doesn't prefix the length, it returns containerLenUnknown.
	// If the expected array was a nil in the stream, it returns containerLenNil.
	ReadArrayStart() int

	// ReadMapStart will return the length of the array.
	// If the format doesn't prefix the length, it returns containerLenUnknown.
	// If the expected array was a nil in the stream, it returns containerLenNil.
	ReadMapStart() int

	reset()

	// atEndOfDecode()

	// nextValueBytes will return the bytes representing the next value in the stream.
	//
	// if start is nil, then treat it as a request to discard the next set of bytes,
	// and the return response does not matter.
	// Typically, this means that the returned []byte is nil/empty/undefined.
	//
	// Optimize for decoding from a []byte, where the nextValueBytes will just be a sub-slice
	// of the input slice. Callers that need to use this to not be a view into the input bytes
	// should handle it appropriately.
	nextValueBytes(start []byte) []byte

	// descBd will describe the token descriptor that signifies what type was decoded
	descBd() string

	decoder() *Decoder

	driverStateManager
	decNegintPosintFloatNumber
}

type decDriverContainerTracker interface {
	ReadArrayElem()
	ReadMapElemKey()
	ReadMapElemValue()
	ReadArrayEnd()
	ReadMapEnd()
}

type decNegintPosintFloatNumber interface {
	decInteger() (ui uint64, neg, ok bool)
	decFloat() (f float64, ok bool)
}

type decDriverNoopNumberHelper struct{}

func (x decDriverNoopNumberHelper) decInteger() (ui uint64, neg, ok bool) {
	panic("decInteger unsupported")
}
func (x decDriverNoopNumberHelper) decFloat() (f float64, ok bool) { panic("decFloat unsupported") }

type decDriverNoopContainerReader struct{}

// func (x decDriverNoopContainerReader) ReadArrayStart() (v int) { panic("ReadArrayStart unsupported") }
// func (x decDriverNoopContainerReader) ReadMapStart() (v int)   { panic("ReadMapStart unsupported") }
func (x decDriverNoopContainerReader) ReadArrayEnd()        {}
func (x decDriverNoopContainerReader) ReadMapEnd()          {}
func (x decDriverNoopContainerReader) CheckBreak() (v bool) { return }

// DecodeOptions captures configuration options during decode.
type DecodeOptions struct {
	// MapType specifies type to use during schema-less decoding of a map in the stream.
	// If nil (unset), we default to map[string]interface{} iff json handle and MapKeyAsString=true,
	// else map[interface{}]interface{}.
	MapType reflect.Type

	// SliceType specifies type to use during schema-less decoding of an array in the stream.
	// If nil (unset), we default to []interface{} for all formats.
	SliceType reflect.Type

	// MaxInitLen defines the maxinum initial length that we "make" a collection
	// (string, slice, map, chan). If 0 or negative, we default to a sensible value
	// based on the size of an element in the collection.
	//
	// For example, when decoding, a stream may say that it has 2^64 elements.
	// We should not auto-matically provision a slice of that size, to prevent Out-Of-Memory crash.
	// Instead, we provision up to MaxInitLen, fill that up, and start appending after that.
	MaxInitLen int

	// ReaderBufferSize is the size of the buffer used when reading.
	//
	// if > 0, we use a smart buffer internally for performance purposes.
	ReaderBufferSize int

	// MaxDepth defines the maximum depth when decoding nested
	// maps and slices. If 0 or negative, we default to a suitably large number (currently 1024).
	MaxDepth int16

	// If ErrorIfNoField, return an error when decoding a map
	// from a codec stream into a struct, and no matching struct field is found.
	ErrorIfNoField bool

	// If ErrorIfNoArrayExpand, return an error when decoding a slice/array that cannot be expanded.
	// For example, the stream contains an array of 8 items, but you are decoding into a [4]T array,
	// or you are decoding into a slice of length 4 which is non-addressable (and so cannot be set).
	ErrorIfNoArrayExpand bool

	// If SignedInteger, use the int64 during schema-less decoding of unsigned values (not uint64).
	SignedInteger bool

	// MapValueReset controls how we decode into a map value.
	//
	// By default, we MAY retrieve the mapping for a key, and then decode into that.
	// However, especially with big maps, that retrieval may be expensive and unnecessary
	// if the stream already contains all that is necessary to recreate the value.
	//
	// If true, we will never retrieve the previous mapping,
	// but rather decode into a new value and set that in the map.
	//
	// If false, we will retrieve the previous mapping if necessary e.g.
	// the previous mapping is a pointer, or is a struct or array with pre-set state,
	// or is an interface.
	MapValueReset bool

	// SliceElementReset: on decoding a slice, reset the element to a zero value first.
	//
	// concern: if the slice already contained some garbage, we will decode into that garbage.
	SliceElementReset bool

	// InterfaceReset controls how we decode into an interface.
	//
	// By default, when we see a field that is an interface{...},
	// or a map with interface{...} value, we will attempt decoding into the
	// "contained" value.
	//
	// However, this prevents us from reading a string into an interface{}
	// that formerly contained a number.
	//
	// If true, we will decode into a new "blank" value, and set that in the interface.
	// If false, we will decode into whatever is contained in the interface.
	InterfaceReset bool

	// InternString controls interning of strings during decoding.
	//
	// Some handles, e.g. json, typically will read map keys as strings.
	// If the set of keys are finite, it may help reduce allocation to
	// look them up from a map (than to allocate them afresh).
	//
	// Note: Handles will be smart when using the intern functionality.
	// Every string should not be interned.
	// An excellent use-case for interning is struct field names,
	// or map keys where key type is string.
	InternString bool

	// PreferArrayOverSlice controls whether to decode to an array or a slice.
	//
	// This only impacts decoding into a nil interface{}.
	//
	// Consequently, it has no effect on codecgen.
	//
	// *Note*: This only applies if using go1.5 and above,
	// as it requires reflect.ArrayOf support which was absent before go1.5.
	PreferArrayOverSlice bool

	// DeleteOnNilMapValue controls how to decode a nil value in the stream.
	//
	// If true, we will delete the mapping of the key.
	// Else, just set the mapping to the zero value of the type.
	//
	// Deprecated: This does NOTHING and is left behind for compiling compatibility.
	// This change is necessitated because 'nil' in a stream now consistently
	// means the zero value (ie reset the value to its zero state).
	DeleteOnNilMapValue bool

	// RawToString controls how raw bytes in a stream are decoded into a nil interface{}.
	// By default, they are decoded as []byte, but can be decoded as string (if configured).
	RawToString bool

	// ZeroCopy controls whether decoded values of []byte or string type
	// point into the input []byte parameter passed to a NewDecoderBytes/ResetBytes(...) call.
	//
	// To illustrate, if ZeroCopy and decoding from a []byte (not io.Writer),
	// then a []byte or string in the output result may just be a slice of (point into)
	// the input bytes.
	//
	// This optimization prevents unnecessary copying.
	//
	// However, it is made optional, as the caller MUST ensure that the input parameter []byte is
	// not modified after the Decode() happens, as any changes are mirrored in the decoded result.
	ZeroCopy bool

	// PreferPointerForStructOrArray controls whether a struct or array
	// is stored in a nil interface{}, or a pointer to it.
	//
	// This mostly impacts when we decode registered extensions.
	PreferPointerForStructOrArray bool

	// ValidateUnicode controls will cause decoding to fail if an expected unicode
	// string is well-formed but include invalid codepoints.
	//
	// This could have a performance impact.
	ValidateUnicode bool
}

// ----------------------------------------

func (d *Decoder) rawExt(f *codecFnInfo, rv reflect.Value) {
	d.d.DecodeExt(rv2i(rv), f.ti.rt, 0, nil)
}

func (d *Decoder) ext(f *codecFnInfo, rv reflect.Value) {
	d.d.DecodeExt(rv2i(rv), f.ti.rt, f.xfTag, f.xfFn)
}

func (d *Decoder) selferUnmarshal(f *codecFnInfo, rv reflect.Value) {
	rv2i(rv).(Selfer).CodecDecodeSelf(d)
}

func (d *Decoder) binaryUnmarshal(f *codecFnInfo, rv reflect.Value) {
	bm := rv2i(rv).(encoding.BinaryUnmarshaler)
	xbs := d.d.DecodeBytes(nil)
	fnerr := bm.UnmarshalBinary(xbs)
	d.onerror(fnerr)
}

func (d *Decoder) textUnmarshal(f *codecFnInfo, rv reflect.Value) {
	tm := rv2i(rv).(encoding.TextUnmarshaler)
	fnerr := tm.UnmarshalText(d.d.DecodeStringAsBytes())
	d.onerror(fnerr)
}

func (d *Decoder) jsonUnmarshal(f *codecFnInfo, rv reflect.Value) {
	d.jsonUnmarshalV(rv2i(rv).(jsonUnmarshaler))
}

func (d *Decoder) jsonUnmarshalV(tm jsonUnmarshaler) {
	// grab the bytes to be read, as UnmarshalJSON needs the full JSON so as to unmarshal it itself.
	var bs0 = []byte{}
	if !d.bytes {
		bs0 = d.blist.get(256)
	}
	bs := d.d.nextValueBytes(bs0)
	fnerr := tm.UnmarshalJSON(bs)
	if !d.bytes {
		d.blist.put(bs)
		if !byteSliceSameData(bs0, bs) {
			d.blist.put(bs0)
		}
	}
	d.onerror(fnerr)
}

func (d *Decoder) kErr(f *codecFnInfo, rv reflect.Value) {
	d.errorf("no decoding function defined for kind %v", rv.Kind())
}

func (d *Decoder) raw(f *codecFnInfo, rv reflect.Value) {
	rvSetBytes(rv, d.rawBytes())
}

func (d *Decoder) kString(f *codecFnInfo, rv reflect.Value) {
	rvSetString(rv, d.stringZC(d.d.DecodeStringAsBytes()))
}

func (d *Decoder) kBool(f *codecFnInfo, rv reflect.Value) {
	rvSetBool(rv, d.d.DecodeBool())
}

func (d *Decoder) kTime(f *codecFnInfo, rv reflect.Value) {
	rvSetTime(rv, d.d.DecodeTime())
}

func (d *Decoder) kFloat32(f *codecFnInfo, rv reflect.Value) {
	rvSetFloat32(rv, d.decodeFloat32())
}

func (d *Decoder) kFloat64(f *codecFnInfo, rv reflect.Value) {
	rvSetFloat64(rv, d.d.DecodeFloat64())
}

func (d *Decoder) kComplex64(f *codecFnInfo, rv reflect.Value) {
	rvSetComplex64(rv, complex(d.decodeFloat32(), 0))
}

func (d *Decoder) kComplex128(f *codecFnInfo, rv reflect.Value) {
	rvSetComplex128(rv, complex(d.d.DecodeFloat64(), 0))
}

func (d *Decoder) kInt(f *codecFnInfo, rv reflect.Value) {
	rvSetInt(rv, int(chkOvf.IntV(d.d.DecodeInt64(), intBitsize)))
}

func (d *Decoder) kInt8(f *codecFnInfo, rv reflect.Value) {
	rvSetInt8(rv, int8(chkOvf.IntV(d.d.DecodeInt64(), 8)))
}

func (d *Decoder) kInt16(f *codecFnInfo, rv reflect.Value) {
	rvSetInt16(rv, int16(chkOvf.IntV(d.d.DecodeInt64(), 16)))
}

func (d *Decoder) kInt32(f *codecFnInfo, rv reflect.Value) {
	rvSetInt32(rv, int32(chkOvf.IntV(d.d.DecodeInt64(), 32)))
}

func (d *Decoder) kInt64(f *codecFnInfo, rv reflect.Value) {
	rvSetInt64(rv, d.d.DecodeInt64())
}

func (d *Decoder) kUint(f *codecFnInfo, rv reflect.Value) {
	rvSetUint(rv, uint(chkOvf.UintV(d.d.DecodeUint64(), uintBitsize)))
}

func (d *Decoder) kUintptr(f *codecFnInfo, rv reflect.Value) {
	rvSetUintptr(rv, uintptr(chkOvf.UintV(d.d.DecodeUint64(), uintBitsize)))
}

func (d *Decoder) kUint8(f *codecFnInfo, rv reflect.Value) {
	rvSetUint8(rv, uint8(chkOvf.UintV(d.d.DecodeUint64(), 8)))
}

func (d *Decoder) kUint16(f *codecFnInfo, rv reflect.Value) {
	rvSetUint16(rv, uint16(chkOvf.UintV(d.d.DecodeUint64(), 16)))
}

func (d *Decoder) kUint32(f *codecFnInfo, rv reflect.Value) {
	rvSetUint32(rv, uint32(chkOvf.UintV(d.d.DecodeUint64(), 32)))
}

func (d *Decoder) kUint64(f *codecFnInfo, rv reflect.Value) {
	rvSetUint64(rv, d.d.DecodeUint64())
}

func (d *Decoder) kInterfaceNaked(f *codecFnInfo) (rvn reflect.Value) {
	// nil interface:
	// use some hieristics to decode it appropriately
	// based on the detected next value in the stream.
	n := d.naked()
	d.d.DecodeNaked()

	// We cannot decode non-nil stream value into nil interface with methods (e.g. io.Reader).
	// Howver, it is possible that the user has ways to pass in a type for a given interface
	//   - MapType
	//   - SliceType
	//   - Extensions
	//
	// Consequently, we should relax this. Put it behind a const flag for now.
	if decFailNonEmptyIntf && f.ti.numMeth > 0 {
		d.errorf("cannot decode non-nil codec value into nil %v (%v methods)", f.ti.rt, f.ti.numMeth)
	}
	switch n.v {
	case valueTypeMap:
		mtid := d.mtid
		if mtid == 0 {
			if d.jsms { // if json, default to a map type with string keys
				mtid = mapStrIntfTypId // for json performance
			} else {
				mtid = mapIntfIntfTypId
			}
		}
		if mtid == mapStrIntfTypId {
			var v2 map[string]interface{}
			d.decode(&v2)
			rvn = rv4iptr(&v2).Elem()
		} else if mtid == mapIntfIntfTypId {
			var v2 map[interface{}]interface{}
			d.decode(&v2)
			rvn = rv4iptr(&v2).Elem()
		} else if d.mtr {
			rvn = reflect.New(d.h.MapType)
			d.decode(rv2i(rvn))
			rvn = rvn.Elem()
		} else {
			rvn = rvZeroAddrK(d.h.MapType, reflect.Map)
			d.decodeValue(rvn, nil)
		}
	case valueTypeArray:
		if d.stid == 0 || d.stid == intfSliceTypId {
			var v2 []interface{}
			d.decode(&v2)
			rvn = rv4iptr(&v2).Elem()
		} else if d.str {
			rvn = reflect.New(d.h.SliceType)
			d.decode(rv2i(rvn))
			rvn = rvn.Elem()
		} else {
			rvn = rvZeroAddrK(d.h.SliceType, reflect.Slice)
			d.decodeValue(rvn, nil)
		}
		if reflectArrayOfSupported && d.h.PreferArrayOverSlice {
			rvn = rvGetArray4Slice(rvn)
		}
	case valueTypeExt:
		tag, bytes := n.u, n.l // calling decode below might taint the values
		bfn := d.h.getExtForTag(tag)
		var re = RawExt{Tag: tag}
		if bytes == nil {
			// it is one of the InterfaceExt ones: json and cbor.
			// most likely cbor, as json decoding never reveals valueTypeExt (no tagging support)
			if bfn == nil {
				d.decode(&re.Value)
				rvn = rv4iptr(&re).Elem()
			} else {
				if bfn.ext == SelfExt {
					rvn = rvZeroAddrK(bfn.rt, bfn.rt.Kind())
					d.decodeValue(rvn, d.h.fnNoExt(bfn.rt))
				} else {
					rvn = reflect.New(bfn.rt)
					d.interfaceExtConvertAndDecode(rv2i(rvn), bfn.ext)
					rvn = rvn.Elem()
				}
			}
		} else {
			// one of the BytesExt ones: binc, msgpack, simple
			if bfn == nil {
				re.setData(bytes, false)
				rvn = rv4iptr(&re).Elem()
			} else {
				rvn = reflect.New(bfn.rt)
				if bfn.ext == SelfExt {
					d.sideDecode(rv2i(rvn), bfn.rt, bytes)
				} else {
					bfn.ext.ReadExt(rv2i(rvn), bytes)
				}
				rvn = rvn.Elem()
			}
		}
		// if struct/array, directly store pointer into the interface
		if d.h.PreferPointerForStructOrArray && rvn.CanAddr() {
			if rk := rvn.Kind(); rk == reflect.Array || rk == reflect.Struct {
				rvn = rvn.Addr()
			}
		}
	case valueTypeNil:
		// rvn = reflect.Zero(f.ti.rt)
		// no-op
	case valueTypeInt:
		rvn = n.ri()
	case valueTypeUint:
		rvn = n.ru()
	case valueTypeFloat:
		rvn = n.rf()
	case valueTypeBool:
		rvn = n.rb()
	case valueTypeString, valueTypeSymbol:
		rvn = n.rs()
	case valueTypeBytes:
		rvn = n.rl()
	case valueTypeTime:
		rvn = n.rt()
	default:
		halt.errorf("kInterfaceNaked: unexpected valueType: %d", n.v)
	}
	return
}

func (d *Decoder) kInterface(f *codecFnInfo, rv reflect.Value) {
	// Note: A consequence of how kInterface works, is that
	// if an interface already contains something, we try
	// to decode into what was there before.
	// We do not replace with a generic value (as got from decodeNaked).
	//
	// every interface passed here MUST be settable.
	//
	// ensure you call rvSetIntf(...) before returning.

	isnilrv := rvIsNil(rv)

	var rvn reflect.Value

	if d.h.InterfaceReset {
		// check if mapping to a type: if so, initialize it and move on
		rvn = d.h.intf2impl(f.ti.rtid)
		if !rvn.IsValid() {
			rvn = d.kInterfaceNaked(f)
			if rvn.IsValid() {
				rvSetIntf(rv, rvn)
			} else if !isnilrv {
				decSetNonNilRV2Zero4Intf(rv)
			}
			return
		}
	} else if isnilrv {
		// check if mapping to a type: if so, initialize it and move on
		rvn = d.h.intf2impl(f.ti.rtid)
		if !rvn.IsValid() {
			rvn = d.kInterfaceNaked(f)
			if rvn.IsValid() {
				rvSetIntf(rv, rvn)
			}
			return
		}
	} else {
		// now we have a non-nil interface value, meaning it contains a type
		rvn = rv.Elem()
	}

	// rvn is now a non-interface type

	canDecode, _ := isDecodeable(rvn)

	// Note: interface{} is settable, but underlying type may not be.
	// Consequently, we MAY have to allocate a value (containing the underlying value),
	// decode into it, and reset the interface to that new value.

	if !canDecode {
		rvn2 := d.oneShotAddrRV(rvn.Type(), rvn.Kind())
		rvSetDirect(rvn2, rvn)
		rvn = rvn2
	}

	d.decodeValue(rvn, nil)
	rvSetIntf(rv, rvn)
}

func decStructFieldKeyNotString(dd decDriver, keyType valueType, b *[decScratchByteArrayLen]byte) (rvkencname []byte) {
	if keyType == valueTypeInt {
		rvkencname = strconv.AppendInt(b[:0], dd.DecodeInt64(), 10)
	} else if keyType == valueTypeUint {
		rvkencname = strconv.AppendUint(b[:0], dd.DecodeUint64(), 10)
	} else if keyType == valueTypeFloat {
		rvkencname = strconv.AppendFloat(b[:0], dd.DecodeFloat64(), 'f', -1, 64)
	} else {
		halt.errorf("invalid struct key type: %v", keyType)
	}
	return
}

func (d *Decoder) kStructField(si *structFieldInfo, rv reflect.Value) {
	if d.d.TryNil() {
		if rv = si.path.field(rv); rv.IsValid() {
			decSetNonNilRV2Zero(rv)
		}
		return
	}
	d.decodeValueNoCheckNil(si.path.fieldAlloc(rv), nil)
}

func (d *Decoder) kStruct(f *codecFnInfo, rv reflect.Value) {
	ctyp := d.d.ContainerType()
	ti := f.ti
	var mf MissingFielder
	if ti.flagMissingFielder {
		mf = rv2i(rv).(MissingFielder)
	} else if ti.flagMissingFielderPtr {
		mf = rv2i(rvAddr(rv, ti.ptr)).(MissingFielder)
	}
	if ctyp == valueTypeMap {
		containerLen := d.mapStart(d.d.ReadMapStart())
		if containerLen == 0 {
			d.mapEnd()
			return
		}
		hasLen := containerLen >= 0
		var name2 []byte
		if mf != nil {
			var namearr2 [16]byte
			name2 = namearr2[:0]
		}
		var rvkencname []byte
		for j := 0; d.containerNext(j, containerLen, hasLen); j++ {
			d.mapElemKey()
			if ti.keyType == valueTypeString {
				rvkencname = d.d.DecodeStringAsBytes()
			} else {
				rvkencname = decStructFieldKeyNotString(d.d, ti.keyType, &d.b)
			}
			d.mapElemValue()
			if si := ti.siForEncName(rvkencname); si != nil {
				d.kStructField(si, rv)
			} else if mf != nil {
				// store rvkencname in new []byte, as it previously shares Decoder.b, which is used in decode
				name2 = append(name2[:0], rvkencname...)
				var f interface{}
				d.decode(&f)
				if !mf.CodecMissingField(name2, f) && d.h.ErrorIfNoField {
					d.errorf("no matching struct field when decoding stream map with key: %s ", stringView(name2))
				}
			} else {
				d.structFieldNotFound(-1, stringView(rvkencname))
			}
		}
		d.mapEnd()
	} else if ctyp == valueTypeArray {
		containerLen := d.arrayStart(d.d.ReadArrayStart())
		if containerLen == 0 {
			d.arrayEnd()
			return
		}
		// Not much gain from doing it two ways for array.
		// Arrays are not used as much for structs.
		tisfi := ti.sfi.source()
		hasLen := containerLen >= 0

		// iterate all the items in the stream
		// if mapped elem-wise to a field, handle it
		// if more stream items than cap be mapped, error it
		for j := 0; d.containerNext(j, containerLen, hasLen); j++ {
			d.arrayElem()
			if j < len(tisfi) {
				d.kStructField(tisfi[j], rv)
			} else {
				d.structFieldNotFound(j, "")
			}
		}

		d.arrayEnd()
	} else {
		d.onerror(errNeedMapOrArrayDecodeToStruct)
	}
}

func (d *Decoder) kSlice(f *codecFnInfo, rv reflect.Value) {
	// A slice can be set from a map or array in stream.
	// This way, the order can be kept (as order is lost with map).

	// Note: rv is a slice type here - guaranteed

	ti := f.ti
	rvCanset := rv.CanSet()

	ctyp := d.d.ContainerType()
	if ctyp == valueTypeBytes || ctyp == valueTypeString {
		// you can only decode bytes or string in the stream into a slice or array of bytes
		if !(ti.rtid == uint8SliceTypId || ti.elemkind == uint8(reflect.Uint8)) {
			d.errorf("bytes/string in stream must decode into slice/array of bytes, not %v", ti.rt)
		}
		rvbs := rvGetBytes(rv)
		if !rvCanset {
			// not addressable byte slice, so do not decode into it past the length
			rvbs = rvbs[:len(rvbs):len(rvbs)]
		}
		bs2 := d.decodeBytesInto(rvbs)
		// if !(len(bs2) == len(rvbs) && byteSliceSameData(rvbs, bs2)) {
		if !(len(bs2) > 0 && len(bs2) == len(rvbs) && &bs2[0] == &rvbs[0]) {
			if rvCanset {
				rvSetBytes(rv, bs2)
			} else if len(rvbs) > 0 && len(bs2) > 0 {
				copy(rvbs, bs2)
			}
		}
		return
	}

	slh, containerLenS := d.decSliceHelperStart() // only expects valueType(Array|Map) - never Nil

	// an array can never return a nil slice. so no need to check f.array here.
	if containerLenS == 0 {
		if rvCanset {
			if rvIsNil(rv) {
				rvSetDirect(rv, rvSliceZeroCap(ti.rt))
			} else {
				rvSetSliceLen(rv, 0)
			}
		}
		slh.End()
		return
	}

	rtelem0Mut := !scalarBitset.isset(ti.elemkind)
	rtelem := ti.elem

	for k := reflect.Kind(ti.elemkind); k == reflect.Ptr; k = rtelem.Kind() {
		rtelem = rtelem.Elem()
	}

	var fn *codecFn

	var rvChanged bool

	var rv0 = rv
	var rv9 reflect.Value

	rvlen := rvLenSlice(rv)
	rvcap := rvCapSlice(rv)
	hasLen := containerLenS > 0
	if hasLen {
		if containerLenS > rvcap {
			oldRvlenGtZero := rvlen > 0
			rvlen1 := decInferLen(containerLenS, d.h.MaxInitLen, int(ti.elemsize))
			if rvlen1 == rvlen {
			} else if rvlen1 <= rvcap {
				if rvCanset {
					rvlen = rvlen1
					rvSetSliceLen(rv, rvlen)
				}
			} else if rvCanset { // rvlen1 > rvcap
				rvlen = rvlen1
				rv, rvCanset = rvMakeSlice(rv, f.ti, rvlen, rvlen)
				rvcap = rvlen
				rvChanged = !rvCanset
			} else { // rvlen1 > rvcap && !canSet
				d.errorf("cannot decode into non-settable slice")
			}
			if rvChanged && oldRvlenGtZero && rtelem0Mut {
				rvCopySlice(rv, rv0, rtelem) // only copy up to length NOT cap i.e. rv0.Slice(0, rvcap)
			}
		} else if containerLenS != rvlen {
			if rvCanset {
				rvlen = containerLenS
				rvSetSliceLen(rv, rvlen)
			}
		}
	}

	// consider creating new element once, and just decoding into it.
	var elemReset = d.h.SliceElementReset

	var j int

	for ; d.containerNext(j, containerLenS, hasLen); j++ {
		if j == 0 {
			if rvIsNil(rv) { // means hasLen = false
				if rvCanset {
					rvlen = decInferLen(containerLenS, d.h.MaxInitLen, int(ti.elemsize))
					rv, rvCanset = rvMakeSlice(rv, f.ti, rvlen, rvlen)
					rvcap = rvlen
					rvChanged = !rvCanset
				} else {
					d.errorf("cannot decode into non-settable slice")
				}
			}
			if fn == nil {
				fn = d.h.fn(rtelem)
			}
		}
		// if indefinite, etc, then expand the slice if necessary
		if j >= rvlen {
			slh.ElemContainerState(j)

			// expand the slice up to the cap.
			// Note that we did, so we have to reset it later.

			if rvlen < rvcap {
				rvlen = rvcap
				if rvCanset {
					rvSetSliceLen(rv, rvlen)
				} else if rvChanged {
					rv = rvSlice(rv, rvlen)
				} else {
					d.onerror(errExpandSliceCannotChange)
				}
			} else {
				if !(rvCanset || rvChanged) {
					d.onerror(errExpandSliceCannotChange)
				}
				rv, rvcap, rvCanset = rvGrowSlice(rv, f.ti, rvcap, 1)
				rvlen = rvcap
				rvChanged = !rvCanset
			}
		} else {
			slh.ElemContainerState(j)
		}
		rv9 = rvSliceIndex(rv, j, f.ti)
		if elemReset {
			rvSetZero(rv9)
		}
		d.decodeValue(rv9, fn)
	}
	if j < rvlen {
		if rvCanset {
			rvSetSliceLen(rv, j)
		} else if rvChanged {
			rv = rvSlice(rv, j)
		}
		// rvlen = j
	} else if j == 0 && rvIsNil(rv) {
		if rvCanset {
			rv = rvSliceZeroCap(ti.rt)
			rvCanset = false
			rvChanged = true
		}
	}
	slh.End()

	if rvChanged { // infers rvCanset=true, so it can be reset
		rvSetDirect(rv0, rv)
	}
}

func (d *Decoder) kArray(f *codecFnInfo, rv reflect.Value) {
	// An array can be set from a map or array in stream.

	ctyp := d.d.ContainerType()
	if handleBytesWithinKArray && (ctyp == valueTypeBytes || ctyp == valueTypeString) {
		// you can only decode bytes or string in the stream into a slice or array of bytes
		if f.ti.elemkind != uint8(reflect.Uint8) {
			d.errorf("bytes/string in stream can decode into array of bytes, but not %v", f.ti.rt)
		}
		rvbs := rvGetArrayBytes(rv, nil)
		bs2 := d.decodeBytesInto(rvbs)
		if !byteSliceSameData(rvbs, bs2) && len(rvbs) > 0 && len(bs2) > 0 {
			copy(rvbs, bs2)
		}
		return
	}

	slh, containerLenS := d.decSliceHelperStart() // only expects valueType(Array|Map) - never Nil

	// an array can never return a nil slice. so no need to check f.array here.
	if containerLenS == 0 {
		slh.End()
		return
	}

	rtelem := f.ti.elem
	for k := reflect.Kind(f.ti.elemkind); k == reflect.Ptr; k = rtelem.Kind() {
		rtelem = rtelem.Elem()
	}

	var fn *codecFn

	var rv9 reflect.Value

	rvlen := rv.Len() // same as cap
	hasLen := containerLenS > 0
	if hasLen && containerLenS > rvlen {
		d.errorf("cannot decode into array with length: %v, less than container length: %v", rvlen, containerLenS)
	}

	// consider creating new element once, and just decoding into it.
	var elemReset = d.h.SliceElementReset

	for j := 0; d.containerNext(j, containerLenS, hasLen); j++ {
		// note that you cannot expand the array if indefinite and we go past array length
		if j >= rvlen {
			slh.arrayCannotExpand(hasLen, rvlen, j, containerLenS)
			return
		}

		slh.ElemContainerState(j)
		rv9 = rvArrayIndex(rv, j, f.ti)
		if elemReset {
			rvSetZero(rv9)
		}

		if fn == nil {
			fn = d.h.fn(rtelem)
		}
		d.decodeValue(rv9, fn)
	}
	slh.End()
}

func (d *Decoder) kChan(f *codecFnInfo, rv reflect.Value) {
	// A slice can be set from a map or array in stream.
	// This way, the order can be kept (as order is lost with map).

	ti := f.ti
	if ti.chandir&uint8(reflect.SendDir) == 0 {
		d.errorf("receive-only channel cannot be decoded")
	}
	ctyp := d.d.ContainerType()
	if ctyp == valueTypeBytes || ctyp == valueTypeString {
		// you can only decode bytes or string in the stream into a slice or array of bytes
		if !(ti.rtid == uint8SliceTypId || ti.elemkind == uint8(reflect.Uint8)) {
			d.errorf("bytes/string in stream must decode into slice/array of bytes, not %v", ti.rt)
		}
		bs2 := d.d.DecodeBytes(nil)
		irv := rv2i(rv)
		ch, ok := irv.(chan<- byte)
		if !ok {
			ch = irv.(chan byte)
		}
		for _, b := range bs2 {
			ch <- b
		}
		return
	}

	var rvCanset = rv.CanSet()

	// only expects valueType(Array|Map - nil handled above)
	slh, containerLenS := d.decSliceHelperStart()

	// an array can never return a nil slice. so no need to check f.array here.
	if containerLenS == 0 {
		if rvCanset && rvIsNil(rv) {
			rvSetDirect(rv, reflect.MakeChan(ti.rt, 0))
		}
		slh.End()
		return
	}

	rtelem := ti.elem
	useTransient := decUseTransient && ti.elemkind != byte(reflect.Ptr) && ti.tielem.flagCanTransient

	for k := reflect.Kind(ti.elemkind); k == reflect.Ptr; k = rtelem.Kind() {
		rtelem = rtelem.Elem()
	}

	var fn *codecFn

	var rvChanged bool
	var rv0 = rv
	var rv9 reflect.Value

	var rvlen int // = rv.Len()
	hasLen := containerLenS > 0

	for j := 0; d.containerNext(j, containerLenS, hasLen); j++ {
		if j == 0 {
			if rvIsNil(rv) {
				if hasLen {
					rvlen = decInferLen(containerLenS, d.h.MaxInitLen, int(ti.elemsize))
				} else {
					rvlen = decDefChanCap
				}
				if rvCanset {
					rv = reflect.MakeChan(ti.rt, rvlen)
					rvChanged = true
				} else {
					d.errorf("cannot decode into non-settable chan")
				}
			}
			if fn == nil {
				fn = d.h.fn(rtelem)
			}
		}
		slh.ElemContainerState(j)
		if rv9.IsValid() {
			rvSetZero(rv9)
		} else if decUseTransient && useTransient {
			rv9 = d.perType.TransientAddrK(ti.elem, reflect.Kind(ti.elemkind))
		} else {
			rv9 = rvZeroAddrK(ti.elem, reflect.Kind(ti.elemkind))
		}
		if !d.d.TryNil() {
			d.decodeValueNoCheckNil(rv9, fn)
		}
		rv.Send(rv9)
	}
	slh.End()

	if rvChanged { // infers rvCanset=true, so it can be reset
		rvSetDirect(rv0, rv)
	}

}

func (d *Decoder) kMap(f *codecFnInfo, rv reflect.Value) {
	containerLen := d.mapStart(d.d.ReadMapStart())
	ti := f.ti
	if rvIsNil(rv) {
		rvlen := decInferLen(containerLen, d.h.MaxInitLen, int(ti.keysize+ti.elemsize))
		rvSetDirect(rv, makeMapReflect(ti.rt, rvlen))
	}

	if containerLen == 0 {
		d.mapEnd()
		return
	}

	ktype, vtype := ti.key, ti.elem
	ktypeId := rt2id(ktype)
	vtypeKind := reflect.Kind(ti.elemkind)
	ktypeKind := reflect.Kind(ti.keykind)
	kfast := mapKeyFastKindFor(ktypeKind)
	visindirect := mapStoresElemIndirect(uintptr(ti.elemsize))
	visref := refBitset.isset(ti.elemkind)

	vtypePtr := vtypeKind == reflect.Ptr
	ktypePtr := ktypeKind == reflect.Ptr

	vTransient := decUseTransient && !vtypePtr && ti.tielem.flagCanTransient
	kTransient := decUseTransient && !ktypePtr && ti.tikey.flagCanTransient

	var vtypeElem reflect.Type

	var keyFn, valFn *codecFn
	var ktypeLo, vtypeLo = ktype, vtype

	if ktypeKind == reflect.Ptr {
		for ktypeLo = ktype.Elem(); ktypeLo.Kind() == reflect.Ptr; ktypeLo = ktypeLo.Elem() {
		}
	}

	if vtypePtr {
		vtypeElem = vtype.Elem()
		for vtypeLo = vtypeElem; vtypeLo.Kind() == reflect.Ptr; vtypeLo = vtypeLo.Elem() {
		}
	}

	rvkMut := !scalarBitset.isset(ti.keykind) // if ktype is immutable, then re-use the same rvk.
	rvvMut := !scalarBitset.isset(ti.elemkind)
	rvvCanNil := isnilBitset.isset(ti.elemkind)

	// rvk: key
	// rvkn: if non-mutable, on each iteration of loop, set rvk to this
	// rvv: value
	// rvvn: if non-mutable, on each iteration of loop, set rvv to this
	//       if mutable, may be used as a temporary value for local-scoped operations
	// rvva: if mutable, used as transient value for use for key lookup
	// rvvz: zero value of map value type, used to do a map set when nil is found in stream
	var rvk, rvkn, rvv, rvvn, rvva, rvvz reflect.Value

	// we do a doMapGet if kind is mutable, and InterfaceReset=true if interface
	var doMapGet, doMapSet bool

	if !d.h.MapValueReset {
		if rvvMut && (vtypeKind != reflect.Interface || !d.h.InterfaceReset) {
			doMapGet = true
			rvva = mapAddrLoopvarRV(vtype, vtypeKind)
		}
	}

	ktypeIsString := ktypeId == stringTypId
	ktypeIsIntf := ktypeId == intfTypId

	hasLen := containerLen > 0

	// kstrbs is used locally for the key bytes, so we can reduce allocation.
	// When we read keys, we copy to this local bytes array, and use a stringView for lookup.
	// We only convert it into a true string if we have to do a set on the map.

	// Since kstr2bs will usually escape to the heap, declaring a [64]byte array may be wasteful.
	// It is only valuable if we are sure that it is declared on the stack.
	// var kstrarr [64]byte // most keys are less than 32 bytes, and even more less than 64
	// var kstrbs = kstrarr[:0]
	var kstrbs []byte
	var kstr2bs []byte
	var s string

	var callFnRvk bool

	fnRvk2 := func() (s string) {
		callFnRvk = false
		if len(kstr2bs) < 2 {
			return string(kstr2bs)
		}
		return d.mapKeyString(&callFnRvk, &kstrbs, &kstr2bs)
	}

	// Use a possibly transient (map) value (and key), to reduce allocation

	for j := 0; d.containerNext(j, containerLen, hasLen); j++ {
		callFnRvk = false
		if j == 0 {
			// if vtypekind is a scalar and thus value will be decoded using TransientAddrK,
			// then it is ok to use TransientAddr2K for the map key.
			if decUseTransient && vTransient && kTransient {
				rvk = d.perType.TransientAddr2K(ktype, ktypeKind)
			} else {
				rvk = rvZeroAddrK(ktype, ktypeKind)
			}
			if !rvkMut {
				rvkn = rvk
			}
			if !rvvMut {
				if decUseTransient && vTransient {
					rvvn = d.perType.TransientAddrK(vtype, vtypeKind)
				} else {
					rvvn = rvZeroAddrK(vtype, vtypeKind)
				}
			}
			if !ktypeIsString && keyFn == nil {
				keyFn = d.h.fn(ktypeLo)
			}
			if valFn == nil {
				valFn = d.h.fn(vtypeLo)
			}
		} else if rvkMut {
			rvSetZero(rvk)
		} else {
			rvk = rvkn
		}

		d.mapElemKey()
		if ktypeIsString {
			kstr2bs = d.d.DecodeStringAsBytes()
			rvSetString(rvk, fnRvk2())
		} else {
			d.decByteState = decByteStateNone
			d.decodeValue(rvk, keyFn)
			// special case if interface wrapping a byte slice
			if ktypeIsIntf {
				if rvk2 := rvk.Elem(); rvk2.IsValid() && rvk2.Type() == uint8SliceTyp {
					kstr2bs = rvGetBytes(rvk2)
					rvSetIntf(rvk, rv4istr(fnRvk2()))
				}
				// NOTE: consider failing early if map/slice/func
			}
		}

		d.mapElemValue()

		if d.d.TryNil() {
			// since a map, we have to set zero value if needed
			if !rvvz.IsValid() {
				rvvz = rvZeroK(vtype, vtypeKind)
			}
			if callFnRvk {
				s = d.string(kstr2bs)
				if ktypeIsString {
					rvSetString(rvk, s)
				} else { // ktypeIsIntf
					rvSetIntf(rvk, rv4istr(s))
				}
			}
			mapSet(rv, rvk, rvvz, kfast, visindirect, visref)
			continue
		}

		// there is non-nil content in the stream to decode ...
		// consequently, it's ok to just directly create new value to the pointer (if vtypePtr)

		// set doMapSet to false iff u do a get, and the return value is a non-nil pointer
		doMapSet = true

		if !rvvMut {
			rvv = rvvn
		} else if !doMapGet {
			goto NEW_RVV
		} else {
			rvv = mapGet(rv, rvk, rvva, kfast, visindirect, visref)
			if !rvv.IsValid() || (rvvCanNil && rvIsNil(rvv)) {
				goto NEW_RVV
			}
			switch vtypeKind {
			case reflect.Ptr, reflect.Map: // ok to decode directly into map
				doMapSet = false
			case reflect.Interface:
				// if an interface{}, just decode into it iff a non-nil ptr/map, else allocate afresh
				rvvn = rvv.Elem()
				if k := rvvn.Kind(); (k == reflect.Ptr || k == reflect.Map) && !rvIsNil(rvvn) {
					d.decodeValueNoCheckNil(rvvn, nil) // valFn is incorrect here
					continue
				}
				// make addressable (so we can set the interface)
				rvvn = rvZeroAddrK(vtype, vtypeKind)
				rvSetIntf(rvvn, rvv)
				rvv = rvvn
			default:
				// make addressable (so you can set the slice/array elements, etc)
				if decUseTransient && vTransient {
					rvvn = d.perType.TransientAddrK(vtype, vtypeKind)
				} else {
					rvvn = rvZeroAddrK(vtype, vtypeKind)
				}
				rvSetDirect(rvvn, rvv)
				rvv = rvvn
			}
		}
		goto DECODE_VALUE_NO_CHECK_NIL

	NEW_RVV:
		if vtypePtr {
			rvv = reflect.New(vtypeElem) // non-nil in stream, so allocate value
		} else if decUseTransient && vTransient {
			rvv = d.perType.TransientAddrK(vtype, vtypeKind)
		} else {
			rvv = rvZeroAddrK(vtype, vtypeKind)
		}

	DECODE_VALUE_NO_CHECK_NIL:
		d.decodeValueNoCheckNil(rvv, valFn)

		if doMapSet {
			if callFnRvk {
				s = d.string(kstr2bs)
				if ktypeIsString {
					rvSetString(rvk, s)
				} else { // ktypeIsIntf
					rvSetIntf(rvk, rv4istr(s))
				}
			}
			mapSet(rv, rvk, rvv, kfast, visindirect, visref)
		}
	}

	d.mapEnd()
}

// Decoder reads and decodes an object from an input stream in a supported format.
//
// Decoder is NOT safe for concurrent use i.e. a Decoder cannot be used
// concurrently in multiple goroutines.
//
// However, as Decoder could be allocation heavy to initialize, a Reset method is provided
// so its state can be reused to decode new input streams repeatedly.
// This is the idiomatic way to use.
type Decoder struct {
	panicHdl

	d decDriver

	// cache the mapTypeId and sliceTypeId for faster comparisons
	mtid uintptr
	stid uintptr

	h *BasicHandle

	blist bytesFreelist

	// ---- cpu cache line boundary?
	decRd

	// ---- cpu cache line boundary?
	n fauxUnion

	hh  Handle
	err error

	perType decPerType

	// used for interning strings
	is internerMap

	// ---- cpu cache line boundary?
	// ---- writable fields during execution --- *try* to keep in sep cache line
	maxdepth int16
	depth    int16

	// Extensions can call Decode() within a current Decode() call.
	// We need to know when the top level Decode() call returns,
	// so we can decide whether to Release() or not.
	calls uint16 // what depth in mustDecode are we in now.

	c containerState

	decByteState

	// b is an always-available scratch buffer used by Decoder and decDrivers.
	// By being always-available, it can be used for one-off things without
	// having to get from freelist, use, and return back to freelist.
	b [decScratchByteArrayLen]byte
}

// NewDecoder returns a Decoder for decoding a stream of bytes from an io.Reader.
//
// For efficiency, Users are encouraged to configure ReaderBufferSize on the handle
// OR pass in a memory buffered reader (eg bufio.Reader, bytes.Buffer).
func NewDecoder(r io.Reader, h Handle) *Decoder {
	d := h.newDecDriver().decoder()
	if r != nil {
		d.Reset(r)
	}
	return d
}

// NewDecoderBytes returns a Decoder which efficiently decodes directly
// from a byte slice with zero copying.
func NewDecoderBytes(in []byte, h Handle) *Decoder {
	d := h.newDecDriver().decoder()
	if in != nil {
		d.ResetBytes(in)
	}
	return d
}

// NewDecoderString returns a Decoder which efficiently decodes directly
// from a string with zero copying.
//
// It is a convenience function that calls NewDecoderBytes with a
// []byte view into the string.
//
// This can be an efficient zero-copy if using default mode i.e. without codec.safe tag.
func NewDecoderString(s string, h Handle) *Decoder {
	return NewDecoderBytes(bytesView(s), h)
}

func (d *Decoder) r() *decRd {
	return &d.decRd
}

func (d *Decoder) init(h Handle) {
	initHandle(h)
	d.cbreak = d.js || d.cbor
	d.bytes = true
	d.err = errDecoderNotInitialized
	d.h = h.getBasicHandle()
	d.hh = h
	d.be = h.isBinary()
	if d.h.InternString && d.is == nil {
		d.is.init()
	}
	// NOTE: do not initialize d.n here. It is lazily initialized in d.naked()
}

func (d *Decoder) resetCommon() {
	d.d.reset()
	d.err = nil
	d.c = 0
	d.decByteState = decByteStateNone
	d.depth = 0
	d.calls = 0
	// reset all things which were cached from the Handle, but could change
	d.maxdepth = decDefMaxDepth
	if d.h.MaxDepth > 0 {
		d.maxdepth = d.h.MaxDepth
	}
	d.mtid = 0
	d.stid = 0
	d.mtr = false
	d.str = false
	if d.h.MapType != nil {
		d.mtid = rt2id(d.h.MapType)
		d.mtr = fastpathAvIndex(d.mtid) != -1
	}
	if d.h.SliceType != nil {
		d.stid = rt2id(d.h.SliceType)
		d.str = fastpathAvIndex(d.stid) != -1
	}
}

// Reset the Decoder with a new Reader to decode from,
// clearing all state from last run(s).
func (d *Decoder) Reset(r io.Reader) {
	if r == nil {
		r = &eofReader
	}
	d.bytes = false
	if d.ri == nil {
		d.ri = new(ioDecReader)
	}
	d.ri.reset(r, d.h.ReaderBufferSize, &d.blist)
	d.decReader = d.ri
	d.resetCommon()
}

// ResetBytes resets the Decoder with a new []byte to decode from,
// clearing all state from last run(s).
func (d *Decoder) ResetBytes(in []byte) {
	if in == nil {
		in = []byte{}
	}
	d.bytes = true
	d.decReader = &d.rb
	d.rb.reset(in)
	d.resetCommon()
}

// ResetString resets the Decoder with a new string to decode from,
// clearing all state from last run(s).
//
// It is a convenience function that calls ResetBytes with a
// []byte view into the string.
//
// This can be an efficient zero-copy if using default mode i.e. without codec.safe tag.
func (d *Decoder) ResetString(s string) {
	d.ResetBytes(bytesView(s))
}

func (d *Decoder) naked() *fauxUnion {
	return &d.n
}

// Decode decodes the stream from reader and stores the result in the
// value pointed to by v. v cannot be a nil pointer. v can also be
// a reflect.Value of a pointer.
//
// Note that a pointer to a nil interface is not a nil pointer.
// If you do not know what type of stream it is, pass in a pointer to a nil interface.
// We will decode and store a value in that nil interface.
//
// Sample usages:
//
//	// Decoding into a non-nil typed value
//	var f float32
//	err = codec.NewDecoder(r, handle).Decode(&f)
//
//	// Decoding into nil interface
//	var v interface{}
//	dec := codec.NewDecoder(r, handle)
//	err = dec.Decode(&v)
//
// When decoding into a nil interface{}, we will decode into an appropriate value based
// on the contents of the stream:
//   - Numbers are decoded as float64, int64 or uint64.
//   - Other values are decoded appropriately depending on the type:
//     bool, string, []byte, time.Time, etc
//   - Extensions are decoded as RawExt (if no ext function registered for the tag)
//
// Configurations exist on the Handle to override defaults
// (e.g. for MapType, SliceType and how to decode raw bytes).
//
// When decoding into a non-nil interface{} value, the mode of encoding is based on the
// type of the value. When a value is seen:
//   - If an extension is registered for it, call that extension function
//   - If it implements BinaryUnmarshaler, call its UnmarshalBinary(data []byte) error
//   - Else decode it based on its reflect.Kind
//
// There are some special rules when decoding into containers (slice/array/map/struct).
// Decode will typically use the stream contents to UPDATE the container i.e. the values
// in these containers will not be zero'ed before decoding.
//   - A map can be decoded from a stream map, by updating matching keys.
//   - A slice can be decoded from a stream array,
//     by updating the first n elements, where n is length of the stream.
//   - A slice can be decoded from a stream map, by decoding as if
//     it contains a sequence of key-value pairs.
//   - A struct can be decoded from a stream map, by updating matching fields.
//   - A struct can be decoded from a stream array,
//     by updating fields as they occur in the struct (by index).
//
// This in-place update maintains consistency in the decoding philosophy (i.e. we ALWAYS update
// in place by default). However, the consequence of this is that values in slices or maps
// which are not zero'ed before hand, will have part of the prior values in place after decode
// if the stream doesn't contain an update for those parts.
//
// This in-place update can be disabled by configuring the MapValueReset and SliceElementReset
// decode options available on every handle.
//
// Furthermore, when decoding a stream map or array with length of 0 into a nil map or slice,
// we reset the destination map or slice to a zero-length value.
//
// However, when decoding a stream nil, we reset the destination container
// to its "zero" value (e.g. nil for slice/map, etc).
//
// Note: we allow nil values in the stream anywhere except for map keys.
// A nil value in the encoded stream where a map key is expected is treated as an error.
func (d *Decoder) Decode(v interface{}) (err error) {
	// tried to use closure, as runtime optimizes defer with no params.
	// This seemed to be causing weird issues (like circular reference found, unexpected panic, etc).
	// Also, see https://github.com/golang/go/issues/14939#issuecomment-417836139
	if !debugging {
		defer func() {
			if x := recover(); x != nil {
				panicValToErr(d, x, &d.err)
				err = d.err
			}
		}()
	}

	d.MustDecode(v)
	return
}

// MustDecode is like Decode, but panics if unable to Decode.
//
// Note: This provides insight to the code location that triggered the error.
func (d *Decoder) MustDecode(v interface{}) {
	halt.onerror(d.err)
	if d.hh == nil {
		halt.onerror(errNoFormatHandle)
	}

	// Top-level: v is a pointer and not nil.
	d.calls++
	d.decode(v)
	d.calls--
}

// Release releases shared (pooled) resources.
//
// It is important to call Release() when done with a Decoder, so those resources
// are released instantly for use by subsequently created Decoders.
//
// By default, Release() is automatically called unless the option ExplicitRelease is set.
//
// Deprecated: Release is a no-op as pooled resources are not used with an Decoder.
// This method is kept for compatibility reasons only.
func (d *Decoder) Release() {
}

func (d *Decoder) swallow() {
	d.d.nextValueBytes(nil)
}

func (d *Decoder) swallowErr() (err error) {
	if !debugging {
		defer func() {
			if x := recover(); x != nil {
				panicValToErr(d, x, &err)
			}
		}()
	}
	d.swallow()
	return
}

func setZero(iv interface{}) {
	if iv == nil {
		return
	}
	rv, ok := isNil(iv)
	if ok {
		return
	}
	// var canDecode bool
	switch v := iv.(type) {
	case *string:
		*v = ""
	case *bool:
		*v = false
	case *int:
		*v = 0
	case *int8:
		*v = 0
	case *int16:
		*v = 0
	case *int32:
		*v = 0
	case *int64:
		*v = 0
	case *uint:
		*v = 0
	case *uint8:
		*v = 0
	case *uint16:
		*v = 0
	case *uint32:
		*v = 0
	case *uint64:
		*v = 0
	case *float32:
		*v = 0
	case *float64:
		*v = 0
	case *complex64:
		*v = 0
	case *complex128:
		*v = 0
	case *[]byte:
		*v = nil
	case *Raw:
		*v = nil
	case *time.Time:
		*v = time.Time{}
	case reflect.Value:
		decSetNonNilRV2Zero(v)
	default:
		if !fastpathDecodeSetZeroTypeSwitch(iv) {
			decSetNonNilRV2Zero(rv)
		}
	}
}

// decSetNonNilRV2Zero will set the non-nil value to its zero value.
func decSetNonNilRV2Zero(v reflect.Value) {
	// If not decodeable (settable), we do not touch it.
	// We considered empty'ing it if not decodeable e.g.
	//    - if chan, drain it
	//    - if map, clear it
	//    - if slice or array, zero all elements up to len
	//
	// However, we decided instead that we either will set the
	// whole value to the zero value, or leave AS IS.

	k := v.Kind()
	if k == reflect.Interface {
		decSetNonNilRV2Zero4Intf(v)
	} else if k == reflect.Ptr {
		decSetNonNilRV2Zero4Ptr(v)
	} else if v.CanSet() {
		rvSetDirectZero(v)
	}
}

func decSetNonNilRV2Zero4Ptr(v reflect.Value) {
	ve := v.Elem()
	if ve.CanSet() {
		rvSetZero(ve) // we can have a pointer to an interface
	} else if v.CanSet() {
		rvSetZero(v)
	}
}

func decSetNonNilRV2Zero4Intf(v reflect.Value) {
	ve := v.Elem()
	if ve.CanSet() {
		rvSetDirectZero(ve) // interfaces always have element as a non-interface
	} else if v.CanSet() {
		rvSetZero(v)
	}
}

func (d *Decoder) decode(iv interface{}) {
	// a switch with only concrete types can be optimized.
	// consequently, we deal with nil and interfaces outside the switch.

	if iv == nil {
		d.onerror(errCannotDecodeIntoNil)
	}

	switch v := iv.(type) {
	// case nil:
	// case Selfer:
	case reflect.Value:
		if x, _ := isDecodeable(v); !x {
			d.haltAsNotDecodeable(v)
		}
		d.decodeValue(v, nil)
	case *string:
		*v = d.stringZC(d.d.DecodeStringAsBytes())
	case *bool:
		*v = d.d.DecodeBool()
	case *int:
		*v = int(chkOvf.IntV(d.d.DecodeInt64(), intBitsize))
	case *int8:
		*v = int8(chkOvf.IntV(d.d.DecodeInt64(), 8))
	case *int16:
		*v = int16(chkOvf.IntV(d.d.DecodeInt64(), 16))
	case *int32:
		*v = int32(chkOvf.IntV(d.d.DecodeInt64(), 32))
	case *int64:
		*v = d.d.DecodeInt64()
	case *uint:
		*v = uint(chkOvf.UintV(d.d.DecodeUint64(), uintBitsize))
	case *uint8:
		*v = uint8(chkOvf.UintV(d.d.DecodeUint64(), 8))
	case *uint16:
		*v = uint16(chkOvf.UintV(d.d.DecodeUint64(), 16))
	case *uint32:
		*v = uint32(chkOvf.UintV(d.d.DecodeUint64(), 32))
	case *uint64:
		*v = d.d.DecodeUint64()
	case *float32:
		*v = d.decodeFloat32()
	case *float64:
		*v = d.d.DecodeFloat64()
	case *complex64:
		*v = complex(d.decodeFloat32(), 0)
	case *complex128:
		*v = complex(d.d.DecodeFloat64(), 0)
	case *[]byte:
		*v = d.decodeBytesInto(*v)
	case []byte:
		// not addressable byte slice, so do not decode into it past the length
		b := d.decodeBytesInto(v[:len(v):len(v)])
		if !(len(b) > 0 && len(b) == len(v) && &b[0] == &v[0]) { // not same slice
			copy(v, b)
		}
	case *time.Time:
		*v = d.d.DecodeTime()
	case *Raw:
		*v = d.rawBytes()

	case *interface{}:
		d.decodeValue(rv4iptr(v), nil)

	default:
		// we can't check non-predefined types, as they might be a Selfer or extension.
		if skipFastpathTypeSwitchInDirectCall || !fastpathDecodeTypeSwitch(iv, d) {
			v := reflect.ValueOf(iv)
			if x, _ := isDecodeable(v); !x {
				d.haltAsNotDecodeable(v)
			}
			d.decodeValue(v, nil)
		}
	}
}

// decodeValue MUST be called by the actual value we want to decode into,
// not its addr or a reference to it.
//
// This way, we know if it is itself a pointer, and can handle nil in
// the stream effectively.
//
// Note that decodeValue will handle nil in the stream early, so that the
// subsequent calls i.e. kXXX methods, etc do not have to handle it themselves.
func (d *Decoder) decodeValue(rv reflect.Value, fn *codecFn) {
	if d.d.TryNil() {
		decSetNonNilRV2Zero(rv)
		return
	}
	d.decodeValueNoCheckNil(rv, fn)
}

func (d *Decoder) decodeValueNoCheckNil(rv reflect.Value, fn *codecFn) {
	// If stream is not containing a nil value, then we can deref to the base
	// non-pointer value, and decode into that.
	var rvp reflect.Value
	var rvpValid bool
PTR:
	if rv.Kind() == reflect.Ptr {
		rvpValid = true
		if rvIsNil(rv) {
			rvSetDirect(rv, reflect.New(rv.Type().Elem()))
		}
		rvp = rv
		rv = rv.Elem()
		goto PTR
	}

	if fn == nil {
		fn = d.h.fn(rv.Type())
	}
	if fn.i.addrD {
		if rvpValid {
			rv = rvp
		} else if rv.CanAddr() {
			rv = rvAddr(rv, fn.i.ti.ptr)
		} else if fn.i.addrDf {
			d.errorf("cannot decode into a non-pointer value")
		}
	}
	fn.fd(d, &fn.i, rv)
}

func (d *Decoder) structFieldNotFound(index int, rvkencname string) {
	// Note: rvkencname is used only if there is an error, to pass into d.errorf.
	// Consequently, it is ok to pass in a stringView
	// Since rvkencname may be a stringView, do NOT pass it to another function.
	if d.h.ErrorIfNoField {
		if index >= 0 {
			d.errorf("no matching struct field found when decoding stream array at index %v", index)
		} else if rvkencname != "" {
			d.errorf("no matching struct field found when decoding stream map with key " + rvkencname)
		}
	}
	d.swallow()
}

func (d *Decoder) arrayCannotExpand(sliceLen, streamLen int) {
	if d.h.ErrorIfNoArrayExpand {
		d.errorf("cannot expand array len during decode from %v to %v", sliceLen, streamLen)
	}
}

func (d *Decoder) haltAsNotDecodeable(rv reflect.Value) {
	if !rv.IsValid() {
		d.onerror(errCannotDecodeIntoNil)
	}
	// check if an interface can be retrieved, before grabbing an interface
	if !rv.CanInterface() {
		d.errorf("cannot decode into a value without an interface: %v", rv)
	}
	d.errorf("cannot decode into value of kind: %v, %#v", rv.Kind(), rv2i(rv))
}

func (d *Decoder) depthIncr() {
	d.depth++
	if d.depth >= d.maxdepth {
		d.onerror(errMaxDepthExceeded)
	}
}

func (d *Decoder) depthDecr() {
	d.depth--
}

// Possibly get an interned version of a string, iff InternString=true and decoding a map key.
//
// This should mostly be used for map keys, where the key type is string.
// This is because keys of a map/struct are typically reused across many objects.
func (d *Decoder) string(v []byte) (s string) {
	if d.is == nil || d.c != containerMapKey || len(v) < 2 || len(v) > internMaxStrLen {
		return string(v)
	}
	return d.is.string(v)
}

func (d *Decoder) zerocopy() bool {
	return d.bytes && d.h.ZeroCopy
}

// decodeBytesInto is a convenience delegate function to decDriver.DecodeBytes.
// It ensures that `in` is not a nil byte, before calling decDriver.DecodeBytes,
// as decDriver.DecodeBytes treats a nil as a hint to use its internal scratch buffer.
func (d *Decoder) decodeBytesInto(in []byte) (v []byte) {
	if in == nil {
		in = []byte{}
	}
	return d.d.DecodeBytes(in)
}

func (d *Decoder) rawBytes() (v []byte) {
	// ensure that this is not a view into the bytes
	// i.e. if necessary, make new copy always.
	v = d.d.nextValueBytes([]byte{})
	if d.bytes && !d.h.ZeroCopy {
		vv := make([]byte, len(v))
		copy(vv, v) // using copy here triggers make+copy optimization eliding memclr
		v = vv
	}
	return
}

func (d *Decoder) wrapErr(v error, err *error) {
	*err = wrapCodecErr(v, d.hh.Name(), d.NumBytesRead(), false)
}

// NumBytesRead returns the number of bytes read
func (d *Decoder) NumBytesRead() int {
	return int(d.r().numread())
}

// decodeFloat32 will delegate to an appropriate DecodeFloat32 implementation (if exists),
// else if will call DecodeFloat64 and ensure the value doesn't overflow.
//
// Note that we return float64 to reduce unnecessary conversions
func (d *Decoder) decodeFloat32() float32 {
	if d.js {
		return d.jsondriver().DecodeFloat32() // custom implementation for 32-bit
	}
	return float32(chkOvf.Float32V(d.d.DecodeFloat64()))
}

// ---- container tracking
// Note: We update the .c after calling the callback.
// This way, the callback can know what the last status was.

// MARKER: do not call mapEnd if mapStart returns containerLenNil.

// MARKER: optimize decoding since all formats do not truly support all decDriver'ish operations.
// - Read(Map|Array)Start is only supported by all formats.
// - CheckBreak is only supported by json and cbor.
// - Read(Map|Array)End is only supported by json.
// - Read(Map|Array)Elem(Kay|Value) is only supported by json.
// Honor these in the code, to reduce the number of interface calls (even if empty).

func (d *Decoder) checkBreak() (v bool) {
	// MARKER: jsonDecDriver.CheckBreak() cannot be inlined (over budget inlining cost).
	// Consequently, there's no benefit in incurring the cost of this wrapping function.
	// It is faster to just call the interface method directly.

	// if d.js {
	// 	return d.jsondriver().CheckBreak()
	// }
	// if d.cbor {
	// 	return d.cbordriver().CheckBreak()
	// }

	if d.cbreak {
		v = d.d.CheckBreak()
	}
	return
}

func (d *Decoder) containerNext(j, containerLen int, hasLen bool) bool {
	// MARKER: keep in sync with gen-helper.go.tmpl

	// return (hasLen && j < containerLen) || !(hasLen || slh.d.checkBreak())
	if hasLen {
		return j < containerLen
	}
	return !d.checkBreak()
}

func (d *Decoder) mapStart(v int) int {
	if v != containerLenNil {
		d.depthIncr()
		d.c = containerMapStart
	}
	return v
}

func (d *Decoder) mapElemKey() {
	if d.js {
		d.jsondriver().ReadMapElemKey()
	}
	d.c = containerMapKey
}

func (d *Decoder) mapElemValue() {
	if d.js {
		d.jsondriver().ReadMapElemValue()
	}
	d.c = containerMapValue
}

func (d *Decoder) mapEnd() {
	if d.js {
		d.jsondriver().ReadMapEnd()
	}
	// d.d.ReadMapEnd()
	d.depthDecr()
	d.c = 0
}

func (d *Decoder) arrayStart(v int) int {
	if v != containerLenNil {
		d.depthIncr()
		d.c = containerArrayStart
	}
	return v
}

func (d *Decoder) arrayElem() {
	if d.js {
		d.jsondriver().ReadArrayElem()
	}
	d.c = containerArrayElem
}

func (d *Decoder) arrayEnd() {
	if d.js {
		d.jsondriver().ReadArrayEnd()
	}
	// d.d.ReadArrayEnd()
	d.depthDecr()
	d.c = 0
}

func (d *Decoder) interfaceExtConvertAndDecode(v interface{}, ext InterfaceExt) {
	// var v interface{} = ext.ConvertExt(rv)
	// d.d.decode(&v)
	// ext.UpdateExt(rv, v)

	// assume v is a pointer:
	// - if struct|array, pass as is to ConvertExt
	// - else make it non-addressable and pass to ConvertExt
	// - make return value from ConvertExt addressable
	// - decode into it
	// - return the interface for passing into UpdateExt.
	// - interface should be a pointer if struct|array, else a value

	var s interface{}
	rv := reflect.ValueOf(v)
	rv2 := rv.Elem()
	rvk := rv2.Kind()
	if rvk == reflect.Struct || rvk == reflect.Array {
		s = ext.ConvertExt(v)
	} else {
		s = ext.ConvertExt(rv2i(rv2))
	}
	rv = reflect.ValueOf(s)

	// We cannot use isDecodeable here, as the value converted may be nil,
	// or it may not be nil but is not addressable and thus we cannot extend it, etc.
	// Instead, we just ensure that the value is addressable.

	if !rv.CanAddr() {
		rvk = rv.Kind()
		rv2 = d.oneShotAddrRV(rv.Type(), rvk)
		if rvk == reflect.Interface {
			rvSetIntf(rv2, rv)
		} else {
			rvSetDirect(rv2, rv)
		}
		rv = rv2
	}

	d.decodeValue(rv, nil)
	ext.UpdateExt(v, rv2i(rv))
}

func (d *Decoder) sideDecode(v interface{}, basetype reflect.Type, bs []byte) {
	// NewDecoderBytes(bs, d.hh).decodeValue(baseRV(v), d.h.fnNoExt(basetype))

	defer func(rb bytesDecReader, bytes bool,
		c containerState, dbs decByteState, depth int16, r decReader, state interface{}) {
		d.rb = rb
		d.bytes = bytes
		d.c = c
		d.decByteState = dbs
		d.depth = depth
		d.decReader = r
		d.d.restoreState(state)
	}(d.rb, d.bytes, d.c, d.decByteState, d.depth, d.decReader, d.d.captureState())

	// d.rb.reset(in)
	d.rb = bytesDecReader{bs[:len(bs):len(bs)], 0}
	d.bytes = true
	d.decReader = &d.rb
	d.d.resetState()
	d.c = 0
	d.decByteState = decByteStateNone
	d.depth = 0

	// must call using fnNoExt
	d.decodeValue(baseRV(v), d.h.fnNoExt(basetype))
}

func (d *Decoder) fauxUnionReadRawBytes(asString bool) {
	if asString || d.h.RawToString {
		d.n.v = valueTypeString
		// fauxUnion is only used within DecodeNaked calls; consequently, we should try to intern.
		d.n.s = d.stringZC(d.d.DecodeBytes(nil))
	} else {
		d.n.v = valueTypeBytes
		d.n.l = d.d.DecodeBytes([]byte{})
	}
}

func (d *Decoder) oneShotAddrRV(rvt reflect.Type, rvk reflect.Kind) reflect.Value {
	if decUseTransient &&
		(numBoolStrSliceBitset.isset(byte(rvk)) ||
			((rvk == reflect.Struct || rvk == reflect.Array) &&
				d.h.getTypeInfo(rt2id(rvt), rvt).flagCanTransient)) {
		return d.perType.TransientAddrK(rvt, rvk)
	}
	return rvZeroAddrK(rvt, rvk)
}

// --------------------------------------------------

// decSliceHelper assists when decoding into a slice, from a map or an array in the stream.
// A slice can be set from a map or array in stream. This supports the MapBySlice interface.
//
// Note: if IsNil, do not call ElemContainerState.
type decSliceHelper struct {
	d     *Decoder
	ct    valueType
	Array bool
	IsNil bool
}

func (d *Decoder) decSliceHelperStart() (x decSliceHelper, clen int) {
	x.ct = d.d.ContainerType()
	x.d = d
	switch x.ct {
	case valueTypeNil:
		x.IsNil = true
	case valueTypeArray:
		x.Array = true
		clen = d.arrayStart(d.d.ReadArrayStart())
	case valueTypeMap:
		clen = d.mapStart(d.d.ReadMapStart())
		clen += clen
	default:
		d.errorf("only encoded map or array can be decoded into a slice (%d)", x.ct)
	}
	return
}

func (x decSliceHelper) End() {
	if x.IsNil {
	} else if x.Array {
		x.d.arrayEnd()
	} else {
		x.d.mapEnd()
	}
}

func (x decSliceHelper) ElemContainerState(index int) {
	// Note: if isnil, clen=0, so we never call into ElemContainerState

	if x.Array {
		x.d.arrayElem()
	} else if index&1 == 0 { // index%2 == 0 {
		x.d.mapElemKey()
	} else {
		x.d.mapElemValue()
	}
}

func (x decSliceHelper) arrayCannotExpand(hasLen bool, lenv, j, containerLenS int) {
	x.d.arrayCannotExpand(lenv, j+1)
	// drain completely and return
	x.ElemContainerState(j)
	x.d.swallow()
	j++
	for ; x.d.containerNext(j, containerLenS, hasLen); j++ {
		x.ElemContainerState(j)
		x.d.swallow()
	}
	x.End()
}

// decNextValueBytesHelper helps with NextValueBytes calls.
//
// Typical usage:
//   - each Handle's decDriver will implement a high level nextValueBytes,
//     which will track the current cursor, delegate to a nextValueBytesR
//     method, and then potentially call bytesRdV at the end.
//
// See simple.go for typical usage model.
type decNextValueBytesHelper struct {
	d *Decoder
}

func (x decNextValueBytesHelper) append1(v *[]byte, b byte) {
	if *v != nil && !x.d.bytes {
		*v = append(*v, b)
	}
}

func (x decNextValueBytesHelper) appendN(v *[]byte, b ...byte) {
	if *v != nil && !x.d.bytes {
		*v = append(*v, b...)
	}
}

func (x decNextValueBytesHelper) appendS(v *[]byte, b string) {
	if *v != nil && !x.d.bytes {
		*v = append(*v, b...)
	}
}

func (x decNextValueBytesHelper) bytesRdV(v *[]byte, startpos uint) {
	if x.d.bytes {
		*v = x.d.rb.b[startpos:x.d.rb.c]
	}
}

// decNegintPosintFloatNumberHelper is used for formats that are binary
// and have distinct ways of storing positive integers vs negative integers
// vs floats, which are uniquely identified by the byte descriptor.
//
// Currently, these formats are binc, cbor and simple.
type decNegintPosintFloatNumberHelper struct {
	d *Decoder
}

func (x decNegintPosintFloatNumberHelper) uint64(ui uint64, neg, ok bool) uint64 {
	if ok && !neg {
		return ui
	}
	return x.uint64TryFloat(ok)
}

func (x decNegintPosintFloatNumberHelper) uint64TryFloat(ok bool) (ui uint64) {
	if ok { // neg = true
		x.d.errorf("assigning negative signed value to unsigned type")
	}
	f, ok := x.d.d.decFloat()
	if ok && f >= 0 && noFrac64(math.Float64bits(f)) {
		ui = uint64(f)
	} else {
		x.d.errorf("invalid number loading uint64, with descriptor: %v", x.d.d.descBd())
	}
	return ui
}

func decNegintPosintFloatNumberHelperInt64v(ui uint64, neg, incrIfNeg bool) (i int64) {
	if neg && incrIfNeg {
		ui++
	}
	i = chkOvf.SignedIntV(ui)
	if neg {
		i = -i
	}
	return
}

func (x decNegintPosintFloatNumberHelper) int64(ui uint64, neg, ok bool) (i int64) {
	if ok {
		return decNegintPosintFloatNumberHelperInt64v(ui, neg, x.d.cbor)
	}
	// 	return x.int64TryFloat()
	// }
	// func (x decNegintPosintFloatNumberHelper) int64TryFloat() (i int64) {
	f, ok := x.d.d.decFloat()
	if ok && noFrac64(math.Float64bits(f)) {
		i = int64(f)
	} else {
		x.d.errorf("invalid number loading uint64, with descriptor: %v", x.d.d.descBd())
	}
	return
}

func (x decNegintPosintFloatNumberHelper) float64(f float64, ok bool) float64 {
	if ok {
		return f
	}
	return x.float64TryInteger()
}

func (x decNegintPosintFloatNumberHelper) float64TryInteger() float64 {
	ui, neg, ok := x.d.d.decInteger()
	if !ok {
		x.d.errorf("invalid descriptor for float: %v", x.d.d.descBd())
	}
	return float64(decNegintPosintFloatNumberHelperInt64v(ui, neg, x.d.cbor))
}

// isDecodeable checks if value can be decoded into
//
// decode can take any reflect.Value that is a inherently addressable i.e.
//   - non-nil chan    (we will SEND to it)
//   - non-nil slice   (we will set its elements)
//   - non-nil map     (we will put into it)
//   - non-nil pointer (we can "update" it)
//   - func: no
//   - interface: no
//   - array:                   if canAddr=true
//   - any other value pointer: if canAddr=true
func isDecodeable(rv reflect.Value) (canDecode bool, reason decNotDecodeableReason) {
	switch rv.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Chan, reflect.Map:
		canDecode = !rvIsNil(rv)
		reason = decNotDecodeableReasonNilReference
	case reflect.Func, reflect.Interface, reflect.Invalid, reflect.UnsafePointer:
		reason = decNotDecodeableReasonBadKind
	default:
		canDecode = rv.CanAddr()
		reason = decNotDecodeableReasonNonAddrValue
	}
	return
}

func decByteSlice(r *decRd, clen, maxInitLen int, bs []byte) (bsOut []byte) {
	if clen <= 0 {
		bsOut = zeroByteSlice
	} else if cap(bs) >= clen {
		bsOut = bs[:clen]
		r.readb(bsOut)
	} else {
		var len2 int
		for len2 < clen {
			len3 := decInferLen(clen-len2, maxInitLen, 1)
			bs3 := bsOut
			bsOut = make([]byte, len2+len3)
			copy(bsOut, bs3)
			r.readb(bsOut[len2:])
			len2 += len3
		}
	}
	return
}

// decInferLen will infer a sensible length, given the following:
//   - clen: length wanted.
//   - maxlen: max length to be returned.
//     if <= 0, it is unset, and we infer it based on the unit size
//   - unit: number of bytes for each element of the collection
func decInferLen(clen, maxlen, unit int) int {
	// anecdotal testing showed increase in allocation with map length of 16.
	// We saw same typical alloc from 0-8, then a 20% increase at 16.
	// Thus, we set it to 8.
	const (
		minLenIfUnset = 8
		maxMem        = 256 * 1024 // 256Kb Memory
	)

	// handle when maxlen is not set i.e. <= 0

	// clen==0:           use 0
	// maxlen<=0, clen<0: use default
	// maxlen> 0, clen<0: use default
	// maxlen<=0, clen>0: infer maxlen, and cap on it
	// maxlen> 0, clen>0: cap at maxlen

	if clen == 0 || clen == containerLenNil {
		return 0
	}
	if clen < 0 {
		// if unspecified, return 64 for bytes, ... 8 for uint64, ... and everything else
		clen = 64 / unit
		if clen > minLenIfUnset {
			return clen
		}
		return minLenIfUnset
	}
	if unit <= 0 {
		return clen
	}
	if maxlen <= 0 {
		maxlen = maxMem / unit
	}
	if clen < maxlen {
		return clen
	}
	return maxlen
}
