// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

import (
	"encoding"
	"errors"
	"io"
	"reflect"
	"sort"
	"strconv"
	"time"
)

// defEncByteBufSize is the default size of []byte used
// for bufio buffer or []byte (when nil passed)
const defEncByteBufSize = 1 << 10 // 4:16, 6:64, 8:256, 10:1024

var errEncoderNotInitialized = errors.New("Encoder not initialized")

// encDriver abstracts the actual codec (binc vs msgpack, etc)
type encDriver interface {
	EncodeNil()
	EncodeInt(i int64)
	EncodeUint(i uint64)
	EncodeBool(b bool)
	EncodeFloat32(f float32)
	EncodeFloat64(f float64)
	EncodeRawExt(re *RawExt)
	EncodeExt(v interface{}, basetype reflect.Type, xtag uint64, ext Ext)
	// EncodeString using cUTF8, honor'ing StringToRaw flag
	EncodeString(v string)
	EncodeStringBytesRaw(v []byte)
	EncodeTime(time.Time)
	WriteArrayStart(length int)
	WriteArrayEnd()
	WriteMapStart(length int)
	WriteMapEnd()

	// reset will reset current encoding runtime state, and cached information from the handle
	reset()

	encoder() *Encoder

	driverStateManager
}

type encDriverContainerTracker interface {
	WriteArrayElem()
	WriteMapElemKey()
	WriteMapElemValue()
}

type encDriverNoState struct{}

func (encDriverNoState) captureState() interface{}  { return nil }
func (encDriverNoState) reset()                     {}
func (encDriverNoState) resetState()                {}
func (encDriverNoState) restoreState(v interface{}) {}

type encDriverNoopContainerWriter struct{}

func (encDriverNoopContainerWriter) WriteArrayStart(length int) {}
func (encDriverNoopContainerWriter) WriteArrayEnd()             {}
func (encDriverNoopContainerWriter) WriteMapStart(length int)   {}
func (encDriverNoopContainerWriter) WriteMapEnd()               {}

// encStructFieldObj[Slice] is used for sorting when there are missing fields and canonical flag is set
type encStructFieldObj struct {
	key   string
	rv    reflect.Value
	intf  interface{}
	ascii bool
	isRv  bool
}

type encStructFieldObjSlice []encStructFieldObj

func (p encStructFieldObjSlice) Len() int      { return len(p) }
func (p encStructFieldObjSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p encStructFieldObjSlice) Less(i, j int) bool {
	return p[uint(i)].key < p[uint(j)].key
}

// EncodeOptions captures configuration options during encode.
type EncodeOptions struct {
	// WriterBufferSize is the size of the buffer used when writing.
	//
	// if > 0, we use a smart buffer internally for performance purposes.
	WriterBufferSize int

	// ChanRecvTimeout is the timeout used when selecting from a chan.
	//
	// Configuring this controls how we receive from a chan during the encoding process.
	//   - If ==0, we only consume the elements currently available in the chan.
	//   - if  <0, we consume until the chan is closed.
	//   - If  >0, we consume until this timeout.
	ChanRecvTimeout time.Duration

	// StructToArray specifies to encode a struct as an array, and not as a map
	StructToArray bool

	// Canonical representation means that encoding a value will always result in the same
	// sequence of bytes.
	//
	// This only affects maps, as the iteration order for maps is random.
	//
	// The implementation MAY use the natural sort order for the map keys if possible:
	//
	//     - If there is a natural sort order (ie for number, bool, string or []byte keys),
	//       then the map keys are first sorted in natural order and then written
	//       with corresponding map values to the strema.
	//     - If there is no natural sort order, then the map keys will first be
	//       encoded into []byte, and then sorted,
	//       before writing the sorted keys and the corresponding map values to the stream.
	//
	Canonical bool

	// CheckCircularRef controls whether we check for circular references
	// and error fast during an encode.
	//
	// If enabled, an error is received if a pointer to a struct
	// references itself either directly or through one of its fields (iteratively).
	//
	// This is opt-in, as there may be a performance hit to checking circular references.
	CheckCircularRef bool

	// RecursiveEmptyCheck controls how we determine whether a value is empty.
	//
	// If true, we descend into interfaces and pointers to reursively check if value is empty.
	//
	// We *might* check struct fields one by one to see if empty
	// (if we cannot directly check if a struct value is equal to its zero value).
	// If so, we honor IsZero, Comparable, IsCodecEmpty(), etc.
	// Note: This *may* make OmitEmpty more expensive due to the large number of reflect calls.
	//
	// If false, we check if the value is equal to its zero value (newly allocated state).
	RecursiveEmptyCheck bool

	// Raw controls whether we encode Raw values.
	// This is a "dangerous" option and must be explicitly set.
	// If set, we blindly encode Raw values as-is, without checking
	// if they are a correct representation of a value in that format.
	// If unset, we error out.
	Raw bool

	// StringToRaw controls how strings are encoded.
	//
	// As a go string is just an (immutable) sequence of bytes,
	// it can be encoded either as raw bytes or as a UTF string.
	//
	// By default, strings are encoded as UTF-8.
	// but can be treated as []byte during an encode.
	//
	// Note that things which we know (by definition) to be UTF-8
	// are ALWAYS encoded as UTF-8 strings.
	// These include encoding.TextMarshaler, time.Format calls, struct field names, etc.
	StringToRaw bool

	// OptimumSize controls whether we optimize for the smallest size.
	//
	// Some formats will use this flag to determine whether to encode
	// in the smallest size possible, even if it takes slightly longer.
	//
	// For example, some formats that support half-floats might check if it is possible
	// to store a float64 as a half float. Doing this check has a small performance cost,
	// but the benefit is that the encoded message will be smaller.
	OptimumSize bool

	// NoAddressableReadonly controls whether we try to force a non-addressable value
	// to be addressable so we can call a pointer method on it e.g. for types
	// that support Selfer, json.Marshaler, etc.
	//
	// Use it in the very rare occurrence that your types modify a pointer value when calling
	// an encode callback function e.g. JsonMarshal, TextMarshal, BinaryMarshal or CodecEncodeSelf.
	NoAddressableReadonly bool
}

// ---------------------------------------------

func (e *Encoder) rawExt(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeRawExt(rv2i(rv).(*RawExt))
}

func (e *Encoder) ext(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeExt(rv2i(rv), f.ti.rt, f.xfTag, f.xfFn)
}

func (e *Encoder) selferMarshal(f *codecFnInfo, rv reflect.Value) {
	rv2i(rv).(Selfer).CodecEncodeSelf(e)
}

func (e *Encoder) binaryMarshal(f *codecFnInfo, rv reflect.Value) {
	bs, fnerr := rv2i(rv).(encoding.BinaryMarshaler).MarshalBinary()
	e.marshalRaw(bs, fnerr)
}

func (e *Encoder) textMarshal(f *codecFnInfo, rv reflect.Value) {
	bs, fnerr := rv2i(rv).(encoding.TextMarshaler).MarshalText()
	e.marshalUtf8(bs, fnerr)
}

func (e *Encoder) jsonMarshal(f *codecFnInfo, rv reflect.Value) {
	bs, fnerr := rv2i(rv).(jsonMarshaler).MarshalJSON()
	e.marshalAsis(bs, fnerr)
}

func (e *Encoder) raw(f *codecFnInfo, rv reflect.Value) {
	e.rawBytes(rv2i(rv).(Raw))
}

func (e *Encoder) encodeComplex64(v complex64) {
	if imag(v) != 0 {
		e.errorf("cannot encode complex number: %v, with imaginary values: %v", v, imag(v))
	}
	e.e.EncodeFloat32(real(v))
}

func (e *Encoder) encodeComplex128(v complex128) {
	if imag(v) != 0 {
		e.errorf("cannot encode complex number: %v, with imaginary values: %v", v, imag(v))
	}
	e.e.EncodeFloat64(real(v))
}

func (e *Encoder) kBool(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeBool(rvGetBool(rv))
}

func (e *Encoder) kTime(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeTime(rvGetTime(rv))
}

func (e *Encoder) kString(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeString(rvGetString(rv))
}

func (e *Encoder) kFloat32(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeFloat32(rvGetFloat32(rv))
}

func (e *Encoder) kFloat64(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeFloat64(rvGetFloat64(rv))
}

func (e *Encoder) kComplex64(f *codecFnInfo, rv reflect.Value) {
	e.encodeComplex64(rvGetComplex64(rv))
}

func (e *Encoder) kComplex128(f *codecFnInfo, rv reflect.Value) {
	e.encodeComplex128(rvGetComplex128(rv))
}

func (e *Encoder) kInt(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeInt(int64(rvGetInt(rv)))
}

func (e *Encoder) kInt8(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeInt(int64(rvGetInt8(rv)))
}

func (e *Encoder) kInt16(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeInt(int64(rvGetInt16(rv)))
}

func (e *Encoder) kInt32(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeInt(int64(rvGetInt32(rv)))
}

func (e *Encoder) kInt64(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeInt(int64(rvGetInt64(rv)))
}

func (e *Encoder) kUint(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeUint(uint64(rvGetUint(rv)))
}

func (e *Encoder) kUint8(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeUint(uint64(rvGetUint8(rv)))
}

func (e *Encoder) kUint16(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeUint(uint64(rvGetUint16(rv)))
}

func (e *Encoder) kUint32(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeUint(uint64(rvGetUint32(rv)))
}

func (e *Encoder) kUint64(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeUint(uint64(rvGetUint64(rv)))
}

func (e *Encoder) kUintptr(f *codecFnInfo, rv reflect.Value) {
	e.e.EncodeUint(uint64(rvGetUintptr(rv)))
}

func (e *Encoder) kErr(f *codecFnInfo, rv reflect.Value) {
	e.errorf("unsupported kind %s, for %#v", rv.Kind(), rv)
}

func chanToSlice(rv reflect.Value, rtslice reflect.Type, timeout time.Duration) (rvcs reflect.Value) {
	rvcs = rvZeroK(rtslice, reflect.Slice)
	if timeout < 0 { // consume until close
		for {
			recv, recvOk := rv.Recv()
			if !recvOk {
				break
			}
			rvcs = reflect.Append(rvcs, recv)
		}
	} else {
		cases := make([]reflect.SelectCase, 2)
		cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: rv}
		if timeout == 0 {
			cases[1] = reflect.SelectCase{Dir: reflect.SelectDefault}
		} else {
			tt := time.NewTimer(timeout)
			cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tt.C)}
		}
		for {
			chosen, recv, recvOk := reflect.Select(cases)
			if chosen == 1 || !recvOk {
				break
			}
			rvcs = reflect.Append(rvcs, recv)
		}
	}
	return
}

func (e *Encoder) kSeqFn(rtelem reflect.Type) (fn *codecFn) {
	for rtelem.Kind() == reflect.Ptr {
		rtelem = rtelem.Elem()
	}
	// if kind is reflect.Interface, do not pre-determine the encoding type,
	// because preEncodeValue may break it down to a concrete type and kInterface will bomb.
	if rtelem.Kind() != reflect.Interface {
		fn = e.h.fn(rtelem)
	}
	return
}

func (e *Encoder) kSliceWMbs(rv reflect.Value, ti *typeInfo) {
	var l = rvLenSlice(rv)
	if l == 0 {
		e.mapStart(0)
	} else {
		e.haltOnMbsOddLen(l)
		e.mapStart(l >> 1) // e.mapStart(l / 2)
		fn := e.kSeqFn(ti.elem)
		for j := 0; j < l; j++ {
			if j&1 == 0 { // j%2 == 0 {
				e.mapElemKey()
			} else {
				e.mapElemValue()
			}
			e.encodeValue(rvSliceIndex(rv, j, ti), fn)
		}
	}
	e.mapEnd()
}

func (e *Encoder) kSliceW(rv reflect.Value, ti *typeInfo) {
	var l = rvLenSlice(rv)
	e.arrayStart(l)
	if l > 0 {
		fn := e.kSeqFn(ti.elem)
		for j := 0; j < l; j++ {
			e.arrayElem()
			e.encodeValue(rvSliceIndex(rv, j, ti), fn)
		}
	}
	e.arrayEnd()
}

func (e *Encoder) kArrayWMbs(rv reflect.Value, ti *typeInfo) {
	var l = rv.Len()
	if l == 0 {
		e.mapStart(0)
	} else {
		e.haltOnMbsOddLen(l)
		e.mapStart(l >> 1) // e.mapStart(l / 2)
		fn := e.kSeqFn(ti.elem)
		for j := 0; j < l; j++ {
			if j&1 == 0 { // j%2 == 0 {
				e.mapElemKey()
			} else {
				e.mapElemValue()
			}
			e.encodeValue(rv.Index(j), fn)
		}
	}
	e.mapEnd()
}

func (e *Encoder) kArrayW(rv reflect.Value, ti *typeInfo) {
	var l = rv.Len()
	e.arrayStart(l)
	if l > 0 {
		fn := e.kSeqFn(ti.elem)
		for j := 0; j < l; j++ {
			e.arrayElem()
			e.encodeValue(rv.Index(j), fn)
		}
	}
	e.arrayEnd()
}

func (e *Encoder) kChan(f *codecFnInfo, rv reflect.Value) {
	if f.ti.chandir&uint8(reflect.RecvDir) == 0 {
		e.errorf("send-only channel cannot be encoded")
	}
	if !f.ti.mbs && uint8TypId == rt2id(f.ti.elem) {
		e.kSliceBytesChan(rv)
		return
	}
	rtslice := reflect.SliceOf(f.ti.elem)
	rv = chanToSlice(rv, rtslice, e.h.ChanRecvTimeout)
	ti := e.h.getTypeInfo(rt2id(rtslice), rtslice)
	if f.ti.mbs {
		e.kSliceWMbs(rv, ti)
	} else {
		e.kSliceW(rv, ti)
	}
}

func (e *Encoder) kSlice(f *codecFnInfo, rv reflect.Value) {
	if f.ti.mbs {
		e.kSliceWMbs(rv, f.ti)
	} else if f.ti.rtid == uint8SliceTypId || uint8TypId == rt2id(f.ti.elem) {
		e.e.EncodeStringBytesRaw(rvGetBytes(rv))
	} else {
		e.kSliceW(rv, f.ti)
	}
}

func (e *Encoder) kArray(f *codecFnInfo, rv reflect.Value) {
	if f.ti.mbs {
		e.kArrayWMbs(rv, f.ti)
	} else if handleBytesWithinKArray && uint8TypId == rt2id(f.ti.elem) {
		e.e.EncodeStringBytesRaw(rvGetArrayBytes(rv, []byte{}))
	} else {
		e.kArrayW(rv, f.ti)
	}
}

func (e *Encoder) kSliceBytesChan(rv reflect.Value) {
	// do not use range, so that the number of elements encoded
	// does not change, and encoding does not hang waiting on someone to close chan.

	bs0 := e.blist.peek(32, true)
	bs := bs0

	irv := rv2i(rv)
	ch, ok := irv.(<-chan byte)
	if !ok {
		ch = irv.(chan byte)
	}

L1:
	switch timeout := e.h.ChanRecvTimeout; {
	case timeout == 0: // only consume available
		for {
			select {
			case b := <-ch:
				bs = append(bs, b)
			default:
				break L1
			}
		}
	case timeout > 0: // consume until timeout
		tt := time.NewTimer(timeout)
		for {
			select {
			case b := <-ch:
				bs = append(bs, b)
			case <-tt.C:
				// close(tt.C)
				break L1
			}
		}
	default: // consume until close
		for b := range ch {
			bs = append(bs, b)
		}
	}

	e.e.EncodeStringBytesRaw(bs)
	e.blist.put(bs)
	if !byteSliceSameData(bs0, bs) {
		e.blist.put(bs0)
	}
}

func (e *Encoder) kStructSfi(f *codecFnInfo) []*structFieldInfo {
	if e.h.Canonical {
		return f.ti.sfi.sorted()
	}
	return f.ti.sfi.source()
}

func (e *Encoder) kStructNoOmitempty(f *codecFnInfo, rv reflect.Value) {
	var tisfi []*structFieldInfo
	if f.ti.toArray || e.h.StructToArray { // toArray
		tisfi = f.ti.sfi.source()
		e.arrayStart(len(tisfi))
		for _, si := range tisfi {
			e.arrayElem()
			e.encodeValue(si.path.field(rv), nil)
		}
		e.arrayEnd()
	} else {
		tisfi = e.kStructSfi(f)
		e.mapStart(len(tisfi))
		keytyp := f.ti.keyType
		for _, si := range tisfi {
			e.mapElemKey()
			e.kStructFieldKey(keytyp, si.path.encNameAsciiAlphaNum, si.encName)
			e.mapElemValue()
			e.encodeValue(si.path.field(rv), nil)
		}
		e.mapEnd()
	}
}

func (e *Encoder) kStructFieldKey(keyType valueType, encNameAsciiAlphaNum bool, encName string) {
	encStructFieldKey(encName, e.e, e.w(), keyType, encNameAsciiAlphaNum, e.js)
}

func (e *Encoder) kStruct(f *codecFnInfo, rv reflect.Value) {
	var newlen int
	ti := f.ti
	toMap := !(ti.toArray || e.h.StructToArray)
	var mf map[string]interface{}
	if ti.flagMissingFielder {
		mf = rv2i(rv).(MissingFielder).CodecMissingFields()
		toMap = true
		newlen += len(mf)
	} else if ti.flagMissingFielderPtr {
		rv2 := e.addrRV(rv, ti.rt, ti.ptr)
		mf = rv2i(rv2).(MissingFielder).CodecMissingFields()
		toMap = true
		newlen += len(mf)
	}
	tisfi := ti.sfi.source()
	newlen += len(tisfi)

	var fkvs = e.slist.get(newlen)[:newlen]

	recur := e.h.RecursiveEmptyCheck

	var kv sfiRv
	var j int
	if toMap {
		newlen = 0
		for _, si := range e.kStructSfi(f) {
			kv.r = si.path.field(rv)
			if si.path.omitEmpty && isEmptyValue(kv.r, e.h.TypeInfos, recur) {
				continue
			}
			kv.v = si
			fkvs[newlen] = kv
			newlen++
		}

		var mf2s []stringIntf
		if len(mf) > 0 {
			mf2s = make([]stringIntf, 0, len(mf))
			for k, v := range mf {
				if k == "" {
					continue
				}
				if ti.infoFieldOmitempty && isEmptyValue(reflect.ValueOf(v), e.h.TypeInfos, recur) {
					continue
				}
				mf2s = append(mf2s, stringIntf{k, v})
			}
		}

		e.mapStart(newlen + len(mf2s))

		// When there are missing fields, and Canonical flag is set,
		// we cannot have the missing fields and struct fields sorted independently.
		// We have to capture them together and sort as a unit.

		if len(mf2s) > 0 && e.h.Canonical {
			mf2w := make([]encStructFieldObj, newlen+len(mf2s))
			for j = 0; j < newlen; j++ {
				kv = fkvs[j]
				mf2w[j] = encStructFieldObj{kv.v.encName, kv.r, nil, kv.v.path.encNameAsciiAlphaNum, true}
			}
			for _, v := range mf2s {
				mf2w[j] = encStructFieldObj{v.v, reflect.Value{}, v.i, false, false}
				j++
			}
			sort.Sort((encStructFieldObjSlice)(mf2w))
			for _, v := range mf2w {
				e.mapElemKey()
				e.kStructFieldKey(ti.keyType, v.ascii, v.key)
				e.mapElemValue()
				if v.isRv {
					e.encodeValue(v.rv, nil)
				} else {
					e.encode(v.intf)
				}
			}
		} else {
			keytyp := ti.keyType
			for j = 0; j < newlen; j++ {
				kv = fkvs[j]
				e.mapElemKey()
				e.kStructFieldKey(keytyp, kv.v.path.encNameAsciiAlphaNum, kv.v.encName)
				e.mapElemValue()
				e.encodeValue(kv.r, nil)
			}
			for _, v := range mf2s {
				e.mapElemKey()
				e.kStructFieldKey(keytyp, false, v.v)
				e.mapElemValue()
				e.encode(v.i)
			}
		}

		e.mapEnd()
	} else {
		newlen = len(tisfi)
		for i, si := range tisfi { // use unsorted array (to match sequence in struct)
			kv.r = si.path.field(rv)
			// use the zero value.
			// if a reference or struct, set to nil (so you do not output too much)
			if si.path.omitEmpty && isEmptyValue(kv.r, e.h.TypeInfos, recur) {
				switch kv.r.Kind() {
				case reflect.Struct, reflect.Interface, reflect.Ptr, reflect.Array, reflect.Map, reflect.Slice:
					kv.r = reflect.Value{} //encode as nil
				}
			}
			fkvs[i] = kv
		}
		// encode it all
		e.arrayStart(newlen)
		for j = 0; j < newlen; j++ {
			e.arrayElem()
			e.encodeValue(fkvs[j].r, nil)
		}
		e.arrayEnd()
	}

	// do not use defer. Instead, use explicit pool return at end of function.
	// defer has a cost we are trying to avoid.
	// If there is a panic and these slices are not returned, it is ok.
	e.slist.put(fkvs)
}

func (e *Encoder) kMap(f *codecFnInfo, rv reflect.Value) {
	l := rvLenMap(rv)
	e.mapStart(l)
	if l == 0 {
		e.mapEnd()
		return
	}

	// determine the underlying key and val encFn's for the map.
	// This eliminates some work which is done for each loop iteration i.e.
	// rv.Type(), ref.ValueOf(rt).Pointer(), then check map/list for fn.
	//
	// However, if kind is reflect.Interface, do not pre-determine the
	// encoding type, because preEncodeValue may break it down to
	// a concrete type and kInterface will bomb.

	var keyFn, valFn *codecFn

	ktypeKind := reflect.Kind(f.ti.keykind)
	vtypeKind := reflect.Kind(f.ti.elemkind)

	rtval := f.ti.elem
	rtvalkind := vtypeKind
	for rtvalkind == reflect.Ptr {
		rtval = rtval.Elem()
		rtvalkind = rtval.Kind()
	}
	if rtvalkind != reflect.Interface {
		valFn = e.h.fn(rtval)
	}

	var rvv = mapAddrLoopvarRV(f.ti.elem, vtypeKind)

	rtkey := f.ti.key
	var keyTypeIsString = stringTypId == rt2id(rtkey) // rtkeyid
	if keyTypeIsString {
		keyFn = e.h.fn(rtkey)
	} else {
		for rtkey.Kind() == reflect.Ptr {
			rtkey = rtkey.Elem()
		}
		if rtkey.Kind() != reflect.Interface {
			keyFn = e.h.fn(rtkey)
		}
	}

	if e.h.Canonical {
		e.kMapCanonical(f.ti, rv, rvv, keyFn, valFn)
		e.mapEnd()
		return
	}

	var rvk = mapAddrLoopvarRV(f.ti.key, ktypeKind)

	var it mapIter
	mapRange(&it, rv, rvk, rvv, true)

	for it.Next() {
		e.mapElemKey()
		if keyTypeIsString {
			e.e.EncodeString(it.Key().String())
		} else {
			e.encodeValue(it.Key(), keyFn)
		}
		e.mapElemValue()
		e.encodeValue(it.Value(), valFn)
	}
	it.Done()

	e.mapEnd()
}

func (e *Encoder) kMapCanonical(ti *typeInfo, rv, rvv reflect.Value, keyFn, valFn *codecFn) {
	// The base kind of the type of the map key is sufficient for ordering.
	// We only do out of band if that kind is not ordered (number or string), bool or time.Time.
	// If the key is a predeclared type, directly call methods on encDriver e.g. EncodeString
	// but if not, call encodeValue, in case it has an extension registered or otherwise.
	rtkey := ti.key
	rtkeydecl := rtkey.PkgPath() == "" && rtkey.Name() != "" // key type is predeclared

	mks := rv.MapKeys()
	rtkeyKind := rtkey.Kind()
	kfast := mapKeyFastKindFor(rtkeyKind)
	visindirect := mapStoresElemIndirect(uintptr(ti.elemsize))
	visref := refBitset.isset(ti.elemkind)

	switch rtkeyKind {
	case reflect.Bool:
		// though bool keys make no sense in a map, it *could* happen.
		// in that case, we MUST support it in reflection mode,
		// as that is the fallback for even codecgen and others.

		// sort the keys so that false comes before true
		// ie if 2 keys in order (true, false), then swap them
		if len(mks) == 2 && mks[0].Bool() {
			mks[0], mks[1] = mks[1], mks[0]
		}
		for i := range mks {
			e.mapElemKey()
			if rtkeydecl {
				e.e.EncodeBool(mks[i].Bool())
			} else {
				e.encodeValueNonNil(mks[i], keyFn)
			}
			e.mapElemValue()
			e.encodeValue(mapGet(rv, mks[i], rvv, kfast, visindirect, visref), valFn)
		}
	case reflect.String:
		mksv := make([]stringRv, len(mks))
		for i, k := range mks {
			v := &mksv[i]
			v.r = k
			v.v = k.String()
		}
		sort.Sort(stringRvSlice(mksv))
		for i := range mksv {
			e.mapElemKey()
			if rtkeydecl {
				e.e.EncodeString(mksv[i].v)
			} else {
				e.encodeValueNonNil(mksv[i].r, keyFn)
			}
			e.mapElemValue()
			e.encodeValue(mapGet(rv, mksv[i].r, rvv, kfast, visindirect, visref), valFn)
		}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Uintptr:
		mksv := make([]uint64Rv, len(mks))
		for i, k := range mks {
			v := &mksv[i]
			v.r = k
			v.v = k.Uint()
		}
		sort.Sort(uint64RvSlice(mksv))
		for i := range mksv {
			e.mapElemKey()
			if rtkeydecl {
				e.e.EncodeUint(mksv[i].v)
			} else {
				e.encodeValueNonNil(mksv[i].r, keyFn)
			}
			e.mapElemValue()
			e.encodeValue(mapGet(rv, mksv[i].r, rvv, kfast, visindirect, visref), valFn)
		}
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		mksv := make([]int64Rv, len(mks))
		for i, k := range mks {
			v := &mksv[i]
			v.r = k
			v.v = k.Int()
		}
		sort.Sort(int64RvSlice(mksv))
		for i := range mksv {
			e.mapElemKey()
			if rtkeydecl {
				e.e.EncodeInt(mksv[i].v)
			} else {
				e.encodeValueNonNil(mksv[i].r, keyFn)
			}
			e.mapElemValue()
			e.encodeValue(mapGet(rv, mksv[i].r, rvv, kfast, visindirect, visref), valFn)
		}
	case reflect.Float32:
		mksv := make([]float64Rv, len(mks))
		for i, k := range mks {
			v := &mksv[i]
			v.r = k
			v.v = k.Float()
		}
		sort.Sort(float64RvSlice(mksv))
		for i := range mksv {
			e.mapElemKey()
			if rtkeydecl {
				e.e.EncodeFloat32(float32(mksv[i].v))
			} else {
				e.encodeValueNonNil(mksv[i].r, keyFn)
			}
			e.mapElemValue()
			e.encodeValue(mapGet(rv, mksv[i].r, rvv, kfast, visindirect, visref), valFn)
		}
	case reflect.Float64:
		mksv := make([]float64Rv, len(mks))
		for i, k := range mks {
			v := &mksv[i]
			v.r = k
			v.v = k.Float()
		}
		sort.Sort(float64RvSlice(mksv))
		for i := range mksv {
			e.mapElemKey()
			if rtkeydecl {
				e.e.EncodeFloat64(mksv[i].v)
			} else {
				e.encodeValueNonNil(mksv[i].r, keyFn)
			}
			e.mapElemValue()
			e.encodeValue(mapGet(rv, mksv[i].r, rvv, kfast, visindirect, visref), valFn)
		}
	default:
		if rtkey == timeTyp {
			mksv := make([]timeRv, len(mks))
			for i, k := range mks {
				v := &mksv[i]
				v.r = k
				v.v = rv2i(k).(time.Time)
			}
			sort.Sort(timeRvSlice(mksv))
			for i := range mksv {
				e.mapElemKey()
				e.e.EncodeTime(mksv[i].v)
				e.mapElemValue()
				e.encodeValue(mapGet(rv, mksv[i].r, rvv, kfast, visindirect, visref), valFn)
			}
			break
		}

		// out-of-band
		// first encode each key to a []byte first, then sort them, then record
		bs0 := e.blist.get(len(mks) * 16)
		mksv := bs0
		mksbv := make([]bytesRv, len(mks))

		func() {
			// replicate sideEncode logic
			defer func(wb bytesEncAppender, bytes bool, c containerState, state interface{}) {
				e.wb = wb
				e.bytes = bytes
				e.c = c
				e.e.restoreState(state)
			}(e.wb, e.bytes, e.c, e.e.captureState())

			// e2 := NewEncoderBytes(&mksv, e.hh)
			e.wb = bytesEncAppender{mksv[:0], &mksv}
			e.bytes = true
			e.c = 0
			e.e.resetState()

			for i, k := range mks {
				v := &mksbv[i]
				l := len(mksv)

				e.encodeValue(k, nil)
				e.atEndOfEncode()
				e.w().end()

				v.r = k
				v.v = mksv[l:]
			}
		}()

		sort.Sort(bytesRvSlice(mksbv))
		for j := range mksbv {
			e.mapElemKey()
			e.encWr.writeb(mksbv[j].v)
			e.mapElemValue()
			e.encodeValue(mapGet(rv, mksbv[j].r, rvv, kfast, visindirect, visref), valFn)
		}
		e.blist.put(mksv)
		if !byteSliceSameData(bs0, mksv) {
			e.blist.put(bs0)
		}
	}
}

// Encoder writes an object to an output stream in a supported format.
//
// Encoder is NOT safe for concurrent use i.e. a Encoder cannot be used
// concurrently in multiple goroutines.
//
// However, as Encoder could be allocation heavy to initialize, a Reset method is provided
// so its state can be reused to decode new input streams repeatedly.
// This is the idiomatic way to use.
type Encoder struct {
	panicHdl

	e encDriver

	h *BasicHandle

	// hopefully, reduce derefencing cost by laying the encWriter inside the Encoder
	encWr

	// ---- cpu cache line boundary
	hh Handle

	blist bytesFreelist
	err   error

	// ---- cpu cache line boundary

	// ---- writable fields during execution --- *try* to keep in sep cache line

	// ci holds interfaces during an encoding (if CheckCircularRef=true)
	//
	// We considered using a []uintptr (slice of pointer addresses) retrievable via rv.UnsafeAddr.
	// However, it is possible for the same pointer to point to 2 different types e.g.
	//    type T struct { tHelper }
	//    Here, for var v T; &v and &v.tHelper are the same pointer.
	// Consequently, we need a tuple of type and pointer, which interface{} natively provides.
	ci []interface{} // []uintptr

	perType encPerType

	slist sfiRvFreelist
}

// NewEncoder returns an Encoder for encoding into an io.Writer.
//
// For efficiency, Users are encouraged to configure WriterBufferSize on the handle
// OR pass in a memory buffered writer (eg bufio.Writer, bytes.Buffer).
func NewEncoder(w io.Writer, h Handle) *Encoder {
	e := h.newEncDriver().encoder()
	if w != nil {
		e.Reset(w)
	}
	return e
}

// NewEncoderBytes returns an encoder for encoding directly and efficiently
// into a byte slice, using zero-copying to temporary slices.
//
// It will potentially replace the output byte slice pointed to.
// After encoding, the out parameter contains the encoded contents.
func NewEncoderBytes(out *[]byte, h Handle) *Encoder {
	e := h.newEncDriver().encoder()
	if out != nil {
		e.ResetBytes(out)
	}
	return e
}

func (e *Encoder) init(h Handle) {
	initHandle(h)
	e.err = errEncoderNotInitialized
	e.bytes = true
	e.hh = h
	e.h = h.getBasicHandle()
	e.be = e.hh.isBinary()
}

func (e *Encoder) w() *encWr {
	return &e.encWr
}

func (e *Encoder) resetCommon() {
	e.e.reset()
	if e.ci != nil {
		e.ci = e.ci[:0]
	}
	e.c = 0
	e.calls = 0
	e.seq = 0
	e.err = nil
}

// Reset resets the Encoder with a new output stream.
//
// This accommodates using the state of the Encoder,
// where it has "cached" information about sub-engines.
func (e *Encoder) Reset(w io.Writer) {
	e.bytes = false
	if e.wf == nil {
		e.wf = new(bufioEncWriter)
	}
	e.wf.reset(w, e.h.WriterBufferSize, &e.blist)
	e.resetCommon()
}

// ResetBytes resets the Encoder with a new destination output []byte.
func (e *Encoder) ResetBytes(out *[]byte) {
	e.bytes = true
	e.wb.reset(encInBytes(out), out)
	e.resetCommon()
}

// Encode writes an object into a stream.
//
// Encoding can be configured via the struct tag for the fields.
// The key (in the struct tags) that we look at is configurable.
//
// By default, we look up the "codec" key in the struct field's tags,
// and fall bak to the "json" key if "codec" is absent.
// That key in struct field's tag value is the key name,
// followed by an optional comma and options.
//
// To set an option on all fields (e.g. omitempty on all fields), you
// can create a field called _struct, and set flags on it. The options
// which can be set on _struct are:
//   - omitempty: so all fields are omitted if empty
//   - toarray: so struct is encoded as an array
//   - int: so struct key names are encoded as signed integers (instead of strings)
//   - uint: so struct key names are encoded as unsigned integers (instead of strings)
//   - float: so struct key names are encoded as floats (instead of strings)
//
// More details on these below.
//
// Struct values "usually" encode as maps. Each exported struct field is encoded unless:
//   - the field's tag is "-", OR
//   - the field is empty (empty or the zero value) and its tag specifies the "omitempty" option.
//
// When encoding as a map, the first string in the tag (before the comma)
// is the map key string to use when encoding.
// ...
// This key is typically encoded as a string.
// However, there are instances where the encoded stream has mapping keys encoded as numbers.
// For example, some cbor streams have keys as integer codes in the stream, but they should map
// to fields in a structured object. Consequently, a struct is the natural representation in code.
// For these, configure the struct to encode/decode the keys as numbers (instead of string).
// This is done with the int,uint or float option on the _struct field (see above).
//
// However, struct values may encode as arrays. This happens when:
//   - StructToArray Encode option is set, OR
//   - the tag on the _struct field sets the "toarray" option
//
// Note that omitempty is ignored when encoding struct values as arrays,
// as an entry must be encoded for each field, to maintain its position.
//
// Values with types that implement MapBySlice are encoded as stream maps.
//
// The empty values (for omitempty option) are false, 0, any nil pointer
// or interface value, and any array, slice, map, or string of length zero.
//
// Anonymous fields are encoded inline except:
//   - the struct tag specifies a replacement name (first value)
//   - the field is of an interface type
//
// Examples:
//
//	// NOTE: 'json:' can be used as struct tag key, in place 'codec:' below.
//	type MyStruct struct {
//	    _struct bool    `codec:",omitempty"`   //set omitempty for every field
//	    Field1 string   `codec:"-"`            //skip this field
//	    Field2 int      `codec:"myName"`       //Use key "myName" in encode stream
//	    Field3 int32    `codec:",omitempty"`   //use key "Field3". Omit if empty.
//	    Field4 bool     `codec:"f4,omitempty"` //use key "f4". Omit if empty.
//	    io.Reader                              //use key "Reader".
//	    MyStruct        `codec:"my1"           //use key "my1".
//	    MyStruct                               //inline it
//	    ...
//	}
//
//	type MyStruct struct {
//	    _struct bool    `codec:",toarray"`     //encode struct as an array
//	}
//
//	type MyStruct struct {
//	    _struct bool    `codec:",uint"`        //encode struct with "unsigned integer" keys
//	    Field1 string   `codec:"1"`            //encode Field1 key using: EncodeInt(1)
//	    Field2 string   `codec:"2"`            //encode Field2 key using: EncodeInt(2)
//	}
//
// The mode of encoding is based on the type of the value. When a value is seen:
//   - If a Selfer, call its CodecEncodeSelf method
//   - If an extension is registered for it, call that extension function
//   - If implements encoding.(Binary|Text|JSON)Marshaler, call Marshal(Binary|Text|JSON) method
//   - Else encode it based on its reflect.Kind
//
// Note that struct field names and keys in map[string]XXX will be treated as symbols.
// Some formats support symbols (e.g. binc) and will properly encode the string
// only once in the stream, and use a tag to refer to it thereafter.
func (e *Encoder) Encode(v interface{}) (err error) {
	// tried to use closure, as runtime optimizes defer with no params.
	// This seemed to be causing weird issues (like circular reference found, unexpected panic, etc).
	// Also, see https://github.com/golang/go/issues/14939#issuecomment-417836139
	if !debugging {
		defer func() {
			// if error occurred during encoding, return that error;
			// else if error occurred on end'ing (i.e. during flush), return that error.
			if x := recover(); x != nil {
				panicValToErr(e, x, &e.err)
				err = e.err
			}
		}()
	}

	e.MustEncode(v)
	return
}

// MustEncode is like Encode, but panics if unable to Encode.
//
// Note: This provides insight to the code location that triggered the error.
func (e *Encoder) MustEncode(v interface{}) {
	halt.onerror(e.err)
	if e.hh == nil {
		halt.onerror(errNoFormatHandle)
	}

	e.calls++
	e.encode(v)
	e.calls--
	if e.calls == 0 {
		e.atEndOfEncode()
		e.w().end()
	}
}

// Release releases shared (pooled) resources.
//
// It is important to call Release() when done with an Encoder, so those resources
// are released instantly for use by subsequently created Encoders.
//
// Deprecated: Release is a no-op as pooled resources are not used with an Encoder.
// This method is kept for compatibility reasons only.
func (e *Encoder) Release() {
}

func (e *Encoder) encode(iv interface{}) {
	// MARKER: a switch with only concrete types can be optimized.
	// consequently, we deal with nil and interfaces outside the switch.

	if iv == nil {
		e.e.EncodeNil()
		return
	}

	rv, ok := isNil(iv)
	if ok {
		e.e.EncodeNil()
		return
	}

	switch v := iv.(type) {
	// case nil:
	// case Selfer:
	case Raw:
		e.rawBytes(v)
	case reflect.Value:
		e.encodeValue(v, nil)

	case string:
		e.e.EncodeString(v)
	case bool:
		e.e.EncodeBool(v)
	case int:
		e.e.EncodeInt(int64(v))
	case int8:
		e.e.EncodeInt(int64(v))
	case int16:
		e.e.EncodeInt(int64(v))
	case int32:
		e.e.EncodeInt(int64(v))
	case int64:
		e.e.EncodeInt(v)
	case uint:
		e.e.EncodeUint(uint64(v))
	case uint8:
		e.e.EncodeUint(uint64(v))
	case uint16:
		e.e.EncodeUint(uint64(v))
	case uint32:
		e.e.EncodeUint(uint64(v))
	case uint64:
		e.e.EncodeUint(v)
	case uintptr:
		e.e.EncodeUint(uint64(v))
	case float32:
		e.e.EncodeFloat32(v)
	case float64:
		e.e.EncodeFloat64(v)
	case complex64:
		e.encodeComplex64(v)
	case complex128:
		e.encodeComplex128(v)
	case time.Time:
		e.e.EncodeTime(v)
	case []byte:
		e.e.EncodeStringBytesRaw(v)
	case *Raw:
		e.rawBytes(*v)
	case *string:
		e.e.EncodeString(*v)
	case *bool:
		e.e.EncodeBool(*v)
	case *int:
		e.e.EncodeInt(int64(*v))
	case *int8:
		e.e.EncodeInt(int64(*v))
	case *int16:
		e.e.EncodeInt(int64(*v))
	case *int32:
		e.e.EncodeInt(int64(*v))
	case *int64:
		e.e.EncodeInt(*v)
	case *uint:
		e.e.EncodeUint(uint64(*v))
	case *uint8:
		e.e.EncodeUint(uint64(*v))
	case *uint16:
		e.e.EncodeUint(uint64(*v))
	case *uint32:
		e.e.EncodeUint(uint64(*v))
	case *uint64:
		e.e.EncodeUint(*v)
	case *uintptr:
		e.e.EncodeUint(uint64(*v))
	case *float32:
		e.e.EncodeFloat32(*v)
	case *float64:
		e.e.EncodeFloat64(*v)
	case *complex64:
		e.encodeComplex64(*v)
	case *complex128:
		e.encodeComplex128(*v)
	case *time.Time:
		e.e.EncodeTime(*v)
	case *[]byte:
		if *v == nil {
			e.e.EncodeNil()
		} else {
			e.e.EncodeStringBytesRaw(*v)
		}
	default:
		// we can't check non-predefined types, as they might be a Selfer or extension.
		if skipFastpathTypeSwitchInDirectCall || !fastpathEncodeTypeSwitch(iv, e) {
			e.encodeValue(rv, nil)
		}
	}
}

// encodeValue will encode a value.
//
// Note that encodeValue will handle nil in the stream early, so that the
// subsequent calls i.e. kXXX methods, etc do not have to handle it themselves.
func (e *Encoder) encodeValue(rv reflect.Value, fn *codecFn) {
	// if a valid fn is passed, it MUST BE for the dereferenced type of rv

	// MARKER: We check if value is nil here, so that the kXXX method do not have to.

	var sptr interface{}
	var rvp reflect.Value
	var rvpValid bool
TOP:
	switch rv.Kind() {
	case reflect.Ptr:
		if rvIsNil(rv) {
			e.e.EncodeNil()
			return
		}
		rvpValid = true
		rvp = rv
		rv = rv.Elem()
		goto TOP
	case reflect.Interface:
		if rvIsNil(rv) {
			e.e.EncodeNil()
			return
		}
		rvpValid = false
		rvp = reflect.Value{}
		rv = rv.Elem()
		goto TOP
	case reflect.Struct:
		if rvpValid && e.h.CheckCircularRef {
			sptr = rv2i(rvp)
			for _, vv := range e.ci {
				if eq4i(sptr, vv) { // error if sptr already seen
					e.errorf("circular reference found: %p, %T", sptr, sptr)
				}
			}
			e.ci = append(e.ci, sptr)
		}
	case reflect.Slice, reflect.Map, reflect.Chan:
		if rvIsNil(rv) {
			e.e.EncodeNil()
			return
		}
	case reflect.Invalid, reflect.Func:
		e.e.EncodeNil()
		return
	}

	if fn == nil {
		fn = e.h.fn(rv.Type())
	}

	if !fn.i.addrE { // typically, addrE = false, so check it first
		// keep rv same
	} else if rvpValid {
		rv = rvp
	} else {
		rv = e.addrRV(rv, fn.i.ti.rt, fn.i.ti.ptr)
	}
	fn.fe(e, &fn.i, rv)

	if sptr != nil { // remove sptr
		e.ci = e.ci[:len(e.ci)-1]
	}
}

// encodeValueNonNil can encode a number, bool, or string
// OR non-nil values of kind map, slice and chan.
func (e *Encoder) encodeValueNonNil(rv reflect.Value, fn *codecFn) {
	if fn == nil {
		fn = e.h.fn(rv.Type())
	}

	if fn.i.addrE { // typically, addrE = false, so check it first
		rv = e.addrRV(rv, fn.i.ti.rt, fn.i.ti.ptr)
	}
	fn.fe(e, &fn.i, rv)
}

// addrRV returns a addressable value which may be readonly
func (e *Encoder) addrRV(rv reflect.Value, typ, ptrType reflect.Type) (rva reflect.Value) {
	if rv.CanAddr() {
		return rvAddr(rv, ptrType)
	}
	if e.h.NoAddressableReadonly {
		rva = reflect.New(typ)
		rvSetDirect(rva.Elem(), rv)
		return
	}
	return rvAddr(e.perType.AddressableRO(rv), ptrType)
}

func (e *Encoder) marshalUtf8(bs []byte, fnerr error) {
	e.onerror(fnerr)
	if bs == nil {
		e.e.EncodeNil()
	} else {
		e.e.EncodeString(stringView(bs))
	}
}

func (e *Encoder) marshalAsis(bs []byte, fnerr error) {
	e.onerror(fnerr)
	if bs == nil {
		e.e.EncodeNil()
	} else {
		e.encWr.writeb(bs) // e.asis(bs)
	}
}

func (e *Encoder) marshalRaw(bs []byte, fnerr error) {
	e.onerror(fnerr)
	if bs == nil {
		e.e.EncodeNil()
	} else {
		e.e.EncodeStringBytesRaw(bs)
	}
}

func (e *Encoder) rawBytes(vv Raw) {
	v := []byte(vv)
	if !e.h.Raw {
		e.errorf("Raw values cannot be encoded: %v", v)
	}
	e.encWr.writeb(v)
}

func (e *Encoder) wrapErr(v error, err *error) {
	*err = wrapCodecErr(v, e.hh.Name(), 0, true)
}

// ---- container tracker methods
// Note: We update the .c after calling the callback.
// This way, the callback can know what the last status was.

func (e *Encoder) mapStart(length int) {
	e.e.WriteMapStart(length)
	e.c = containerMapStart
}

func (e *Encoder) mapElemKey() {
	if e.js {
		e.jsondriver().WriteMapElemKey()
	}
	e.c = containerMapKey
}

func (e *Encoder) mapElemValue() {
	if e.js {
		e.jsondriver().WriteMapElemValue()
	}
	e.c = containerMapValue
}

func (e *Encoder) mapEnd() {
	e.e.WriteMapEnd()
	e.c = 0
}

func (e *Encoder) arrayStart(length int) {
	e.e.WriteArrayStart(length)
	e.c = containerArrayStart
}

func (e *Encoder) arrayElem() {
	if e.js {
		e.jsondriver().WriteArrayElem()
	}
	e.c = containerArrayElem
}

func (e *Encoder) arrayEnd() {
	e.e.WriteArrayEnd()
	e.c = 0
}

// ----------

func (e *Encoder) haltOnMbsOddLen(length int) {
	if length&1 != 0 { // similar to &1==1 or %2 == 1
		e.errorf("mapBySlice requires even slice length, but got %v", length)
	}
}

func (e *Encoder) atEndOfEncode() {
	// e.e.atEndOfEncode()
	if e.js {
		e.jsondriver().atEndOfEncode()
	}
}

func (e *Encoder) sideEncode(v interface{}, basetype reflect.Type, bs *[]byte) {
	// rv := baseRV(v)
	// e2 := NewEncoderBytes(bs, e.hh)
	// e2.encodeValue(rv, e2.h.fnNoExt(basetype))
	// e2.atEndOfEncode()
	// e2.w().end()

	defer func(wb bytesEncAppender, bytes bool, c containerState, state interface{}) {
		e.wb = wb
		e.bytes = bytes
		e.c = c
		e.e.restoreState(state)
	}(e.wb, e.bytes, e.c, e.e.captureState())

	e.wb = bytesEncAppender{encInBytes(bs)[:0], bs}
	e.bytes = true
	e.c = 0
	e.e.resetState()

	// must call using fnNoExt
	rv := baseRV(v)
	e.encodeValue(rv, e.h.fnNoExt(basetype))
	e.atEndOfEncode()
	e.w().end()
}

func encInBytes(out *[]byte) (in []byte) {
	in = *out
	if in == nil {
		in = make([]byte, defEncByteBufSize)
	}
	return
}

func encStructFieldKey(encName string, ee encDriver, w *encWr,
	keyType valueType, encNameAsciiAlphaNum bool, js bool) {
	// use if-else-if, not switch (which compiles to binary-search)
	// since keyType is typically valueTypeString, branch prediction is pretty good.

	if keyType == valueTypeString {
		if js && encNameAsciiAlphaNum { // keyType == valueTypeString
			w.writeqstr(encName)
		} else { // keyType == valueTypeString
			ee.EncodeString(encName)
		}
	} else if keyType == valueTypeInt {
		ee.EncodeInt(must.Int(strconv.ParseInt(encName, 10, 64)))
	} else if keyType == valueTypeUint {
		ee.EncodeUint(must.Uint(strconv.ParseUint(encName, 10, 64)))
	} else if keyType == valueTypeFloat {
		ee.EncodeFloat64(must.Float(strconv.ParseFloat(encName, 64)))
	} else {
		halt.errorf("invalid struct key type: %v", keyType)
	}
}
