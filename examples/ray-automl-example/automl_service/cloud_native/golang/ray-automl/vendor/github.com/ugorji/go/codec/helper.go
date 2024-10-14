// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

// Contains code shared by both encode and decode.

// Some shared ideas around encoding/decoding
// ------------------------------------------
//
// If an interface{} is passed, we first do a type assertion to see if it is
// a primitive type or a map/slice of primitive types, and use a fastpath to handle it.
//
// If we start with a reflect.Value, we are already in reflect.Value land and
// will try to grab the function for the underlying Type and directly call that function.
// This is more performant than calling reflect.Value.Interface().
//
// This still helps us bypass many layers of reflection, and give best performance.
//
// Containers
// ------------
// Containers in the stream are either associative arrays (key-value pairs) or
// regular arrays (indexed by incrementing integers).
//
// Some streams support indefinite-length containers, and use a breaking
// byte-sequence to denote that the container has come to an end.
//
// Some streams also are text-based, and use explicit separators to denote the
// end/beginning of different values.
//
// Philosophy
// ------------
// On decode, this codec will update containers appropriately:
//    - If struct, update fields from stream into fields of struct.
//      If field in stream not found in struct, handle appropriately (based on option).
//      If a struct field has no corresponding value in the stream, leave it AS IS.
//      If nil in stream, set value to nil/zero value.
//    - If map, update map from stream.
//      If the stream value is NIL, set the map to nil.
//    - if slice, try to update up to length of array in stream.
//      if container len is less than stream array length,
//      and container cannot be expanded, handled (based on option).
//      This means you can decode 4-element stream array into 1-element array.
//
// ------------------------------------
// On encode, user can specify omitEmpty. This means that the value will be omitted
// if the zero value. The problem may occur during decode, where omitted values do not affect
// the value being decoded into. This means that if decoding into a struct with an
// int field with current value=5, and the field is omitted in the stream, then after
// decoding, the value will still be 5 (not 0).
// omitEmpty only works if you guarantee that you always decode into zero-values.
//
// ------------------------------------
// We could have truncated a map to remove keys not available in the stream,
// or set values in the struct which are not in the stream to their zero values.
// We decided against it because there is no efficient way to do it.
// We may introduce it as an option later.
// However, that will require enabling it for both runtime and code generation modes.
//
// To support truncate, we need to do 2 passes over the container:
//   map
//   - first collect all keys (e.g. in k1)
//   - for each key in stream, mark k1 that the key should not be removed
//   - after updating map, do second pass and call delete for all keys in k1 which are not marked
//   struct:
//   - for each field, track the *typeInfo s1
//   - iterate through all s1, and for each one not marked, set value to zero
//   - this involves checking the possible anonymous fields which are nil ptrs.
//     too much work.
//
// ------------------------------------------
// Error Handling is done within the library using panic.
//
// This way, the code doesn't have to keep checking if an error has happened,
// and we don't have to keep sending the error value along with each call
// or storing it in the En|Decoder and checking it constantly along the way.
//
// We considered storing the error is En|Decoder.
//   - once it has its err field set, it cannot be used again.
//   - panicing will be optional, controlled by const flag.
//   - code should always check error first and return early.
//
// We eventually decided against it as it makes the code clumsier to always
// check for these error conditions.
//
// ------------------------------------------
// We use sync.Pool only for the aid of long-lived objects shared across multiple goroutines.
// Encoder, Decoder, enc|decDriver, reader|writer, etc do not fall into this bucket.
//
// Also, GC is much better now, eliminating some of the reasons to use a shared pool structure.
// Instead, the short-lived objects use free-lists that live as long as the object exists.
//
// ------------------------------------------
// Performance is affected by the following:
//    - Bounds Checking
//    - Inlining
//    - Pointer chasing
// This package tries hard to manage the performance impact of these.
//
// ------------------------------------------
// To alleviate performance due to pointer-chasing:
//    - Prefer non-pointer values in a struct field
//    - Refer to these directly within helper classes
//      e.g. json.go refers directly to d.d.decRd
//
// We made the changes to embed En/Decoder in en/decDriver,
// but we had to explicitly reference the fields as opposed to using a function
// to get the better performance that we were looking for.
// For example, we explicitly call d.d.decRd.fn() instead of d.d.r().fn().
//
// ------------------------------------------
// Bounds Checking
//    - Allow bytesDecReader to incur "bounds check error", and
//      recover that as an io.EOF.
//      This allows the bounds check branch to always be taken by the branch predictor,
//      giving better performance (in theory), while ensuring that the code is shorter.
//
// ------------------------------------------
// Escape Analysis
//    - Prefer to return non-pointers if the value is used right away.
//      Newly allocated values returned as pointers will be heap-allocated as they escape.
//
// Prefer functions and methods that
//    - take no parameters and
//    - return no results and
//    - do not allocate.
// These are optimized by the runtime.
// For example, in json, we have dedicated functions for ReadMapElemKey, etc
// which do not delegate to readDelim, as readDelim takes a parameter.
// The difference in runtime was as much as 5%.
//
// ------------------------------------------
// Handling Nil
//   - In dynamic (reflection) mode, decodeValue and encodeValue handle nil at the top
//   - Consequently, methods used with them as a parent in the chain e.g. kXXX
//     methods do not handle nil.
//   - Fastpath methods also do not handle nil.
//     The switch called in (en|de)code(...) handles it so the dependent calls don't have to.
//   - codecgen will handle nil before calling into the library for further work also.
//
// ------------------------------------------
// Passing reflect.Kind to functions that take a reflect.Value
//   - Note that reflect.Value.Kind() is very cheap, as its fundamentally a binary AND of 2 numbers
//
// ------------------------------------------
// Transient values during decoding
//
// With reflection, the stack is not used. Consequently, values which may be stack-allocated in
// normal use will cause a heap allocation when using reflection.
//
// There are cases where we know that a value is transient, and we just need to decode into it
// temporarily so we can right away use its value for something else.
//
// In these situations, we can elide the heap allocation by being deliberate with use of a pre-cached
// scratch memory or scratch value.
//
// We use this for situations:
// - decode into a temp value x, and then set x into an interface
// - decode into a temp value, for use as a map key, to lookup up a map value
// - decode into a temp value, for use as a map value, to set into a map
// - decode into a temp value, for sending into a channel
//
// By definition, Transient values are NEVER pointer-shaped values,
// like pointer, func, map, chan. Using transient for pointer-shaped values
// can lead to data corruption when GC tries to follow what it saw as a pointer at one point.
//
// In general, transient values are values which can be decoded as an atomic value
// using a single call to the decDriver. This naturally includes bool or numeric types.
//
// Note that some values which "contain" pointers, specifically string and slice,
// can also be transient. In the case of string, it is decoded as an atomic value.
// In the case of a slice, decoding into its elements always uses an addressable
// value in memory ie we grow the slice, and then decode directly into the memory
// address corresponding to that index in the slice.
//
// To handle these string and slice values, we have to use a scratch value
// which has the same shape of a string or slice.
//
// Consequently, the full range of types which can be transient is:
// - numbers
// - bool
// - string
// - slice
//
// and whbut we MUST use a scratch space with that element
// being defined as an unsafe.Pointer to start with.
//
// We have to be careful with maps. Because we iterate map keys and values during a range,
// we must have 2 variants of the scratch space/value for maps and keys separately.
//
// These are the TransientAddrK and TransientAddr2K methods of decPerType.

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// if debugging is true, then
//   - within Encode/Decode, do not recover from panic's
//   - etc
//
// Note: Negative tests that check for errors will fail, so only use this
// when debugging, and run only one test at a time preferably.
//
// Note: RPC tests depend on getting the error from an Encode/Decode call.
// Consequently, they will always fail if debugging = true.
const debugging = false

const (
	// containerLenUnknown is length returned from Read(Map|Array)Len
	// when a format doesn't know apiori.
	// For example, json doesn't pre-determine the length of a container (sequence/map).
	containerLenUnknown = -1

	// containerLenNil is length returned from Read(Map|Array)Len
	// when a 'nil' was encountered in the stream.
	containerLenNil = math.MinInt32

	// [N]byte is handled by converting to []byte first,
	// and sending to the dedicated fast-path function for []byte.
	//
	// Code exists in case our understanding is wrong.
	// keep the defensive code behind this flag, so we can remove/hide it if needed.
	// For now, we enable the defensive code (ie set it to true).
	handleBytesWithinKArray = true

	// Support encoding.(Binary|Text)(Unm|M)arshaler.
	// This constant flag will enable or disable it.
	supportMarshalInterfaces = true

	// bytesFreeListNoCache is used for debugging, when we want to skip using a cache of []byte.
	bytesFreeListNoCache = false

	// size of the cacheline: defaulting to value for archs: amd64, arm64, 386
	// should use "runtime/internal/sys".CacheLineSize, but that is not exposed.
	cacheLineSize = 64

	wordSizeBits = 32 << (^uint(0) >> 63) // strconv.IntSize
	wordSize     = wordSizeBits / 8

	// MARKER: determines whether to skip calling fastpath(En|De)codeTypeSwitch.
	// Calling the fastpath switch in encode() or decode() could be redundant,
	// as we still have to introspect it again within fnLoad
	// to determine the function to use for values of that type.
	skipFastpathTypeSwitchInDirectCall = false
)

const cpu32Bit = ^uint(0)>>32 == 0

type rkind byte

const (
	rkindPtr    = rkind(reflect.Ptr)
	rkindString = rkind(reflect.String)
	rkindChan   = rkind(reflect.Chan)
)

type mapKeyFastKind uint8

const (
	mapKeyFastKind32 = iota + 1
	mapKeyFastKind32ptr
	mapKeyFastKind64
	mapKeyFastKind64ptr
	mapKeyFastKindStr
)

var (
	// use a global mutex to ensure each Handle is initialized.
	// We do this, so we don't have to store the basicHandle mutex
	// directly in BasicHandle, so it can be shallow-copied.
	handleInitMu sync.Mutex

	must mustHdl
	halt panicHdl

	digitCharBitset      bitset256
	numCharBitset        bitset256
	whitespaceCharBitset bitset256
	asciiAlphaNumBitset  bitset256

	// numCharWithExpBitset64 bitset64
	// numCharNoExpBitset64   bitset64
	// whitespaceCharBitset64 bitset64
	//
	// // hasptrBitset sets bit for all kinds which always have internal pointers
	// hasptrBitset bitset32

	// refBitset sets bit for all kinds which are direct internal references
	refBitset bitset32

	// isnilBitset sets bit for all kinds which can be compared to nil
	isnilBitset bitset32

	// numBoolBitset sets bit for all number and bool kinds
	numBoolBitset bitset32

	// numBoolStrSliceBitset sets bits for all kinds which are numbers, bool, strings and slices
	numBoolStrSliceBitset bitset32

	// scalarBitset sets bit for all kinds which are scalars/primitives and thus immutable
	scalarBitset bitset32

	mapKeyFastKindVals [32]mapKeyFastKind

	// codecgen is set to true by codecgen, so that tests, etc can use this information as needed.
	codecgen bool

	oneByteArr    [1]byte
	zeroByteSlice = oneByteArr[:0:0]

	eofReader devNullReader
)

var (
	errMapTypeNotMapKind     = errors.New("MapType MUST be of Map Kind")
	errSliceTypeNotSliceKind = errors.New("SliceType MUST be of Slice Kind")

	errExtFnWriteExtUnsupported   = errors.New("BytesExt.WriteExt is not supported")
	errExtFnReadExtUnsupported    = errors.New("BytesExt.ReadExt is not supported")
	errExtFnConvertExtUnsupported = errors.New("InterfaceExt.ConvertExt is not supported")
	errExtFnUpdateExtUnsupported  = errors.New("InterfaceExt.UpdateExt is not supported")

	errPanicUndefined = errors.New("panic: undefined error")

	errHandleInited = errors.New("cannot modify initialized Handle")

	errNoFormatHandle = errors.New("no handle (cannot identify format)")
)

var pool4tiload = sync.Pool{
	New: func() interface{} {
		return &typeInfoLoad{
			etypes:   make([]uintptr, 0, 4),
			sfis:     make([]structFieldInfo, 0, 4),
			sfiNames: make(map[string]uint16, 4),
		}
	},
}

func init() {
	xx := func(f mapKeyFastKind, k ...reflect.Kind) {
		for _, v := range k {
			mapKeyFastKindVals[byte(v)&31] = f // 'v % 32' equal to 'v & 31'
		}
	}

	var f mapKeyFastKind

	f = mapKeyFastKind64
	if wordSizeBits == 32 {
		f = mapKeyFastKind32
	}
	xx(f, reflect.Int, reflect.Uint, reflect.Uintptr)

	f = mapKeyFastKind64ptr
	if wordSizeBits == 32 {
		f = mapKeyFastKind32ptr
	}
	xx(f, reflect.Ptr)

	xx(mapKeyFastKindStr, reflect.String)
	xx(mapKeyFastKind32, reflect.Uint32, reflect.Int32, reflect.Float32)
	xx(mapKeyFastKind64, reflect.Uint64, reflect.Int64, reflect.Float64)

	numBoolBitset.
		set(byte(reflect.Bool)).
		set(byte(reflect.Int)).
		set(byte(reflect.Int8)).
		set(byte(reflect.Int16)).
		set(byte(reflect.Int32)).
		set(byte(reflect.Int64)).
		set(byte(reflect.Uint)).
		set(byte(reflect.Uint8)).
		set(byte(reflect.Uint16)).
		set(byte(reflect.Uint32)).
		set(byte(reflect.Uint64)).
		set(byte(reflect.Uintptr)).
		set(byte(reflect.Float32)).
		set(byte(reflect.Float64)).
		set(byte(reflect.Complex64)).
		set(byte(reflect.Complex128))

	numBoolStrSliceBitset = numBoolBitset

	numBoolStrSliceBitset.
		set(byte(reflect.String)).
		set(byte(reflect.Slice))

	scalarBitset = numBoolBitset

	scalarBitset.
		set(byte(reflect.String))

	// MARKER: reflect.Array is not a scalar, as its contents can be modified.

	refBitset.
		set(byte(reflect.Map)).
		set(byte(reflect.Ptr)).
		set(byte(reflect.Func)).
		set(byte(reflect.Chan)).
		set(byte(reflect.UnsafePointer))

	isnilBitset = refBitset

	isnilBitset.
		set(byte(reflect.Interface)).
		set(byte(reflect.Slice))

	// hasptrBitset = isnilBitset
	//
	// hasptrBitset.
	// 	set(byte(reflect.String))

	for i := byte(0); i <= utf8.RuneSelf; i++ {
		if (i >= '0' && i <= '9') || (i >= 'a' && i <= 'z') || (i >= 'A' && i <= 'Z') {
			asciiAlphaNumBitset.set(i)
		}
		switch i {
		case ' ', '\t', '\r', '\n':
			whitespaceCharBitset.set(i)
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			digitCharBitset.set(i)
			numCharBitset.set(i)
		case '.', '+', '-':
			numCharBitset.set(i)
		case 'e', 'E':
			numCharBitset.set(i)
		}
	}
}

// driverStateManager supports the runtime state of an (enc|dec)Driver.
//
// During a side(En|De)code call, we can capture the state, reset it,
// and then restore it later to continue the primary encoding/decoding.
type driverStateManager interface {
	resetState()
	captureState() interface{}
	restoreState(state interface{})
}

type bdAndBdread struct {
	bdRead bool
	bd     byte
}

func (x bdAndBdread) captureState() interface{}   { return x }
func (x *bdAndBdread) resetState()                { x.bd, x.bdRead = 0, false }
func (x *bdAndBdread) reset()                     { x.resetState() }
func (x *bdAndBdread) restoreState(v interface{}) { *x = v.(bdAndBdread) }

type clsErr struct {
	err    error // error on closing
	closed bool  // is it closed?
}

type charEncoding uint8

const (
	_ charEncoding = iota // make 0 unset
	cUTF8
	cUTF16LE
	cUTF16BE
	cUTF32LE
	cUTF32BE
	// Deprecated: not a true char encoding value
	cRAW charEncoding = 255
)

// valueType is the stream type
type valueType uint8

const (
	valueTypeUnset valueType = iota
	valueTypeNil
	valueTypeInt
	valueTypeUint
	valueTypeFloat
	valueTypeBool
	valueTypeString
	valueTypeSymbol
	valueTypeBytes
	valueTypeMap
	valueTypeArray
	valueTypeTime
	valueTypeExt

	// valueTypeInvalid = 0xff
)

var valueTypeStrings = [...]string{
	"Unset",
	"Nil",
	"Int",
	"Uint",
	"Float",
	"Bool",
	"String",
	"Symbol",
	"Bytes",
	"Map",
	"Array",
	"Timestamp",
	"Ext",
}

func (x valueType) String() string {
	if int(x) < len(valueTypeStrings) {
		return valueTypeStrings[x]
	}
	return strconv.FormatInt(int64(x), 10)
}

// note that containerMapStart and containerArraySend are not sent.
// This is because the ReadXXXStart and EncodeXXXStart already does these.
type containerState uint8

const (
	_ containerState = iota

	containerMapStart
	containerMapKey
	containerMapValue
	containerMapEnd
	containerArrayStart
	containerArrayElem
	containerArrayEnd
)

// do not recurse if a containing type refers to an embedded type
// which refers back to its containing type (via a pointer).
// The second time this back-reference happens, break out,
// so as not to cause an infinite loop.
const rgetMaxRecursion = 2

// fauxUnion is used to keep track of the primitives decoded.
//
// Without it, we would have to decode each primitive and wrap it
// in an interface{}, causing an allocation.
// In this model, the primitives are decoded in a "pseudo-atomic" fashion,
// so we can rest assured that no other decoding happens while these
// primitives are being decoded.
//
// maps and arrays are not handled by this mechanism.
type fauxUnion struct {
	// r RawExt // used for RawExt, uint, []byte.

	// primitives below
	u uint64
	i int64
	f float64
	l []byte
	s string

	// ---- cpu cache line boundary?
	t time.Time
	b bool

	// state
	v valueType
}

// typeInfoLoad is a transient object used while loading up a typeInfo.
type typeInfoLoad struct {
	etypes   []uintptr
	sfis     []structFieldInfo
	sfiNames map[string]uint16
}

func (x *typeInfoLoad) reset() {
	x.etypes = x.etypes[:0]
	x.sfis = x.sfis[:0]
	for k := range x.sfiNames { // optimized to zero the map
		delete(x.sfiNames, k)
	}
}

// mirror json.Marshaler and json.Unmarshaler here,
// so we don't import the encoding/json package

type jsonMarshaler interface {
	MarshalJSON() ([]byte, error)
}
type jsonUnmarshaler interface {
	UnmarshalJSON([]byte) error
}

type isZeroer interface {
	IsZero() bool
}

type isCodecEmptyer interface {
	IsCodecEmpty() bool
}

type codecError struct {
	err    error
	name   string
	pos    int
	encode bool
}

func (e *codecError) Cause() error {
	return e.err
}

func (e *codecError) Unwrap() error {
	return e.err
}

func (e *codecError) Error() string {
	if e.encode {
		return fmt.Sprintf("%s encode error: %v", e.name, e.err)
	}
	return fmt.Sprintf("%s decode error [pos %d]: %v", e.name, e.pos, e.err)
}

func wrapCodecErr(in error, name string, numbytesread int, encode bool) (out error) {
	x, ok := in.(*codecError)
	if ok && x.pos == numbytesread && x.name == name && x.encode == encode {
		return in
	}
	return &codecError{in, name, numbytesread, encode}
}

var (
	bigen bigenHelper

	bigenstd = binary.BigEndian

	structInfoFieldName = "_struct"

	mapStrIntfTyp  = reflect.TypeOf(map[string]interface{}(nil))
	mapIntfIntfTyp = reflect.TypeOf(map[interface{}]interface{}(nil))
	intfSliceTyp   = reflect.TypeOf([]interface{}(nil))
	intfTyp        = intfSliceTyp.Elem()

	reflectValTyp = reflect.TypeOf((*reflect.Value)(nil)).Elem()

	stringTyp     = reflect.TypeOf("")
	timeTyp       = reflect.TypeOf(time.Time{})
	rawExtTyp     = reflect.TypeOf(RawExt{})
	rawTyp        = reflect.TypeOf(Raw{})
	uintptrTyp    = reflect.TypeOf(uintptr(0))
	uint8Typ      = reflect.TypeOf(uint8(0))
	uint8SliceTyp = reflect.TypeOf([]uint8(nil))
	uintTyp       = reflect.TypeOf(uint(0))
	intTyp        = reflect.TypeOf(int(0))

	mapBySliceTyp = reflect.TypeOf((*MapBySlice)(nil)).Elem()

	binaryMarshalerTyp   = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	binaryUnmarshalerTyp = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()

	textMarshalerTyp   = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	textUnmarshalerTyp = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

	jsonMarshalerTyp   = reflect.TypeOf((*jsonMarshaler)(nil)).Elem()
	jsonUnmarshalerTyp = reflect.TypeOf((*jsonUnmarshaler)(nil)).Elem()

	selferTyp                = reflect.TypeOf((*Selfer)(nil)).Elem()
	missingFielderTyp        = reflect.TypeOf((*MissingFielder)(nil)).Elem()
	iszeroTyp                = reflect.TypeOf((*isZeroer)(nil)).Elem()
	isCodecEmptyerTyp        = reflect.TypeOf((*isCodecEmptyer)(nil)).Elem()
	isSelferViaCodecgenerTyp = reflect.TypeOf((*isSelferViaCodecgener)(nil)).Elem()

	uint8TypId      = rt2id(uint8Typ)
	uint8SliceTypId = rt2id(uint8SliceTyp)
	rawExtTypId     = rt2id(rawExtTyp)
	rawTypId        = rt2id(rawTyp)
	intfTypId       = rt2id(intfTyp)
	timeTypId       = rt2id(timeTyp)
	stringTypId     = rt2id(stringTyp)

	mapStrIntfTypId  = rt2id(mapStrIntfTyp)
	mapIntfIntfTypId = rt2id(mapIntfIntfTyp)
	intfSliceTypId   = rt2id(intfSliceTyp)
	// mapBySliceTypId  = rt2id(mapBySliceTyp)

	intBitsize  = uint8(intTyp.Bits())
	uintBitsize = uint8(uintTyp.Bits())

	// bsAll0x00 = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	bsAll0xff = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

	chkOvf checkOverflow
)

var defTypeInfos = NewTypeInfos([]string{"codec", "json"})

// SelfExt is a sentinel extension signifying that types
// registered with it SHOULD be encoded and decoded
// based on the native mode of the format.
//
// This allows users to define a tag for an extension,
// but signify that the types should be encoded/decoded as the native encoding.
// This way, users need not also define how to encode or decode the extension.
var SelfExt = &extFailWrapper{}

// Selfer defines methods by which a value can encode or decode itself.
//
// Any type which implements Selfer will be able to encode or decode itself.
// Consequently, during (en|de)code, this takes precedence over
// (text|binary)(M|Unm)arshal or extension support.
//
// By definition, it is not allowed for a Selfer to directly call Encode or Decode on itself.
// If that is done, Encode/Decode will rightfully fail with a Stack Overflow style error.
// For example, the snippet below will cause such an error.
//
//	type testSelferRecur struct{}
//	func (s *testSelferRecur) CodecEncodeSelf(e *Encoder) { e.MustEncode(s) }
//	func (s *testSelferRecur) CodecDecodeSelf(d *Decoder) { d.MustDecode(s) }
//
// Note: *the first set of bytes of any value MUST NOT represent nil in the format*.
// This is because, during each decode, we first check the the next set of bytes
// represent nil, and if so, we just set the value to nil.
type Selfer interface {
	CodecEncodeSelf(*Encoder)
	CodecDecodeSelf(*Decoder)
}

type isSelferViaCodecgener interface {
	codecSelferViaCodecgen()
}

// MissingFielder defines the interface allowing structs to internally decode or encode
// values which do not map to struct fields.
//
// We expect that this interface is bound to a pointer type (so the mutation function works).
//
// A use-case is if a version of a type unexports a field, but you want compatibility between
// both versions during encoding and decoding.
//
// Note that the interface is completely ignored during codecgen.
type MissingFielder interface {
	// CodecMissingField is called to set a missing field and value pair.
	//
	// It returns true if the missing field was set on the struct.
	CodecMissingField(field []byte, value interface{}) bool

	// CodecMissingFields returns the set of fields which are not struct fields.
	//
	// Note that the returned map may be mutated by the caller.
	CodecMissingFields() map[string]interface{}
}

// MapBySlice is a tag interface that denotes the slice or array value should encode as a map
// in the stream, and can be decoded from a map in the stream.
//
// The slice or array must contain a sequence of key-value pairs.
// The length of the slice or array must be even (fully divisible by 2).
//
// This affords storing a map in a specific sequence in the stream.
//
// Example usage:
//
//	type T1 []string         // or []int or []Point or any other "slice" type
//	func (_ T1) MapBySlice{} // T1 now implements MapBySlice, and will be encoded as a map
//	type T2 struct { KeyValues T1 }
//
//	var kvs = []string{"one", "1", "two", "2", "three", "3"}
//	var v2 = T2{ KeyValues: T1(kvs) }
//	// v2 will be encoded like the map: {"KeyValues": {"one": "1", "two": "2", "three": "3"} }
//
// The support of MapBySlice affords the following:
//   - A slice or array type which implements MapBySlice will be encoded as a map
//   - A slice can be decoded from a map in the stream
type MapBySlice interface {
	MapBySlice()
}

// basicHandleRuntimeState holds onto all BasicHandle runtime and cached config information.
//
// Storing this outside BasicHandle allows us create shallow copies of a Handle,
// which can be used e.g. when we need to modify config fields temporarily.
// Shallow copies are used within tests, so we can modify some config fields for a test
// temporarily when running tests in parallel, without running the risk that a test executing
// in parallel with other tests does not see a transient modified values not meant for it.
type basicHandleRuntimeState struct {
	// these are used during runtime.
	// At init time, they should have nothing in them.
	rtidFns      atomicRtidFnSlice
	rtidFnsNoExt atomicRtidFnSlice

	// Note: basicHandleRuntimeState is not comparable, due to these slices here (extHandle, intf2impls).
	// If *[]T is used instead, this becomes comparable, at the cost of extra indirection.
	// Thses slices are used all the time, so keep as slices (not pointers).

	extHandle

	intf2impls

	mu sync.Mutex

	jsonHandle   bool
	binaryHandle bool

	// timeBuiltin is initialized from TimeNotBuiltin, and used internally.
	// once initialized, it cannot be changed, as the function for encoding/decoding time.Time
	// will have been cached and the TimeNotBuiltin value will not be consulted thereafter.
	timeBuiltin bool
	_           bool // padding
}

// BasicHandle encapsulates the common options and extension functions.
//
// Deprecated: DO NOT USE DIRECTLY. EXPORTED FOR GODOC BENEFIT. WILL BE REMOVED.
type BasicHandle struct {
	// BasicHandle is always a part of a different type.
	// It doesn't have to fit into it own cache lines.

	// TypeInfos is used to get the type info for any type.
	//
	// If not configured, the default TypeInfos is used, which uses struct tag keys: codec, json
	TypeInfos *TypeInfos

	*basicHandleRuntimeState

	// ---- cache line

	DecodeOptions

	// ---- cache line

	EncodeOptions

	RPCOptions

	// TimeNotBuiltin configures whether time.Time should be treated as a builtin type.
	//
	// All Handlers should know how to encode/decode time.Time as part of the core
	// format specification, or as a standard extension defined by the format.
	//
	// However, users can elect to handle time.Time as a custom extension, or via the
	// standard library's encoding.Binary(M|Unm)arshaler or Text(M|Unm)arshaler interface.
	// To elect this behavior, users can set TimeNotBuiltin=true.
	//
	// Note: Setting TimeNotBuiltin=true can be used to enable the legacy behavior
	// (for Cbor and Msgpack), where time.Time was not a builtin supported type.
	//
	// Note: DO NOT CHANGE AFTER FIRST USE.
	//
	// Once a Handle has been initialized (used), do not modify this option. It will be ignored.
	TimeNotBuiltin bool

	// ExplicitRelease configures whether Release() is implicitly called after an encode or
	// decode call.
	//
	// If you will hold onto an Encoder or Decoder for re-use, by calling Reset(...)
	// on it or calling (Must)Encode repeatedly into a given []byte or io.Writer,
	// then you do not want it to be implicitly closed after each Encode/Decode call.
	// Doing so will unnecessarily return resources to the shared pool, only for you to
	// grab them right after again to do another Encode/Decode call.
	//
	// Instead, you configure ExplicitRelease=true, and you explicitly call Release() when
	// you are truly done.
	//
	// As an alternative, you can explicitly set a finalizer - so its resources
	// are returned to the shared pool before it is garbage-collected. Do it as below:
	//    runtime.SetFinalizer(e, (*Encoder).Release)
	//    runtime.SetFinalizer(d, (*Decoder).Release)
	//
	// Deprecated: This is not longer used as pools are only used for long-lived objects
	// which are shared across goroutines.
	// Setting this value has no effect. It is maintained for backward compatibility.
	ExplicitRelease bool

	// ---- cache line
	inited uint32 // holds if inited, and also handle flags (binary encoding, json handler, etc)

}

// initHandle does a one-time initialization of the handle.
// After this is run, do not modify the Handle, as some modifications are ignored
// e.g. extensions, registered interfaces, TimeNotBuiltIn, etc
func initHandle(hh Handle) {
	x := hh.getBasicHandle()

	// MARKER: We need to simulate once.Do, to ensure no data race within the block.
	// Consequently, below would not work.
	//
	// if atomic.CompareAndSwapUint32(&x.inited, 0, 1) {
	// 	x.be = hh.isBinary()
	// 	x.js = hh.isJson
	// 	x.n = hh.Name()[0]
	// }

	// simulate once.Do using our own stored flag and mutex as a CompareAndSwap
	// is not sufficient, since a race condition can occur within init(Handle) function.
	// init is made noinline, so that this function can be inlined by its caller.
	if atomic.LoadUint32(&x.inited) == 0 {
		x.initHandle(hh)
	}
}

func (x *BasicHandle) basicInit() {
	x.rtidFns.store(nil)
	x.rtidFnsNoExt.store(nil)
	x.timeBuiltin = !x.TimeNotBuiltin
}

func (x *BasicHandle) init() {}

func (x *BasicHandle) isInited() bool {
	return atomic.LoadUint32(&x.inited) != 0
}

// clearInited: DANGEROUS - only use in testing, etc
func (x *BasicHandle) clearInited() {
	atomic.StoreUint32(&x.inited, 0)
}

// TimeBuiltin returns whether time.Time OOTB support is used,
// based on the initial configuration of TimeNotBuiltin
func (x *basicHandleRuntimeState) TimeBuiltin() bool {
	return x.timeBuiltin
}

func (x *basicHandleRuntimeState) isJs() bool {
	return x.jsonHandle
}

func (x *basicHandleRuntimeState) isBe() bool {
	return x.binaryHandle
}

func (x *basicHandleRuntimeState) setExt(rt reflect.Type, tag uint64, ext Ext) (err error) {
	rk := rt.Kind()
	for rk == reflect.Ptr {
		rt = rt.Elem()
		rk = rt.Kind()
	}

	if rt.PkgPath() == "" || rk == reflect.Interface { // || rk == reflect.Ptr {
		return fmt.Errorf("codec.Handle.SetExt: Takes named type, not a pointer or interface: %v", rt)
	}

	rtid := rt2id(rt)
	// handle all natively supported type appropriately, so they cannot have an extension.
	// However, we do not return an error for these, as we do not document that.
	// Instead, we silently treat as a no-op, and return.
	switch rtid {
	case rawTypId, rawExtTypId:
		return
	case timeTypId:
		if x.timeBuiltin {
			return
		}
	}

	for i := range x.extHandle {
		v := &x.extHandle[i]
		if v.rtid == rtid {
			v.tag, v.ext = tag, ext
			return
		}
	}
	rtidptr := rt2id(reflect.PtrTo(rt))
	x.extHandle = append(x.extHandle, extTypeTagFn{rtid, rtidptr, rt, tag, ext})
	return
}

// initHandle should be called only from codec.initHandle global function.
// make it uninlineable, as it is called at most once for each handle.
//
//go:noinline
func (x *BasicHandle) initHandle(hh Handle) {
	handleInitMu.Lock()
	defer handleInitMu.Unlock() // use defer, as halt may panic below
	if x.inited == 0 {
		if x.basicHandleRuntimeState == nil {
			x.basicHandleRuntimeState = new(basicHandleRuntimeState)
		}
		x.jsonHandle = hh.isJson()
		x.binaryHandle = hh.isBinary()
		// ensure MapType and SliceType are of correct type
		if x.MapType != nil && x.MapType.Kind() != reflect.Map {
			halt.onerror(errMapTypeNotMapKind)
		}
		if x.SliceType != nil && x.SliceType.Kind() != reflect.Slice {
			halt.onerror(errSliceTypeNotSliceKind)
		}
		x.basicInit()
		hh.init()
		atomic.StoreUint32(&x.inited, 1)
	}
}

func (x *BasicHandle) getBasicHandle() *BasicHandle {
	return x
}

func (x *BasicHandle) typeInfos() *TypeInfos {
	if x.TypeInfos != nil {
		return x.TypeInfos
	}
	return defTypeInfos
}

func (x *BasicHandle) getTypeInfo(rtid uintptr, rt reflect.Type) (pti *typeInfo) {
	return x.typeInfos().get(rtid, rt)
}

func findRtidFn(s []codecRtidFn, rtid uintptr) (i uint, fn *codecFn) {
	// binary search. adapted from sort/search.go.
	// Note: we use goto (instead of for loop) so this can be inlined.

	// h, i, j := 0, 0, len(s)
	var h uint // var h, i uint
	var j = uint(len(s))
LOOP:
	if i < j {
		h = (i + j) >> 1 // avoid overflow when computing h // h = i + (j-i)/2
		if s[h].rtid < rtid {
			i = h + 1
		} else {
			j = h
		}
		goto LOOP
	}
	if i < uint(len(s)) && s[i].rtid == rtid {
		fn = s[i].fn
	}
	return
}

func (x *BasicHandle) fn(rt reflect.Type) (fn *codecFn) {
	return x.fnVia(rt, x.typeInfos(), &x.rtidFns, x.CheckCircularRef, true)
}

func (x *BasicHandle) fnNoExt(rt reflect.Type) (fn *codecFn) {
	return x.fnVia(rt, x.typeInfos(), &x.rtidFnsNoExt, x.CheckCircularRef, false)
}

func (x *basicHandleRuntimeState) fnVia(rt reflect.Type, tinfos *TypeInfos, fs *atomicRtidFnSlice, checkCircularRef, checkExt bool) (fn *codecFn) {
	rtid := rt2id(rt)
	sp := fs.load()
	if sp != nil {
		if _, fn = findRtidFn(sp, rtid); fn != nil {
			return
		}
	}

	fn = x.fnLoad(rt, rtid, tinfos, checkCircularRef, checkExt)
	x.mu.Lock()
	sp = fs.load()
	// since this is an atomic load/store, we MUST use a different array each time,
	// else we have a data race when a store is happening simultaneously with a findRtidFn call.
	if sp == nil {
		sp = []codecRtidFn{{rtid, fn}}
		fs.store(sp)
	} else {
		idx, fn2 := findRtidFn(sp, rtid)
		if fn2 == nil {
			sp2 := make([]codecRtidFn, len(sp)+1)
			copy(sp2[idx+1:], sp[idx:])
			copy(sp2, sp[:idx])
			sp2[idx] = codecRtidFn{rtid, fn}
			fs.store(sp2)
		}
	}
	x.mu.Unlock()
	return
}

func fnloadFastpathUnderlying(ti *typeInfo) (f *fastpathE, u reflect.Type) {
	var rtid uintptr
	var idx int
	rtid = rt2id(ti.fastpathUnderlying)
	idx = fastpathAvIndex(rtid)
	if idx == -1 {
		return
	}
	f = &fastpathAv[idx]
	if uint8(reflect.Array) == ti.kind {
		u = reflectArrayOf(ti.rt.Len(), ti.elem)
	} else {
		u = f.rt
	}
	return
}

func (x *basicHandleRuntimeState) fnLoad(rt reflect.Type, rtid uintptr, tinfos *TypeInfos, checkCircularRef, checkExt bool) (fn *codecFn) {
	fn = new(codecFn)
	fi := &(fn.i)
	ti := tinfos.get(rtid, rt)
	fi.ti = ti
	rk := reflect.Kind(ti.kind)

	// anything can be an extension except the built-in ones: time, raw and rawext.
	// ensure we check for these types, then if extension, before checking if
	// it implementes one of the pre-declared interfaces.

	fi.addrDf = true
	// fi.addrEf = true

	if rtid == timeTypId && x.timeBuiltin {
		fn.fe = (*Encoder).kTime
		fn.fd = (*Decoder).kTime
	} else if rtid == rawTypId {
		fn.fe = (*Encoder).raw
		fn.fd = (*Decoder).raw
	} else if rtid == rawExtTypId {
		fn.fe = (*Encoder).rawExt
		fn.fd = (*Decoder).rawExt
		fi.addrD = true
		fi.addrE = true
	} else if xfFn := x.getExt(rtid, checkExt); xfFn != nil {
		fi.xfTag, fi.xfFn = xfFn.tag, xfFn.ext
		fn.fe = (*Encoder).ext
		fn.fd = (*Decoder).ext
		fi.addrD = true
		if rk == reflect.Struct || rk == reflect.Array {
			fi.addrE = true
		}
	} else if (ti.flagSelfer || ti.flagSelferPtr) &&
		!(checkCircularRef && ti.flagSelferViaCodecgen && ti.kind == byte(reflect.Struct)) {
		// do not use Selfer generated by codecgen if it is a struct and CheckCircularRef=true
		fn.fe = (*Encoder).selferMarshal
		fn.fd = (*Decoder).selferUnmarshal
		fi.addrD = ti.flagSelferPtr
		fi.addrE = ti.flagSelferPtr
	} else if supportMarshalInterfaces && x.isBe() &&
		(ti.flagBinaryMarshaler || ti.flagBinaryMarshalerPtr) &&
		(ti.flagBinaryUnmarshaler || ti.flagBinaryUnmarshalerPtr) {
		fn.fe = (*Encoder).binaryMarshal
		fn.fd = (*Decoder).binaryUnmarshal
		fi.addrD = ti.flagBinaryUnmarshalerPtr
		fi.addrE = ti.flagBinaryMarshalerPtr
	} else if supportMarshalInterfaces && !x.isBe() && x.isJs() &&
		(ti.flagJsonMarshaler || ti.flagJsonMarshalerPtr) &&
		(ti.flagJsonUnmarshaler || ti.flagJsonUnmarshalerPtr) {
		//If JSON, we should check JSONMarshal before textMarshal
		fn.fe = (*Encoder).jsonMarshal
		fn.fd = (*Decoder).jsonUnmarshal
		fi.addrD = ti.flagJsonUnmarshalerPtr
		fi.addrE = ti.flagJsonMarshalerPtr
	} else if supportMarshalInterfaces && !x.isBe() &&
		(ti.flagTextMarshaler || ti.flagTextMarshalerPtr) &&
		(ti.flagTextUnmarshaler || ti.flagTextUnmarshalerPtr) {
		fn.fe = (*Encoder).textMarshal
		fn.fd = (*Decoder).textUnmarshal
		fi.addrD = ti.flagTextUnmarshalerPtr
		fi.addrE = ti.flagTextMarshalerPtr
	} else {
		if fastpathEnabled && (rk == reflect.Map || rk == reflect.Slice || rk == reflect.Array) {
			// by default (without using unsafe),
			// if an array is not addressable, converting from an array to a slice
			// requires an allocation (see helper_not_unsafe.go: func rvGetSlice4Array).
			//
			// (Non-addressable arrays mostly occur as keys/values from a map).
			//
			// However, fastpath functions are mostly for slices of numbers or strings,
			// which are small by definition and thus allocation should be fast/cheap in time.
			//
			// Consequently, the value of doing this quick allocation to elide the overhead cost of
			// non-optimized (not-unsafe) reflection is a fair price.
			var rtid2 uintptr
			if !ti.flagHasPkgPath { // un-named type (slice or mpa or array)
				rtid2 = rtid
				if rk == reflect.Array {
					rtid2 = rt2id(ti.key) // ti.key for arrays = reflect.SliceOf(ti.elem)
				}
				if idx := fastpathAvIndex(rtid2); idx != -1 {
					fn.fe = fastpathAv[idx].encfn
					fn.fd = fastpathAv[idx].decfn
					fi.addrD = true
					fi.addrDf = false
					if rk == reflect.Array {
						fi.addrD = false // decode directly into array value (slice made from it)
					}
				}
			} else { // named type (with underlying type of map or slice or array)
				// try to use mapping for underlying type
				xfe, xrt := fnloadFastpathUnderlying(ti)
				if xfe != nil {
					xfnf := xfe.encfn
					xfnf2 := xfe.decfn
					if rk == reflect.Array {
						fi.addrD = false // decode directly into array value (slice made from it)
						fn.fd = func(d *Decoder, xf *codecFnInfo, xrv reflect.Value) {
							xfnf2(d, xf, rvConvert(xrv, xrt))
						}
					} else {
						fi.addrD = true
						fi.addrDf = false // meaning it can be an address(ptr) or a value
						xptr2rt := reflect.PtrTo(xrt)
						fn.fd = func(d *Decoder, xf *codecFnInfo, xrv reflect.Value) {
							if xrv.Kind() == reflect.Ptr {
								xfnf2(d, xf, rvConvert(xrv, xptr2rt))
							} else {
								xfnf2(d, xf, rvConvert(xrv, xrt))
							}
						}
					}
					fn.fe = func(e *Encoder, xf *codecFnInfo, xrv reflect.Value) {
						xfnf(e, xf, rvConvert(xrv, xrt))
					}
				}
			}
		}
		if fn.fe == nil && fn.fd == nil {
			switch rk {
			case reflect.Bool:
				fn.fe = (*Encoder).kBool
				fn.fd = (*Decoder).kBool
			case reflect.String:
				// Do not use different functions based on StringToRaw option, as that will statically
				// set the function for a string type, and if the Handle is modified thereafter,
				// behaviour is non-deterministic
				// i.e. DO NOT DO:
				//   if x.StringToRaw {
				//   	fn.fe = (*Encoder).kStringToRaw
				//   } else {
				//   	fn.fe = (*Encoder).kStringEnc
				//   }

				fn.fe = (*Encoder).kString
				fn.fd = (*Decoder).kString
			case reflect.Int:
				fn.fd = (*Decoder).kInt
				fn.fe = (*Encoder).kInt
			case reflect.Int8:
				fn.fe = (*Encoder).kInt8
				fn.fd = (*Decoder).kInt8
			case reflect.Int16:
				fn.fe = (*Encoder).kInt16
				fn.fd = (*Decoder).kInt16
			case reflect.Int32:
				fn.fe = (*Encoder).kInt32
				fn.fd = (*Decoder).kInt32
			case reflect.Int64:
				fn.fe = (*Encoder).kInt64
				fn.fd = (*Decoder).kInt64
			case reflect.Uint:
				fn.fd = (*Decoder).kUint
				fn.fe = (*Encoder).kUint
			case reflect.Uint8:
				fn.fe = (*Encoder).kUint8
				fn.fd = (*Decoder).kUint8
			case reflect.Uint16:
				fn.fe = (*Encoder).kUint16
				fn.fd = (*Decoder).kUint16
			case reflect.Uint32:
				fn.fe = (*Encoder).kUint32
				fn.fd = (*Decoder).kUint32
			case reflect.Uint64:
				fn.fe = (*Encoder).kUint64
				fn.fd = (*Decoder).kUint64
			case reflect.Uintptr:
				fn.fe = (*Encoder).kUintptr
				fn.fd = (*Decoder).kUintptr
			case reflect.Float32:
				fn.fe = (*Encoder).kFloat32
				fn.fd = (*Decoder).kFloat32
			case reflect.Float64:
				fn.fe = (*Encoder).kFloat64
				fn.fd = (*Decoder).kFloat64
			case reflect.Complex64:
				fn.fe = (*Encoder).kComplex64
				fn.fd = (*Decoder).kComplex64
			case reflect.Complex128:
				fn.fe = (*Encoder).kComplex128
				fn.fd = (*Decoder).kComplex128
			case reflect.Chan:
				fn.fe = (*Encoder).kChan
				fn.fd = (*Decoder).kChan
			case reflect.Slice:
				fn.fe = (*Encoder).kSlice
				fn.fd = (*Decoder).kSlice
			case reflect.Array:
				fi.addrD = false // decode directly into array value (slice made from it)
				fn.fe = (*Encoder).kArray
				fn.fd = (*Decoder).kArray
			case reflect.Struct:
				if ti.anyOmitEmpty ||
					ti.flagMissingFielder ||
					ti.flagMissingFielderPtr {
					fn.fe = (*Encoder).kStruct
				} else {
					fn.fe = (*Encoder).kStructNoOmitempty
				}
				fn.fd = (*Decoder).kStruct
			case reflect.Map:
				fn.fe = (*Encoder).kMap
				fn.fd = (*Decoder).kMap
			case reflect.Interface:
				// encode: reflect.Interface are handled already by preEncodeValue
				fn.fd = (*Decoder).kInterface
				fn.fe = (*Encoder).kErr
			default:
				// reflect.Ptr and reflect.Interface are handled already by preEncodeValue
				fn.fe = (*Encoder).kErr
				fn.fd = (*Decoder).kErr
			}
		}
	}
	return
}

// Handle defines a specific encoding format. It also stores any runtime state
// used during an Encoding or Decoding session e.g. stored state about Types, etc.
//
// Once a handle is configured, it can be shared across multiple Encoders and Decoders.
//
// Note that a Handle is NOT safe for concurrent modification.
//
// A Handle also should not be modified after it is configured and has
// been used at least once. This is because stored state may be out of sync with the
// new configuration, and a data race can occur when multiple goroutines access it.
// i.e. multiple Encoders or Decoders in different goroutines.
//
// Consequently, the typical usage model is that a Handle is pre-configured
// before first time use, and not modified while in use.
// Such a pre-configured Handle is safe for concurrent access.
type Handle interface {
	Name() string
	getBasicHandle() *BasicHandle
	newEncDriver() encDriver
	newDecDriver() decDriver
	isBinary() bool
	isJson() bool // json is special for now, so track it
	// desc describes the current byte descriptor, or returns "unknown[XXX]" if not understood.
	desc(bd byte) string
	// init initializes the handle based on handle-specific info (beyond what is in BasicHandle)
	init()
}

// Raw represents raw formatted bytes.
// We "blindly" store it during encode and retrieve the raw bytes during decode.
// Note: it is dangerous during encode, so we may gate the behaviour
// behind an Encode flag which must be explicitly set.
type Raw []byte

// RawExt represents raw unprocessed extension data.
// Some codecs will decode extension data as a *RawExt
// if there is no registered extension for the tag.
//
// Only one of Data or Value is nil.
// If Data is nil, then the content of the RawExt is in the Value.
type RawExt struct {
	Tag uint64
	// Data is the []byte which represents the raw ext. If nil, ext is exposed in Value.
	// Data is used by codecs (e.g. binc, msgpack, simple) which do custom serialization of types
	Data []byte
	// Value represents the extension, if Data is nil.
	// Value is used by codecs (e.g. cbor, json) which leverage the format to do
	// custom serialization of the types.
	Value interface{}
}

func (re *RawExt) setData(xbs []byte, zerocopy bool) {
	if zerocopy {
		re.Data = xbs
	} else {
		re.Data = append(re.Data[:0], xbs...)
	}
}

// BytesExt handles custom (de)serialization of types to/from []byte.
// It is used by codecs (e.g. binc, msgpack, simple) which do custom serialization of the types.
type BytesExt interface {
	// WriteExt converts a value to a []byte.
	//
	// Note: v is a pointer iff the registered extension type is a struct or array kind.
	WriteExt(v interface{}) []byte

	// ReadExt updates a value from a []byte.
	//
	// Note: dst is always a pointer kind to the registered extension type.
	ReadExt(dst interface{}, src []byte)
}

// InterfaceExt handles custom (de)serialization of types to/from another interface{} value.
// The Encoder or Decoder will then handle the further (de)serialization of that known type.
//
// It is used by codecs (e.g. cbor, json) which use the format to do custom serialization of types.
type InterfaceExt interface {
	// ConvertExt converts a value into a simpler interface for easy encoding
	// e.g. convert time.Time to int64.
	//
	// Note: v is a pointer iff the registered extension type is a struct or array kind.
	ConvertExt(v interface{}) interface{}

	// UpdateExt updates a value from a simpler interface for easy decoding
	// e.g. convert int64 to time.Time.
	//
	// Note: dst is always a pointer kind to the registered extension type.
	UpdateExt(dst interface{}, src interface{})
}

// Ext handles custom (de)serialization of custom types / extensions.
type Ext interface {
	BytesExt
	InterfaceExt
}

// addExtWrapper is a wrapper implementation to support former AddExt exported method.
type addExtWrapper struct {
	encFn func(reflect.Value) ([]byte, error)
	decFn func(reflect.Value, []byte) error
}

func (x addExtWrapper) WriteExt(v interface{}) []byte {
	bs, err := x.encFn(reflect.ValueOf(v))
	halt.onerror(err)
	return bs
}

func (x addExtWrapper) ReadExt(v interface{}, bs []byte) {
	halt.onerror(x.decFn(reflect.ValueOf(v), bs))
}

func (x addExtWrapper) ConvertExt(v interface{}) interface{} {
	return x.WriteExt(v)
}

func (x addExtWrapper) UpdateExt(dest interface{}, v interface{}) {
	x.ReadExt(dest, v.([]byte))
}

type bytesExtFailer struct{}

func (bytesExtFailer) WriteExt(v interface{}) []byte {
	halt.onerror(errExtFnWriteExtUnsupported)
	return nil
}
func (bytesExtFailer) ReadExt(v interface{}, bs []byte) {
	halt.onerror(errExtFnReadExtUnsupported)
}

type interfaceExtFailer struct{}

func (interfaceExtFailer) ConvertExt(v interface{}) interface{} {
	halt.onerror(errExtFnConvertExtUnsupported)
	return nil
}
func (interfaceExtFailer) UpdateExt(dest interface{}, v interface{}) {
	halt.onerror(errExtFnUpdateExtUnsupported)
}

type bytesExtWrapper struct {
	interfaceExtFailer
	BytesExt
}

type interfaceExtWrapper struct {
	bytesExtFailer
	InterfaceExt
}

type extFailWrapper struct {
	bytesExtFailer
	interfaceExtFailer
}

type binaryEncodingType struct{}

func (binaryEncodingType) isBinary() bool { return true }
func (binaryEncodingType) isJson() bool   { return false }

type textEncodingType struct{}

func (textEncodingType) isBinary() bool { return false }
func (textEncodingType) isJson() bool   { return false }

type notJsonType struct{}

func (notJsonType) isJson() bool { return false }

// noBuiltInTypes is embedded into many types which do not support builtins
// e.g. msgpack, simple, cbor.

type noBuiltInTypes struct{}

func (noBuiltInTypes) EncodeBuiltin(rt uintptr, v interface{}) {}
func (noBuiltInTypes) DecodeBuiltin(rt uintptr, v interface{}) {}

// bigenHelper handles ByteOrder operations directly using
// arrays of bytes (not slice of bytes).
//
// Since byteorder operations are very common for encoding and decoding
// numbers, lengths, etc - it is imperative that this operation is as
// fast as possible. Removing indirection (pointer chasing) to look
// at up to 8 bytes helps a lot here.
//
// For times where it is expedient to use a slice, delegate to the
// bigenstd (equal to the binary.BigEndian value).
//
// retrofitted from stdlib: encoding/binary/BigEndian (ByteOrder)
type bigenHelper struct{}

func (z bigenHelper) PutUint16(v uint16) (b [2]byte) {
	return [...]byte{
		byte(v >> 8),
		byte(v),
	}
}

func (z bigenHelper) PutUint32(v uint32) (b [4]byte) {
	return [...]byte{
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	}
}

func (z bigenHelper) PutUint64(v uint64) (b [8]byte) {
	return [...]byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	}
}

func (z bigenHelper) Uint16(b [2]byte) (v uint16) {
	return uint16(b[1]) |
		uint16(b[0])<<8
}

func (z bigenHelper) Uint32(b [4]byte) (v uint32) {
	return uint32(b[3]) |
		uint32(b[2])<<8 |
		uint32(b[1])<<16 |
		uint32(b[0])<<24
}

func (z bigenHelper) Uint64(b [8]byte) (v uint64) {
	return uint64(b[7]) |
		uint64(b[6])<<8 |
		uint64(b[5])<<16 |
		uint64(b[4])<<24 |
		uint64(b[3])<<32 |
		uint64(b[2])<<40 |
		uint64(b[1])<<48 |
		uint64(b[0])<<56
}

func (z bigenHelper) writeUint16(w *encWr, v uint16) {
	x := z.PutUint16(v)
	w.writen2(x[0], x[1])
}

func (z bigenHelper) writeUint32(w *encWr, v uint32) {
	// w.writeb((z.PutUint32(v))[:])
	// x := z.PutUint32(v)
	// w.writeb(x[:])
	// w.writen4(x[0], x[1], x[2], x[3])
	w.writen4(z.PutUint32(v))
}

func (z bigenHelper) writeUint64(w *encWr, v uint64) {
	w.writen8(z.PutUint64(v))
}

type extTypeTagFn struct {
	rtid    uintptr
	rtidptr uintptr
	rt      reflect.Type
	tag     uint64
	ext     Ext
}

type extHandle []extTypeTagFn

// AddExt registes an encode and decode function for a reflect.Type.
// To deregister an Ext, call AddExt with nil encfn and/or nil decfn.
//
// Deprecated: Use SetBytesExt or SetInterfaceExt on the Handle instead.
func (x *BasicHandle) AddExt(rt reflect.Type, tag byte,
	encfn func(reflect.Value) ([]byte, error),
	decfn func(reflect.Value, []byte) error) (err error) {
	if encfn == nil || decfn == nil {
		return x.SetExt(rt, uint64(tag), nil)
	}
	return x.SetExt(rt, uint64(tag), addExtWrapper{encfn, decfn})
}

// SetExt will set the extension for a tag and reflect.Type.
// Note that the type must be a named type, and specifically not a pointer or Interface.
// An error is returned if that is not honored.
// To Deregister an ext, call SetExt with nil Ext.
//
// Deprecated: Use SetBytesExt or SetInterfaceExt on the Handle instead.
func (x *BasicHandle) SetExt(rt reflect.Type, tag uint64, ext Ext) (err error) {
	if x.isInited() {
		return errHandleInited
	}
	if x.basicHandleRuntimeState == nil {
		x.basicHandleRuntimeState = new(basicHandleRuntimeState)
	}
	return x.basicHandleRuntimeState.setExt(rt, tag, ext)
}

func (o extHandle) getExtForI(x interface{}) (v *extTypeTagFn) {
	if len(o) > 0 {
		v = o.getExt(i2rtid(x), true)
	}
	return
}

func (o extHandle) getExt(rtid uintptr, check bool) (v *extTypeTagFn) {
	if !check {
		return
	}
	for i := range o {
		v = &o[i]
		if v.rtid == rtid || v.rtidptr == rtid {
			return
		}
	}
	return nil
}

func (o extHandle) getExtForTag(tag uint64) (v *extTypeTagFn) {
	for i := range o {
		v = &o[i]
		if v.tag == tag {
			return
		}
	}
	return nil
}

type intf2impl struct {
	rtid uintptr // for intf
	impl reflect.Type
}

type intf2impls []intf2impl

// Intf2Impl maps an interface to an implementing type.
// This allows us support infering the concrete type
// and populating it when passed an interface.
// e.g. var v io.Reader can be decoded as a bytes.Buffer, etc.
//
// Passing a nil impl will clear the mapping.
func (o *intf2impls) Intf2Impl(intf, impl reflect.Type) (err error) {
	if impl != nil && !impl.Implements(intf) {
		return fmt.Errorf("Intf2Impl: %v does not implement %v", impl, intf)
	}
	rtid := rt2id(intf)
	o2 := *o
	for i := range o2 {
		v := &o2[i]
		if v.rtid == rtid {
			v.impl = impl
			return
		}
	}
	*o = append(o2, intf2impl{rtid, impl})
	return
}

func (o intf2impls) intf2impl(rtid uintptr) (rv reflect.Value) {
	for i := range o {
		v := &o[i]
		if v.rtid == rtid {
			if v.impl == nil {
				return
			}
			vkind := v.impl.Kind()
			if vkind == reflect.Ptr {
				return reflect.New(v.impl.Elem())
			}
			return rvZeroAddrK(v.impl, vkind)
		}
	}
	return
}

// structFieldinfopathNode is a node in a tree, which allows us easily
// walk the anonymous path.
//
// In the typical case, the node is not embedded/anonymous, and thus the parent
// will be nil and this information becomes a value (not needing any indirection).
type structFieldInfoPathNode struct {
	parent *structFieldInfoPathNode

	offset   uint16
	index    uint16
	kind     uint8
	numderef uint8

	// encNameAsciiAlphaNum and omitEmpty should be in structFieldInfo,
	// but are kept here for tighter packaging.

	encNameAsciiAlphaNum bool // the encName only contains ascii alphabet and numbers
	omitEmpty            bool

	typ reflect.Type
}

// depth returns number of valid nodes in the hierachy
func (path *structFieldInfoPathNode) depth() (d int) {
TOP:
	if path != nil {
		d++
		path = path.parent
		goto TOP
	}
	return
}

// field returns the field of the struct.
func (path *structFieldInfoPathNode) field(v reflect.Value) (rv2 reflect.Value) {
	if parent := path.parent; parent != nil {
		v = parent.field(v)
		for j, k := uint8(0), parent.numderef; j < k; j++ {
			if rvIsNil(v) {
				return
			}
			v = v.Elem()
		}
	}
	return path.rvField(v)
}

// fieldAlloc returns the field of the struct.
// It allocates if a nil value was seen while searching.
func (path *structFieldInfoPathNode) fieldAlloc(v reflect.Value) (rv2 reflect.Value) {
	if parent := path.parent; parent != nil {
		v = parent.fieldAlloc(v)
		for j, k := uint8(0), parent.numderef; j < k; j++ {
			if rvIsNil(v) {
				rvSetDirect(v, reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
	}
	return path.rvField(v)
}

type structFieldInfo struct {
	encName string // encode name

	// encNameHash uintptr

	// fieldName string // currently unused

	// encNameAsciiAlphaNum and omitEmpty should be here,
	// but are stored in structFieldInfoPathNode for tighter packaging.

	path structFieldInfoPathNode
}

func parseStructInfo(stag string) (toArray, omitEmpty bool, keytype valueType) {
	keytype = valueTypeString // default
	if stag == "" {
		return
	}
	ss := strings.Split(stag, ",")
	if len(ss) < 2 {
		return
	}
	for _, s := range ss[1:] {
		switch s {
		case "omitempty":
			omitEmpty = true
		case "toarray":
			toArray = true
		case "int":
			keytype = valueTypeInt
		case "uint":
			keytype = valueTypeUint
		case "float":
			keytype = valueTypeFloat
			// case "bool":
			// 	keytype = valueTypeBool
		case "string":
			keytype = valueTypeString
		}
	}
	return
}

func (si *structFieldInfo) parseTag(stag string) {
	if stag == "" {
		return
	}
	for i, s := range strings.Split(stag, ",") {
		if i == 0 {
			if s != "" {
				si.encName = s
			}
		} else {
			switch s {
			case "omitempty":
				si.path.omitEmpty = true
			}
		}
	}
}

type sfiSortedByEncName []*structFieldInfo

func (p sfiSortedByEncName) Len() int           { return len(p) }
func (p sfiSortedByEncName) Swap(i, j int)      { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p sfiSortedByEncName) Less(i, j int) bool { return p[uint(i)].encName < p[uint(j)].encName }

// typeInfo4Container holds information that is only available for
// containers like map, array, chan, slice.
type typeInfo4Container struct {
	elem reflect.Type
	// key is:
	//   - if map kind: map key
	//   - if array kind: sliceOf(elem)
	//   - if chan kind: sliceof(elem)
	key reflect.Type

	// fastpathUnderlying is underlying type of a named slice/map/array, as defined by go spec,
	// that is used by fastpath where we defined fastpath functions for the underlying type.
	//
	// for a map, it's a map; for a slice or array, it's a slice; else its nil.
	fastpathUnderlying reflect.Type

	tikey  *typeInfo
	tielem *typeInfo
}

// typeInfo keeps static (non-changing readonly)information
// about each (non-ptr) type referenced in the encode/decode sequence.
//
// During an encode/decode sequence, we work as below:
//   - If base is a built in type, en/decode base value
//   - If base is registered as an extension, en/decode base value
//   - If type is binary(M/Unm)arshaler, call Binary(M/Unm)arshal method
//   - If type is text(M/Unm)arshaler, call Text(M/Unm)arshal method
//   - Else decode appropriately based on the reflect.Kind
type typeInfo struct {
	rt  reflect.Type
	ptr reflect.Type

	// pkgpath string

	rtid uintptr

	numMeth uint16 // number of methods
	kind    uint8
	chandir uint8

	anyOmitEmpty bool      // true if a struct, and any of the fields are tagged "omitempty"
	toArray      bool      // whether this (struct) type should be encoded as an array
	keyType      valueType // if struct, how is the field name stored in a stream? default is string
	mbs          bool      // base type (T or *T) is a MapBySlice

	sfi4Name map[string]*structFieldInfo // map. used for finding sfi given a name

	*typeInfo4Container

	// ---- cpu cache line boundary?

	size, keysize, elemsize uint32

	keykind, elemkind uint8

	flagHasPkgPath   bool // Type.PackagePath != ""
	flagComparable   bool
	flagCanTransient bool

	flagMarshalInterface  bool // does this have custom (un)marshal implementation?
	flagSelferViaCodecgen bool

	// custom implementation flags
	flagIsZeroer    bool
	flagIsZeroerPtr bool

	flagIsCodecEmptyer    bool
	flagIsCodecEmptyerPtr bool

	flagBinaryMarshaler    bool
	flagBinaryMarshalerPtr bool

	flagBinaryUnmarshaler    bool
	flagBinaryUnmarshalerPtr bool

	flagTextMarshaler    bool
	flagTextMarshalerPtr bool

	flagTextUnmarshaler    bool
	flagTextUnmarshalerPtr bool

	flagJsonMarshaler    bool
	flagJsonMarshalerPtr bool

	flagJsonUnmarshaler    bool
	flagJsonUnmarshalerPtr bool

	flagSelfer    bool
	flagSelferPtr bool

	flagMissingFielder    bool
	flagMissingFielderPtr bool

	infoFieldOmitempty bool

	sfi structFieldInfos
}

func (ti *typeInfo) siForEncName(name []byte) (si *structFieldInfo) {
	return ti.sfi4Name[string(name)]
}

func (ti *typeInfo) resolve(x []structFieldInfo, ss map[string]uint16) (n int) {
	n = len(x)

	for i := range x {
		ui := uint16(i)
		xn := x[i].encName
		j, ok := ss[xn]
		if ok {
			i2clear := ui                              // index to be cleared
			if x[i].path.depth() < x[j].path.depth() { // this one is shallower
				ss[xn] = ui
				i2clear = j
			}
			if x[i2clear].encName != "" {
				x[i2clear].encName = ""
				n--
			}
		} else {
			ss[xn] = ui
		}
	}

	return
}

func (ti *typeInfo) init(x []structFieldInfo, n int) {
	var anyOmitEmpty bool

	// remove all the nils (non-ready)
	m := make(map[string]*structFieldInfo, n)
	w := make([]structFieldInfo, n)
	y := make([]*structFieldInfo, n+n)
	z := y[n:]
	y = y[:n]
	n = 0
	for i := range x {
		if x[i].encName == "" {
			continue
		}
		if !anyOmitEmpty && x[i].path.omitEmpty {
			anyOmitEmpty = true
		}
		w[n] = x[i]
		y[n] = &w[n]
		m[x[i].encName] = &w[n]
		n++
	}
	if n != len(y) {
		halt.errorf("failure reading struct %v - expecting %d of %d valid fields, got %d", ti.rt, len(y), len(x), n)
	}

	copy(z, y)
	sort.Sort(sfiSortedByEncName(z))

	ti.anyOmitEmpty = anyOmitEmpty
	ti.sfi.load(y, z)
	ti.sfi4Name = m
}

// Handling flagCanTransient
//
// We support transient optimization if the kind of the type is
// a number, bool, string, or slice (of number/bool).
// In addition, we also support if the kind is struct or array,
// and the type does not contain any pointers recursively).
//
// Noteworthy that all reference types (string, slice, func, map, ptr, interface, etc) have pointers.
//
// If using transient for a type with a pointer, there is the potential for data corruption
// when GC tries to follow a "transient" pointer which may become a non-pointer soon after.
//

func transientBitsetFlags() *bitset32 {
	if transientValueHasStringSlice {
		return &numBoolStrSliceBitset
	}
	return &numBoolBitset
}

func isCanTransient(t reflect.Type, k reflect.Kind) (v bool) {
	var bs = transientBitsetFlags()
	if bs.isset(byte(k)) {
		v = true
	} else if k == reflect.Slice {
		elem := t.Elem()
		v = numBoolBitset.isset(byte(elem.Kind()))
	} else if k == reflect.Array {
		elem := t.Elem()
		v = isCanTransient(elem, elem.Kind())
	} else if k == reflect.Struct {
		v = true
		for j, jlen := 0, t.NumField(); j < jlen; j++ {
			f := t.Field(j)
			if !isCanTransient(f.Type, f.Type.Kind()) {
				v = false
				return
			}
		}
	} else {
		v = false
	}
	return
}

func (ti *typeInfo) doSetFlagCanTransient() {
	if transientSizeMax > 0 {
		ti.flagCanTransient = ti.size <= transientSizeMax
	} else {
		ti.flagCanTransient = true
	}
	if ti.flagCanTransient {
		if !transientBitsetFlags().isset(ti.kind) {
			ti.flagCanTransient = isCanTransient(ti.rt, reflect.Kind(ti.kind))
		}
	}
}

type rtid2ti struct {
	rtid uintptr
	ti   *typeInfo
}

// TypeInfos caches typeInfo for each type on first inspection.
//
// It is configured with a set of tag keys, which are used to get
// configuration for the type.
type TypeInfos struct {
	infos atomicTypeInfoSlice
	mu    sync.Mutex
	_     uint64 // padding (cache-aligned)
	tags  []string
	_     uint64 // padding (cache-aligned)
}

// NewTypeInfos creates a TypeInfos given a set of struct tags keys.
//
// This allows users customize the struct tag keys which contain configuration
// of their types.
func NewTypeInfos(tags []string) *TypeInfos {
	return &TypeInfos{tags: tags}
}

func (x *TypeInfos) structTag(t reflect.StructTag) (s string) {
	// check for tags: codec, json, in that order.
	// this allows seamless support for many configured structs.
	for _, x := range x.tags {
		s = t.Get(x)
		if s != "" {
			return s
		}
	}
	return
}

func findTypeInfo(s []rtid2ti, rtid uintptr) (i uint, ti *typeInfo) {
	// binary search. adapted from sort/search.go.
	// Note: we use goto (instead of for loop) so this can be inlined.

	var h uint
	var j = uint(len(s))
LOOP:
	if i < j {
		h = (i + j) >> 1 // avoid overflow when computing h // h = i + (j-i)/2
		if s[h].rtid < rtid {
			i = h + 1
		} else {
			j = h
		}
		goto LOOP
	}
	if i < uint(len(s)) && s[i].rtid == rtid {
		ti = s[i].ti
	}
	return
}

func (x *TypeInfos) get(rtid uintptr, rt reflect.Type) (pti *typeInfo) {
	if pti = x.find(rtid); pti == nil {
		pti = x.load(rt)
	}
	return
}

func (x *TypeInfos) find(rtid uintptr) (pti *typeInfo) {
	sp := x.infos.load()
	if sp != nil {
		_, pti = findTypeInfo(sp, rtid)
	}
	return
}

func (x *TypeInfos) load(rt reflect.Type) (pti *typeInfo) {
	rk := rt.Kind()

	if rk == reflect.Ptr { // || (rk == reflect.Interface && rtid != intfTypId) {
		halt.errorf("invalid kind passed to TypeInfos.get: %v - %v", rk, rt)
	}

	rtid := rt2id(rt)

	// do not hold lock while computing this.
	// it may lead to duplication, but that's ok.
	ti := typeInfo{
		rt:      rt,
		ptr:     reflect.PtrTo(rt),
		rtid:    rtid,
		kind:    uint8(rk),
		size:    uint32(rt.Size()),
		numMeth: uint16(rt.NumMethod()),
		keyType: valueTypeString, // default it - so it's never 0

		// pkgpath: rt.PkgPath(),
		flagHasPkgPath: rt.PkgPath() != "",
	}

	// bset sets custom implementation flags
	bset := func(when bool, b *bool) {
		if when {
			*b = true
		}
	}

	var b1, b2 bool

	b1, b2 = implIntf(rt, binaryMarshalerTyp)
	bset(b1, &ti.flagBinaryMarshaler)
	bset(b2, &ti.flagBinaryMarshalerPtr)
	b1, b2 = implIntf(rt, binaryUnmarshalerTyp)
	bset(b1, &ti.flagBinaryUnmarshaler)
	bset(b2, &ti.flagBinaryUnmarshalerPtr)
	b1, b2 = implIntf(rt, textMarshalerTyp)
	bset(b1, &ti.flagTextMarshaler)
	bset(b2, &ti.flagTextMarshalerPtr)
	b1, b2 = implIntf(rt, textUnmarshalerTyp)
	bset(b1, &ti.flagTextUnmarshaler)
	bset(b2, &ti.flagTextUnmarshalerPtr)
	b1, b2 = implIntf(rt, jsonMarshalerTyp)
	bset(b1, &ti.flagJsonMarshaler)
	bset(b2, &ti.flagJsonMarshalerPtr)
	b1, b2 = implIntf(rt, jsonUnmarshalerTyp)
	bset(b1, &ti.flagJsonUnmarshaler)
	bset(b2, &ti.flagJsonUnmarshalerPtr)
	b1, b2 = implIntf(rt, selferTyp)
	bset(b1, &ti.flagSelfer)
	bset(b2, &ti.flagSelferPtr)
	b1, b2 = implIntf(rt, missingFielderTyp)
	bset(b1, &ti.flagMissingFielder)
	bset(b2, &ti.flagMissingFielderPtr)
	b1, b2 = implIntf(rt, iszeroTyp)
	bset(b1, &ti.flagIsZeroer)
	bset(b2, &ti.flagIsZeroerPtr)
	b1, b2 = implIntf(rt, isCodecEmptyerTyp)
	bset(b1, &ti.flagIsCodecEmptyer)
	bset(b2, &ti.flagIsCodecEmptyerPtr)

	b1, b2 = implIntf(rt, isSelferViaCodecgenerTyp)
	ti.flagSelferViaCodecgen = b1 || b2

	ti.flagMarshalInterface = ti.flagSelfer || ti.flagSelferPtr ||
		ti.flagSelferViaCodecgen ||
		ti.flagBinaryMarshaler || ti.flagBinaryMarshalerPtr ||
		ti.flagBinaryUnmarshaler || ti.flagBinaryUnmarshalerPtr ||
		ti.flagTextMarshaler || ti.flagTextMarshalerPtr ||
		ti.flagTextUnmarshaler || ti.flagTextUnmarshalerPtr ||
		ti.flagJsonMarshaler || ti.flagJsonMarshalerPtr ||
		ti.flagJsonUnmarshaler || ti.flagJsonUnmarshalerPtr

	b1 = rt.Comparable()
	// bset(b1, &ti.flagComparable)
	ti.flagComparable = b1

	ti.doSetFlagCanTransient()

	var tt reflect.Type
	switch rk {
	case reflect.Struct:
		var omitEmpty bool
		if f, ok := rt.FieldByName(structInfoFieldName); ok {
			ti.toArray, omitEmpty, ti.keyType = parseStructInfo(x.structTag(f.Tag))
			ti.infoFieldOmitempty = omitEmpty
		} else {
			ti.keyType = valueTypeString
		}
		pp, pi := &pool4tiload, pool4tiload.Get()
		pv := pi.(*typeInfoLoad)
		pv.reset()
		pv.etypes = append(pv.etypes, ti.rtid)
		x.rget(rt, rtid, nil, pv, omitEmpty)
		n := ti.resolve(pv.sfis, pv.sfiNames)
		ti.init(pv.sfis, n)
		pp.Put(pi)
	case reflect.Map:
		ti.typeInfo4Container = new(typeInfo4Container)
		ti.elem = rt.Elem()
		for tt = ti.elem; tt.Kind() == reflect.Ptr; tt = tt.Elem() {
		}
		ti.tielem = x.get(rt2id(tt), tt)
		ti.elemkind = uint8(ti.elem.Kind())
		ti.elemsize = uint32(ti.elem.Size())
		ti.key = rt.Key()
		for tt = ti.key; tt.Kind() == reflect.Ptr; tt = tt.Elem() {
		}
		ti.tikey = x.get(rt2id(tt), tt)
		ti.keykind = uint8(ti.key.Kind())
		ti.keysize = uint32(ti.key.Size())
		if ti.flagHasPkgPath {
			ti.fastpathUnderlying = reflect.MapOf(ti.key, ti.elem)
		}
	case reflect.Slice:
		ti.typeInfo4Container = new(typeInfo4Container)
		ti.mbs, b2 = implIntf(rt, mapBySliceTyp)
		if !ti.mbs && b2 {
			ti.mbs = b2
		}
		ti.elem = rt.Elem()
		for tt = ti.elem; tt.Kind() == reflect.Ptr; tt = tt.Elem() {
		}
		ti.tielem = x.get(rt2id(tt), tt)
		ti.elemkind = uint8(ti.elem.Kind())
		ti.elemsize = uint32(ti.elem.Size())
		if ti.flagHasPkgPath {
			ti.fastpathUnderlying = reflect.SliceOf(ti.elem)
		}
	case reflect.Chan:
		ti.typeInfo4Container = new(typeInfo4Container)
		ti.elem = rt.Elem()
		for tt = ti.elem; tt.Kind() == reflect.Ptr; tt = tt.Elem() {
		}
		ti.tielem = x.get(rt2id(tt), tt)
		ti.elemkind = uint8(ti.elem.Kind())
		ti.elemsize = uint32(ti.elem.Size())
		ti.chandir = uint8(rt.ChanDir())
		ti.key = reflect.SliceOf(ti.elem)
		ti.keykind = uint8(reflect.Slice)
	case reflect.Array:
		ti.typeInfo4Container = new(typeInfo4Container)
		ti.mbs, b2 = implIntf(rt, mapBySliceTyp)
		if !ti.mbs && b2 {
			ti.mbs = b2
		}
		ti.elem = rt.Elem()
		ti.elemkind = uint8(ti.elem.Kind())
		ti.elemsize = uint32(ti.elem.Size())
		for tt = ti.elem; tt.Kind() == reflect.Ptr; tt = tt.Elem() {
		}
		ti.tielem = x.get(rt2id(tt), tt)
		ti.key = reflect.SliceOf(ti.elem)
		ti.keykind = uint8(reflect.Slice)
		ti.keysize = uint32(ti.key.Size())
		if ti.flagHasPkgPath {
			ti.fastpathUnderlying = ti.key
		}

		// MARKER: reflect.Ptr cannot happen here, as we halt early if reflect.Ptr passed in
		// case reflect.Ptr:
		// 	ti.elem = rt.Elem()
		// 	ti.elemkind = uint8(ti.elem.Kind())
		// 	ti.elemsize = uint32(ti.elem.Size())
	}

	x.mu.Lock()
	sp := x.infos.load()
	// since this is an atomic load/store, we MUST use a different array each time,
	// else we have a data race when a store is happening simultaneously with a findRtidFn call.
	if sp == nil {
		pti = &ti
		sp = []rtid2ti{{rtid, pti}}
		x.infos.store(sp)
	} else {
		var idx uint
		idx, pti = findTypeInfo(sp, rtid)
		if pti == nil {
			pti = &ti
			sp2 := make([]rtid2ti, len(sp)+1)
			copy(sp2[idx+1:], sp[idx:])
			copy(sp2, sp[:idx])
			sp2[idx] = rtid2ti{rtid, pti}
			x.infos.store(sp2)
		}
	}
	x.mu.Unlock()
	return
}

func (x *TypeInfos) rget(rt reflect.Type, rtid uintptr,
	path *structFieldInfoPathNode, pv *typeInfoLoad, omitEmpty bool) {
	// Read up fields and store how to access the value.
	//
	// It uses go's rules for message selectors,
	// which say that the field with the shallowest depth is selected.
	//
	// Note: we consciously use slices, not a map, to simulate a set.
	//       Typically, types have < 16 fields,
	//       and iteration using equals is faster than maps there
	flen := rt.NumField()
LOOP:
	for j, jlen := uint16(0), uint16(flen); j < jlen; j++ {
		f := rt.Field(int(j))
		fkind := f.Type.Kind()

		// skip if a func type, or is unexported, or structTag value == "-"
		switch fkind {
		case reflect.Func, reflect.UnsafePointer:
			continue LOOP
		}

		isUnexported := f.PkgPath != ""
		if isUnexported && !f.Anonymous {
			continue
		}
		stag := x.structTag(f.Tag)
		if stag == "-" {
			continue
		}
		var si structFieldInfo

		var numderef uint8 = 0
		for xft := f.Type; xft.Kind() == reflect.Ptr; xft = xft.Elem() {
			numderef++
		}

		var parsed bool
		// if anonymous and no struct tag (or it's blank),
		// and a struct (or pointer to struct), inline it.
		if f.Anonymous && fkind != reflect.Interface {
			// ^^ redundant but ok: per go spec, an embedded pointer type cannot be to an interface
			ft := f.Type
			isPtr := ft.Kind() == reflect.Ptr
			for ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			isStruct := ft.Kind() == reflect.Struct

			// Ignore embedded fields of unexported non-struct types.
			// Also, from go1.10, ignore pointers to unexported struct types
			// because unmarshal cannot assign a new struct to an unexported field.
			// See https://golang.org/issue/21357
			if (isUnexported && !isStruct) || (!allowSetUnexportedEmbeddedPtr && isUnexported && isPtr) {
				continue
			}
			doInline := stag == ""
			if !doInline {
				si.parseTag(stag)
				parsed = true
				doInline = si.encName == "" // si.isZero()
			}
			if doInline && isStruct {
				// if etypes contains this, don't call rget again (as fields are already seen here)
				ftid := rt2id(ft)
				// We cannot recurse forever, but we need to track other field depths.
				// So - we break if we see a type twice (not the first time).
				// This should be sufficient to handle an embedded type that refers to its
				// owning type, which then refers to its embedded type.
				processIt := true
				numk := 0
				for _, k := range pv.etypes {
					if k == ftid {
						numk++
						if numk == rgetMaxRecursion {
							processIt = false
							break
						}
					}
				}
				if processIt {
					pv.etypes = append(pv.etypes, ftid)
					path2 := &structFieldInfoPathNode{
						parent:   path,
						typ:      f.Type,
						offset:   uint16(f.Offset),
						index:    j,
						kind:     uint8(fkind),
						numderef: numderef,
					}
					x.rget(ft, ftid, path2, pv, omitEmpty)
				}
				continue
			}
		}

		// after the anonymous dance: if an unexported field, skip
		if isUnexported || f.Name == "" { // f.Name cannot be "", but defensively handle it
			continue
		}

		si.path = structFieldInfoPathNode{
			parent:   path,
			typ:      f.Type,
			offset:   uint16(f.Offset),
			index:    j,
			kind:     uint8(fkind),
			numderef: numderef,
			// set asciiAlphaNum to true (default); checked and may be set to false below
			encNameAsciiAlphaNum: true,
			// note: omitEmpty might have been set in an earlier parseTag call, etc - so carry it forward
			omitEmpty: si.path.omitEmpty,
		}

		if !parsed {
			si.encName = f.Name
			si.parseTag(stag)
			parsed = true
		} else if si.encName == "" {
			si.encName = f.Name
		}

		// si.encNameHash = maxUintptr() // hashShortString(bytesView(si.encName))

		if omitEmpty {
			si.path.omitEmpty = true
		}

		for i := len(si.encName) - 1; i >= 0; i-- { // bounds-check elimination
			if !asciiAlphaNumBitset.isset(si.encName[i]) {
				si.path.encNameAsciiAlphaNum = false
				break
			}
		}

		pv.sfis = append(pv.sfis, si)
	}
}

func implIntf(rt, iTyp reflect.Type) (base bool, indir bool) {
	// return rt.Implements(iTyp), reflect.PtrTo(rt).Implements(iTyp)

	// if I's method is defined on T (ie T implements I), then *T implements I.
	// The converse is not true.

	// Type.Implements can be expensive, as it does a simulataneous linear search across 2 lists
	// with alphanumeric string comparisons.
	// If we can avoid running one of these 2 calls, we should.

	base = rt.Implements(iTyp)
	if base {
		indir = true
	} else {
		indir = reflect.PtrTo(rt).Implements(iTyp)
	}
	return
}

func bool2int(b bool) (v uint8) {
	// MARKER: optimized to be a single instruction
	if b {
		v = 1
	}
	return
}

func isSliceBoundsError(s string) bool {
	return strings.Contains(s, "index out of range") ||
		strings.Contains(s, "slice bounds out of range")
}

func sprintf(format string, v ...interface{}) string {
	return fmt.Sprintf(format, v...)
}

func panicValToErr(h errDecorator, v interface{}, err *error) {
	if v == *err {
		return
	}
	switch xerr := v.(type) {
	case nil:
	case runtime.Error:
		d, dok := h.(*Decoder)
		if dok && d.bytes && isSliceBoundsError(xerr.Error()) {
			*err = io.EOF
		} else {
			h.wrapErr(xerr, err)
		}
	case error:
		switch xerr {
		case nil:
		case io.EOF, io.ErrUnexpectedEOF, errEncoderNotInitialized, errDecoderNotInitialized:
			// treat as special (bubble up)
			*err = xerr
		default:
			h.wrapErr(xerr, err)
		}
	default:
		// we don't expect this to happen (as this library always panics with an error)
		h.wrapErr(fmt.Errorf("%v", v), err)
	}
}

func usableByteSlice(bs []byte, slen int) (out []byte, changed bool) {
	const maxCap = 1024 * 1024 * 64 // 64MB
	const skipMaxCap = false        // allow to test
	if slen <= 0 {
		return []byte{}, true
	}
	if slen <= cap(bs) {
		return bs[:slen], false
	}
	// slen > cap(bs) ... handle memory overload appropriately
	if skipMaxCap || slen <= maxCap {
		return make([]byte, slen), true
	}
	return make([]byte, maxCap), true
}

func mapKeyFastKindFor(k reflect.Kind) mapKeyFastKind {
	return mapKeyFastKindVals[k&31]
}

// ----

type codecFnInfo struct {
	ti     *typeInfo
	xfFn   Ext
	xfTag  uint64
	addrD  bool
	addrDf bool // force: if addrD, then decode function MUST take a ptr
	addrE  bool
	// addrEf bool // force: if addrE, then encode function MUST take a ptr
}

// codecFn encapsulates the captured variables and the encode function.
// This way, we only do some calculations one times, and pass to the
// code block that should be called (encapsulated in a function)
// instead of executing the checks every time.
type codecFn struct {
	i  codecFnInfo
	fe func(*Encoder, *codecFnInfo, reflect.Value)
	fd func(*Decoder, *codecFnInfo, reflect.Value)
	// _  [1]uint64 // padding (cache-aligned)
}

type codecRtidFn struct {
	rtid uintptr
	fn   *codecFn
}

func makeExt(ext interface{}) Ext {
	switch t := ext.(type) {
	case Ext:
		return t
	case BytesExt:
		return &bytesExtWrapper{BytesExt: t}
	case InterfaceExt:
		return &interfaceExtWrapper{InterfaceExt: t}
	}
	return &extFailWrapper{}
}

func baseRV(v interface{}) (rv reflect.Value) {
	// use reflect.ValueOf, not rv4i, as of go 1.16beta, rv4i was not inlineable
	for rv = reflect.ValueOf(v); rv.Kind() == reflect.Ptr; rv = rv.Elem() {
	}
	return
}

// ----

// these "checkOverflow" functions must be inlinable, and not call anybody.
// Overflow means that the value cannot be represented without wrapping/overflow.
// Overflow=false does not mean that the value can be represented without losing precision
// (especially for floating point).

type checkOverflow struct{}

func (checkOverflow) Float32(v float64) (overflow bool) {
	if v < 0 {
		v = -v
	}
	return math.MaxFloat32 < v && v <= math.MaxFloat64
}
func (checkOverflow) Uint(v uint64, bitsize uint8) (overflow bool) {
	if v != 0 && v != (v<<(64-bitsize))>>(64-bitsize) {
		overflow = true
	}
	return
}
func (checkOverflow) Int(v int64, bitsize uint8) (overflow bool) {
	if v != 0 && v != (v<<(64-bitsize))>>(64-bitsize) {
		overflow = true
	}
	return
}

func (checkOverflow) Uint2Int(v uint64, neg bool) (overflow bool) {
	return (neg && v > 1<<63) || (!neg && v >= 1<<63)
}

func (checkOverflow) SignedInt(v uint64) (overflow bool) {
	//e.g. -127 to 128 for int8
	// pos := (v >> 63) == 0
	// ui2 := v & 0x7fffffffffffffff
	// if pos {
	// 	if ui2 > math.MaxInt64 {
	// 		overflow = true
	// 	}
	// } else {
	// 	if ui2 > math.MaxInt64-1 {
	// 		overflow = true
	// 	}
	// }

	// a signed integer has overflow if the sign (first) bit is 1 (negative)
	// and the numbers after the sign bit is > maxint64 - 1
	overflow = (v>>63) != 0 && v&0x7fffffffffffffff > math.MaxInt64-1

	return
}

func (x checkOverflow) Float32V(v float64) float64 {
	if x.Float32(v) {
		halt.errorf("float32 overflow: %v", v)
	}
	return v
}
func (x checkOverflow) UintV(v uint64, bitsize uint8) uint64 {
	if x.Uint(v, bitsize) {
		halt.errorf("uint64 overflow: %v", v)
	}
	return v
}
func (x checkOverflow) IntV(v int64, bitsize uint8) int64 {
	if x.Int(v, bitsize) {
		halt.errorf("int64 overflow: %v", v)
	}
	return v
}
func (x checkOverflow) SignedIntV(v uint64) int64 {
	if x.SignedInt(v) {
		halt.errorf("uint64 to int64 overflow: %v", v)
	}
	return int64(v)
}

// ------------------ FLOATING POINT -----------------

func isNaN64(f float64) bool { return f != f }

func isWhitespaceChar(v byte) bool {
	// these are in order of speed below ...

	return v < 33
	// return v < 33 && whitespaceCharBitset64.isset(v)
	// return v < 33 && (v == ' ' || v == '\n' || v == '\t' || v == '\r')
	// return v == ' ' || v == '\n' || v == '\t' || v == '\r'
	// return whitespaceCharBitset.isset(v)
}

func isNumberChar(v byte) bool {
	// these are in order of speed below ...

	return numCharBitset.isset(v)
	// return v < 64 && numCharNoExpBitset64.isset(v) || v == 'e' || v == 'E'
	// return v > 42 && v < 102 && numCharWithExpBitset64.isset(v-42)
}

// -----------------------

type ioFlusher interface {
	Flush() error
}

type ioBuffered interface {
	Buffered() int
}

// -----------------------

type sfiRv struct {
	v *structFieldInfo
	r reflect.Value
}

// ------

// bitset types are better than [256]bool, because they permit the whole
// bitset array being on a single cache line and use less memory.
//
// Also, since pos is a byte (0-255), there's no bounds checks on indexing (cheap).
//
// We previously had bitset128 [16]byte, and bitset32 [4]byte, but those introduces
// bounds checking, so we discarded them, and everyone uses bitset256.
//
// given x > 0 and n > 0 and x is exactly 2^n, then pos/x === pos>>n AND pos%x === pos&(x-1).
// consequently, pos/32 === pos>>5, pos/16 === pos>>4, pos/8 === pos>>3, pos%8 == pos&7
//
// Note that using >> or & is faster than using / or %, as division is quite expensive if not optimized.

// MARKER:
// We noticed a little performance degradation when using bitset256 as [32]byte (or bitset32 as uint32).
// For example, json encoding went from 188K ns/op to 168K ns/op (~ 10% reduction).
// Consequently, we are using a [NNN]bool for bitsetNNN.
// To eliminate bounds-checking, we use x % v as that is guaranteed to be within bounds.

// ----
type bitset32 [32]bool

func (x *bitset32) set(pos byte) *bitset32 {
	x[pos&31] = true // x[pos%32] = true
	return x
}
func (x *bitset32) isset(pos byte) bool {
	return x[pos&31] // x[pos%32]
}

type bitset256 [256]bool

func (x *bitset256) set(pos byte) *bitset256 {
	x[pos] = true
	return x
}
func (x *bitset256) isset(pos byte) bool {
	return x[pos]
}

// ------------

type panicHdl struct{}

// errorv will panic if err is defined (not nil)
func (panicHdl) onerror(err error) {
	if err != nil {
		panic(err)
	}
}

// errorf will always panic, using the parameters passed.
//
// Note: it is ok to pass in a stringView, as it will just pass it directly
// to a fmt.Sprintf call and not hold onto it.
//
//go:noinline
func (panicHdl) errorf(format string, params ...interface{}) {
	if format == "" {
		panic(errPanicUndefined)
	}
	if len(params) == 0 {
		panic(errors.New(format))
	}
	panic(fmt.Errorf(format, params...))
}

// ----------------------------------------------------

type errDecorator interface {
	wrapErr(in error, out *error)
}

type errDecoratorDef struct{}

func (errDecoratorDef) wrapErr(v error, e *error) { *e = v }

// ----------------------------------------------------

type mustHdl struct{}

func (mustHdl) String(s string, err error) string {
	halt.onerror(err)
	return s
}
func (mustHdl) Int(s int64, err error) int64 {
	halt.onerror(err)
	return s
}
func (mustHdl) Uint(s uint64, err error) uint64 {
	halt.onerror(err)
	return s
}
func (mustHdl) Float(s float64, err error) float64 {
	halt.onerror(err)
	return s
}

// -------------------

func freelistCapacity(length int) (capacity int) {
	for capacity = 8; capacity <= length; capacity *= 2 {
	}
	return
}

// bytesFreelist is a list of byte buffers, sorted by cap.
//
// In anecdotal testing (running go test -tsd 1..6), we couldn't get
// the length ofthe list > 4 at any time. So we believe a linear search
// without bounds checking is sufficient.
//
// Typical usage model:
//
//	peek may go together with put, iff pop=true. peek gets largest byte slice temporarily.
//	check is used to switch a []byte if necessary
//	get/put go together
//
// Given that folks may get a []byte, and then append to it a lot which may re-allocate
// a new []byte, we should try to return both (one received from blist and new one allocated).
//
// Typical usage model for get/put, when we don't know whether we may need more than requested
//
//	v0 := blist.get()
//	v1 := v0
//	... use v1 ...
//	blist.put(v1)
//	if byteSliceAddr(v0) != byteSliceAddr(v1) {
//	  blist.put(v0)
//	}
type bytesFreelist [][]byte

// peek returns a slice of possibly non-zero'ed bytes, with len=0,
// and with the largest capacity from the list.
func (x *bytesFreelist) peek(length int, pop bool) (out []byte) {
	if bytesFreeListNoCache {
		return make([]byte, 0, freelistCapacity(length))
	}
	y := *x
	if len(y) > 0 {
		out = y[len(y)-1]
	}
	// start buf with a minimum of 64 bytes
	const minLenBytes = 64
	if length < minLenBytes {
		length = minLenBytes
	}
	if cap(out) < length {
		out = make([]byte, 0, freelistCapacity(length))
		y = append(y, out)
		*x = y
	}
	if pop && len(y) > 0 {
		y = y[:len(y)-1]
		*x = y
	}
	return
}

// get returns a slice of possibly non-zero'ed bytes, with len=0,
// and with cap >= length requested.
func (x *bytesFreelist) get(length int) (out []byte) {
	if bytesFreeListNoCache {
		return make([]byte, 0, freelistCapacity(length))
	}
	y := *x
	// MARKER: do not use range, as range is not currently inlineable as of go 1.16-beta
	// for i, v := range y {
	for i := 0; i < len(y); i++ {
		v := y[i]
		if cap(v) >= length {
			// *x = append(y[:i], y[i+1:]...)
			copy(y[i:], y[i+1:])
			*x = y[:len(y)-1]
			return v
		}
	}
	return make([]byte, 0, freelistCapacity(length))
}

func (x *bytesFreelist) put(v []byte) {
	if bytesFreeListNoCache || cap(v) == 0 {
		return
	}
	if len(v) != 0 {
		v = v[:0]
	}
	// append the new value, then try to put it in a better position
	y := append(*x, v)
	*x = y
	// MARKER: do not use range, as range is not currently inlineable as of go 1.16-beta
	// for i, z := range y[:len(y)-1] {
	for i := 0; i < len(y)-1; i++ {
		z := y[i]
		if cap(z) > cap(v) {
			copy(y[i+1:], y[i:])
			y[i] = v
			return
		}
	}
}

func (x *bytesFreelist) check(v []byte, length int) (out []byte) {
	// ensure inlineable, by moving slow-path out to its own function
	if cap(v) >= length {
		return v[:0]
	}
	return x.checkPutGet(v, length)
}

func (x *bytesFreelist) checkPutGet(v []byte, length int) []byte {
	// checkPutGet broken out into its own function, so check is inlineable in general case
	const useSeparateCalls = false

	if useSeparateCalls {
		x.put(v)
		return x.get(length)
	}

	if bytesFreeListNoCache {
		return make([]byte, 0, freelistCapacity(length))
	}

	// assume cap(v) < length, so put must happen before get
	y := *x
	var put = cap(v) == 0 // if empty, consider it already put
	if !put {
		y = append(y, v)
		*x = y
	}
	for i := 0; i < len(y); i++ {
		z := y[i]
		if put {
			if cap(z) >= length {
				copy(y[i:], y[i+1:])
				y = y[:len(y)-1]
				*x = y
				return z
			}
		} else {
			if cap(z) > cap(v) {
				copy(y[i+1:], y[i:])
				y[i] = v
				put = true
			}
		}
	}
	return make([]byte, 0, freelistCapacity(length))
}

// -------------------------

// sfiRvFreelist is used by Encoder for encoding structs,
// where we have to gather the fields first and then
// analyze them for omitEmpty, before knowing the length of the array/map to encode.
//
// Typically, the length here will depend on the number of cycles e.g.
// if type T1 has reference to T1, or T1 has reference to type T2 which has reference to T1.
//
// In the general case, the length of this list at most times is 1,
// so linear search is fine.
type sfiRvFreelist [][]sfiRv

func (x *sfiRvFreelist) get(length int) (out []sfiRv) {
	y := *x

	// MARKER: do not use range, as range is not currently inlineable as of go 1.16-beta
	// for i, v := range y {
	for i := 0; i < len(y); i++ {
		v := y[i]
		if cap(v) >= length {
			// *x = append(y[:i], y[i+1:]...)
			copy(y[i:], y[i+1:])
			*x = y[:len(y)-1]
			return v
		}
	}
	return make([]sfiRv, 0, freelistCapacity(length))
}

func (x *sfiRvFreelist) put(v []sfiRv) {
	if len(v) != 0 {
		v = v[:0]
	}
	// append the new value, then try to put it in a better position
	y := append(*x, v)
	*x = y
	// MARKER: do not use range, as range is not currently inlineable as of go 1.16-beta
	// for i, z := range y[:len(y)-1] {
	for i := 0; i < len(y)-1; i++ {
		z := y[i]
		if cap(z) > cap(v) {
			copy(y[i+1:], y[i:])
			y[i] = v
			return
		}
	}
}

// ---- multiple interner implementations ----

// Hard to tell which is most performant:
//   - use a map[string]string - worst perf, no collisions, and unlimited entries
//   - use a linear search with move to front heuristics - no collisions, and maxed at 64 entries
//   - use a computationally-intensive hash - best performance, some collisions, maxed at 64 entries

const (
	internMaxStrLen = 16     // if more than 16 bytes, faster to copy than compare bytes
	internCap       = 64 * 2 // 64 uses 1K bytes RAM, so 128 (anecdotal sweet spot) uses 2K bytes
)

type internerMap map[string]string

func (x *internerMap) init() {
	*x = make(map[string]string, internCap)
}

func (x internerMap) string(v []byte) (s string) {
	s, ok := x[string(v)] // no allocation here, per go implementation
	if !ok {
		s = string(v) // new allocation here
		x[s] = s
	}
	return
}
