package msgpack

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

const (
	looseInterfaceDecodingFlag uint32 = 1 << iota
	disallowUnknownFieldsFlag
)

const (
	bytesAllocLimit = 1e6 // 1mb
	sliceAllocLimit = 1e4
	maxMapSize      = 1e6
)

type bufReader interface {
	io.Reader
	io.ByteScanner
}

//------------------------------------------------------------------------------

var decPool = sync.Pool{
	New: func() interface{} {
		return NewDecoder(nil)
	},
}

func GetDecoder() *Decoder {
	return decPool.Get().(*Decoder)
}

func PutDecoder(dec *Decoder) {
	dec.r = nil
	dec.s = nil
	decPool.Put(dec)
}

//------------------------------------------------------------------------------

// Unmarshal decodes the MessagePack-encoded data and stores the result
// in the value pointed to by v.
func Unmarshal(data []byte, v interface{}) error {
	dec := GetDecoder()

	dec.Reset(bytes.NewReader(data))
	err := dec.Decode(v)

	PutDecoder(dec)

	return err
}

// A Decoder reads and decodes MessagePack values from an input stream.
type Decoder struct {
	r   io.Reader
	s   io.ByteScanner
	buf []byte

	rec []byte // accumulates read data if not nil

	dict       []string
	flags      uint32
	structTag  string
	mapDecoder func(*Decoder) (interface{}, error)
}

// NewDecoder returns a new decoder that reads from r.
//
// The decoder introduces its own buffering and may read data from r
// beyond the requested msgpack values. Buffering can be disabled
// by passing a reader that implements io.ByteScanner interface.
func NewDecoder(r io.Reader) *Decoder {
	d := new(Decoder)
	d.Reset(r)
	return d
}

// Reset discards any buffered data, resets all state, and switches the buffered
// reader to read from r.
func (d *Decoder) Reset(r io.Reader) {
	d.ResetDict(r, nil)
}

// ResetDict is like Reset, but also resets the dict.
func (d *Decoder) ResetDict(r io.Reader, dict []string) {
	d.resetReader(r)
	d.flags = 0
	d.structTag = ""
	d.mapDecoder = nil
	d.dict = dict
}

func (d *Decoder) WithDict(dict []string, fn func(*Decoder) error) error {
	oldDict := d.dict
	d.dict = dict
	err := fn(d)
	d.dict = oldDict
	return err
}

func (d *Decoder) resetReader(r io.Reader) {
	if br, ok := r.(bufReader); ok {
		d.r = br
		d.s = br
	} else {
		br := bufio.NewReader(r)
		d.r = br
		d.s = br
	}
}

func (d *Decoder) SetMapDecoder(fn func(*Decoder) (interface{}, error)) {
	d.mapDecoder = fn
}

// UseLooseInterfaceDecoding causes decoder to use DecodeInterfaceLoose
// to decode msgpack value into Go interface{}.
func (d *Decoder) UseLooseInterfaceDecoding(on bool) {
	if on {
		d.flags |= looseInterfaceDecodingFlag
	} else {
		d.flags &= ^looseInterfaceDecodingFlag
	}
}

// SetCustomStructTag causes the decoder to use the supplied tag as a fallback option
// if there is no msgpack tag.
func (d *Decoder) SetCustomStructTag(tag string) {
	d.structTag = tag
}

// DisallowUnknownFields causes the Decoder to return an error when the destination
// is a struct and the input contains object keys which do not match any
// non-ignored, exported fields in the destination.
func (d *Decoder) DisallowUnknownFields(on bool) {
	if on {
		d.flags |= disallowUnknownFieldsFlag
	} else {
		d.flags &= ^disallowUnknownFieldsFlag
	}
}

// UseInternedStrings enables support for decoding interned strings.
func (d *Decoder) UseInternedStrings(on bool) {
	if on {
		d.flags |= useInternedStringsFlag
	} else {
		d.flags &= ^useInternedStringsFlag
	}
}

// Buffered returns a reader of the data remaining in the Decoder's buffer.
// The reader is valid until the next call to Decode.
func (d *Decoder) Buffered() io.Reader {
	return d.r
}

//nolint:gocyclo
func (d *Decoder) Decode(v interface{}) error {
	var err error
	switch v := v.(type) {
	case *string:
		if v != nil {
			*v, err = d.DecodeString()
			return err
		}
	case *[]byte:
		if v != nil {
			return d.decodeBytesPtr(v)
		}
	case *int:
		if v != nil {
			*v, err = d.DecodeInt()
			return err
		}
	case *int8:
		if v != nil {
			*v, err = d.DecodeInt8()
			return err
		}
	case *int16:
		if v != nil {
			*v, err = d.DecodeInt16()
			return err
		}
	case *int32:
		if v != nil {
			*v, err = d.DecodeInt32()
			return err
		}
	case *int64:
		if v != nil {
			*v, err = d.DecodeInt64()
			return err
		}
	case *uint:
		if v != nil {
			*v, err = d.DecodeUint()
			return err
		}
	case *uint8:
		if v != nil {
			*v, err = d.DecodeUint8()
			return err
		}
	case *uint16:
		if v != nil {
			*v, err = d.DecodeUint16()
			return err
		}
	case *uint32:
		if v != nil {
			*v, err = d.DecodeUint32()
			return err
		}
	case *uint64:
		if v != nil {
			*v, err = d.DecodeUint64()
			return err
		}
	case *bool:
		if v != nil {
			*v, err = d.DecodeBool()
			return err
		}
	case *float32:
		if v != nil {
			*v, err = d.DecodeFloat32()
			return err
		}
	case *float64:
		if v != nil {
			*v, err = d.DecodeFloat64()
			return err
		}
	case *[]string:
		return d.decodeStringSlicePtr(v)
	case *map[string]string:
		return d.decodeMapStringStringPtr(v)
	case *map[string]interface{}:
		return d.decodeMapStringInterfacePtr(v)
	case *time.Duration:
		if v != nil {
			vv, err := d.DecodeInt64()
			*v = time.Duration(vv)
			return err
		}
	case *time.Time:
		if v != nil {
			*v, err = d.DecodeTime()
			return err
		}
	}

	vv := reflect.ValueOf(v)
	if !vv.IsValid() {
		return errors.New("msgpack: Decode(nil)")
	}
	if vv.Kind() != reflect.Ptr {
		return fmt.Errorf("msgpack: Decode(non-pointer %T)", v)
	}
	if vv.IsNil() {
		return fmt.Errorf("msgpack: Decode(non-settable %T)", v)
	}

	vv = vv.Elem()
	if vv.Kind() == reflect.Interface {
		if !vv.IsNil() {
			vv = vv.Elem()
			if vv.Kind() != reflect.Ptr {
				return fmt.Errorf("msgpack: Decode(non-pointer %s)", vv.Type().String())
			}
		}
	}

	return d.DecodeValue(vv)
}

func (d *Decoder) DecodeMulti(v ...interface{}) error {
	for _, vv := range v {
		if err := d.Decode(vv); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) decodeInterfaceCond() (interface{}, error) {
	if d.flags&looseInterfaceDecodingFlag != 0 {
		return d.DecodeInterfaceLoose()
	}
	return d.DecodeInterface()
}

func (d *Decoder) DecodeValue(v reflect.Value) error {
	decode := getDecoder(v.Type())
	return decode(d, v)
}

func (d *Decoder) DecodeNil() error {
	c, err := d.readCode()
	if err != nil {
		return err
	}
	if c != msgpcode.Nil {
		return fmt.Errorf("msgpack: invalid code=%x decoding nil", c)
	}
	return nil
}

func (d *Decoder) decodeNilValue(v reflect.Value) error {
	err := d.DecodeNil()
	if v.IsNil() {
		return err
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	v.Set(reflect.Zero(v.Type()))
	return err
}

func (d *Decoder) DecodeBool() (bool, error) {
	c, err := d.readCode()
	if err != nil {
		return false, err
	}
	return d.bool(c)
}

func (d *Decoder) bool(c byte) (bool, error) {
	if c == msgpcode.False {
		return false, nil
	}
	if c == msgpcode.True {
		return true, nil
	}
	return false, fmt.Errorf("msgpack: invalid code=%x decoding bool", c)
}

func (d *Decoder) DecodeDuration() (time.Duration, error) {
	n, err := d.DecodeInt64()
	if err != nil {
		return 0, err
	}
	return time.Duration(n), nil
}

// DecodeInterface decodes value into interface. It returns following types:
//   - nil,
//   - bool,
//   - int8, int16, int32, int64,
//   - uint8, uint16, uint32, uint64,
//   - float32 and float64,
//   - string,
//   - []byte,
//   - slices of any of the above,
//   - maps of any of the above.
//
// DecodeInterface should be used only when you don't know the type of value
// you are decoding. For example, if you are decoding number it is better to use
// DecodeInt64 for negative numbers and DecodeUint64 for positive numbers.
func (d *Decoder) DecodeInterface() (interface{}, error) {
	c, err := d.readCode()
	if err != nil {
		return nil, err
	}

	if msgpcode.IsFixedNum(c) {
		return int8(c), nil
	}
	if msgpcode.IsFixedMap(c) {
		err = d.s.UnreadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeMapDefault()
	}
	if msgpcode.IsFixedArray(c) {
		return d.decodeSlice(c)
	}
	if msgpcode.IsFixedString(c) {
		return d.string(c)
	}

	switch c {
	case msgpcode.Nil:
		return nil, nil
	case msgpcode.False, msgpcode.True:
		return d.bool(c)
	case msgpcode.Float:
		return d.float32(c)
	case msgpcode.Double:
		return d.float64(c)
	case msgpcode.Uint8:
		return d.uint8()
	case msgpcode.Uint16:
		return d.uint16()
	case msgpcode.Uint32:
		return d.uint32()
	case msgpcode.Uint64:
		return d.uint64()
	case msgpcode.Int8:
		return d.int8()
	case msgpcode.Int16:
		return d.int16()
	case msgpcode.Int32:
		return d.int32()
	case msgpcode.Int64:
		return d.int64()
	case msgpcode.Bin8, msgpcode.Bin16, msgpcode.Bin32:
		return d.bytes(c, nil)
	case msgpcode.Str8, msgpcode.Str16, msgpcode.Str32:
		return d.string(c)
	case msgpcode.Array16, msgpcode.Array32:
		return d.decodeSlice(c)
	case msgpcode.Map16, msgpcode.Map32:
		err = d.s.UnreadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeMapDefault()
	case msgpcode.FixExt1, msgpcode.FixExt2, msgpcode.FixExt4, msgpcode.FixExt8, msgpcode.FixExt16,
		msgpcode.Ext8, msgpcode.Ext16, msgpcode.Ext32:
		return d.decodeInterfaceExt(c)
	}

	return 0, fmt.Errorf("msgpack: unknown code %x decoding interface{}", c)
}

// DecodeInterfaceLoose is like DecodeInterface except that:
//   - int8, int16, and int32 are converted to int64,
//   - uint8, uint16, and uint32 are converted to uint64,
//   - float32 is converted to float64.
//   - []byte is converted to string.
func (d *Decoder) DecodeInterfaceLoose() (interface{}, error) {
	c, err := d.readCode()
	if err != nil {
		return nil, err
	}

	if msgpcode.IsFixedNum(c) {
		return int64(int8(c)), nil
	}
	if msgpcode.IsFixedMap(c) {
		err = d.s.UnreadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeMapDefault()
	}
	if msgpcode.IsFixedArray(c) {
		return d.decodeSlice(c)
	}
	if msgpcode.IsFixedString(c) {
		return d.string(c)
	}

	switch c {
	case msgpcode.Nil:
		return nil, nil
	case msgpcode.False, msgpcode.True:
		return d.bool(c)
	case msgpcode.Float, msgpcode.Double:
		return d.float64(c)
	case msgpcode.Uint8, msgpcode.Uint16, msgpcode.Uint32, msgpcode.Uint64:
		return d.uint(c)
	case msgpcode.Int8, msgpcode.Int16, msgpcode.Int32, msgpcode.Int64:
		return d.int(c)
	case msgpcode.Str8, msgpcode.Str16, msgpcode.Str32,
		msgpcode.Bin8, msgpcode.Bin16, msgpcode.Bin32:
		return d.string(c)
	case msgpcode.Array16, msgpcode.Array32:
		return d.decodeSlice(c)
	case msgpcode.Map16, msgpcode.Map32:
		err = d.s.UnreadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeMapDefault()
	case msgpcode.FixExt1, msgpcode.FixExt2, msgpcode.FixExt4, msgpcode.FixExt8, msgpcode.FixExt16,
		msgpcode.Ext8, msgpcode.Ext16, msgpcode.Ext32:
		return d.decodeInterfaceExt(c)
	}

	return 0, fmt.Errorf("msgpack: unknown code %x decoding interface{}", c)
}

// Skip skips next value.
func (d *Decoder) Skip() error {
	c, err := d.readCode()
	if err != nil {
		return err
	}

	if msgpcode.IsFixedNum(c) {
		return nil
	}
	if msgpcode.IsFixedMap(c) {
		return d.skipMap(c)
	}
	if msgpcode.IsFixedArray(c) {
		return d.skipSlice(c)
	}
	if msgpcode.IsFixedString(c) {
		return d.skipBytes(c)
	}

	switch c {
	case msgpcode.Nil, msgpcode.False, msgpcode.True:
		return nil
	case msgpcode.Uint8, msgpcode.Int8:
		return d.skipN(1)
	case msgpcode.Uint16, msgpcode.Int16:
		return d.skipN(2)
	case msgpcode.Uint32, msgpcode.Int32, msgpcode.Float:
		return d.skipN(4)
	case msgpcode.Uint64, msgpcode.Int64, msgpcode.Double:
		return d.skipN(8)
	case msgpcode.Bin8, msgpcode.Bin16, msgpcode.Bin32:
		return d.skipBytes(c)
	case msgpcode.Str8, msgpcode.Str16, msgpcode.Str32:
		return d.skipBytes(c)
	case msgpcode.Array16, msgpcode.Array32:
		return d.skipSlice(c)
	case msgpcode.Map16, msgpcode.Map32:
		return d.skipMap(c)
	case msgpcode.FixExt1, msgpcode.FixExt2, msgpcode.FixExt4, msgpcode.FixExt8, msgpcode.FixExt16,
		msgpcode.Ext8, msgpcode.Ext16, msgpcode.Ext32:
		return d.skipExt(c)
	}

	return fmt.Errorf("msgpack: unknown code %x", c)
}

func (d *Decoder) DecodeRaw() (RawMessage, error) {
	d.rec = make([]byte, 0)
	if err := d.Skip(); err != nil {
		return nil, err
	}
	msg := RawMessage(d.rec)
	d.rec = nil
	return msg, nil
}

// PeekCode returns the next MessagePack code without advancing the reader.
// Subpackage msgpack/codes defines the list of available msgpcode.
func (d *Decoder) PeekCode() (byte, error) {
	c, err := d.s.ReadByte()
	if err != nil {
		return 0, err
	}
	return c, d.s.UnreadByte()
}

// ReadFull reads exactly len(buf) bytes into the buf.
func (d *Decoder) ReadFull(buf []byte) error {
	_, err := readN(d.r, buf, len(buf))
	return err
}

func (d *Decoder) hasNilCode() bool {
	code, err := d.PeekCode()
	return err == nil && code == msgpcode.Nil
}

func (d *Decoder) readCode() (byte, error) {
	c, err := d.s.ReadByte()
	if err != nil {
		return 0, err
	}
	if d.rec != nil {
		d.rec = append(d.rec, c)
	}
	return c, nil
}

func (d *Decoder) readFull(b []byte) error {
	_, err := io.ReadFull(d.r, b)
	if err != nil {
		return err
	}
	if d.rec != nil {
		d.rec = append(d.rec, b...)
	}
	return nil
}

func (d *Decoder) readN(n int) ([]byte, error) {
	var err error
	d.buf, err = readN(d.r, d.buf, n)
	if err != nil {
		return nil, err
	}
	if d.rec != nil {
		// TODO: read directly into d.rec?
		d.rec = append(d.rec, d.buf...)
	}
	return d.buf, nil
}

func readN(r io.Reader, b []byte, n int) ([]byte, error) {
	if b == nil {
		if n == 0 {
			return make([]byte, 0), nil
		}
		switch {
		case n < 64:
			b = make([]byte, 0, 64)
		case n <= bytesAllocLimit:
			b = make([]byte, 0, n)
		default:
			b = make([]byte, 0, bytesAllocLimit)
		}
	}

	if n <= cap(b) {
		b = b[:n]
		_, err := io.ReadFull(r, b)
		return b, err
	}
	b = b[:cap(b)]

	var pos int
	for {
		alloc := min(n-len(b), bytesAllocLimit)
		b = append(b, make([]byte, alloc)...)

		_, err := io.ReadFull(r, b[pos:])
		if err != nil {
			return b, err
		}

		if len(b) == n {
			break
		}
		pos = len(b)
	}

	return b, nil
}

func min(a, b int) int { //nolint:unparam
	if a <= b {
		return a
	}
	return b
}
