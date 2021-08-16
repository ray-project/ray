package msgpack

import (
	"fmt"
	"math"
	"reflect"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

const (
	minInternedStringLen = 3
	maxDictLen           = math.MaxUint16
)

var internedStringExtID = int8(math.MinInt8)

func init() {
	extTypes[internedStringExtID] = &extInfo{
		Type:    stringType,
		Decoder: decodeInternedStringExt,
	}
}

func decodeInternedStringExt(d *Decoder, v reflect.Value, extLen int) error {
	idx, err := d.decodeInternedStringIndex(extLen)
	if err != nil {
		return err
	}

	s, err := d.internedStringAtIndex(idx)
	if err != nil {
		return err
	}

	v.SetString(s)
	return nil
}

//------------------------------------------------------------------------------

func encodeInternedInterfaceValue(e *Encoder, v reflect.Value) error {
	if v.IsNil() {
		return e.EncodeNil()
	}

	v = v.Elem()
	if v.Kind() == reflect.String {
		return e.encodeInternedString(v.String(), true)
	}
	return e.EncodeValue(v)
}

func encodeInternedStringValue(e *Encoder, v reflect.Value) error {
	return e.encodeInternedString(v.String(), true)
}

func (e *Encoder) encodeInternedString(s string, intern bool) error {
	// Interned string takes at least 3 bytes. Plain string 1 byte + string len.
	if len(s) >= minInternedStringLen {
		if idx, ok := e.dict[s]; ok {
			return e.encodeInternedStringIndex(idx)
		}

		if intern && len(e.dict) < maxDictLen {
			if e.dict == nil {
				e.dict = make(map[string]int)
			}
			idx := len(e.dict)
			e.dict[s] = idx
		}
	}

	return e.encodeNormalString(s)
}

func (e *Encoder) encodeInternedStringIndex(idx int) error {
	if idx <= math.MaxUint8 {
		if err := e.writeCode(msgpcode.FixExt1); err != nil {
			return err
		}
		return e.write1(byte(internedStringExtID), uint8(idx))
	}

	if idx <= math.MaxUint16 {
		if err := e.writeCode(msgpcode.FixExt2); err != nil {
			return err
		}
		return e.write2(byte(internedStringExtID), uint16(idx))
	}

	if uint64(idx) <= math.MaxUint32 {
		if err := e.writeCode(msgpcode.FixExt4); err != nil {
			return err
		}
		return e.write4(byte(internedStringExtID), uint32(idx))
	}

	return fmt.Errorf("msgpack: interned string index=%d is too large", idx)
}

//------------------------------------------------------------------------------

func decodeInternedInterfaceValue(d *Decoder, v reflect.Value) error {
	s, err := d.decodeInternedString(true)
	if err == nil {
		v.Set(reflect.ValueOf(s))
		return nil
	}
	if err != nil {
		if _, ok := err.(unexpectedCodeError); !ok {
			return err
		}
	}

	if err := d.s.UnreadByte(); err != nil {
		return err
	}
	return decodeInterfaceValue(d, v)
}

func decodeInternedStringValue(d *Decoder, v reflect.Value) error {
	s, err := d.decodeInternedString(true)
	if err != nil {
		return err
	}

	v.SetString(s)
	return nil
}

func (d *Decoder) decodeInternedString(intern bool) (string, error) {
	c, err := d.readCode()
	if err != nil {
		return "", err
	}

	if msgpcode.IsFixedString(c) {
		n := int(c & msgpcode.FixedStrMask)
		return d.decodeInternedStringWithLen(n, intern)
	}

	switch c {
	case msgpcode.Nil:
		return "", nil
	case msgpcode.FixExt1, msgpcode.FixExt2, msgpcode.FixExt4:
		typeID, extLen, err := d.extHeader(c)
		if err != nil {
			return "", err
		}
		if typeID != internedStringExtID {
			err := fmt.Errorf("msgpack: got ext type=%d, wanted %d",
				typeID, internedStringExtID)
			return "", err
		}

		idx, err := d.decodeInternedStringIndex(extLen)
		if err != nil {
			return "", err
		}

		return d.internedStringAtIndex(idx)
	case msgpcode.Str8, msgpcode.Bin8:
		n, err := d.uint8()
		if err != nil {
			return "", err
		}
		return d.decodeInternedStringWithLen(int(n), intern)
	case msgpcode.Str16, msgpcode.Bin16:
		n, err := d.uint16()
		if err != nil {
			return "", err
		}
		return d.decodeInternedStringWithLen(int(n), intern)
	case msgpcode.Str32, msgpcode.Bin32:
		n, err := d.uint32()
		if err != nil {
			return "", err
		}
		return d.decodeInternedStringWithLen(int(n), intern)
	}

	return "", unexpectedCodeError{
		code: c,
		hint: "interned string",
	}
}

func (d *Decoder) decodeInternedStringIndex(extLen int) (int, error) {
	switch extLen {
	case 1:
		n, err := d.uint8()
		if err != nil {
			return 0, err
		}
		return int(n), nil
	case 2:
		n, err := d.uint16()
		if err != nil {
			return 0, err
		}
		return int(n), nil
	case 4:
		n, err := d.uint32()
		if err != nil {
			return 0, err
		}
		return int(n), nil
	}

	err := fmt.Errorf("msgpack: unsupported ext len=%d decoding interned string", extLen)
	return 0, err
}

func (d *Decoder) internedStringAtIndex(idx int) (string, error) {
	if idx >= len(d.dict) {
		err := fmt.Errorf("msgpack: interned string at index=%d does not exist", idx)
		return "", err
	}
	return d.dict[idx], nil
}

func (d *Decoder) decodeInternedStringWithLen(n int, intern bool) (string, error) {
	if n <= 0 {
		return "", nil
	}

	s, err := d.stringWithLen(n)
	if err != nil {
		return "", err
	}

	if intern && len(s) >= minInternedStringLen && len(d.dict) < maxDictLen {
		d.dict = append(d.dict, s)
	}

	return s, nil
}
