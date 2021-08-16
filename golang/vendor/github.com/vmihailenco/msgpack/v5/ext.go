package msgpack

import (
	"fmt"
	"math"
	"reflect"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

type extInfo struct {
	Type    reflect.Type
	Decoder func(d *Decoder, v reflect.Value, extLen int) error
}

var extTypes = make(map[int8]*extInfo)

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

func RegisterExt(extID int8, value MarshalerUnmarshaler) {
	RegisterExtEncoder(extID, value, func(e *Encoder, v reflect.Value) ([]byte, error) {
		marshaler := v.Interface().(Marshaler)
		return marshaler.MarshalMsgpack()
	})
	RegisterExtDecoder(extID, value, func(d *Decoder, v reflect.Value, extLen int) error {
		b, err := d.readN(extLen)
		if err != nil {
			return err
		}
		return v.Interface().(Unmarshaler).UnmarshalMsgpack(b)
	})
}

func UnregisterExt(extID int8) {
	unregisterExtEncoder(extID)
	unregisterExtDecoder(extID)
}

func RegisterExtEncoder(
	extID int8,
	value interface{},
	encoder func(enc *Encoder, v reflect.Value) ([]byte, error),
) {
	unregisterExtEncoder(extID)

	typ := reflect.TypeOf(value)
	extEncoder := makeExtEncoder(extID, typ, encoder)
	typeEncMap.Store(extID, typ)
	typeEncMap.Store(typ, extEncoder)
	if typ.Kind() == reflect.Ptr {
		typeEncMap.Store(typ.Elem(), makeExtEncoderAddr(extEncoder))
	}
}

func unregisterExtEncoder(extID int8) {
	t, ok := typeEncMap.Load(extID)
	if !ok {
		return
	}
	typeEncMap.Delete(extID)
	typ := t.(reflect.Type)
	typeEncMap.Delete(typ)
	if typ.Kind() == reflect.Ptr {
		typeEncMap.Delete(typ.Elem())
	}
}

func makeExtEncoder(
	extID int8,
	typ reflect.Type,
	encoder func(enc *Encoder, v reflect.Value) ([]byte, error),
) encoderFunc {
	nilable := typ.Kind() == reflect.Ptr

	return func(e *Encoder, v reflect.Value) error {
		if nilable && v.IsNil() {
			return e.EncodeNil()
		}

		b, err := encoder(e, v)
		if err != nil {
			return err
		}

		if err := e.EncodeExtHeader(extID, len(b)); err != nil {
			return err
		}

		return e.write(b)
	}
}

func makeExtEncoderAddr(extEncoder encoderFunc) encoderFunc {
	return func(e *Encoder, v reflect.Value) error {
		if !v.CanAddr() {
			return fmt.Errorf("msgpack: Decode(nonaddressable %T)", v.Interface())
		}
		return extEncoder(e, v.Addr())
	}
}

func RegisterExtDecoder(
	extID int8,
	value interface{},
	decoder func(dec *Decoder, v reflect.Value, extLen int) error,
) {
	unregisterExtDecoder(extID)

	typ := reflect.TypeOf(value)
	extDecoder := makeExtDecoder(extID, typ, decoder)
	extTypes[extID] = &extInfo{
		Type:    typ,
		Decoder: decoder,
	}

	typeDecMap.Store(extID, typ)
	typeDecMap.Store(typ, extDecoder)
	if typ.Kind() == reflect.Ptr {
		typeDecMap.Store(typ.Elem(), makeExtDecoderAddr(extDecoder))
	}
}

func unregisterExtDecoder(extID int8) {
	t, ok := typeDecMap.Load(extID)
	if !ok {
		return
	}
	typeDecMap.Delete(extID)
	delete(extTypes, extID)
	typ := t.(reflect.Type)
	typeDecMap.Delete(typ)
	if typ.Kind() == reflect.Ptr {
		typeDecMap.Delete(typ.Elem())
	}
}

func makeExtDecoder(
	wantedExtID int8,
	typ reflect.Type,
	decoder func(d *Decoder, v reflect.Value, extLen int) error,
) decoderFunc {
	return nilAwareDecoder(typ, func(d *Decoder, v reflect.Value) error {
		extID, extLen, err := d.DecodeExtHeader()
		if err != nil {
			return err
		}
		if extID != wantedExtID {
			return fmt.Errorf("msgpack: got ext type=%d, wanted %d", extID, wantedExtID)
		}
		return decoder(d, v, extLen)
	})
}

func makeExtDecoderAddr(extDecoder decoderFunc) decoderFunc {
	return func(d *Decoder, v reflect.Value) error {
		if !v.CanAddr() {
			return fmt.Errorf("msgpack: Decode(nonaddressable %T)", v.Interface())
		}
		return extDecoder(d, v.Addr())
	}
}

func (e *Encoder) EncodeExtHeader(extID int8, extLen int) error {
	if err := e.encodeExtLen(extLen); err != nil {
		return err
	}
	if err := e.w.WriteByte(byte(extID)); err != nil {
		return err
	}
	return nil
}

func (e *Encoder) encodeExtLen(l int) error {
	switch l {
	case 1:
		return e.writeCode(msgpcode.FixExt1)
	case 2:
		return e.writeCode(msgpcode.FixExt2)
	case 4:
		return e.writeCode(msgpcode.FixExt4)
	case 8:
		return e.writeCode(msgpcode.FixExt8)
	case 16:
		return e.writeCode(msgpcode.FixExt16)
	}
	if l <= math.MaxUint8 {
		return e.write1(msgpcode.Ext8, uint8(l))
	}
	if l <= math.MaxUint16 {
		return e.write2(msgpcode.Ext16, uint16(l))
	}
	return e.write4(msgpcode.Ext32, uint32(l))
}

func (d *Decoder) DecodeExtHeader() (extID int8, extLen int, err error) {
	c, err := d.readCode()
	if err != nil {
		return
	}
	return d.extHeader(c)
}

func (d *Decoder) extHeader(c byte) (int8, int, error) {
	extLen, err := d.parseExtLen(c)
	if err != nil {
		return 0, 0, err
	}

	extID, err := d.readCode()
	if err != nil {
		return 0, 0, err
	}

	return int8(extID), extLen, nil
}

func (d *Decoder) parseExtLen(c byte) (int, error) {
	switch c {
	case msgpcode.FixExt1:
		return 1, nil
	case msgpcode.FixExt2:
		return 2, nil
	case msgpcode.FixExt4:
		return 4, nil
	case msgpcode.FixExt8:
		return 8, nil
	case msgpcode.FixExt16:
		return 16, nil
	case msgpcode.Ext8:
		n, err := d.uint8()
		return int(n), err
	case msgpcode.Ext16:
		n, err := d.uint16()
		return int(n), err
	case msgpcode.Ext32:
		n, err := d.uint32()
		return int(n), err
	default:
		return 0, fmt.Errorf("msgpack: invalid code=%x decoding ext len", c)
	}
}

func (d *Decoder) decodeInterfaceExt(c byte) (interface{}, error) {
	extID, extLen, err := d.extHeader(c)
	if err != nil {
		return nil, err
	}

	info, ok := extTypes[extID]
	if !ok {
		return nil, fmt.Errorf("msgpack: unknown ext id=%d", extID)
	}

	v := reflect.New(info.Type).Elem()
	if nilable(v.Kind()) && v.IsNil() {
		v.Set(reflect.New(info.Type.Elem()))
	}

	if err := info.Decoder(d, v, extLen); err != nil {
		return nil, err
	}

	return v.Interface(), nil
}

func (d *Decoder) skipExt(c byte) error {
	n, err := d.parseExtLen(c)
	if err != nil {
		return err
	}
	return d.skipN(n + 1)
}

func (d *Decoder) skipExtHeader(c byte) error {
	// Read ext type.
	_, err := d.readCode()
	if err != nil {
		return err
	}
	// Read ext body len.
	for i := 0; i < extHeaderLen(c); i++ {
		_, err := d.readCode()
		if err != nil {
			return err
		}
	}
	return nil
}

func extHeaderLen(c byte) int {
	switch c {
	case msgpcode.Ext8:
		return 1
	case msgpcode.Ext16:
		return 2
	case msgpcode.Ext32:
		return 4
	}
	return 0
}
