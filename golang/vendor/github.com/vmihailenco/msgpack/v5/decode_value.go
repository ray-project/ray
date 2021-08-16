package msgpack

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
)

var (
	interfaceType = reflect.TypeOf((*interface{})(nil)).Elem()
	stringType    = reflect.TypeOf((*string)(nil)).Elem()
)

var valueDecoders []decoderFunc

//nolint:gochecknoinits
func init() {
	valueDecoders = []decoderFunc{
		reflect.Bool:          decodeBoolValue,
		reflect.Int:           decodeInt64Value,
		reflect.Int8:          decodeInt64Value,
		reflect.Int16:         decodeInt64Value,
		reflect.Int32:         decodeInt64Value,
		reflect.Int64:         decodeInt64Value,
		reflect.Uint:          decodeUint64Value,
		reflect.Uint8:         decodeUint64Value,
		reflect.Uint16:        decodeUint64Value,
		reflect.Uint32:        decodeUint64Value,
		reflect.Uint64:        decodeUint64Value,
		reflect.Float32:       decodeFloat32Value,
		reflect.Float64:       decodeFloat64Value,
		reflect.Complex64:     decodeUnsupportedValue,
		reflect.Complex128:    decodeUnsupportedValue,
		reflect.Array:         decodeArrayValue,
		reflect.Chan:          decodeUnsupportedValue,
		reflect.Func:          decodeUnsupportedValue,
		reflect.Interface:     decodeInterfaceValue,
		reflect.Map:           decodeMapValue,
		reflect.Ptr:           decodeUnsupportedValue,
		reflect.Slice:         decodeSliceValue,
		reflect.String:        decodeStringValue,
		reflect.Struct:        decodeStructValue,
		reflect.UnsafePointer: decodeUnsupportedValue,
	}
}

func getDecoder(typ reflect.Type) decoderFunc {
	if v, ok := typeDecMap.Load(typ); ok {
		return v.(decoderFunc)
	}
	fn := _getDecoder(typ)
	typeDecMap.Store(typ, fn)
	return fn
}

func _getDecoder(typ reflect.Type) decoderFunc {
	kind := typ.Kind()

	if kind == reflect.Ptr {
		if _, ok := typeDecMap.Load(typ.Elem()); ok {
			return ptrValueDecoder(typ)
		}
	}

	if typ.Implements(customDecoderType) {
		return nilAwareDecoder(typ, decodeCustomValue)
	}
	if typ.Implements(unmarshalerType) {
		return nilAwareDecoder(typ, unmarshalValue)
	}
	if typ.Implements(binaryUnmarshalerType) {
		return nilAwareDecoder(typ, unmarshalBinaryValue)
	}
	if typ.Implements(textUnmarshalerType) {
		return nilAwareDecoder(typ, unmarshalTextValue)
	}

	// Addressable struct field value.
	if kind != reflect.Ptr {
		ptr := reflect.PtrTo(typ)
		if ptr.Implements(customDecoderType) {
			return addrDecoder(nilAwareDecoder(typ, decodeCustomValue))
		}
		if ptr.Implements(unmarshalerType) {
			return addrDecoder(nilAwareDecoder(typ, unmarshalValue))
		}
		if ptr.Implements(binaryUnmarshalerType) {
			return addrDecoder(nilAwareDecoder(typ, unmarshalBinaryValue))
		}
		if ptr.Implements(textUnmarshalerType) {
			return addrDecoder(nilAwareDecoder(typ, unmarshalTextValue))
		}
	}

	switch kind {
	case reflect.Ptr:
		return ptrValueDecoder(typ)
	case reflect.Slice:
		elem := typ.Elem()
		if elem.Kind() == reflect.Uint8 {
			return decodeBytesValue
		}
		if elem == stringType {
			return decodeStringSliceValue
		}
	case reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 {
			return decodeByteArrayValue
		}
	case reflect.Map:
		if typ.Key() == stringType {
			switch typ.Elem() {
			case stringType:
				return decodeMapStringStringValue
			case interfaceType:
				return decodeMapStringInterfaceValue
			}
		}
	}

	return valueDecoders[kind]
}

func ptrValueDecoder(typ reflect.Type) decoderFunc {
	decoder := getDecoder(typ.Elem())
	return func(d *Decoder, v reflect.Value) error {
		if d.hasNilCode() {
			if !v.IsNil() {
				v.Set(reflect.Zero(v.Type()))
			}
			return d.DecodeNil()
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return decoder(d, v.Elem())
	}
}

func addrDecoder(fn decoderFunc) decoderFunc {
	return func(d *Decoder, v reflect.Value) error {
		if !v.CanAddr() {
			return fmt.Errorf("msgpack: Decode(nonaddressable %T)", v.Interface())
		}
		return fn(d, v.Addr())
	}
}

func nilAwareDecoder(typ reflect.Type, fn decoderFunc) decoderFunc {
	if nilable(typ.Kind()) {
		return func(d *Decoder, v reflect.Value) error {
			if d.hasNilCode() {
				return d.decodeNilValue(v)
			}
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			return fn(d, v)
		}
	}

	return func(d *Decoder, v reflect.Value) error {
		if d.hasNilCode() {
			return d.decodeNilValue(v)
		}
		return fn(d, v)
	}
}

func decodeBoolValue(d *Decoder, v reflect.Value) error {
	flag, err := d.DecodeBool()
	if err != nil {
		return err
	}
	v.SetBool(flag)
	return nil
}

func decodeInterfaceValue(d *Decoder, v reflect.Value) error {
	if v.IsNil() {
		return d.interfaceValue(v)
	}
	return d.DecodeValue(v.Elem())
}

func (d *Decoder) interfaceValue(v reflect.Value) error {
	vv, err := d.decodeInterfaceCond()
	if err != nil {
		return err
	}

	if vv != nil {
		if v.Type() == errorType {
			if vv, ok := vv.(string); ok {
				v.Set(reflect.ValueOf(errors.New(vv)))
				return nil
			}
		}

		v.Set(reflect.ValueOf(vv))
	}

	return nil
}

func decodeUnsupportedValue(d *Decoder, v reflect.Value) error {
	return fmt.Errorf("msgpack: Decode(unsupported %s)", v.Type())
}

//------------------------------------------------------------------------------

func decodeCustomValue(d *Decoder, v reflect.Value) error {
	decoder := v.Interface().(CustomDecoder)
	return decoder.DecodeMsgpack(d)
}

func unmarshalValue(d *Decoder, v reflect.Value) error {
	var b []byte

	d.rec = make([]byte, 0, 64)
	if err := d.Skip(); err != nil {
		return err
	}
	b = d.rec
	d.rec = nil

	unmarshaler := v.Interface().(Unmarshaler)
	return unmarshaler.UnmarshalMsgpack(b)
}

func unmarshalBinaryValue(d *Decoder, v reflect.Value) error {
	data, err := d.DecodeBytes()
	if err != nil {
		return err
	}

	unmarshaler := v.Interface().(encoding.BinaryUnmarshaler)
	return unmarshaler.UnmarshalBinary(data)
}

func unmarshalTextValue(d *Decoder, v reflect.Value) error {
	data, err := d.DecodeBytes()
	if err != nil {
		return err
	}

	unmarshaler := v.Interface().(encoding.TextUnmarshaler)
	return unmarshaler.UnmarshalText(data)
}
