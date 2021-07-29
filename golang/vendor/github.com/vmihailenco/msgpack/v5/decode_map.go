package msgpack

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

var errArrayStruct = errors.New("msgpack: number of fields in array-encoded struct has changed")

var (
	mapStringStringPtrType = reflect.TypeOf((*map[string]string)(nil))
	mapStringStringType    = mapStringStringPtrType.Elem()
)

var (
	mapStringInterfacePtrType = reflect.TypeOf((*map[string]interface{})(nil))
	mapStringInterfaceType    = mapStringInterfacePtrType.Elem()
)

func decodeMapValue(d *Decoder, v reflect.Value) error {
	n, err := d.DecodeMapLen()
	if err != nil {
		return err
	}

	typ := v.Type()
	if n == -1 {
		v.Set(reflect.Zero(typ))
		return nil
	}

	if v.IsNil() {
		v.Set(reflect.MakeMap(typ))
	}
	if n == 0 {
		return nil
	}

	return d.decodeTypedMapValue(v, n)
}

func (d *Decoder) decodeMapDefault() (interface{}, error) {
	if d.mapDecoder != nil {
		return d.mapDecoder(d)
	}
	return d.DecodeMap()
}

// DecodeMapLen decodes map length. Length is -1 when map is nil.
func (d *Decoder) DecodeMapLen() (int, error) {
	c, err := d.readCode()
	if err != nil {
		return 0, err
	}

	if msgpcode.IsExt(c) {
		if err = d.skipExtHeader(c); err != nil {
			return 0, err
		}

		c, err = d.readCode()
		if err != nil {
			return 0, err
		}
	}
	return d.mapLen(c)
}

func (d *Decoder) mapLen(c byte) (int, error) {
	if c == msgpcode.Nil {
		return -1, nil
	}
	if c >= msgpcode.FixedMapLow && c <= msgpcode.FixedMapHigh {
		return int(c & msgpcode.FixedMapMask), nil
	}
	if c == msgpcode.Map16 {
		size, err := d.uint16()
		return int(size), err
	}
	if c == msgpcode.Map32 {
		size, err := d.uint32()
		return int(size), err
	}
	return 0, unexpectedCodeError{code: c, hint: "map length"}
}

func decodeMapStringStringValue(d *Decoder, v reflect.Value) error {
	mptr := v.Addr().Convert(mapStringStringPtrType).Interface().(*map[string]string)
	return d.decodeMapStringStringPtr(mptr)
}

func (d *Decoder) decodeMapStringStringPtr(ptr *map[string]string) error {
	size, err := d.DecodeMapLen()
	if err != nil {
		return err
	}
	if size == -1 {
		*ptr = nil
		return nil
	}

	m := *ptr
	if m == nil {
		*ptr = make(map[string]string, min(size, maxMapSize))
		m = *ptr
	}

	for i := 0; i < size; i++ {
		mk, err := d.DecodeString()
		if err != nil {
			return err
		}
		mv, err := d.DecodeString()
		if err != nil {
			return err
		}
		m[mk] = mv
	}

	return nil
}

func decodeMapStringInterfaceValue(d *Decoder, v reflect.Value) error {
	ptr := v.Addr().Convert(mapStringInterfacePtrType).Interface().(*map[string]interface{})
	return d.decodeMapStringInterfacePtr(ptr)
}

func (d *Decoder) decodeMapStringInterfacePtr(ptr *map[string]interface{}) error {
	m, err := d.DecodeMap()
	if err != nil {
		return err
	}
	*ptr = m
	return nil
}

func (d *Decoder) DecodeMap() (map[string]interface{}, error) {
	n, err := d.DecodeMapLen()
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	m := make(map[string]interface{}, min(n, maxMapSize))

	for i := 0; i < n; i++ {
		mk, err := d.DecodeString()
		if err != nil {
			return nil, err
		}
		mv, err := d.decodeInterfaceCond()
		if err != nil {
			return nil, err
		}
		m[mk] = mv
	}

	return m, nil
}

func (d *Decoder) DecodeUntypedMap() (map[interface{}]interface{}, error) {
	n, err := d.DecodeMapLen()
	if err != nil {
		return nil, err
	}

	if n == -1 {
		return nil, nil
	}

	m := make(map[interface{}]interface{}, min(n, maxMapSize))

	for i := 0; i < n; i++ {
		mk, err := d.decodeInterfaceCond()
		if err != nil {
			return nil, err
		}

		mv, err := d.decodeInterfaceCond()
		if err != nil {
			return nil, err
		}

		m[mk] = mv
	}

	return m, nil
}

// DecodeTypedMap decodes a typed map. Typed map is a map that has a fixed type for keys and values.
// Key and value types may be different.
func (d *Decoder) DecodeTypedMap() (interface{}, error) {
	n, err := d.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}

	key, err := d.decodeInterfaceCond()
	if err != nil {
		return nil, err
	}

	value, err := d.decodeInterfaceCond()
	if err != nil {
		return nil, err
	}

	keyType := reflect.TypeOf(key)
	valueType := reflect.TypeOf(value)

	if !keyType.Comparable() {
		return nil, fmt.Errorf("msgpack: unsupported map key: %s", keyType.String())
	}

	mapType := reflect.MapOf(keyType, valueType)
	mapValue := reflect.MakeMap(mapType)
	mapValue.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))

	n--
	if err := d.decodeTypedMapValue(mapValue, n); err != nil {
		return nil, err
	}

	return mapValue.Interface(), nil
}

func (d *Decoder) decodeTypedMapValue(v reflect.Value, n int) error {
	typ := v.Type()
	keyType := typ.Key()
	valueType := typ.Elem()

	for i := 0; i < n; i++ {
		mk := reflect.New(keyType).Elem()
		if err := d.DecodeValue(mk); err != nil {
			return err
		}

		mv := reflect.New(valueType).Elem()
		if err := d.DecodeValue(mv); err != nil {
			return err
		}

		v.SetMapIndex(mk, mv)
	}

	return nil
}

func (d *Decoder) skipMap(c byte) error {
	n, err := d.mapLen(c)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if err := d.Skip(); err != nil {
			return err
		}
		if err := d.Skip(); err != nil {
			return err
		}
	}
	return nil
}

func decodeStructValue(d *Decoder, v reflect.Value) error {
	c, err := d.readCode()
	if err != nil {
		return err
	}

	n, err := d.mapLen(c)
	if err == nil {
		return d.decodeStruct(v, n)
	}

	var err2 error
	n, err2 = d.arrayLen(c)
	if err2 != nil {
		return err
	}

	if n <= 0 {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}

	fields := structs.Fields(v.Type(), d.structTag)
	if n != len(fields.List) {
		return errArrayStruct
	}

	for _, f := range fields.List {
		if err := f.DecodeValue(d, v); err != nil {
			return err
		}
	}

	return nil
}

func (d *Decoder) decodeStruct(v reflect.Value, n int) error {
	if n == -1 {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}

	fields := structs.Fields(v.Type(), d.structTag)
	for i := 0; i < n; i++ {
		name, err := d.decodeStringTemp()
		if err != nil {
			return err
		}

		if f := fields.Map[name]; f != nil {
			if err := f.DecodeValue(d, v); err != nil {
				return err
			}
			continue
		}

		if d.flags&disallowUnknownFieldsFlag != 0 {
			return fmt.Errorf("msgpack: unknown field %q", name)
		}
		if err := d.Skip(); err != nil {
			return err
		}
	}

	return nil
}
