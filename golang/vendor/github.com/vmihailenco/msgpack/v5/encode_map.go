package msgpack

import (
	"math"
	"reflect"
	"sort"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

func encodeMapValue(e *Encoder, v reflect.Value) error {
	if v.IsNil() {
		return e.EncodeNil()
	}

	if err := e.EncodeMapLen(v.Len()); err != nil {
		return err
	}

	for _, key := range v.MapKeys() {
		if err := e.EncodeValue(key); err != nil {
			return err
		}
		if err := e.EncodeValue(v.MapIndex(key)); err != nil {
			return err
		}
	}

	return nil
}

func encodeMapStringStringValue(e *Encoder, v reflect.Value) error {
	if v.IsNil() {
		return e.EncodeNil()
	}

	if err := e.EncodeMapLen(v.Len()); err != nil {
		return err
	}

	m := v.Convert(mapStringStringType).Interface().(map[string]string)
	if e.flags&sortMapKeysFlag != 0 {
		return e.encodeSortedMapStringString(m)
	}

	for mk, mv := range m {
		if err := e.EncodeString(mk); err != nil {
			return err
		}
		if err := e.EncodeString(mv); err != nil {
			return err
		}
	}

	return nil
}

func encodeMapStringInterfaceValue(e *Encoder, v reflect.Value) error {
	if v.IsNil() {
		return e.EncodeNil()
	}
	m := v.Convert(mapStringInterfaceType).Interface().(map[string]interface{})
	if e.flags&sortMapKeysFlag != 0 {
		return e.EncodeMapSorted(m)
	}
	return e.EncodeMap(m)
}

func (e *Encoder) EncodeMap(m map[string]interface{}) error {
	if m == nil {
		return e.EncodeNil()
	}
	if err := e.EncodeMapLen(len(m)); err != nil {
		return err
	}
	for mk, mv := range m {
		if err := e.EncodeString(mk); err != nil {
			return err
		}
		if err := e.Encode(mv); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) EncodeMapSorted(m map[string]interface{}) error {
	if m == nil {
		return e.EncodeNil()
	}
	if err := e.EncodeMapLen(len(m)); err != nil {
		return err
	}

	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		if err := e.EncodeString(k); err != nil {
			return err
		}
		if err := e.Encode(m[k]); err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) encodeSortedMapStringString(m map[string]string) error {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		err := e.EncodeString(k)
		if err != nil {
			return err
		}
		if err = e.EncodeString(m[k]); err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) EncodeMapLen(l int) error {
	if l < 16 {
		return e.writeCode(msgpcode.FixedMapLow | byte(l))
	}
	if l <= math.MaxUint16 {
		return e.write2(msgpcode.Map16, uint16(l))
	}
	return e.write4(msgpcode.Map32, uint32(l))
}

func encodeStructValue(e *Encoder, strct reflect.Value) error {
	structFields := structs.Fields(strct.Type(), e.structTag)
	if e.flags&arrayEncodedStructsFlag != 0 || structFields.AsArray {
		return encodeStructValueAsArray(e, strct, structFields.List)
	}
	fields := structFields.OmitEmpty(strct, e.flags&omitEmptyFlag != 0)

	if err := e.EncodeMapLen(len(fields)); err != nil {
		return err
	}

	for _, f := range fields {
		if err := e.EncodeString(f.name); err != nil {
			return err
		}
		if err := f.EncodeValue(e, strct); err != nil {
			return err
		}
	}

	return nil
}

func encodeStructValueAsArray(e *Encoder, strct reflect.Value, fields []*field) error {
	if err := e.EncodeArrayLen(len(fields)); err != nil {
		return err
	}
	for _, f := range fields {
		if err := f.EncodeValue(e, strct); err != nil {
			return err
		}
	}
	return nil
}
