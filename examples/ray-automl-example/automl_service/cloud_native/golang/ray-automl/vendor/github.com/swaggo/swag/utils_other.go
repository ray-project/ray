//go:build !go1.18
// +build !go1.18

package swag

import (
	"reflect"
	"unicode/utf8"
)

// AppendUtf8Rune appends the UTF-8 encoding of r to the end of p and
// returns the extended buffer. If the rune is out of range,
// it appends the encoding of RuneError.
func AppendUtf8Rune(p []byte, r rune) []byte {
	length := utf8.RuneLen(rune(r))
	if length > 0 {
		utf8Slice := make([]byte, length)
		utf8.EncodeRune(utf8Slice, rune(r))
		p = append(p, utf8Slice...)
	}
	return p
}

// CanIntegerValue a wrapper of reflect.Value
type CanIntegerValue struct {
	reflect.Value
}

// CanInt reports whether Uint can be used without panicking.
func (v CanIntegerValue) CanInt() bool {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

// CanUint reports whether Uint can be used without panicking.
func (v CanIntegerValue) CanUint() bool {
	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	default:
		return false
	}
}
