//go:build go1.18
// +build go1.18

package swag

import (
	"reflect"
	"unicode/utf8"
)

// AppendUtf8Rune appends the UTF-8 encoding of r to the end of p and
// returns the extended buffer. If the rune is out of range,
// it appends the encoding of RuneError.
func AppendUtf8Rune(p []byte, r rune) []byte {
	return utf8.AppendRune(p, r)
}

// CanIntegerValue a wrapper of reflect.Value
type CanIntegerValue struct {
	reflect.Value
}

// CanInt reports whether Uint can be used without panicking.
func (v CanIntegerValue) CanInt() bool {
	return v.Value.CanInt()
}

// CanUint reports whether Uint can be used without panicking.
func (v CanIntegerValue) CanUint() bool {
	return v.Value.CanUint()
}
