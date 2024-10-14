// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

//go:build go1.20 && !safe && !codec.safe && !appengine
// +build go1.20,!safe,!codec.safe,!appengine

package codec

import (
	_ "reflect" // needed for go linkname(s)
	"unsafe"
)

func growslice(typ unsafe.Pointer, old unsafeSlice, num int) (s unsafeSlice) {
	// culled from GOROOT/runtime/slice.go
	num -= old.Cap - old.Len
	s = rtgrowslice(old.Data, old.Cap+num, old.Cap, num, typ)
	s.Len = old.Len
	return
}

//go:linkname rtgrowslice runtime.growslice
//go:noescape
func rtgrowslice(oldPtr unsafe.Pointer, newLen, oldCap, num int, typ unsafe.Pointer) unsafeSlice

// //go:linkname growslice reflect.growslice
// //go:noescape
// func growslice(typ unsafe.Pointer, old unsafeSlice, cap int) unsafeSlice
