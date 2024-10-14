// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

//go:build go1.9 && !go1.20 && !safe && !codec.safe && !appengine
// +build go1.9,!go1.20,!safe,!codec.safe,!appengine

package codec

import (
	_ "runtime" // needed for go linkname(s)
	"unsafe"
)

//go:linkname growslice runtime.growslice
//go:noescape
func growslice(typ unsafe.Pointer, old unsafeSlice, num int) unsafeSlice
