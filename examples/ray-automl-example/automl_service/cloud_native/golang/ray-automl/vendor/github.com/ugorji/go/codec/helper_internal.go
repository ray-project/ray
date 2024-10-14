// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

// maxArrayLen is the size of uint, which determines
// the maximum length of any array.
const maxArrayLen = 1<<((32<<(^uint(0)>>63))-1) - 1

// All non-std package dependencies live in this file,
// so porting to different environment is easy (just update functions).

func pruneSignExt(v []byte, pos bool) (n int) {
	if len(v) < 2 {
	} else if pos && v[0] == 0 {
		for ; v[n] == 0 && n+1 < len(v) && (v[n+1]&(1<<7) == 0); n++ {
		}
	} else if !pos && v[0] == 0xff {
		for ; v[n] == 0xff && n+1 < len(v) && (v[n+1]&(1<<7) != 0); n++ {
		}
	}
	return
}

func halfFloatToFloatBits(h uint16) (f uint32) {
	// retrofitted from:
	// - OGRE (Object-Oriented Graphics Rendering Engine)
	//   function: halfToFloatI https://www.ogre3d.org/docs/api/1.9/_ogre_bitwise_8h_source.html

	s := uint32(h >> 15)
	m := uint32(h & 0x03ff)
	e := int32((h >> 10) & 0x1f)

	if e == 0 {
		if m == 0 { // plus or minus 0
			return s << 31
		}
		// Denormalized number -- renormalize it
		for (m & 0x0400) == 0 {
			m <<= 1
			e -= 1
		}
		e += 1
		m &= ^uint32(0x0400)
	} else if e == 31 {
		if m == 0 { // Inf
			return (s << 31) | 0x7f800000
		}
		return (s << 31) | 0x7f800000 | (m << 13) // NaN
	}
	e = e + (127 - 15)
	m = m << 13
	return (s << 31) | (uint32(e) << 23) | m
}

func floatToHalfFloatBits(i uint32) (h uint16) {
	// retrofitted from:
	// - OGRE (Object-Oriented Graphics Rendering Engine)
	//   function: halfToFloatI https://www.ogre3d.org/docs/api/1.9/_ogre_bitwise_8h_source.html
	// - http://www.java2s.com/example/java-utility-method/float-to/floattohalf-float-f-fae00.html
	s := (i >> 16) & 0x8000
	e := int32(((i >> 23) & 0xff) - (127 - 15))
	m := i & 0x7fffff

	var h32 uint32

	if e <= 0 {
		if e < -10 { // zero
			h32 = s // track -0 vs +0
		} else {
			m = (m | 0x800000) >> uint32(1-e)
			h32 = s | (m >> 13)
		}
	} else if e == 0xff-(127-15) {
		if m == 0 { // Inf
			h32 = s | 0x7c00
		} else { // NAN
			m >>= 13
			var me uint32
			if m == 0 {
				me = 1
			}
			h32 = s | 0x7c00 | m | me
		}
	} else {
		if e > 30 { // Overflow
			h32 = s | 0x7c00
		} else {
			h32 = s | (uint32(e) << 10) | (m >> 13)
		}
	}
	h = uint16(h32)
	return
}

// growCap will return a new capacity for a slice, given the following:
//   - oldCap: current capacity
//   - unit: in-memory size of an element
//   - num: number of elements to add
func growCap(oldCap, unit, num uint) (newCap uint) {
	// appendslice logic (if cap < 1024, *2, else *1.25):
	//   leads to many copy calls, especially when copying bytes.
	//   bytes.Buffer model (2*cap + n): much better for bytes.
	// smarter way is to take the byte-size of the appended element(type) into account

	// maintain 1 thresholds:
	// t1: if cap <= t1, newcap = 2x
	//     else          newcap = 1.5x
	//
	// t1 is always >= 1024.
	// This means that, if unit size >= 16, then always do 2x or 1.5x (ie t1, t2, t3 are all same)
	//
	// With this, appending for bytes increase by:
	//    100% up to 4K
	//     50% beyond that

	// unit can be 0 e.g. for struct{}{}; handle that appropriately
	maxCap := num + (oldCap * 3 / 2)
	if unit == 0 || maxCap > maxArrayLen || maxCap < oldCap { // handle wraparound, etc
		return maxArrayLen
	}

	var t1 uint = 1024 // default thresholds for large values
	if unit <= 4 {
		t1 = 8 * 1024
	} else if unit <= 16 {
		t1 = 2 * 1024
	}

	newCap = 2 + num
	if oldCap > 0 {
		if oldCap <= t1 { // [0,t1]
			newCap = num + (oldCap * 2)
		} else { // (t1,infinity]
			newCap = maxCap
		}
	}

	// ensure newCap takes multiples of a cache line (size is a multiple of 64)
	t1 = newCap * unit
	if t2 := t1 % 64; t2 != 0 {
		t1 += 64 - t2
		newCap = t1 / unit
	}

	return
}
