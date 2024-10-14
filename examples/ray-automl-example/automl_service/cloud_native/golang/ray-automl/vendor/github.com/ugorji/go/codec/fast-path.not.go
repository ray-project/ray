// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

//go:build notfastpath || codec.notfastpath
// +build notfastpath codec.notfastpath

package codec

import "reflect"

const fastpathEnabled = false

// The generated fast-path code is very large, and adds a few seconds to the build time.
// This causes test execution, execution of small tools which use codec, etc
// to take a long time.
//
// To mitigate, we now support the notfastpath tag.
// This tag disables fastpath during build, allowing for faster build, test execution,
// short-program runs, etc.

func fastpathDecodeTypeSwitch(iv interface{}, d *Decoder) bool { return false }
func fastpathEncodeTypeSwitch(iv interface{}, e *Encoder) bool { return false }

// func fastpathEncodeTypeSwitchSlice(iv interface{}, e *Encoder) bool { return false }
// func fastpathEncodeTypeSwitchMap(iv interface{}, e *Encoder) bool   { return false }

func fastpathDecodeSetZeroTypeSwitch(iv interface{}) bool { return false }

type fastpathT struct{}
type fastpathE struct {
	rtid  uintptr
	rt    reflect.Type
	encfn func(*Encoder, *codecFnInfo, reflect.Value)
	decfn func(*Decoder, *codecFnInfo, reflect.Value)
}
type fastpathA [0]fastpathE

func fastpathAvIndex(rtid uintptr) int { return -1 }

var fastpathAv fastpathA
var fastpathTV fastpathT
