// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

//go:build !go1.5
// +build !go1.5

package codec

import (
	"errors"
	"reflect"
)

const reflectArrayOfSupported = false

var errNoReflectArrayOf = errors.New("codec: reflect.ArrayOf unsupported by this go version")

func reflectArrayOf(count int, elem reflect.Type) reflect.Type {
	panic(errNoReflectArrayOf)
}
