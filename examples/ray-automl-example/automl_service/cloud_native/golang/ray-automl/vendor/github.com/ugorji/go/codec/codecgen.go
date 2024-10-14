// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

//go:build codecgen || generated
// +build codecgen generated

package codec

// this file sets the codecgen variable to true
// when the build tag codecgen is set.
//
// some tests depend on knowing whether in the context of codecgen or not.
// For example, some tests should be skipped during codecgen e.g. missing fields tests.

func init() {
	codecgen = true
}
