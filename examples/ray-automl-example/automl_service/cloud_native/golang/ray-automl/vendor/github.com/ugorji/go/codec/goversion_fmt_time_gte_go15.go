// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

//go:build go1.5
// +build go1.5

package codec

import "time"

func fmtTime(t time.Time, fmt string, b []byte) []byte {
	return t.AppendFormat(b, fmt)
}
