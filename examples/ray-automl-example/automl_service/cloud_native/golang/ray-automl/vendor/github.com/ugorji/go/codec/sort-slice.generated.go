// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

// Code generated from sort-slice.go.tmpl - DO NOT EDIT.

package codec

import (
	"bytes"
	"reflect"
	"time"
)

type stringSlice []string

func (p stringSlice) Len() int      { return len(p) }
func (p stringSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p stringSlice) Less(i, j int) bool {
	return p[uint(i)] < p[uint(j)]
}

type uint8Slice []uint8

func (p uint8Slice) Len() int      { return len(p) }
func (p uint8Slice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p uint8Slice) Less(i, j int) bool {
	return p[uint(i)] < p[uint(j)]
}

type uint64Slice []uint64

func (p uint64Slice) Len() int      { return len(p) }
func (p uint64Slice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p uint64Slice) Less(i, j int) bool {
	return p[uint(i)] < p[uint(j)]
}

type intSlice []int

func (p intSlice) Len() int      { return len(p) }
func (p intSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p intSlice) Less(i, j int) bool {
	return p[uint(i)] < p[uint(j)]
}

type int32Slice []int32

func (p int32Slice) Len() int      { return len(p) }
func (p int32Slice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p int32Slice) Less(i, j int) bool {
	return p[uint(i)] < p[uint(j)]
}

type stringRv struct {
	v string
	r reflect.Value
}
type stringRvSlice []stringRv

func (p stringRvSlice) Len() int      { return len(p) }
func (p stringRvSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p stringRvSlice) Less(i, j int) bool {
	return p[uint(i)].v < p[uint(j)].v
}

type stringIntf struct {
	v string
	i interface{}
}
type stringIntfSlice []stringIntf

func (p stringIntfSlice) Len() int      { return len(p) }
func (p stringIntfSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p stringIntfSlice) Less(i, j int) bool {
	return p[uint(i)].v < p[uint(j)].v
}

type float64Rv struct {
	v float64
	r reflect.Value
}
type float64RvSlice []float64Rv

func (p float64RvSlice) Len() int      { return len(p) }
func (p float64RvSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p float64RvSlice) Less(i, j int) bool {
	return p[uint(i)].v < p[uint(j)].v || isNaN64(p[uint(i)].v) && !isNaN64(p[uint(j)].v)
}

type uint64Rv struct {
	v uint64
	r reflect.Value
}
type uint64RvSlice []uint64Rv

func (p uint64RvSlice) Len() int      { return len(p) }
func (p uint64RvSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p uint64RvSlice) Less(i, j int) bool {
	return p[uint(i)].v < p[uint(j)].v
}

type int64Rv struct {
	v int64
	r reflect.Value
}
type int64RvSlice []int64Rv

func (p int64RvSlice) Len() int      { return len(p) }
func (p int64RvSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p int64RvSlice) Less(i, j int) bool {
	return p[uint(i)].v < p[uint(j)].v
}

type timeRv struct {
	v time.Time
	r reflect.Value
}
type timeRvSlice []timeRv

func (p timeRvSlice) Len() int      { return len(p) }
func (p timeRvSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p timeRvSlice) Less(i, j int) bool {
	return p[uint(i)].v.Before(p[uint(j)].v)
}

type bytesRv struct {
	v []byte
	r reflect.Value
}
type bytesRvSlice []bytesRv

func (p bytesRvSlice) Len() int      { return len(p) }
func (p bytesRvSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p bytesRvSlice) Less(i, j int) bool {
	return bytes.Compare(p[uint(i)].v, p[uint(j)].v) == -1
}

type bytesIntf struct {
	v []byte
	i interface{}
}
type bytesIntfSlice []bytesIntf

func (p bytesIntfSlice) Len() int      { return len(p) }
func (p bytesIntfSlice) Swap(i, j int) { p[uint(i)], p[uint(j)] = p[uint(j)], p[uint(i)] }
func (p bytesIntfSlice) Less(i, j int) bool {
	return bytes.Compare(p[uint(i)].v, p[uint(j)].v) == -1
}
