#!/usr/bin/env bash

pwd=$(pwd)
export SONIC_NO_ASYNC_GC=1

cd $pwd/ast
go test -benchmem -run=^$ -benchtime=1000000x -bench "^(BenchmarkGet.*|BenchmarkSet.*)$"

go test -benchmem -run=^$ -benchtime=10000x -bench "^(BenchmarkParser_.*|BenchmarkEncode.*)$"

go test -benchmem -run=^$ -benchtime=10000000x -bench "^(BenchmarkNodeGetByPath|BenchmarkStructGetByPath|BenchmarkNodeIndex|BenchmarkStructIndex|BenchmarkSliceIndex|BenchmarkMapIndex|BenchmarkNodeGet|BenchmarkSliceGet|BenchmarkMapGet|BenchmarkNodeSet|BenchmarkMapSet|BenchmarkNodeSetByIndex|BenchmarkSliceSetByIndex|BenchmarkStructSetByIndex|BenchmarkNodeUnset|BenchmarkMapUnset|BenchmarkNodUnsetByIndex|BenchmarkSliceUnsetByIndex|BenchmarkNodeAdd|BenchmarkSliceAdd|BenchmarkMapAdd)$"

unset SONIC_NO_ASYNC_GC
cd $pwd