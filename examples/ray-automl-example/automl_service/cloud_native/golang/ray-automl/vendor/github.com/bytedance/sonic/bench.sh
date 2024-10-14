#!/usr/bin/env bash

pwd=$(pwd)
export SONIC_NO_ASYNC_GC=1

cd $pwd/encoder
go test -benchmem -run=^$ -benchtime=100000x -bench "^(BenchmarkEncoder_.*)$"

cd $pwd/decoder
go test -benchmem -run=^$ -benchtime=100000x -bench "^(BenchmarkDecoder_.*)$"

cd $pwd/ast
go test -benchmem -run=^$ -benchtime=1000000x -bench "^(BenchmarkGet.*|BenchmarkSet.*)$"

go test -benchmem -run=^$ -benchtime=10000x -bench "^(BenchmarkParser_.*|BenchmarkEncode.*)$"

go test -benchmem -run=^$ -benchtime=10000000x -bench "^(BenchmarkNodeGetByPath|BenchmarkStructGetByPath|BenchmarkNodeIndex|BenchmarkStructIndex|BenchmarkSliceIndex|BenchmarkMapIndex|BenchmarkNodeGet|BenchmarkSliceGet|BenchmarkMapGet|BenchmarkNodeSet|BenchmarkMapSet|BenchmarkNodeSetByIndex|BenchmarkSliceSetByIndex|BenchmarkStructSetByIndex|BenchmarkNodeUnset|BenchmarkMapUnset|BenchmarkNodUnsetByIndex|BenchmarkSliceUnsetByIndex|BenchmarkNodeAdd|BenchmarkSliceAdd|BenchmarkMapAdd)$"

cd $pwd/external_jsonlib_test/benchmark_test
go test -benchmem -run=^$ -benchtime=100000x -bench "^(BenchmarkEncoder_.*|BenchmarkDecoder_.*)$"

go test -benchmem -run=^$ -benchtime=1000000x -bench "^(BenchmarkGet.*|BenchmarkSet.*)$"

go test -benchmem -run=^$ -benchtime=10000x -bench "^(BenchmarkParser_.*)$"

unset SONIC_NO_ASYNC_GC
cd $pwd