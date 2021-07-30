package main

/*
   #cgo CFLAGS: -I../src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include "go_worker.h"
*/
import "C"
import "unsafe"

type GlobalStateAccessor struct {
    redisAddress  string
    redisPassword string
    p             unsafe.Pointer
}

func NewGlobalStateAccessor(redisAddress, redisPassword string) *GlobalStateAccessor {
    gsa := &GlobalStateAccessor{
        redisAddress:  redisAddress,
        redisPassword: redisPassword,
    }
    gsa.p = C.go_worker_CreateGlobalStateAccessor(C.CString(redisAddress), C.CString(redisPassword))
    return gsa
}

func (g *GlobalStateAccessor) GetNextJobID() int {
    return C.go_worker_GetNextJobID(g.p)
}
