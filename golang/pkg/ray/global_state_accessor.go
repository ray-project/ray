package ray

/*
   #cgo CFLAGS: -I/root/ray/src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include <stdlib.h>
   #include "go_worker.h"
*/
import "C"
import (
    "fmt"
    "unsafe"
)

type globalStateAccessor struct {
    redisAddress  string
    redisPassword string
    p             unsafe.Pointer
}

func NewGlobalStateAccessor(redisAddress, redisPassword string) (*globalStateAccessor, error) {
    gsa := &globalStateAccessor{
        redisAddress:  redisAddress,
        redisPassword: redisPassword,
    }
    gsa.p = C.go_worker_CreateGlobalStateAccessor(C.CString(redisAddress), C.CString(redisPassword))
    connected := bool(C.go_worker_GlobalStateAccessorConnet(gsa.p))
    if !connected {
        return nil, fmt.Errorf("failed to connect %s", redisAddress)
    }
    return gsa, nil
}

func (g *globalStateAccessor) GetNextJobID() int {
    return int(C.go_worker_GetNextJobID(g.p))
}

func (g *globalStateAccessor) GetInternalKV(key string) string {
    v := C.go_worker_GlobalStateAccessorGetInternalKV(g.p, C.CString(key))
    if v != nil {
        result := C.GoString(v)
        C.free(unsafe.Pointer(v))
        return result
    }
    return ""
}

func (g *globalStateAccessor) GetNodeToConnectForDriver(nodeIpAddress string) []byte {
    var res *C.char
    dataLen := C.go_worker_GetNodeToConnectForDriver(g.p, C.CString(nodeIpAddress), &res)
    if dataLen > 0 {
        defer C.free(unsafe.Pointer(res))
        return C.GoBytes(unsafe.Pointer(res), dataLen)
    }
    return nil
}
