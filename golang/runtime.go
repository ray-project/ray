package main

/*
   #cgo CFLAGS: -I../src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include "go_worker.h"
*/
import "C"

func main() {
//     options := C.struct_CoreWorkerOptions()
//     fmt.Print(options)
    C.goInitialize()
}
