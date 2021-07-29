package main

/*
   #cgo LDFLAGS: -shared  -L./ -lgo_worker -lstdc++
   #include "go_worker.h"
*/
import "C"

func main() {
//     options := C.struct_CoreWorkerOptions()
//     fmt.Print(options)
    C.have_fun()
}