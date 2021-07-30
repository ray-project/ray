package main

/*
   #cgo CFLAGS: -I../src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include "go_worker.h"
*/
import "C"
import "fmt"

func main() {
    //     void goInitialize(
    //            int workerMode, char *store_socket, char *raylet_socket, char *log_dir,
    //            char *node_ip_address, int node_manager_port, char *raylet_ip_address, char* driver_name)

    gsa := NewGlobalStateAccessor("127.0.0.1:6379","5241590000000000")
    jobId := gsa.GetNextJobID()
    C.go_worker_Initialize(C.int(1), C.CString("/tmp/ray/session_latest/sockets/plasma_store"), C.CString("/tmp/ray/session_latest/sockets/raylet"),
        C.CString("/tmp/ray/session_latest/logs"), C.CString("192.168.121.61"), C.int(9999), C.CString("192.168.121.61"), C.CString("GOLANG"), C.int(jobId))

}


//export SayHello
func SayHello(str *C.char) {
    fmt.Println(C.GoString(str) + " in go")
}
