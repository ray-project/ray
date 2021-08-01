package ray

/*
   #cgo CFLAGS: -I../src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include "go_worker.h"
*/
import "C"
import (
    "fmt"

    "github.com/ray-project/ray-go-worker/pkg/globalstateaccessor"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

const sessionDir = "session_dir"

func Init(address, _redis_password string) {
    util.Logger.Debug("Initializing runtime with config")
    gsa, err := globalstateaccessor.NewGlobalStateAccessor(address, _redis_password)
    if err != nil {
        panic(err)
    }
    jobId := gsa.GetNextJobID()
    raySessionDir := gsa.GetInternalKV(sessionDir)
    if raySessionDir == "" {
        panic(fmt.Errorf("Failed to get session dir"))
    }
    C.go_worker_Initialize(C.int(1), C.CString("/tmp/ray/session_latest/sockets/plasma_store"), C.CString("/tmp/ray/session_latest/sockets/raylet"),
        C.CString("/tmp/ray/session_latest/logs"), C.CString("192.168.121.61"), C.int(9999), C.CString("192.168.121.61"),
        C.CString("GOLANG"), C.int(jobId), C.CString("127.0.0.1"), C.int(6379), C.CStrint("5241590000000000"))
}

//export SayHello
func SayHello(str *C.char) {
    fmt.Println(C.GoString(str) + " in go")
}
