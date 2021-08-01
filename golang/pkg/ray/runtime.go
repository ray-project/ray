package ray

/*
   #cgo CFLAGS: -I/root/ray/src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include "go_worker.h"
*/
import "C"
import (
    "fmt"
    "strconv"
    "strings"

    "github.com/golang/protobuf/proto"
    ray_rpc "github.com/ray-project/ray-go-worker/pkg/generated"
    "github.com/ray-project/ray-go-worker/pkg/globalstateaccessor"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

const sessionDir = "session_dir"

func Init(address, _redis_password string) {
    util.Logger.Debug("Initializing runtime with config")
    rayConfig := NewRayConfig()
    gsa, err := globalstateaccessor.NewGlobalStateAccessor(address, _redis_password)
    if err != nil {
        panic(err)
    }
    // for driver
    rayConfig.SetJobId(gsa.GetNextJobID())
    raySessionDir := gsa.GetInternalKV(sessionDir)
    if raySessionDir == "" {
        panic(fmt.Errorf("Failed to get session dir"))
    }
    rayConfig.SetSessionDir(raySessionDir)
    logDir := fmt.Sprintf("%s/logs", raySessionDir)

    localIp, err := util.GetLocalIp()
    if err != nil {
        panic(err)
    }
    util.Logger.Debugf("Using local ip: %s", localIp)

    gcsNodeInfo := &ray_rpc.GcsNodeInfo{}
    nodeInfoData := gsa.GetNodeToConnectForDriver(localIp)
    err = proto.Unmarshal(nodeInfoData, gcsNodeInfo)
    if err != nil {
        panic(err)
    }
    util.Logger.Debug("GcsNodeInfo:%v", *gcsNodeInfo)
    addressInfo := strings.Split(address, ":")
    addressPort, err := strconv.Atoi(addressInfo[1])
    if err != nil {
        panic(err)
    }
    C.go_worker_Initialize(C.int(ray_rpc.Language_GOLANG), C.CString(gcsNodeInfo.GetObjectStoreSocketName()),
        C.CString(gcsNodeInfo.GetRayletSocketName()), C.CString(logDir),
        C.CString(gcsNodeInfo.GetNodeManagerAddress()), C.int(gcsNodeInfo.GetNodeManagerPort()),
        C.CString(gcsNodeInfo.GetNodeManagerAddress()),
        C.CString("GOLANG"), C.int(rayConfig.jobId), C.CString(addressInfo[0]), C.int(addressPort),
        C.CString(_redis_password))
}

//export SayHello
func SayHello(str *C.char) {
    fmt.Println(C.GoString(str) + " in go")
}
