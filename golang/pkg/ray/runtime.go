package ray

/*
   #cgo CFLAGS: -I/root/ray/src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include <stdlib.h>
   #include "go_worker.h"
*/
import "C"
import (
    "errors"
    "fmt"
    "reflect"
    "strconv"
    "strings"
    "unsafe"

    "github.com/golang/protobuf/proto"
    ray_rpc "github.com/ray-project/ray-go-worker/pkg/ray/generated"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

const sessionDir = "session_dir"

var typesMap = make(map[string]reflect.Type)

func Init(address, _redis_password string) {
    innerInit(address, _redis_password, ray_rpc.WorkerType_DRIVER)
}

func innerInit(address, _redis_password string, workerType ray_rpc.WorkerType) {
    util.Logger.Debug("Initializing runtime with config")
    gsa, err := NewGlobalStateAccessor(address, _redis_password)
    if err != nil {
        panic(err)
    }
    // for driver
    if workerType == ray_rpc.WorkerType_DRIVER {
        SetJobId(gsa.GetNextJobID())
        raySessionDir := gsa.GetInternalKV(sessionDir)
        if raySessionDir == "" {
            panic(fmt.Errorf("failed to get session dir"))
        }
        SetSessionDir(raySessionDir)
        gcsNodeInfo := &ray_rpc.GcsNodeInfo{}
        localIp, err := util.GetLocalIp()
        if err != nil {
            panic(err)
        }
        util.Logger.Debugf("Using local ip: %s", localIp)
        nodeInfoData := gsa.GetNodeToConnectForDriver(localIp)
        err = proto.Unmarshal(nodeInfoData, gcsNodeInfo)
        if err != nil {
            panic(err)
        }
        SetNodeManagerPort(gcsNodeInfo.GetNodeManagerPort())
        SetNodeManagerAddress(gcsNodeInfo.GetNodeManagerAddress())
        SetObjectStoreSocket(gcsNodeInfo.GetObjectStoreSocketName())
        SetRayletSocket(gcsNodeInfo.GetRayletSocketName())
        //todo add job config
    } else {
        // check session dir is not empty
        if GetSessionDir() == "" {
            panic(errors.New("session dir is empty"))
        }
    }

    util.Logger.Debugf("Session dir: %s", GetSessionDir())
    logDir := fmt.Sprintf("%s/logs", GetSessionDir())

    addressInfo := strings.Split(address, ":")
    addressPort, err := strconv.Atoi(addressInfo[1])
    if err != nil {
        panic(err)
    }
    serializedJobConfig := "{}"
    C.go_worker_Initialize(C.int(workerType), C.CString(GetObjectStoreSocket()),
        C.CString(GetRayletSocket()), C.CString(logDir),
        C.CString(GetNodeManagerAddress()), C.int(GetNodeManagerPort()),
        C.CString(GetNodeManagerAddress()),
        C.CString("GOLANG"), C.int(GetJobId()), C.CString(addressInfo[0]), C.int(addressPort),
        C.CString(_redis_password), C.CString(serializedJobConfig))
}

func innerRun() {
    util.Logger.Infof("ray worker running...")
    C.go_worker_Run()
    util.Logger.Infof("ray worker exiting...")
}

func RegisterType(t reflect.Type) error {
    typeName := getRegisterTypeKey(t.Elem())
    typesMap[typeName] = t.Elem()
    util.Logger.Debugf("register type: %s", typeName)
    // todo check conflict
    return nil
}

func getRegisterTypeKey(t reflect.Type) string {
    return t.PkgPath() + "." + t.Name()
}

func Actor(p interface{}) *ActorCreator {
    //todo check contains
    return &ActorCreator{
        registerTypeName: getRegisterTypeKey(reflect.TypeOf(p).Elem()),
    }
}

type ActorCreator struct {
    registerTypeName string
}

// 创建actor
func (ac *ActorCreator) Remote() *ActorHandle {
    var res *C.char
    dataLen := C.go_worker_CreateActor(C.CString(ac.registerTypeName), &res)
    if dataLen > 0 {
        defer C.free(unsafe.Pointer(res))
        return &ActorHandle{
            actorId:   C.GoBytes(unsafe.Pointer(res), dataLen),
            language:  ray_rpc.Language_GOLANG,
            actorType: typesMap[ac.registerTypeName],
        }
    }
    return nil
}

type ActorHandle struct {
    actorId   []byte
    language  ray_rpc.Language
    actorType reflect.Type
}

type Param interface {
}

type Convert func(a, i Param)

// 缺少泛型的支持，所以只能传入参数名
// 参数填这里
func (ah *ActorHandle) Task(f interface{}) *ActorTaskCaller {
    methodType := reflect.TypeOf(f)
    return &ActorTaskCaller{
        actorHandle:  ah,
        invokeMethod: methodType,
        params:       []reflect.Value{},
    }
}

type ActorTaskCaller struct {
    actorHandle  *ActorHandle
    invokeMethod reflect.Type
    params       []reflect.Value
}

// 发出调用
func (or *ActorTaskCaller) Remote() *ObjectRef {
    returnNum := or.invokeMethod.NumOut()
    objectIds := C.go_worker_SubmitActorTask(C.CBytes(or.actorHandle.actorId), C.CString(or.invokeMethod.Name()), C.int(returnNum))
    resultIds := make([]ObjectId, 0, objectIds.len)
    v := (*[1 << 28]*C.struct_DataBuffer)(objectIds.data)[:objectIds.len:objectIds.len]
    for _, objectId := range v {
        resultIds = append(resultIds, ObjectId{
            id: C.GoBytes(unsafe.Pointer(objectId.p), objectId.size),
        })
    }
    return &ObjectRef{
        ids: resultIds,
    }
}

type ObjectRef struct {
    ids   []ObjectId
    types []reflect.Type
}

type ObjectId struct {
    id []byte
}

func (or *ObjectRef) Get() {

}

//export SayHello
func SayHello(str *C.char) {
    fmt.Println(C.GoString(str) + " in go")
}
