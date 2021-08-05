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
    "runtime"
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
        return &ActorHandle{
            actorId:   res,
            language:  ray_rpc.Language_GOLANG,
            actorType: typesMap[ac.registerTypeName],
        }
    }
    return nil
}

type ActorHandle struct {
    actorId   *C.char
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
    methodName := GetFunctionName(f)
    lastIndex := strings.LastIndex(methodName, ".")
    if lastIndex != -1 {
        methodName = methodName[lastIndex+1:]
    }
    returnNum := methodType.NumOut()
    methodReturnTypes := make([]reflect.Type, 0, returnNum)
    for i := 0; i < returnNum; i++ {
        methodReturnTypes = append(methodReturnTypes, methodType.Out(i))
    }

    return &ActorTaskCaller{
        actorHandle:             ah,
        invokeMethod:            methodType,
        invokeMethodName:        methodName,
        invokeMethodReturnTypes: methodReturnTypes,
        params:                  []reflect.Value{},
    }
}

type ActorTaskCaller struct {
    actorHandle             *ActorHandle
    invokeMethod            reflect.Type
    invokeMethodName        string
    invokeMethodReturnTypes []reflect.Type
    params                  []reflect.Value
}

func GetFunctionName(i interface{}) string {
    return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

type ID unsafe.Pointer

// 发出调用
func (atc *ActorTaskCaller) Remote() *ObjectRef {
    returnNum := atc.invokeMethod.NumOut()
    objectIds := C.go_worker_SubmitActorTask(unsafe.Pointer(atc.actorHandle.actorId), C.CString(atc.invokeMethodName), C.int(returnNum))
    util.Logger.Debugf("objectIds:%v", objectIds)
    resultIds := make([]ID, 0, objectIds.len)
    v := (*[1 << 28]unsafe.Pointer)(objectIds.data)[:objectIds.len:objectIds.len]
    util.Logger.Debugf("v:%v", v)
    for _, objectId := range v {
        util.Logger.Debugf("objectId:%v", objectId)
        resultIds = append(resultIds, ID(objectId))
    }
    return &ObjectRef{
        returnObjectIds: resultIds,
        returnTypes:     atc.invokeMethodReturnTypes,
    }
}

type ObjectRef struct {
    returnObjectIds []ID
    returnTypes     []reflect.Type
}

type ObjectId []byte

func (or *ObjectRef) Get() []interface{} {
    returnObjectIdsSize := len(or.returnObjectIds)
    returnValues := C.go_worker_Get((*unsafe.Pointer)(&or.returnObjectIds[0]), C.int(returnObjectIdsSize), C.int(-1))
    values := (*[1 << 28]*C.struct_ReturnValue)(returnValues.data)[:returnObjectIdsSize:returnObjectIdsSize]
    for _, returnValue := range values {
        dataBytes := C.GoBytes(unsafe.Pointer(returnValue.data.p), returnValue.data.size)
        return []interface{}{dataBytes[0]}
    }
    return nil
}

//export SayHello
func SayHello(str *C.char) {
    fmt.Println(C.GoString(str) + " in go")
}
