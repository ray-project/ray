package ray

/*
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
    "sync"
    "unsafe"

    "github.com/golang/protobuf/proto"
    ray_generated "github.com/ray-project/ray-go-worker/pkg/ray/generated/common"
    ray_gcs_generated "github.com/ray-project/ray-go-worker/pkg/ray/generated/gcs"
    "github.com/ray-project/ray-go-worker/pkg/util"
    "github.com/vmihailenco/msgpack/v5"
)

const (
    sessionDir = "session_dir"
)

var elemType = map[reflect.Kind]bool{
    reflect.Array: true,
    reflect.Chan:  true,
    reflect.Map:   true,
    reflect.Ptr:   true,
    reflect.Slice: true,
}

var types = typeRegister{
    typesMap: make(map[string]reflect.Type),
}

func init() {
    RegisterActorType(0)
    RegisterActorType("")
}

type typeRegister struct {
    sync.RWMutex
    typesMap map[string]reflect.Type
}

func RegisterActorType(a interface{}) error {
    goType := reflect.TypeOf(a)
    typeName := ""
    if _, contains := elemType[goType.Kind()]; contains {
        typeName = getRegisterTypeKey(goType.Elem())
        goType = goType.Elem()
    } else {
        typeName = getRegisterTypeKey(goType)
    }
    types.Lock()
    defer types.Unlock()
    types.typesMap[typeName] = goType
    util.Logger.Debugf("register type: %s", typeName)
    // TODO check conflict
    return nil
}

func GetActorType(t string) reflect.Type {
    types.RLock()
    defer types.RUnlock()
    return types.typesMap[t]
}

func getRegisterTypeKey(t reflect.Type) string {
    pkgName := t.PkgPath()
    if pkgName == "" {
        return t.Name()
    }
    return pkgName + "." + t.Name()
}

func Init(address, redis_password string) {
    innerInit(address, redis_password, ray_generated.WorkerType_DRIVER)
}

func innerInit(address, redis_password string, workerType ray_generated.WorkerType) {
    // TODO: redirect log, otherwise the log will print in raylet.out
    gsa, err := NewGlobalStateAccessor(address, redis_password)
    if err != nil {
        panic(err)
    }
    // for driver
    if workerType == ray_generated.WorkerType_DRIVER {
        SetJobId(gsa.GetNextJobID())
        raySessionDir := gsa.GetInternalKV(sessionDir)
        if raySessionDir == "" {
            panic(fmt.Errorf("failed to get session dir"))
        }
        SetSessionDir(raySessionDir)
        gcsNodeInfo := &ray_gcs_generated.GcsNodeInfo{}
        localIp, err := util.GetLocalIp()
        if err != nil {
            panic(err)
        }
        util.Logger.Debugf("Using local ip: %s", localIp)
        nodeInfoData := gsa.GetNodeToConnectForDriver(localIp)
        err = proto.Unmarshal(nodeInfoData, gcsNodeInfo)
        if err != nil {
            panic(fmt.Errorf("get node info failed: %s %v", nodeInfoData, err))
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
    util.Logger.Debug("Initializing runtime with config:", rayConfigVaue)
    // These C.CString allocated memory don't need to be freed.
    // They are used by CoreWorkerProcess.
    C.go_worker_Initialize(C.int(workerType), C.CString(GetObjectStoreSocket()),
        C.CString(GetRayletSocket()), C.CString(logDir),
        C.CString(GetNodeManagerAddress()), C.int(GetNodeManagerPort()),
        C.CString(GetNodeManagerAddress()),
        C.CString("GOLANG"), C.int(GetJobId()), C.CString(addressInfo[0]), C.int(addressPort),
        C.CString(redis_password), C.CString(serializedJobConfig))
}

func innerRun() {
    util.Logger.Infof("ray worker running...")
    C.go_worker_Run()
    util.Logger.Infof("ray worker exiting...")
}

func Actor(p interface{}) *ActorCreator {
    registerTypeName := getRegisterTypeKey(reflect.TypeOf(p).Elem())
    if registerTypeName == "" {
        panic("failed to submit task")
    }
    return &ActorCreator{
        registerTypeName: registerTypeName,
    }
}

type ActorCreator struct {
    registerTypeName string
}

// Create Actor
func (ac *ActorCreator) Remote() (*ActorHandle, error) {
    var res *C.char
    registerTypeName := C.CString(ac.registerTypeName)
    defer C.free(unsafe.Pointer(registerTypeName))
    dataLen := C.go_worker_CreateActor(registerTypeName, &res)
    if dataLen > 0 {
        return &ActorHandle{
            actorId:  res,
            language: ray_generated.Language_GOLANG,
        }, nil
    }
    return nil, fmt.Errorf("failed to create actor for:%s", ac.registerTypeName)
}

type ActorHandle struct {
    actorId  *C.char
    language ray_generated.Language
}

func (ah *ActorHandle) Task(f interface{}, args ...interface{}) *actorTaskCaller {
    methodType := reflect.TypeOf(f)
    methodName := util.GetFunctionName(reflect.ValueOf(f))
    returnNum := methodType.NumOut()
    methodReturnTypes := make([]reflect.Type, 0, returnNum)
    for i := 0; i < returnNum; i++ {
        methodReturnTypes = append(methodReturnTypes, methodType.Out(i))
    }

    return &actorTaskCaller{
        actorHandle:             ah,
        invokeMethod:            methodType,
        invokeMethodName:        methodName,
        invokeMethodReturnTypes: methodReturnTypes,
        params:                  []reflect.Value{},
    }
}

type actorTaskCaller struct {
    actorHandle             *ActorHandle
    invokeMethod            reflect.Type
    invokeMethodName        string
    invokeMethodReturnTypes []reflect.Type
    params                  []reflect.Value
}

func (atc *actorTaskCaller) Remote() *ObjectRef {
    returnNum := atc.invokeMethod.NumOut()
    returObjectIds := make([]unsafe.Pointer, returnNum, returnNum)
    var returObjectIdArrayPointer *unsafe.Pointer = nil
    if returnNum != 0 {
        returObjectIdArrayPointer = &returObjectIds[0]
    }
    methodName := C.CString(atc.invokeMethodName)
    defer C.free(unsafe.Pointer(methodName))
    success := C.go_worker_SubmitActorTask(unsafe.Pointer(atc.actorHandle.actorId), methodName, C.int(returnNum), returObjectIdArrayPointer)
    if success != 0 {
        panic("failed to submit task")
    }
    util.Logger.Debugf("remote task return object ids:%v", returObjectIds)
    return &ObjectRef{
        returnObjectIds: unsafe.Pointer(returObjectIdArrayPointer),
        returnObjectNum: returnNum,
        returnTypes:     atc.invokeMethodReturnTypes,
        atc:             atc,
    }
}

type ObjectRef struct {
    returnObjectIds unsafe.Pointer
    returnObjectNum int
    returnTypes     []reflect.Type
    atc             *actorTaskCaller
}

var errorMap = map[string]error{
    strconv.Itoa(int(ray_generated.ErrorType_WORKER_DIED)):              errors.New("worker died"),
    strconv.Itoa(int(ray_generated.ErrorType_ACTOR_DIED)):               errors.New("actor died"),
    strconv.Itoa(int(ray_generated.ErrorType_OBJECT_UNRECONSTRUCTABLE)): errors.New("object unreconstructable"),
    strconv.Itoa(int(ray_generated.ErrorType_TASK_EXECUTION_EXCEPTION)): errors.New("task execution exception"),
    strconv.Itoa(int(ray_generated.ErrorType_OBJECT_IN_PLASMA)):         errors.New("object in plasma"),
    strconv.Itoa(int(ray_generated.ErrorType_TASK_CANCELLED)):           errors.New("task cancelled"),
    strconv.Itoa(int(ray_generated.ErrorType_ACTOR_CREATION_FAILED)):    errors.New("actor creation failed"),
    strconv.Itoa(int(ray_generated.ErrorType_RUNTIME_ENV_SETUP_FAILED)): errors.New("runtime env setup failed"),
}

func (or *ObjectRef) Get() ([]interface{}, error) {
    util.Logger.Debugf("get result returObjectIdArrayPointer:%v ,return object num:%d", or.returnObjectIds, or.returnObjectNum)
    if or.returnObjectNum == 0 {
        return nil, nil
    }
    returnValues := make([]unsafe.Pointer, or.returnObjectNum, or.returnObjectNum)
    success := C.go_worker_Get((*unsafe.Pointer)(or.returnObjectIds), C.int(or.returnObjectNum), C.int(-1), &returnValues[0])
    if success != 0 {
        panic("failed to get task result")
    }
    returnGoValues := make([]interface{}, or.returnObjectNum, or.returnObjectNum)
    for index, returnValue := range returnValues {
        rv := (*C.struct_ReturnValue)(returnValue)
        metaBytes := C.GoBytes(unsafe.Pointer(rv.meta.p), rv.meta.size)
        typeString := string(metaBytes)
        if err, contains := errorMap[typeString]; contains {
            return nil, err
        }
        goType := GetActorType(typeString)
        if goType == nil {
            panic(fmt.Errorf("failed to get type:%s", typeString))
        }
        returnGoValue := reflect.New(goType).Interface()
        dataBytes := C.GoBytes(unsafe.Pointer(rv.data.p), rv.data.size)
        err := msgpack.Unmarshal(dataBytes, returnGoValue)
        if err != nil {
            panic(fmt.Errorf("failed to unmarshal %d return value", index))
        }
        returnGoValues[index] = returnGoValue
    }
    return returnGoValues, nil
}

func Shutdown(){
    C.go_worker_shutdown()
}