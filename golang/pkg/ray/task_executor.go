package ray

// #include <string.h>
// #include "go_worker.h"
import "C"
import (
    "fmt"
    "reflect"
    "unsafe"

    "github.com/ray-project/ray-go-worker/pkg/ray/generated/common"
    "github.com/ray-project/ray-go-worker/pkg/util"
    "github.com/vmihailenco/msgpack/v5"
)

var actor interface{}
var actorReflectValue reflect.Value
var actorMethodMap = map[string]reflect.Value{}

//export go_worker_execute
func go_worker_execute(taskType int, rayFunctionInfo []*C.char, args []C.struct_DataBuffer, returnValue []*C.struct_ReturnValue) {
    if taskType == int(generated.TaskType_ACTOR_CREATION_TASK) {
        d := (*reflect.SliceHeader)(unsafe.Pointer(&rayFunctionInfo))
        util.Logger.Debugf("create actor info:%v", *d)
        goTypeName := C.GoString(rayFunctionInfo[0])
        goType := GetActorType(goTypeName)
        if goType == nil {
            panic(fmt.Errorf("type not found:%s", goTypeName))
        }
        actor = reflect.New(goType).Interface()
        actorReflectValue = reflect.ValueOf(actor)
        actorReflectType := reflect.TypeOf(actor)
        for i := 0; i < actorReflectValue.NumMethod(); i++ {
            methodValue := actorReflectValue.Method(i)
            methodName := actorReflectType.Method(i).Name
            actorMethodMap[methodName] = methodValue
            util.Logger.Debugf("register actor method: %s", methodName)
        }
        util.Logger.Debugf("created actor for:%s", goTypeName)
    } else if taskType == int(generated.TaskType_ACTOR_TASK) {
        methodName := C.GoString(rayFunctionInfo[0])
        util.Logger.Debugf("invoke actor task %s", methodName)
        methodValue, contains := actorMethodMap[methodName]
        if !contains {
            panic(fmt.Errorf("method not found:%s", methodName))
        }
        callResults := methodValue.Call([]reflect.Value{})
        util.Logger.Debugf("invoke actor task %s result:%v %v", methodName, callResults, returnValue)
        for index, callResult := range callResults {
            rv := returnValue[index]
            rv.data = createDataBuffer(callResult.Interface())
            rv.meta = createMetaBuffer(callResult.Interface())
        }
    }
}

func createDataBuffer(callResult interface{}) *C.struct_DataBuffer {
    b, err := msgpack.Marshal(callResult)
    if err != nil {
        panic(fmt.Errorf("failed to marshall msgpack:%v %v", callResult, err))
    }
    dataSize := C.ulong(len(b))

    p := C.malloc(dataSize)
    C.memcpy(p, unsafe.Pointer(&b[0]), dataSize)
    return &C.struct_DataBuffer{
        size: C.int(len(b)),
        p:    p,
    }
}

func createMetaBuffer(callResult interface{}) *C.struct_DataBuffer {
    typeString := getInterfaceType(callResult)
    typeStringBytes := []byte(typeString)
    dataSize := C.ulong(len(typeStringBytes))
    util.Logger.Infof("return type %s ,size %d", typeStringBytes, dataSize)
    p := C.malloc(dataSize)
    C.memcpy(p, unsafe.Pointer(&typeStringBytes[0]), dataSize)
    return &C.struct_DataBuffer{
        size: C.int(len(typeStringBytes)),
        p:    p,
    }
}

func getInterfaceType(v interface{}) string {
    return reflect.TypeOf(v).String()
}
