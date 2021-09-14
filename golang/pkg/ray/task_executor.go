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
func go_worker_execute(taskType int, rayFunctionInfo []*C.char, args []*C.struct_DataValue, returnValue []unsafe.Pointer) {
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
        inputGoValues := make([]reflect.Value, len(args), len(args))
        for index, arg := range args {
            goValue, err := dataValueToGoValue(arg)
            if err != nil {
                // TODO
            }
            inputGoValues[index] = reflect.ValueOf(goValue)
            inputGoType := methodValue.Type().In(index)
            if inputGoType.Kind() != reflect.Ptr {
                inputGoValues[index] = reflect.ValueOf(reflect.ValueOf(goValue).Elem().Interface())
            }
        }
        callResults := methodValue.Call(inputGoValues)
        util.Logger.Debugf("invoke actor task %s result:%v %v", methodName, callResults, returnValue)
        for index, callResult := range callResults {
            // should not return a GO pointer
            dataPtr, dataSize := createDataBuffer(callResult.Interface())
            metaPtr, metaSize := createMetaBuffer(callResult.Interface())
            // TODO: reuse DataValue to avoid cgo call
            dv := C.go_worker_AllocateDataValue(dataPtr, dataSize, metaPtr, metaSize)
            returnValue[index] = unsafe.Pointer(dv)
        }
    }
}

func createDataBuffer(callResult interface{}) (unsafe.Pointer, C.ulong) {
    b, err := msgpack.Marshal(callResult)
    if err != nil {
        panic(fmt.Errorf("failed to marshall msgpack:%v %v", callResult, err))
    }
    dataSize := C.ulong(len(b))
    // we need copy to avoid go runtime change the address
    p := C.malloc(dataSize)
    C.memcpy(p, unsafe.Pointer(&b[0]), dataSize)
    return p, dataSize
}

func createMetaBuffer(callResult interface{}) (unsafe.Pointer, C.ulong) {
    typeString := getInterfaceType(callResult)
    typeStringBytes := []byte(typeString)
    dataSize := C.ulong(len(typeStringBytes))
    util.Logger.Infof("return type %s ,size %d", typeStringBytes, dataSize)
    p := C.malloc(dataSize)
    C.memcpy(p, unsafe.Pointer(&typeStringBytes[0]), dataSize)
    return p, dataSize
}

func getInterfaceType(v interface{}) string {
    return reflect.TypeOf(v).String()
}
