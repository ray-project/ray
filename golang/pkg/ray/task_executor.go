package ray

/*
   #cgo CFLAGS: -I../../../ray/src/ray/core_worker/lib/golang
   #cgo LDFLAGS: -shared  -L/root/ray/bazel-bin/ -lcore_worker_library_go -lstdc++
   #include <string.h>
   #include "go_worker.h"
*/
import "C"
import (
    "fmt"
    "reflect"
    "unsafe"

    "github.com/ray-project/ray-go-worker/pkg/ray/generated"
    "github.com/ray-project/ray-go-worker/pkg/util"
    "github.com/vmihailenco/msgpack/v5"
)

var actor interface{}
var actorType reflect.Value

//export go_worker_execute
func go_worker_execute(taskType int, rayFunctionInfo []*C.char, args []C.struct_DataBuffer, returnValue []*C.struct_ReturnValue) {
    if taskType == int(generated.TaskType_ACTOR_CREATION_TASK) {
        d := (*reflect.SliceHeader)(unsafe.Pointer(&rayFunctionInfo))
        util.Logger.Debugf("slice info:%v", *d)
        goTypeName := C.GoString(rayFunctionInfo[0])
        goType, ok := typesMap[goTypeName]
        if !ok {
            panic(fmt.Errorf("type not found:%s", goTypeName))
        }
        actor = reflect.New(goType).Interface()
        actorType = reflect.ValueOf(actor)
        util.Logger.Debugf("created actor for:%s", goTypeName)
    } else if taskType == int(generated.TaskType_ACTOR_TASK) {
        methodName := C.GoString(rayFunctionInfo[0])
        util.Logger.Debugf("invoke %s", methodName)
        methodValue := actorType.MethodByName(methodName)
        //if methodValue == nil {
        //    panic(fmt.Errorf("method not found:%s", methodName))
        //}
        callResults := methodValue.Call([]reflect.Value{})
        util.Logger.Debugf("invoke result:%v %v", callResults, returnValue)
        for index, callResult := range callResults {
            rv := returnValue[index]
            rv.data = create_data_buffer(callResult)
            rv.meta = create_meta_buffer(callResult)
        }
    }
}

func create_data_buffer(callResult interface{}) *C.struct_DataBuffer {
    b, err := msgpack.Marshal(callResult)
    if err != nil {
        panic(fmt.Errorf("failed to marshall msgpack:%v %v", callResult, err))
    }
    dataSize := len(b)

    p := C.malloc(dataSize)
    C.memcpy(p, unsafe.Pointer(&b[0]), dataSize)
    return &C.struct_DataBuffer{
        size: dataSize,
        p:    p,
    }
}

func create_meta_buffer(callResult interface{}) *C.struct_DataBuffer {
    typeString := getInterfaceType(callResult)
    typeStringBytes := []byte(typeString)
    dataSize := len(typeStringBytes)
    p := C.malloc(dataSize)
    C.memcpy(p, unsafe.Pointer(&typeStringBytes[0]), dataSize)
    return &C.struct_DataBuffer{
        size: dataSize,
        p:    p,
    }
}

func getInterfaceType(v interface{}) string {
    return reflect.TypeOf(v).String()
}
