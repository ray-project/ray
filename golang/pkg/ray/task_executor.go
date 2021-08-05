package ray

/*
   #cgo CFLAGS: -I/root/ray/src/ray/core_worker/lib/golang
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
        for index, _ := range callResults {
            rv := (*C.struct_ReturnValue)(unsafe.Pointer(returnValue[index]))
            rv.data = create_data_buffer()
            rv.meta = create_data_buffer()
        }
    }
}

func create_data_buffer() *C.struct_DataBuffer {
    p := C.malloc(1)
    C.memset(p, 1, 1)
    return &C.struct_DataBuffer{
        size: 1,
        p:    (*C.uchar)(p),
    }
}
