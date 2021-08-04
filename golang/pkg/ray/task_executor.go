package ray

import "C"
import (
    "fmt"
    "reflect"
    "unsafe"

    "github.com/ray-project/ray-go-worker/pkg/ray/generated"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

var actor interface{}

//export go_worker_execute
func go_worker_execute(taskType int, rayFunctionInfo []*C.char, args []C.struct_DataBuffer) {
    if taskType == int(generated.TaskType_ACTOR_CREATION_TASK) {
        d := (*reflect.SliceHeader)(unsafe.Pointer(&rayFunctionInfo))
	util.Logger.Debugf("slice info:%v",*d)
        go_type_name := C.GoString((*C.char)(unsafe.Pointer(rayFunctionInfo[0])))
        go_type, ok := typesMap[go_type_name]
        if !ok {
            panic(fmt.Errorf("type not found:%s", go_type_name))
        }
        actor = reflect.New(go_type).Interface()
        util.Logger.Debugf("created actor for:%s", go_type_name)
    } else if taskType == int(generated.TaskType_ACTOR_TASK) {

    }
}
