package ray

import "C"
import (
    "fmt"
    "reflect"
    "unsafe"

    "github.com/ray-project/ray-go-worker/pkg/generated"
)

var actor interface{}

//export go_worker_execute
func goExecute(taskType int, rayFunctionInfo []*C.char, args []unsafe.Pointer) {
    if taskType == int(generated.TaskType_ACTOR_CREATION_TASK) {
        go_type_name := C.GoString(rayFunctionInfo[0])
        go_type, ok := typesMap[go_type_name]
        if !ok {
            panic(fmt.Errorf("type not found:%s", go_type_name))
        }
        actor = reflect.New(go_type).Interface()
    } else if taskType == int(generated.TaskType_ACTOR_TASK) {
        
    }
}
