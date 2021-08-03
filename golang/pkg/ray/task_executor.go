package ray

import "C"
import (
    "fmt"
    "reflect"

    "github.com/ray-project/ray-go-worker/pkg/generated"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

var actor interface{}

//export go_worker_execute
func go_worker_execute(taskType int, rayFunctionInfo []string, args []C.struct_DataBuffer) {
    if taskType == int(generated.TaskType_ACTOR_CREATION_TASK) {
        go_type_name := rayFunctionInfo[0]
        go_type, ok := typesMap[go_type_name]
        if !ok {
            panic(fmt.Errorf("type not found:%s", go_type_name))
        }
        actor = reflect.New(go_type).Interface()
        util.Logger.Debugf("created actor for:%s", go_type_name)
    } else if taskType == int(generated.TaskType_ACTOR_TASK) {

    }
}
