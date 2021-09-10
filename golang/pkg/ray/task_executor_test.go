package ray

import (
    "reflect"
    "testing"

    "github.com/ray-project/ray-go-worker/pkg/util"
)

func TestGetFunctionName(t *testing.T) {
    actor = &TestActor{}
    actorReflectValue = reflect.ValueOf(actor)
    for i := 0; i < actorReflectValue.NumMethod(); i++ {
        methodValue := actorReflectValue.Method(i)
        methodName := util.GetFunctionName(methodValue)
        actorMethodMap[methodName] = methodValue
        util.Logger.Debugf("register actor method: %s", methodName)
    }
}