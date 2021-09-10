package util

import (
    "reflect"
    "runtime"
    "strings"
)

func GetFunctionName(v reflect.Value) string {
    methodName := runtime.FuncForPC(v.Pointer()).Name()
    lastIndex := strings.LastIndex(methodName, ".")
    if lastIndex != -1 {
        methodName = methodName[lastIndex+1:]
    }
    return methodName
}
