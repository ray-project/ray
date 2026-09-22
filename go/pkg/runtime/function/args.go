// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"fmt"
	"reflect"

	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// ParamTypesOf returns the first count parameter types of fnType.
//
// fnType must be a function or bound-method type (for a bound method obtained
// via reflect, the receiver is already stripped, so In(i) is the i-th argument).
// This is the shared helper used by WrapGoFunction to build the parameter type
// list for DeserializeArgs.
func ParamTypesOf(fnType reflect.Type, count int) []reflect.Type {
	types := make([]reflect.Type, count)
	for i := range types {
		types[i] = fnType.In(i)
	}
	return types
}

// DeserializeArgs converts task arguments into reflect.Values matching the
// parameter types of a function.
//
// Pass-by-value arguments are deserialized from their serialized data; the
// expected type is given by paramTypes[i]. Pass-by-reference arguments are not
// resolved here and produce an error: the local-mode executor materializes them
// from the object store before invoking the wrapped function, and cluster mode
// resolves them in C++ before the task ever reaches the Go worker. Absent or
// nil arguments are zero-filled.
//
// This is the single shared implementation used by WrapGoFunction, so argument
// handling stays in one place.
func DeserializeArgs(args []FunctionArg, paramTypes []reflect.Type) ([]reflect.Value, error) {
	if len(args) != len(paramTypes) {
		return nil, fmt.Errorf("argument count mismatch: expected %d, got %d",
			len(paramTypes), len(args))
	}
	in := make([]reflect.Value, len(args))
	ser := object.GetSerializer()
	for i, arg := range args {
		if arg.IsPassByValue() && arg.Data != nil {
			expectedType := paramTypes[i]
			nativeObj := &object.NativeRayObject{
				Data:     arg.Data.Data,
				Metadata: arg.Data.Metadata,
			}
			// Deserialize directly to the target type: msgpack decodes small
			// integers as the narrowest fitting type, so decoding into an
			// interface{} first would not match the parameter type.
			deserialized := reflect.New(expectedType).Interface()
			if err := ser.DeserializeTo(nativeObj, deserialized); err != nil {
				return nil, fmt.Errorf("failed to deserialize argument %d: %w", i, err)
			}
			in[i] = reflect.ValueOf(deserialized).Elem()
		} else if arg.IsPassByRef() {
			return nil, fmt.Errorf("pass-by-reference arguments not yet supported")
		} else {
			in[i] = reflect.Zero(paramTypes[i])
		}
	}
	return in, nil
}
