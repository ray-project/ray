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

// WrapGoFunction wraps a Go function (interface{}) to Function type.
// The wrapper handles argument deserialization and result serialization.
//
// The function's parameter types are fixed at wrap time, so they are computed
// once and reused on every call instead of rebuilding the slice per task.
//
// This is the single shared implementation used by both the cluster worker
// (go/internal/worker) and the local-mode runtime
// (go/internal/runtime/local_mode), so argument/result handling stays in one
// place. It only depends on pure-Go packages, so using it does not pull in the
// CGO native runtime.
func WrapGoFunction(fn interface{}) Function {
	funcValue := reflect.ValueOf(fn)
	funcType := funcValue.Type()

	paramTypes := ParamTypesOf(funcType, funcType.NumIn())

	return func(args []FunctionArg) ([]SerializedObject, error) {
		in, err := DeserializeArgs(args, paramTypes)
		if err != nil {
			return nil, err
		}

		out := funcValue.Call(in)

		ser := object.GetSerializer()
		results := make([]SerializedObject, len(out))
		for i, val := range out {
			nativeObj, err := ser.Serialize(val.Interface())
			if err != nil {
				return nil, fmt.Errorf("failed to serialize return value %d: %w", i, err)
			}
			// SerializedObjectFromNative deep-copies a pooled payload and
			// returns the pooled buffer, so the task spec never aliases a
			// recycled pool buffer.
			results[i] = SerializedObjectFromNative(nativeObj)
		}

		return results, nil
	}
}
