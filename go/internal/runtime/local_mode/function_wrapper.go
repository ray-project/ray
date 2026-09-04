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

package local_mode

import (
	"fmt"
	"reflect"

	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// syncFunctionsFromRegistry copies all functions registered in the global
// api function.Registry into this submitter's FunctionManager so that tasks
// submitted through the public API (api.Remote / api.Actor) can be executed
// in local mode. Functions stay registered lazily, right before submission.
func (s *LocalModeTaskSubmitter) syncFunctionsFromRegistry() {
	if s.functionMgr == nil {
		return
	}
	entries, ok := function.Registry.ListEntries()
	if !ok {
		return
	}
	for _, entry := range entries {
		if s.functionMgr.IsRegistered(entry.Descriptor()) {
			continue
		}
		wrappedFn := wrapLocalGoFunction(entry.Function())
		if err := s.functionMgr.RegisterFunction(entry.Descriptor(), wrappedFn); err != nil {
			// Registration failure is not fatal: submission will surface a
			// clearer "function not registered" error if execution is attempted.
			continue
		}
	}
}

// wrapLocalGoFunction adapts a raw Go function into a function.Function that
// deserializes pass-by-value args, invokes the function via reflection, and
// serializes the return values. It mirrors the worker's wrapGoFunction so the
// local-mode runtime can execute functions registered via api.Remote.
func wrapLocalGoFunction(fn interface{}) function.Function {
	funcValue := reflect.ValueOf(fn)
	funcType := funcValue.Type()

	return func(args []function.FunctionArg) ([]function.SerializedObject, error) {
		in := make([]reflect.Value, len(args))
		ser := object.GetSerializer()

		for i, arg := range args {
			if arg.IsPassByValue() && arg.Data != nil {
				expectedType := funcType.In(i)
				nativeObj := &object.NativeRayObject{
					Data:     arg.Data.Data,
					Metadata: arg.Data.Metadata,
				}
				deserialized := reflect.New(expectedType).Interface()
				if err := ser.DeserializeTo(nativeObj, deserialized); err != nil {
					return nil, fmt.Errorf("failed to deserialize argument %d: %w", i, err)
				}
				in[i] = reflect.ValueOf(deserialized).Elem()
			} else {
				// Unsupported (pass-by-reference) arguments default to zero values.
				in[i] = reflect.Zero(funcType.In(i))
			}
		}

		out := funcValue.Call(in)

		results := make([]function.SerializedObject, len(out))
		for i, val := range out {
			nativeObj, err := ser.Serialize(val.Interface())
			if err != nil {
				return nil, fmt.Errorf("failed to serialize return value %d: %w", i, err)
			}
			results[i] = function.SerializedObject{
				Data:     nativeObj.Data,
				Metadata: nativeObj.Metadata,
			}
		}

		return results, nil
	}
}
