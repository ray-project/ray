// Copyright 2025 The Ray Authors.
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

package userfuncs

import (
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// echoBytes returns its input unchanged. It is the worker-side remote function
// used by the zero-copy benchmark's cross-process load: a large payload is put
// to plasma, passed to this task by reference (>100KB), and returned so the
// driver Get exercises the zero-copy view path.
//
// It is registered via api.RegisterFunction in RegisterFunctions (funcs.go).
// Note: deliberately NOT annotated //go:ray:task — raygen code generation is
// currently broken, so this function is registered manually like goAdd.
func echoBytes(payload []byte) []byte {
	return payload
}

// GetEchoBytes returns the echoBytes function for direct invocation, matching
// the existing GetGoAdd/GetGoMultiply accessor pattern in funcs.go.
func GetEchoBytes() func([]byte) []byte {
	return echoBytes
}

// runtimeEnvOfTask returns the serialized runtime env observed inside a remote
// task worker (RuntimeContext.RuntimeEnv()). It is the task-side receiver for
// the task-level submission path (CallOptions.WithRuntimeEnv): the driver
// submits a task carrying a runtime env and asserts the worker reports it back.
//
// It is registered manually in RegisterFunctions (like echoBytes); raygen
// generation is not used so the function can be invoked with zero arguments.
func runtimeEnvOfTask() (string, error) {
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return "", err
	}
	return ctx.RuntimeEnv(), nil
}

// GetRuntimeEnvOfTask returns the runtimeEnvOfTask function for direct
// invocation from the driver.
func GetRuntimeEnvOfTask() func() (string, error) {
	return runtimeEnvOfTask
}
