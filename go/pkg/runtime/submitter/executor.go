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

package submitter

import (
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/ids"
)

// TaskExecutor defines the interface for executing tasks.
//
// Design notes:
// 1. Corresponds to Java's io.ray.runtime.task.TaskExecutor.
// 2. Responsible for executing tasks assigned by raylet.
// 3. Handles both normal tasks and actor tasks.
type TaskExecutor interface {
	// Execute executes a normal task and returns the results.
	//
	// Parameters:
	//   - functionDescriptor: The function to execute.
	//   - args: Arguments for the task.
	//   - numReturns: Number of return values.
	//
	// Returns:
	//   - []function.SerializedObject: The serialized return objects.
	//   - error: Any error encountered during execution.
	Execute(functionDescriptor function.FunctionDescriptor, args []function.FunctionArg, numReturns int) ([]function.SerializedObject, error)

	// ExecuteActorTask executes an actor task and returns the results.
	//
	// Parameters:
	//   - actorID: The ID of the actor to execute the task.
	//   - functionDescriptor: The actor method to execute.
	//   - args: Arguments for the task.
	//   - numReturns: Number of return values.
	//
	// Returns:
	//   - []function.SerializedObject: The serialized return objects.
	//   - error: Any error encountered during execution.
	ExecuteActorTask(actorID ids.ActorID, functionDescriptor function.FunctionDescriptor, args []function.FunctionArg, numReturns int) ([]function.SerializedObject, error)
}

// TaskExecutorOptions contains options for task executor.
type TaskExecutorOptions struct {
	// Runtime is the runtime instance used by the executor.
	Runtime interface{}

	// ObjectStore is the object store used by the executor.
	ObjectStore interface{}
}
