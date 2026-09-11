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

// Package submitter provides interfaces for submitting tasks and creating actors.
package submitter

import (
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// ActorHandle represents a handle to an actor.
// This interface is used to unify NativeActorHandle and ActorHandleImpl[T].
type ActorHandle interface {
	// ID returns the actor ID.
	ID() ids.ActorID
}

// TaskSubmitter provides methods to submit tasks and create actors.
//
// Design notes:
// 1. Interface is kept minimal, containing only essential submission methods.
// 2. Corresponds to Java's io.ray.runtime.task.TaskSubmitter.
// 3. Implementation is provided by NativeTaskSubmitter in cluster mode.
type TaskSubmitter interface {
	// SubmitTask submits a normal task to be executed.
	//
	// Parameters:
	//   - functionDescriptor: The remote function to execute.
	//   - args: Arguments of this task.
	//   - numReturns: Number of return objects.
	//   - options: Options for this task (e.g., resources, placement).
	//
	// Returns:
	//   - []ids.ObjectID: IDs of the return objects.
	//   - error: Any error encountered during submission.
	SubmitTask(
		functionDescriptor function.FunctionDescriptor,
		args []function.FunctionArg,
		numReturns int,
		options *TaskOptions,
	) ([]ids.ObjectID, error)

	// CreateActor creates a new actor.
	//
	// Parameters:
	//   - functionDescriptor: The actor class to instantiate.
	//   - args: Arguments for the actor constructor.
	//   - options: Options for this actor creation (e.g., resources, name).
	//
	// Returns:
	//   - ids.ActorID: The ID of the created actor.
	//   - error: Any error encountered during actor creation.
	CreateActor(
		functionDescriptor function.FunctionDescriptor,
		args []function.FunctionArg,
		options *ActorCreationOptions,
	) (ids.ActorID, error)

	// SubmitActorTask submits a task to be executed by an actor.
	//
	// Parameters:
	//   - actorID: The ID of the actor to execute the task.
	//   - functionDescriptor: The actor method to execute.
	//   - args: Arguments of this task.
	//   - numReturns: Number of return objects.
	//   - options: Options for this task.
	//
	// Returns:
	//   - []ids.ObjectID: IDs of the return objects.
	//   - error: Any error encountered during submission.
	SubmitActorTask(
		actorID ids.ActorID,
		functionDescriptor function.FunctionDescriptor,
		args []function.FunctionArg,
		numReturns int,
		options *TaskOptions,
	) ([]ids.ObjectID, error)

	// GetActor retrieves a named actor by its name and namespace.
	//
	// Parameters:
	//   - name: The name of the actor.
	//   - namespace: The namespace of the actor (empty string for default namespace).
	//
	// Returns:
	//   - ActorHandle: The handle to the actor, or nil if not found.
	//   - error: Any error encountered during retrieval.
	GetActor(name string, namespace string) (ActorHandle, error)
}
