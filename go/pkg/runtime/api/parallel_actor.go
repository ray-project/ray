// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// parallelActorInstanceGroupPrefix is the prefix for the concurrency group
// names that route method calls to each parallel actor instance. It mirrors
// Java's ParallelActorContextImpl.PARALLEL_INSTANCE naming.
const parallelActorInstanceGroupPrefix = "PARALLEL_INSTANCE_"

// parallelActorExecuteMethod is the method name on the wrapper actor to which
// every instance-routed call is submitted. The worker's ParallelActorExecutor
// dispatches Execute(instanceId, methodDesc, args...) to the target instance.
const parallelActorExecuteMethod = "Execute"

// ParallelActorContext is the SPI used by the fluent builders to create the
// wrapper actor and submit instance-routed method calls. It is wired through
// the internal() thin layer to the submitter, mirroring Java's
// ParallelActorContextImpl.
type ParallelActorContext[T any] interface {
	// CreateParallelActor creates one physical wrapper actor with N
	// PARALLEL_INSTANCE_<i> concurrency groups (maxConcurrency=1).
	CreateParallelActor(desc *function.GoFunctionDescriptor, args []interface{}, parallelism int, options *submitter.ActorCreationOptions) (*ParallelActorHandle[T], error)
	// SubmitTask submits an actor task routed to the given instance's
	// concurrency group.
	SubmitTask(actorID ids.ActorID, desc *function.GoFunctionDescriptor, args []interface{}, instanceID int, numReturns int) (*ObjectRef[T], error)
}

// defaultParallelActorContext implements ParallelActorContext through the
// internal() thin layer helpers (createActorWithSubmitter / submitTaskRef),
// keeping the go/pkg layer free of any go/internal dependency.
type defaultParallelActorContext[T any] struct{}

// CreateParallelActor creates the wrapper actor and its N instance
// concurrency groups, mirroring Java's ParallelActorContextImpl.create.
func (c *defaultParallelActorContext[T]) CreateParallelActor(desc *function.GoFunctionDescriptor, args []interface{}, parallelism int, options *submitter.ActorCreationOptions) (*ParallelActorHandle[T], error) {
	if options == nil {
		// MaxPendingCalls defaults to unlimited, matching newActorOptions'
		// default for ordinary actors.
		options = &submitter.ActorCreationOptions{MaxPendingCalls: submitter.MaxPendingCallsUnlimited}
	}
	groups := make([]submitter.ConcurrencyGroup, parallelism)
	for i := 0; i < parallelism; i++ {
		groups[i] = submitter.ConcurrencyGroup{
			Name:     parallelActorInstanceGroupPrefix + strconv.Itoa(i),
			MaxCalls: 1,
			Methods:  []string{parallelActorExecuteMethod},
		}
	}
	options.ConcurrencyGroups = groups

	functionArgs := convertArgs(args...)
	actorID, err := createActorWithSubmitter("create_actor", functionArgs, func(s submitter.TaskSubmitter) (ids.ActorID, error) {
		return s.CreateActor(desc, functionArgs, options)
	})
	if err != nil {
		return nil, err
	}
	return &ParallelActorHandle[T]{
		wrapperHandle: NewActorHandleImpl[T](actorID),
		parallelism:   parallelism,
		context:       c,
	}, nil
}

// SubmitTask submits a call to the given instance's concurrency group on the
// wrapper actor's Execute method. The payload carries the instance ID and the
// target method descriptor so the worker can dispatch to the right instance.
func (c *defaultParallelActorContext[T]) SubmitTask(actorID ids.ActorID, desc *function.GoFunctionDescriptor, args []interface{}, instanceID int, numReturns int) (*ObjectRef[T], error) {
	cgName := parallelActorInstanceGroupPrefix + strconv.Itoa(instanceID)
	functionArgs := convertArgs(args...)
	return submitTaskRef[T]("submit_actor_task", functionArgs, func(s submitter.TaskSubmitter) ([]ids.ObjectID, error) {
		return s.SubmitActorTask(actorID, desc, functionArgs, numReturns, &submitter.TaskOptions{ConcurrencyGroupName: cgName})
	})
}

// ParallelActor is the fluent builder entry point for a parallel actor.
// Consistent with Java's io.ray.api.parallelactor.ParallelActor.
//
// Type parameter T is the actor type (conventionally a pointer, e.g. *MyActor).
type ParallelActor[T any] struct {
	ctor        interface{}
	parallelism int
	ctorArgs    []interface{}
	context     ParallelActorContext[T]
}

// NewParallelActor creates a parallel actor builder for the given actor class
// (a constructor factory function or an actor instance/pointer). The builder
// starts with parallelism 1; call SetParallelism to override it.
func NewParallelActor[T any](ctor interface{}) *ParallelActor[T] {
	// Register the actor constructor so a Go worker can build instances,
	// mirroring api.Actor.
	registerActorConstructor[T](ctor)
	return &ParallelActor[T]{ctor: ctor, parallelism: 1}
}

// SetParallelism sets the number of parallel instances (default 1). It must
// be >= 1.
func (p *ParallelActor[T]) SetParallelism(n int) *ParallelActor[T] {
	if n < 1 {
		panic("parallel actor parallelism must be >= 1")
	}
	p.parallelism = n
	return p
}

// Remote is kept as an alias for API symmetry with the task callers, but the
// canonical creation method is Create (aligned with ActorCreator.Create).
func (p *ParallelActor[T]) Remote() (*ParallelActorHandle[T], error) {
	return p.Create()
}

// Create creates the parallel actor: one physical wrapper actor plus N
// concurrency groups (PARALLEL_INSTANCE_0..N-1, maxConcurrency=1), mirroring
// Java's ParallelActorContextImpl.
func (p *ParallelActor[T]) Create() (*ParallelActorHandle[T], error) {
	ctx := p.context
	if ctx == nil {
		ctx = &defaultParallelActorContext[T]{}
	}
	// The wrapper actor's actor type is the fixed ParallelActorExecutor (aligned
	// with Java: the wrapper actor type is ParallelActorExecutorImpl itself), so
	// the worker recognises it and constructs N instances. The user actor's
	// <init> descriptor is carried in the creation args so the executor can build
	// the N user instances. Order: [parallelism, userCtorDesc, userArgs...].
	userCtorDesc := extractActorTypeDescriptor[T]()
	executorDesc := function.NewParallelActorExecutorDescriptor()
	createArgs := append([]interface{}{p.parallelism, userCtorDesc.ToList()}, p.ctorArgs...)
	return ctx.CreateParallelActor(executorDesc, createArgs, p.parallelism, nil)
}

// ParallelActorHandle is a handle to a parallel actor. It combines the
// underlying wrapper actor handle with the parallelism and exposes per-instance
// task callers.
type ParallelActorHandle[T any] struct {
	wrapperHandle ActorHandle
	parallelism   int
	context       ParallelActorContext[T]
}

// GetParallelism returns the number of parallel instances.
func (h *ParallelActorHandle[T]) GetParallelism() int { return h.parallelism }

// GetHandle returns the underlying wrapper actor handle.
func (h *ParallelActorHandle[T]) GetHandle() ActorHandle { return h.wrapperHandle }

// GetInstance returns the i-th parallel actor instance.
//
// i must be in [0, GetParallelism()); an out-of-range index panics (fail-fast,
// mirroring SetParallelism) instead of failing later on the worker when the
// PARALLEL_INSTANCE_<i> concurrency group cannot be found.
func (h *ParallelActorHandle[T]) GetInstance(i int) *ParallelActorInstance[T] {
	if i < 0 || i >= h.parallelism {
		panic(fmt.Sprintf("parallel actor instance index %d out of range [0, %d)", i, h.parallelism))
	}
	return &ParallelActorInstance[T]{handle: h, instanceID: i}
}

// ParallelActorInstance wraps a single parallel actor instance and binds method
// calls to it.
type ParallelActorInstance[T any] struct {
	handle     *ParallelActorHandle[T]
	instanceID int
}

// Task binds a method call on this instance. The returned caller routes the
// call to this instance's concurrency group when Remote is invoked.
func (i *ParallelActorInstance[T]) Task(method interface{}, args ...interface{}) *ParallelActorTaskCaller[T] {
	methodDesc, err := (&MethodExtractor{}).ExtractActorMethodDescriptor(method)
	if err != nil {
		return &ParallelActorTaskCaller[T]{
			instance: i,
			err:      fmt.Errorf("failed to extract method descriptor: %w", err),
		}
	}
	numReturns := 1
	if funcType := reflect.TypeOf(method); funcType != nil && funcType.Kind() == reflect.Func {
		numReturns = function.NonErrorReturnCount(funcType)
	}
	return &ParallelActorTaskCaller[T]{
		instance:   i,
		methodDesc: methodDesc,
		args:       args,
		numReturns: numReturns,
	}
}

// ParallelActorTaskCaller is the caller for a value-returning parallel actor
// method call. Its Remote method submits the call to the instance's
// concurrency group.
type ParallelActorTaskCaller[T any] struct {
	instance   *ParallelActorInstance[T]
	methodDesc *function.GoFunctionDescriptor
	args       []interface{}
	numReturns int
	err        error
}

// Remote submits the instance-routed call and returns a reference to the result.
func (c *ParallelActorTaskCaller[T]) Remote() (*ObjectRef[T], error) {
	if c.err != nil {
		return nil, c.err
	}
	instanceID := c.instance.instanceID
	executeDesc := executeMethodDescriptor(c.methodDesc)
	submitArgs := append([]interface{}{instanceID, c.methodDesc.ToList()}, c.args...)
	return c.instance.handle.context.SubmitTask(c.instance.handle.wrapperHandle.ID(), executeDesc, submitArgs, instanceID, c.numReturns)
}

// executeMethodDescriptor builds the wrapper actor's Execute method descriptor
// from the target method descriptor, reusing its module/package/actorType
// prefix (the first three elements) and appending the Execute method name.
func executeMethodDescriptor(methodDesc *function.GoFunctionDescriptor) *function.GoFunctionDescriptor {
	if methodDesc == nil {
		return function.NewGoActorMethodDescriptorOrUnknown("unknown", "unknown", "unknown", parallelActorExecuteMethod)
	}
	list := methodDesc.ToList()
	return function.NewGoActorMethodDescriptorOrUnknown(list[0], list[1], list[2], parallelActorExecuteMethod)
}
