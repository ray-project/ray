// Copyright 2025 The Ray Authors.
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

package api_test

import (
	"fmt"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// ExampleRay_Init demonstrates how to initialize Ray.
func ExampleRay_Init() {
	// Initialize Ray runtime
	err := api.Instance().Init()
	if err != nil {
		panic(err)
	}
	defer api.Instance().Shutdown()

	fmt.Println("Ray initialized successfully")
}

// ExampleRay_Remote demonstrates how to submit a remote task.
// Note: This example requires a fully initialized Ray runtime.
func ExampleRay_Remote() {
	// Initialize Ray runtime
	if err := api.Instance().Init(); err != nil {
		fmt.Printf("Failed to initialize Ray: %v\n", err)
		return
	}
	defer api.Instance().Shutdown()

	// Define a remote function
	add := func(x, y int) int {
		return x + y
	}

	// Submit task using the builder pattern
	resultRef, err := api.Instance().Remote(add).Call(1, 2)
	if err != nil {
		fmt.Printf("Failed to submit task: %v\n", err)
		return
	}

	// Get result
	result, err := api.Instance().Get(resultRef)
	if err != nil {
		fmt.Printf("Failed to get result: %v\n", err)
		return
	}

	fmt.Println(result)
}

// ExampleRay_Actor demonstrates how to create and call an actor.
func ExampleRay_Actor() {
	api.Instance().Init()
	defer api.Instance().Shutdown()

	// Define an actor class
	type Counter struct {
		value int
	}

	// Note: In real code, these methods would be defined on the Counter type
	// This is a simplified example for documentation purposes

	// Create actor
	actor, err := api.Instance().Actor(&Counter{}).
		WithName("counter").
		Create()
	if err != nil {
		panic(err)
	}

	_ = actor
}

// ExampleRay_PutGet demonstrates how to use object store.
func ExampleRay_PutGet() {
	api.Instance().Init()
	defer api.Instance().Shutdown()

	// Put an object
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	ref, err := api.Instance().Put(data)
	if err != nil {
		panic(err)
	}

	// Get the object
	result, err := api.Instance().Get(ref)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Retrieved: %v\n", result)
}

// ExampleRay_Wait demonstrates how to wait for multiple objects.
func ExampleRay_Wait() {
	api.Instance().Init()
	defer api.Instance().Shutdown()

	// Submit multiple tasks
	refs := make([]*api.ObjectRef[interface{}], 5)
	for i := 0; i < 5; i++ {
		ref, _ := api.Instance().Remote(func(x int) int { return x * 2 }).Call(i)
		refs[i] = ref
	}

	// Wait for at least 3 objects to complete
	waitResult, err := api.Instance().Wait(refs, 3, 1000, true)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Ready: %d, Unready: %d\n",
		len(waitResult.Ready()), len(waitResult.Unready()))
}

// ExampleRay_ActorOptions demonstrates how to use actor creation options.
func ExampleRay_ActorOptions() {
	api.Instance().Init()
	defer api.Instance().Shutdown()

	type MyActor struct{}

	// Create actor with options
	actor, err := api.Instance().Actor(&MyActor{}).
		WithName("my_actor").
		WithMaxRestarts(3).
		WithMaxConcurrency(10).
		WithResources(map[string]float64{"CPU": 1.0}).
		Create()

	if err != nil {
		panic(err)
	}

	_ = actor
}

// ExampleRay_TaskOptions demonstrates how to use task options.
func ExampleRay_TaskOptions() {
	api.Instance().Init()
	defer api.Instance().Shutdown()

	add := func(x, y int) int { return x + y }

	// Submit task with options
	resultRef, err := api.Instance().Remote(add).
		WithName("add_task").
		WithResources(map[string]float64{"CPU": 0.5}).
		WithMaxRetries(3).
		Call(1, 2)

	if err != nil {
		panic(err)
	}

	result, _ := api.Instance().Get(resultRef)
	fmt.Println(result)
}

// ExampleRay_RuntimeContext demonstrates how to get runtime context.
func ExampleRay_RuntimeContext() {
	api.Instance().Init()
	defer api.Instance().Shutdown()

	ctx, err := api.Instance().GetRuntimeContext()
	if err != nil {
		panic(err)
	}

	fmt.Printf("JobID: %s, NodeID: %s, LocalMode: %v\n",
		ctx.JobID().Hex(), ctx.NodeID().Hex(), ctx.IsLocalMode())
}

// ExampleRay_ErrorHandling demonstrates error handling.
func ExampleRay_ErrorHandling() {
	// Using API without initialization
	_, err := api.Instance().Get(nil)
	if err != nil {
		if errors.IsRayError(err) {
			fmt.Printf("Ray error: %v\n", err)
		}
	}

	api.Instance().Init()
	defer api.Instance().Shutdown()

	// Normal operation
	add := func(x, y int) int { return x + y }
	ref, err := api.Instance().Remote(add).Call(1, 2)
	if err != nil {
		panic(err)
	}

	result, err := api.Instance().Get(ref)
	if err != nil {
		if errors.IsTimeout(err) {
			fmt.Println("Get operation timed out")
		} else if err != nil {
			panic(err)
		}
	}

	fmt.Printf("Result: %d\n", result)
}
