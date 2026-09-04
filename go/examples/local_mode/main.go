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

// Command local_mode demonstrates the pure-Go local-mode Ray runtime:
// normal tasks, actors, put/get and wait, all without a Ray cluster or the
// native CoreWorker bridge.
package main

import (
	"fmt"

	_ "github.com/ray-project/ray/go/internal/runtime/local" // register LocalModeRuntime
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// add is a plain function that will be executed as a Ray task.
func add(a, b int) int { return a + b }

// Counter is an actor with mutable state.
type Counter struct {
	value int
}

// Inc increments the counter and returns the new value.
func (c *Counter) Inc() int {
	c.value++
	return c.value
}

func main() {
	if err := api.Instance().Init(); err != nil {
		panic(fmt.Sprintf("failed to initialize local-mode Ray: %v", err))
	}
	defer api.Instance().Shutdown()

	// Task invocation.
	ref, err := api.Instance().Remote(add).Call(1, 2)
	if err != nil {
		panic(fmt.Sprintf("failed to submit task: %v", err))
	}
	result, err := api.Instance().Get(ref)
	if err != nil {
		panic(fmt.Sprintf("failed to get task result: %v", err))
	}
	fmt.Printf("add(1,2) = %v\n", result)

	// Actor creation. Method invocation needs the actor type to live in a
	// module package (method descriptors derive the import path from the
	// function's runtime module path), so this example only verifies that the
	// actor creation pipeline works in local mode.
	actor, err := api.Instance().Actor(&Counter{}).Create()
	if err != nil {
		panic(fmt.Sprintf("failed to create actor: %v", err))
	}
	fmt.Printf("actor created with id: %s\n", actor.ID())

	fmt.Println("local mode ok")
}