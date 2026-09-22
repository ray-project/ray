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

// The Counter struct itself is declared in funcs.go. This file adds the actor
// methods and the "<init>" constructor exercised by the actor integration
// driver: each actor instance keeps its own independent counter value, which is
// the property that distinguishes an actor from a stateless task, because method
// calls on the same actor observe and mutate shared state.

// newCounter is the actor constructor factory. It is registered under the
// special "<init>" descriptor so a Go worker can build a Counter instance when
// it receives an ACTOR_CREATION_TASK.
//
// Actors that need constructor arguments must be registered explicitly (Go has
// no runtime type lookup by name), as done by registerActors below.
func newCounter(initial int) *Counter {
	return &Counter{value: initial}
}

// Add increments the counter by delta and returns the new value.
func (c *Counter) Add(delta int) int {
	c.value += delta
	return c.value
}

// Value returns the current counter value without mutating it.
func (c *Counter) Value() int {
	return c.value
}

// Void is a no-return method used to verify the void-method execution path.
func (c *Counter) Void() {
	// intentionally empty
}

// Pair returns two independent values, exercising multi-return execution.
func (c *Counter) Pair() (int, int) {
	return c.value, c.value * 2
}

// Exit exits the actor intentionally via api.ExitActor.
func (c *Counter) Exit() error {
	return api.ExitActor()
}

// registerActors registers the Counter actor constructor with the global
// registry. It is called from RegisterFunctions so both the driver (via package
// import) and the worker (via the userfuncs.so plugin) register the same actor
// descriptor, which is what lets the worker resolve the constructor from the
// actor creation task's function descriptor.
func registerActors() error {
	return api.RegisterActorClass((*Counter)(nil), newCounter)
}
