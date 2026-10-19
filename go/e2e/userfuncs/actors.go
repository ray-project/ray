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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// Counter is a stateful actor used to demonstrate the Go actor execution
// framework. Each actor instance keeps its own independent counter value, which
// is the property that distinguishes an actor from a stateless task: method
// calls on the same actor observe and mutate shared state.
type Counter struct {
	value int
}

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

// RuntimeContextProbe is an actor that reports the RuntimeContext values
// observed from inside an actor method execution. It verifies that the
// RuntimeContext APIs aligned with Java and C++ resolve correctly in an actor
// context: the current actor ID, the
// current actor handle, the restart status, cluster node/actor listings, and
// the worker's resource/GPU IDs.
type RuntimeContextProbe struct{}

// newRuntimeContextProbe is the RuntimeContextProbe constructor factory,
// registered under "<init>" alongside Counter.
func newRuntimeContextProbe() *RuntimeContextProbe {
	return &RuntimeContextProbe{}
}

// Probe captures the RuntimeContext values inside an actor method and returns
// them as a string map so the driver can assert the actor-context semantics:
//
//	job_id, actor_id, task_id, node_id, namespace, is_local_mode
//	actor_handle_present   ("true"/"false"): GetCurrentActorHandle() non-nil
//	was_actor_restarted    ("true"/"false")
//	node_count             len(GetAllNodeInfo())
//	actor_count            len(GetAllActorInfo())
//	gpu_ids                comma-joined GetGpuIds()
//	resource_names         comma-joined GetAvailableResourceIds() keys
func (p *RuntimeContextProbe) Probe() (map[string]string, error) {
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return nil, err
	}
	result := map[string]string{
		"job_id":        ctx.JobID().Hex(),
		"actor_id":      ctx.ActorID().Hex(),
		"task_id":       ctx.TaskID().Hex(),
		"node_id":       ctx.NodeID().Hex(),
		"namespace":     ctx.Namespace(),
		"is_local_mode": strconv.FormatBool(ctx.IsLocalMode()),
	}
	handle := ctx.GetCurrentActorHandle()
	result["actor_handle_present"] = strconv.FormatBool(handle != nil)
	if handle != nil {
		result["actor_handle_id"] = handle.ID().Hex()
	}
	result["was_actor_restarted"] = strconv.FormatBool(ctx.WasCurrentActorRestarted())

	nodes := ctx.GetAllNodeInfo()
	result["node_count"] = strconv.Itoa(len(nodes))
	for i, n := range nodes {
		if i >= 5 {
			break
		}
		result[fmt.Sprintf("node_%d_id", i)] = n.NodeID.Hex()
		result[fmt.Sprintf("node_%d_address", i)] = n.NodeAddress
		result[fmt.Sprintf("node_%d_alive", i)] = strconv.FormatBool(n.IsAlive)
	}

	actors := ctx.GetAllActorInfo()
	result["actor_count"] = strconv.Itoa(len(actors))

	gpuIDs := ctx.GetGpuIds()
	result["gpu_ids"] = joinInt64(gpuIDs)

	resourceIDs, err := api.GetAvailableResourceIds()
	if err != nil {
		result["available_resource_ids_error"] = err.Error()
	} else {
		var names []string
		for name := range resourceIDs {
			names = append(names, name)
		}
		sort.Strings(names)
		result["resource_names"] = strings.Join(names, ",")
	}
	return result, nil
}

// joinInt64 formats an int64 slice as a comma-joined string.
func joinInt64(ids []int64) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, strconv.FormatInt(id, 10))
	}
	return strings.Join(parts, ",")
}

// RuntimeEnvProbe is an actor that reports the serialized runtime env observed
// inside an actor method. It verifies that a runtime env submitted via
// WithRuntimeEnv reaches the Go worker: the worker-side
// RuntimeContext.RuntimeEnv() must reflect the submitted fields,
// and the "config" key must be preserved so worker-side extractConfigField can
// parse it.
type RuntimeEnvProbe struct{}

// newRuntimeEnvProbe is the zero-argument constructor factory for
// RuntimeEnvProbe, registered under its "<init>" descriptor.
func newRuntimeEnvProbe() *RuntimeEnvProbe {
	return &RuntimeEnvProbe{}
}

// RuntimeEnv returns the serialized runtime env observed inside the actor
// method (RuntimeContext.RuntimeEnv()). The driver asserts it contains the
// fields and the "config" key it submitted via WithRuntimeEnv.
func (p *RuntimeEnvProbe) RuntimeEnv() (string, error) {
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return "", err
	}
	return ctx.RuntimeEnv(), nil
}

// Exit exits the actor intentionally via api.ExitActor.
func (p *RuntimeEnvProbe) Exit() error {
	return api.ExitActor()
}

// ParallelCounter is a zero-argument actor type used to exercise the Parallel
// Actor programming model end to end. Unlike Counter, its constructor takes no
// arguments (ParallelActor currently builds every instance from the same
// constructor with no per-instance args), so it is a clean target for verifying
// instance isolation: each PARALLEL_INSTANCE_<i> concurrency group owns one
// independent ParallelCounter whose Value is private to that instance.
//
// Value is exported so the cross-process msgpack serializer can carry it in the
// method result, letting the driver observe each instance's private state.
type ParallelCounter struct {
	Value int
}

// newParallelCounter is the zero-argument constructor factory for
// ParallelCounter, registered under its "<init>" descriptor.
func newParallelCounter() *ParallelCounter {
	return &ParallelCounter{}
}

// Add increments this instance's counter by delta and returns the (serialized)
// receiver itself. Returning *ParallelCounter keeps the result type identical
// to the actor type, which is what ParallelActorTaskCaller[T] expects, so the
// driver can read the new state from the deserialized snapshot via GetValue.
func (c *ParallelCounter) Add(delta int) *ParallelCounter {
	c.Value += delta
	return c
}

// GetValue returns this instance's current counter value without mutating it.
func (c *ParallelCounter) GetValue() int {
	return c.Value
}

// Sleep blocks this instance for ms milliseconds and returns the (serialized)
// receiver itself. It lets the e2e driver verify the two concurrency
// properties that define a parallel actor (aligned with Java's
// ParallelActorExecutorImpl): calls routed to different PARALLEL_INSTANCE_<i>
// groups must overlap (inter-instance parallelism), while calls on the same
// instance must run one at a time (intra-instance serialization, enforced by
// the group's maxConcurrency=1).
func (c *ParallelCounter) Sleep(ms int) *ParallelCounter {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return c
}

// registerActors registers the Counter, ParallelCounter and RuntimeContextProbe
// actor constructors with the global registry. It is called from
// RegisterFunctions so both the driver (via package import) and the worker (via
// the userfuncs.so plugin) register the same actor descriptors, which is what
// lets the worker resolve the constructor from the actor creation task's
// function descriptor.
func registerActors() error {
	if err := api.RegisterActorClass((*Counter)(nil), newCounter); err != nil {
		return err
	}
	if err := api.RegisterActorClass((*ParallelCounter)(nil), newParallelCounter); err != nil {
		return err
	}
	if err := api.RegisterActorClass((*RuntimeContextProbe)(nil), newRuntimeContextProbe); err != nil {
		return err
	}
	return api.RegisterActorClass((*RuntimeEnvProbe)(nil), newRuntimeEnvProbe)
}
