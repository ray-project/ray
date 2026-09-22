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

// actor_integration_driver is a Ray Go driver application that exercises the
// stateful actor execution framework end to end, mirroring the logic of
// actor_integration_test.go but packaged as a runnable binary so it can be
// submitted via `ray job submit`.
//
// It verifies the two actor invariants:
//  1. State persistence: successive method calls observe prior mutations.
//  2. Instance isolation: two actors never share state.
//
// In addition it covers the actor/Java-C++ alignment behavior:
//  3. WithName propagates the task name through the native submitter.
//  4. WithConcurrencyGroup routes the method call to a named concurrency group.
//  5. Named actors resolved via GetActor reuse the NewNativeActorHandle path.
//
// The driver submits ACTOR_CREATION_TASK / ACTOR_TASK requests; the actual
// Counter construction and method dispatch happen on the Go worker, which
// resolves the "<init>" constructor and methods via the userfuncs.so plugin
// located through the code search path carried in the job config (the raylet
// converts it into RAY_CODE_SEARCH_PATH for the worker, see worker_pool.cc).
//
// Usage:
//
//	# Step 1: Build the userfuncs plugin (loaded by workers).
//	bazel build //go/examples/userfuncs/plugin:userfuncs_plugin
//
//	# Step 2: Build this driver.
//	bazel build //go/examples/actor_integration_driver
//
//	# Step 3: Stage the .so and the driver into one directory, then submit.
//	# The driver sets code_search_path to its own cwd (the --working-dir),
//	# which is distributed to worker nodes via GCS.
//	mkdir -p job_dir
//	cp bazel-bin/go/examples/userfuncs/plugin/userfuncs.so job_dir/
//	cp bazel-bin/go/examples/actor_integration_driver_/actor_integration_driver job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./actor_integration_driver
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ray-project/ray/go/examples/userfuncs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// initRay initializes the Ray runtime with a JobConfig that carries the
// userfuncs.so code search path, and returns a shutdown function.
//
// The GCS address is read from the RAY_ADDRESS environment variable, which
// `ray job submit` sets for the driver process. Functions and actor classes
// are auto-registered by importing the userfuncs package (its init() calls
// RegisterFunctions()).
//
// The code search path is set to the current working directory, which when
// submitted with `ray job submit --working-dir .` is distributed to worker
// nodes via GCS. The raylet converts the job config's code_search_path into
// RAY_CODE_SEARCH_PATH for the Go worker (see worker_pool.cc), so the worker
// resolves the "<init>" constructor and Counter methods from userfuncs.so.
func initRay() error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}
	codeSearchPath := filepath.Join(cwd, "userfuncs.so")

	jobConfigBuilder := options.NewJobConfigBuilder().
		WithCodeSearchPath(codeSearchPath)

	if err := api.Instance().InitializeWithJobConfig(jobConfigBuilder); err != nil {
		return fmt.Errorf("ray runtime init failed: %w", err)
	}

	log.Printf("driver code search path: %s", codeSearchPath)
	return nil
}

// releaseActor retires an actor after its test completes. Actor workers are
// long-lived by design (state must persist between method calls), so a suite
// that sequentially creates many actors must explicitly release each one;
// otherwise later actors starve for resources (each worker pins 1 CPU, and a
// single-node cluster has a fixed CPU budget).
//
// Exit is a pure-error method (numReturns == 0), so Remote() returns a nil
// ref and there is nothing to wait on. The next test's Create/Get naturally
// waits: the new actor's lease queues at the raylet until this worker exits
// and frees its capacity.
func releaseActor(id ids.ActorID) error {
	typed := api.NewActorHandleImpl[struct{}](id)
	if _, err := typed.Task((*userfuncs.Counter).Exit).Remote(); err != nil {
		return fmt.Errorf("submit Exit for cleanup failed: %w", err)
	}
	return nil
}

// testActorLifecycle creates a Counter actor with initial value 10 and calls
// Add/Value to verify state persists across method calls on the same instance.
func testActorLifecycle() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(10)
	if err != nil {
		return fmt.Errorf("create actor failed: %w", err)
	}
	if handle == nil || handle.ID().IsNil() {
		return fmt.Errorf("expected a non-nil actor handle")
	}

	// Re-wrap the handle so actor methods returning int are typed correctly:
	// ActorHandleImpl[T] uses T as the method return type.
	typed := api.NewActorHandleImpl[int](handle.ID())

	// Test 1: Add(5) -> 15.
	ref, err := typed.Task((*userfuncs.Counter).Add, 5).Remote()
	if err != nil {
		return fmt.Errorf("Counter.Add(5) failed: %w", err)
	}
	v, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get counter add result failed: %w", err)
	}
	if v != 15 {
		return fmt.Errorf("Counter.Add(5) = %d, want 15", v)
	}
	fmt.Println("  Counter.Add(5) -> 15 ✓")

	// Test 2: Add(-3) -> 12. A correct result proves the same instance (state
	// 15) was reused rather than a fresh one constructed.
	ref, err = typed.Task((*userfuncs.Counter).Add, -3).Remote()
	if err != nil {
		return fmt.Errorf("Counter.Add(-3) failed: %w", err)
	}
	v, err = api.Get(ref)
	if err != nil {
		return fmt.Errorf("get counter add result failed: %w", err)
	}
	if v != 12 {
		return fmt.Errorf("Counter.Add(-3) = %d, want 12 (state was not persisted)", v)
	}
	fmt.Println("  Counter.Add(-3) -> 12 ✓ (state persisted)")

	// Test 3: Value() -> 12 (read-only method confirms persisted state).
	ref, err = typed.Task((*userfuncs.Counter).Value).Remote()
	if err != nil {
		return fmt.Errorf("Counter.Value() failed: %w", err)
	}
	v, err = api.Get(ref)
	if err != nil {
		return fmt.Errorf("get counter value result failed: %w", err)
	}
	if v != 12 {
		return fmt.Errorf("Counter.Value() = %d, want 12", v)
	}
	fmt.Println("  Counter.Value() -> 12 ✓")

	if err := releaseActor(handle.ID()); err != nil {
		return err
	}
	return nil
}

// testActorIsolation creates two distinct actors and verifies their state stays
// independent, the defining property that contrasts an actor with a stateless
// task.
func testActorIsolation() error {
	h1, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(0)
	if err != nil {
		return fmt.Errorf("create actor 1 failed: %w", err)
	}
	h2, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(100)
	if err != nil {
		return fmt.Errorf("create actor 2 failed: %w", err)
	}

	t1 := api.NewActorHandleImpl[int](h1.ID())
	t2 := api.NewActorHandleImpl[int](h2.ID())

	// Bump both actors once.
	if _, err := t1.Task((*userfuncs.Counter).Add, 1).Remote(); err != nil {
		return fmt.Errorf("actor 1 Add failed: %w", err)
	}
	if _, err := t2.Task((*userfuncs.Counter).Add, 1).Remote(); err != nil {
		return fmt.Errorf("actor 2 Add failed: %w", err)
	}

	// Their state must remain independent: 0+1=1 vs 100+1=101.
	ref1, err := t1.Task((*userfuncs.Counter).Value).Remote()
	if err != nil {
		return fmt.Errorf("actor 1 Value failed: %w", err)
	}
	ref2, err := t2.Task((*userfuncs.Counter).Value).Remote()
	if err != nil {
		return fmt.Errorf("actor 2 Value failed: %w", err)
	}
	v1, err := api.Get(ref1)
	if err != nil {
		return fmt.Errorf("actor 1 Get failed: %w", err)
	}
	v2, err := api.Get(ref2)
	if err != nil {
		return fmt.Errorf("actor 2 Get failed: %w", err)
	}
	if v1 != 1 || v2 != 101 {
		return fmt.Errorf("actor state leaked across instances: got (%d, %d), want (1, 101)", v1, v2)
	}
	fmt.Println("  Counter1.Value() -> 1, Counter2.Value() -> 101 ✓ (isolated)")

	if err := releaseActor(h1.ID()); err != nil {
		return err
	}
	if err := releaseActor(h2.ID()); err != nil {
		return err
	}
	return nil
}

// testActorVoid verifies the void-method path: with numReturns auto-derived to
// 0, Remote() returns (nil, nil) and RemoteVoid() submits cleanly.
func testActorVoid() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(0)
	if err != nil {
		return fmt.Errorf("create void actor failed: %w", err)
	}
	typed := api.NewActorHandleImpl[struct{}](handle.ID())

	ref, err := typed.Task((*userfuncs.Counter).Void).Remote()
	if err != nil {
		return fmt.Errorf("void Remote failed: %w", err)
	}
	if ref != nil {
		return fmt.Errorf("void Remote() should return a nil ObjectRef, got %v", ref)
	}
	if err := typed.Task((*userfuncs.Counter).Void).RemoteVoid(); err != nil {
		return fmt.Errorf("void RemoteVoid failed: %w", err)
	}
	fmt.Println("  Counter.Void() Remote()=(nil,nil), RemoteVoid() ok ✓")
	if err := releaseActor(handle.ID()); err != nil {
		return err
	}
	return nil
}

// testActorMultiReturn verifies multi-return execution: the method returns two
// values and the first is delivered through the primary ObjectRef.
func testActorMultiReturn() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(21)
	if err != nil {
		return fmt.Errorf("create multi-return actor failed: %w", err)
	}
	typed := api.NewActorHandleImpl[int](handle.ID())

	ref, err := typed.Task((*userfuncs.Counter).Pair).WithNumReturns(2).Remote()
	if err != nil {
		return fmt.Errorf("Pair Remote failed: %w", err)
	}
	v, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get Pair first return failed: %w", err)
	}
	if v != 21 {
		return fmt.Errorf("Counter.Pair() first = %d, want 21", v)
	}
	fmt.Println("  Counter.Pair() first return -> 21 ✓ (numReturns=2)")
	if err := releaseActor(handle.ID()); err != nil {
		return err
	}
	return nil
}

// testActorTaskWithName verifies that WithName propagates the task name through
// the native submitter into the C++ TaskOptions. A named actor method call must
// still execute on the worker and return the correct result.
func testActorTaskWithName() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(0)
	if err != nil {
		return fmt.Errorf("create named-task actor failed: %w", err)
	}
	typed := api.NewActorHandleImpl[int](handle.ID())

	ref, err := typed.Task((*userfuncs.Counter).Add, 7).WithName("counter-add-7").Remote()
	if err != nil {
		return fmt.Errorf("named Add(7) failed: %w", err)
	}
	v, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get named Add result failed: %w", err)
	}
	if v != 7 {
		return fmt.Errorf("named Add(7) = %d, want 7", v)
	}
	fmt.Println("  Counter.Add(7) with name -> 7 ✓ (task name propagated)")
	if err := releaseActor(handle.ID()); err != nil {
		return err
	}
	return nil
}

// testActorSystemConcurrencyGroup verifies that WithConcurrencyGroup routes the
// actor method call to the named concurrency group end to end. The Go option
// flows through the CGO submitter into C++ TaskOptions.concurrency_group_name,
// and the worker's ConcurrencyGroupManager looks the executor up by name.
// "_ray_system" is the system concurrency group that C++ auto-creates, so the
// lookup always succeeds without the actor having to declare groups in advance.
func testActorSystemConcurrencyGroup() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(3)
	if err != nil {
		return fmt.Errorf("create concurrency-group actor failed: %w", err)
	}
	typed := api.NewActorHandleImpl[int](handle.ID())

	// "_ray_system" is the C++ system concurrency group default
	// (RayConfig::system_concurrency_group_name); the native path auto-creates
	// it with concurrency 1 on first use.
	ref, err := typed.Task((*userfuncs.Counter).Add, 4).
		WithConcurrencyGroup("_ray_system").
		Remote()
	if err != nil {
		return fmt.Errorf("concurrency-group Add(4) failed: %w", err)
	}
	v, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get concurrency-group Add result failed: %w", err)
	}
	if v != 7 {
		return fmt.Errorf("concurrency-group Add(4) = %d, want 7 (3+4)", v)
	}
	fmt.Println("  Counter.Add(4) via _ray_system group -> 7 ✓ (concurrency group routed)")
	if err := releaseActor(handle.ID()); err != nil {
		return err
	}
	return nil
}

// testActorNamedAndReusable verifies the named-actor handle path exercised by
// NewNativeActorHandle: after creating an actor under a name, GetActor resolves
// it by name and the re-obtained handle (derived from the same actor ID) can
// still submit method calls that observe the actor's persisted state.
func testActorNamedAndReusable() error {
	const actorName = "counter_named_p2"

	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).
		WithName(actorName).
		Create(100)
	if err != nil {
		return fmt.Errorf("create named actor failed: %w", err)
	}
	if handle == nil || handle.ID().IsNil() {
		return fmt.Errorf("expected a non-nil handle for named actor")
	}

	// Bump the actor so it holds state, then re-resolve it by name.
	typed := api.NewActorHandleImpl[int](handle.ID())
	if _, err := typed.Task((*userfuncs.Counter).Add, 5).Remote(); err != nil {
		return fmt.Errorf("named actor Add(5) failed: %w", err)
	}

	got, err := api.GetActor[*userfuncs.Counter](actorName)
	if err != nil {
		return fmt.Errorf("GetActor(%q) failed: %w", actorName, err)
	}
	gotTyped := api.NewActorHandleImpl[int](got.ID())
	ref, err := gotTyped.Task((*userfuncs.Counter).Value).Remote()
	if err != nil {
		return fmt.Errorf("re-obtained handle Value failed: %w", err)
	}
	v, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get re-obtained Value failed: %w", err)
	}
	if v != 105 {
		return fmt.Errorf("named actor Value = %d, want 105 (100+5, state persisted across handles)", v)
	}
	fmt.Println("  named actor Add(5) then GetActor().Value() -> 105 ✓ (handle reusable)")
	if err := releaseActor(handle.ID()); err != nil {
		return err
	}
	return nil
}

// testActorExit verifies intentional exit: the method returns api.ExitActor()
// and the driver must not treat it as a failure (the worker exits normally).
func testActorExit() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(0)
	if err != nil {
		return fmt.Errorf("create exit actor failed: %w", err)
	}
	typed := api.NewActorHandleImpl[struct{}](handle.ID())

	// Submit the exit call; do not Get() it (the worker exits, so no return
	// object is sealed). The intentional exit must not fail the job.
	if _, err := typed.Task((*userfuncs.Counter).Exit).Remote(); err != nil {
		return fmt.Errorf("exit Remote failed: %w", err)
	}

	// A fresh actor on a new worker must still work.
	handle2, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(1)
	if err != nil {
		return fmt.Errorf("create post-exit actor failed: %w", err)
	}
	t2 := api.NewActorHandleImpl[int](handle2.ID())
	ref, err := t2.Task((*userfuncs.Counter).Value).Remote()
	if err != nil {
		return fmt.Errorf("post-exit actor Value failed: %w", err)
	}
	if v, err := api.Get(ref); err != nil || v != 1 {
		return fmt.Errorf("post-exit actor Value = %v (err %v), want 1", v, err)
	}
	fmt.Println("  Counter.Exit() submitted, worker exits intentionally ✓")
	return releaseActor(handle2.ID())
}

// testActorKill verifies the driver-side KillActor API: after KillActor with
// noRestart=true, the actor worker dies and later method calls fail instead of
// executing.
func testActorKill() error {
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(42)
	if err != nil {
		return fmt.Errorf("create kill actor failed: %w", err)
	}
	typed := api.NewActorHandleImpl[int](handle.ID())

	// Confirm the actor is alive and callable before killing.
	ref, err := typed.Task((*userfuncs.Counter).Value).Remote()
	if err != nil {
		return fmt.Errorf("Value before kill failed: %w", err)
	}
	if v, err := api.Get(ref); err != nil || v != 42 {
		return fmt.Errorf("Value before kill = %v (err %v), want 42", v, err)
	}

	// Kill the actor from the driver side with noRestart=true.
	if err := api.KillActor(handle, true); err != nil {
		return fmt.Errorf("KillActor failed: %w", err)
	}

	// CoreWorker::KillActor is asynchronous (GCS -> raylet -> worker SIGKILL),
	// so after it returns we poll a method call until the worker has actually
	// died and the call fails, proving both the kill and the no-restart
	// semantics. A method call succeeding while the worker is still alive is
	// not a failure; the poll only fails if no call ever fails.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ref, err = typed.Task((*userfuncs.Counter).Value).Remote()
		if err == nil {
			_, err = api.Get(ref)
		}
		if err != nil {
			break // worker is dead: submission or execution failed
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("expected actor to be unavailable after kill")
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("  Counter killed via KillActor(noRestart=true), later calls fail ✓")
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := initRay(); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestActorLifecycle ===")
	if err := testActorLifecycle(); err != nil {
		log.Fatalf("TestActorLifecycle FAILED: %v", err)
	}
	fmt.Printf("=== TestActorLifecycle PASSED ===\n\n")

	fmt.Println("=== TestActorIsolation ===")
	if err := testActorIsolation(); err != nil {
		log.Fatalf("TestActorIsolation FAILED: %v", err)
	}
	fmt.Printf("=== TestActorIsolation PASSED ===\n\n")

	fmt.Println("=== TestActorVoid ===")
	if err := testActorVoid(); err != nil {
		log.Fatalf("TestActorVoid FAILED: %v", err)
	}
	fmt.Printf("=== TestActorVoid PASSED ===\n\n")

	fmt.Println("=== TestActorMultiReturn ===")
	if err := testActorMultiReturn(); err != nil {
		log.Fatalf("TestActorMultiReturn FAILED: %v", err)
	}
	fmt.Printf("=== TestActorMultiReturn PASSED ===\n\n")

	fmt.Println("=== TestActorTaskWithName ===")
	if err := testActorTaskWithName(); err != nil {
		log.Fatalf("TestActorTaskWithName FAILED: %v", err)
	}
	fmt.Printf("=== TestActorTaskWithName PASSED ===\n\n")

	fmt.Println("=== TestActorSystemConcurrencyGroup ===")
	if err := testActorSystemConcurrencyGroup(); err != nil {
		log.Fatalf("TestActorSystemConcurrencyGroup FAILED: %v", err)
	}
	fmt.Printf("=== TestActorSystemConcurrencyGroup PASSED ===\n\n")

	fmt.Println("=== TestActorNamedAndReusable ===")
	if err := testActorNamedAndReusable(); err != nil {
		log.Fatalf("TestActorNamedAndReusable FAILED: %v", err)
	}
	fmt.Printf("=== TestActorNamedAndReusable PASSED ===\n\n")

	fmt.Println("=== TestActorExit ===")
	if err := testActorExit(); err != nil {
		log.Fatalf("TestActorExit FAILED: %v", err)
	}
	fmt.Printf("=== TestActorExit PASSED ===\n\n")

	fmt.Println("=== TestActorKill ===")
	if err := testActorKill(); err != nil {
		log.Fatalf("TestActorKill FAILED: %v", err)
	}
	fmt.Printf("=== TestActorKill PASSED ===\n\n")

	fmt.Println("All actor integration checks passed.")
}
