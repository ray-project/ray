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

// driver_parallel_actor is a Ray Go driver application that exercises the
// Parallel Actor programming model (aligned with Java's
// io.ray.api.parallelactor) end to end against a real cluster.
//
// It verifies:
//  1. ParallelActor creation: one physical wrapper actor + N
//     PARALLEL_INSTANCE_<i> concurrency groups (maxConcurrency=1).
//  2. Instance routing: GetInstance(i).Task(...).Remote() is routed to instance
//     i's concurrency group and dispatched by the worker's ParallelActorExecutor.
//  3. Instance isolation: each instance owns independent state (a ParallelCounter
//     whose value is private to that instance).
//  4. Instance state persistence: successive calls on the same instance observe
//     prior mutations, proving the same instance (not a fresh one) is reused.
//  5. Parallelism semantics: calls routed to different instances' groups run
//     concurrently (inter-instance parallel), while calls on the same instance
//     run one at a time (intra-instance serial, group maxConcurrency=1).
//
// The wrapper actor and method dispatch happen on the Go worker via the
// userfuncs.so plugin (ParallelCounter's "<init>" constructor and methods), so
// this driver is only meaningful when submitted against a running Ray cluster.
//
// Usage (see README.md):
//
//	# 1. Build the userfuncs plugin (loaded by workers).
//	bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
//
//	# 2. Build this driver.
//	bazel build //go/e2e/driver_parallel_actor
//
//	# 3. Stage the .so and the driver into one directory, then submit.
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
//	cp bazel-bin/go/e2e/driver_parallel_actor_/driver_parallel_actor job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./driver_parallel_actor
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ray-project/ray/go/e2e/userfuncs"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// initRay initializes the Ray runtime with a JobConfig carrying the
// userfuncs.so code search path, mirroring the other E2E drivers.
func initRay() error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}
	codeSearchPath := filepath.Join(cwd, "userfuncs.so")

	jobConfigBuilder := options.NewJobConfigBuilder().WithCodeSearchPath(codeSearchPath)

	if err := api.Instance().InitializeWithJobConfig(jobConfigBuilder); err != nil {
		return fmt.Errorf("ray runtime init failed: %w", err)
	}
	log.Printf("driver code search path: %s", codeSearchPath)
	return nil
}

// testParallelActorInstanceIsolation creates a 3-instance ParallelActor and
// verifies that calls routed to different instances operate on independent
// state.
//
// Because ParallelActorTaskCaller[T] carries the actor type T (not the method
// return type) in its ObjectRef, every remote method here returns the receiver
// (*ParallelCounter) so the driver can read the resulting state via GetValue().
func testParallelActorInstanceIsolation() error {
	const parallelism = 3
	ctor := func() *userfuncs.ParallelCounter { return &userfuncs.ParallelCounter{} }
	handle, err := api.NewParallelActor[*userfuncs.ParallelCounter](ctor).SetParallelism(parallelism).Create()
	if err != nil {
		return fmt.Errorf("create parallel actor failed: %w", err)
	}
	if handle == nil {
		return fmt.Errorf("expected a non-nil parallel actor handle")
	}
	if handle.GetParallelism() != parallelism {
		return fmt.Errorf("GetParallelism() = %d, want %d", handle.GetParallelism(), parallelism)
	}
	if handle.GetHandle() == nil {
		return fmt.Errorf("GetHandle() = nil, want non-nil wrapper actor handle")
	}
	fmt.Printf("  created ParallelActor with parallelism=%d\n", parallelism)

	// Instance 0: Add(5) -> snapshot Value 5.
	ref, err := handle.GetInstance(0).Task((*userfuncs.ParallelCounter).Add, 5).Remote()
	if err != nil {
		return fmt.Errorf("instance 0 Add(5) submit failed: %w", err)
	}
	snap, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("instance 0 Add(5) failed: %w", err)
	}
	if snap.GetValue() != 5 {
		return fmt.Errorf("instance 0 Add(5) = %d, want 5", snap.GetValue())
	}
	fmt.Println("  instance 0 Add(5) -> 5 ✓")

	// Instance 1: Add(5) -> 5 (independent of instance 0's state).
	ref, err = handle.GetInstance(1).Task((*userfuncs.ParallelCounter).Add, 5).Remote()
	if err != nil {
		return fmt.Errorf("instance 1 Add(5) submit failed: %w", err)
	}
	snap, err = api.Get(ref)
	if err != nil {
		return fmt.Errorf("instance 1 Add(5) failed: %w", err)
	}
	if snap.GetValue() != 5 {
		return fmt.Errorf("instance 1 Add(5) = %d, want 5 (instance not isolated)", snap.GetValue())
	}
	fmt.Println("  instance 1 Add(5) -> 5 ✓ (isolated from instance 0)")

	// Instance 0 again: Add(3) -> 8. A correct result proves instance 0's state
	// (5) was reused rather than a fresh instance constructed.
	ref, err = handle.GetInstance(0).Task((*userfuncs.ParallelCounter).Add, 3).Remote()
	if err != nil {
		return fmt.Errorf("instance 0 Add(3) submit failed: %w", err)
	}
	snap, err = api.Get(ref)
	if err != nil {
		return fmt.Errorf("instance 0 Add(3) failed: %w", err)
	}
	if snap.GetValue() != 8 {
		return fmt.Errorf("instance 0 Add(3) = %d, want 8 (state was not persisted)", snap.GetValue())
	}
	fmt.Println("  instance 0 Add(3) -> 8 ✓ (state persisted on instance 0)")

	// Instance 1 read-only: Add(0) -> 5, confirming instance 1 was unaffected by
	// instance 0's Add(3).
	ref, err = handle.GetInstance(1).Task((*userfuncs.ParallelCounter).Add, 0).Remote()
	if err != nil {
		return fmt.Errorf("instance 1 Add(0) submit failed: %w", err)
	}
	snap, err = api.Get(ref)
	if err != nil {
		return fmt.Errorf("instance 1 Add(0) failed: %w", err)
	}
	if snap.GetValue() != 5 {
		return fmt.Errorf("instance 1 Add(0) = %d, want 5 (instance isolation broken)", snap.GetValue())
	}
	fmt.Println("  instance 1 Add(0) -> 5 ✓ (unaffected by instance 0)")

	// Instance 2 untouched: Add(0) -> 0.
	ref, err = handle.GetInstance(2).Task((*userfuncs.ParallelCounter).Add, 0).Remote()
	if err != nil {
		return fmt.Errorf("instance 2 Add(0) submit failed: %w", err)
	}
	snap, err = api.Get(ref)
	if err != nil {
		return fmt.Errorf("instance 2 Add(0) failed: %w", err)
	}
	if snap.GetValue() != 0 {
		return fmt.Errorf("instance 2 Add(0) = %d, want 0", snap.GetValue())
	}
	fmt.Println("  instance 2 Add(0) -> 0 ✓")

	return nil
}

// testParallelActorParallelism verifies the two concurrency properties that
// define a parallel actor (aligned with Java's ParallelActorExecutorImpl and
// the design's "per-instance serial, inter-instance parallel" requirement):
//
//   - Inter-instance parallelism: calls routed to different instances' groups
//     run on concurrent group threads, so 3 x Sleep(2000) on instances 0..2
//     finishes in ~2s, not ~6s.
//   - Intra-instance serialization: calls on the same instance's group
//     (maxConcurrency=1) run one at a time, so 2 x Sleep(1500) on instance 0
//     finishes in ~3s, not ~1.5s.
//
// Sleep returns the receiver (*ParallelCounter), keeping the result type
// identical to the actor type so api.Get deserializes it as a snapshot.
func testParallelActorParallelism() error {
	const parallelism = 3
	ctor := func() *userfuncs.ParallelCounter { return &userfuncs.ParallelCounter{} }
	handle, err := api.NewParallelActor[*userfuncs.ParallelCounter](ctor).SetParallelism(parallelism).Create()
	if err != nil {
		return fmt.Errorf("create parallel actor failed: %w", err)
	}

	// Inter-instance parallelism: submit one Sleep to each instance without
	// waiting, then collect all results. Parallel -> ~2s; wrongly serialized
	// -> ~6s. The <4.5s threshold cleanly separates the two (2s sleep leaves a
	// large margin for scheduling/init overhead).
	start := time.Now()
	refs := make([]*api.ObjectRef[*userfuncs.ParallelCounter], parallelism)
	for i := 0; i < parallelism; i++ {
		ref, err := handle.GetInstance(i).Task((*userfuncs.ParallelCounter).Sleep, 2000).Remote()
		if err != nil {
			return fmt.Errorf("instance %d Sleep(2000) submit failed: %w", i, err)
		}
		refs[i] = ref
	}
	for i := 0; i < parallelism; i++ {
		if _, err := api.Get(refs[i]); err != nil {
			return fmt.Errorf("instance %d Sleep(2000) failed: %w", i, err)
		}
	}
	parallelElapsed := time.Since(start)
	fmt.Printf("  inter-instance: 3 x Sleep(2000) across instances 0..2 in %v\n", parallelElapsed.Round(time.Millisecond))
	if parallelElapsed >= 4500*time.Millisecond {
		return fmt.Errorf("instances did not run in parallel: 3x2s took %v (>= 4.5s)", parallelElapsed.Round(time.Millisecond))
	}

	// Intra-instance serialization: two Sleep(1500) on the same instance must
	// run back-to-back (~3s); if the group's maxConcurrency=1 were broken they
	// would overlap (~1.5s). The >=2.2s threshold proves serialization while
	// tolerating a small margin of scheduling noise.
	start = time.Now()
	refA, err := handle.GetInstance(0).Task((*userfuncs.ParallelCounter).Sleep, 1500).Remote()
	if err != nil {
		return fmt.Errorf("instance 0 Sleep(1500) #1 submit failed: %w", err)
	}
	refB, err := handle.GetInstance(0).Task((*userfuncs.ParallelCounter).Sleep, 1500).Remote()
	if err != nil {
		return fmt.Errorf("instance 0 Sleep(1500) #2 submit failed: %w", err)
	}
	if _, err := api.Get(refA); err != nil {
		return fmt.Errorf("instance 0 Sleep(1500) #1 failed: %w", err)
	}
	if _, err := api.Get(refB); err != nil {
		return fmt.Errorf("instance 0 Sleep(1500) #2 failed: %w", err)
	}
	serialElapsed := time.Since(start)
	fmt.Printf("  intra-instance: 2 x Sleep(1500) on instance 0 in %v\n", serialElapsed.Round(time.Millisecond))
	if serialElapsed < 2200*time.Millisecond {
		return fmt.Errorf("instance 0 did not serialize calls: 2x1.5s took %v (< 2.2s)", serialElapsed.Round(time.Millisecond))
	}

	// State must be intact after the sleeps: instance 0 untouched -> 0.
	ref, err := handle.GetInstance(0).Task((*userfuncs.ParallelCounter).Add, 0).Remote()
	if err != nil {
		return fmt.Errorf("instance 0 Add(0) submit failed: %w", err)
	}
	snap, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("instance 0 Add(0) failed: %w", err)
	}
	if snap.GetValue() != 0 {
		return fmt.Errorf("instance 0 Add(0) = %d, want 0", snap.GetValue())
	}
	fmt.Println("  instance 0 state intact after sleeps ✓")

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := initRay(); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestParallelActorInstanceIsolation ===")
	if err := testParallelActorInstanceIsolation(); err != nil {
		log.Fatalf("TestParallelActorInstanceIsolation FAILED: %v", err)
	}
	fmt.Println("=== TestParallelActorInstanceIsolation PASSED ===\n")

	fmt.Println("=== TestParallelActorParallelism ===")
	if err := testParallelActorParallelism(); err != nil {
		log.Fatalf("TestParallelActorParallelism FAILED: %v", err)
	}
	fmt.Println("=== TestParallelActorParallelism PASSED ===\n")

	fmt.Println("All parallel actor checks passed.")
}
