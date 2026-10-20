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

// runtime_context_driver is a Ray Go driver application that verifies the
// RuntimeContext and resource-ID APIs end to end on a real cluster. It
// exercises the surfaces where the Go API previously trailed Java and C++:
//
//	RuntimeContext methods: the previously-placeholder methods
//	    WasCurrentActorRestarted / GetAllNodeInfo / GetAllActorInfo /
//	    GetAllActorInfoByFilter / GetCurrentActorHandle / GetGpuIds
//	Resource-ID APIs: GetAvailableResourceIds / GetGpuIds user APIs
//
// It asserts both the driver (non-actor) context and the actor context (via
// the userfuncs.RuntimeContextProbe actor), matching Java's RuntimeContext
// semantics.
//
// Build & submit as a Ray job:
//
//	bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
//	bazel build //go/e2e/runtime_context_driver
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
//	cp bazel-bin/go/e2e/runtime_context_driver_/runtime_context_driver job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./runtime_context_driver
package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/e2e/userfuncs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// requireUserfuncsPlugin fails fast when the plugin artifact is missing from
// the working directory: without the check, worker startup fails late inside
// raygo's plugin discovery with an opaque error.
func requireUserfuncsPlugin(path string) {
	if _, err := os.Stat(path); err != nil {
		fmt.Fprintf(os.Stderr, "userfuncs plugin not found at %s: build it with `bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin` and ship it in the job's working dir (%v)\n", path, err)
		os.Exit(1)
	}
}

// initRay initializes the Ray runtime with a JobConfig that carries the
// userfuncs.so code search path, so the RuntimeContextProbe actor created
// below can be resolved by the Go worker. The GCS address is read from the
// RAY_ADDRESS environment variable (set by `ray job submit`).
func initRay() error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %w", err)
	}
	codeSearchPath := filepath.Join(cwd, "userfuncs.so")
	requireUserfuncsPlugin(codeSearchPath)

	jobConfigBuilder := options.NewJobConfigBuilder().
		WithCodeSearchPath(codeSearchPath)

	if err := api.Instance().InitializeWithJobConfig(jobConfigBuilder); err != nil {
		return fmt.Errorf("ray runtime init failed: %w", err)
	}
	log.Printf("driver code search path: %s", codeSearchPath)
	return nil
}

// maxE2ETimeout caps the scaled e2e deadline so a huge
// RAY_GO_E2E_TIMEOUT_SCALE cannot overflow time.Duration into a negative
// (shorter) deadline.
const maxE2ETimeout = 24 * time.Hour

// e2eTimeout scales a base deadline by RAY_GO_E2E_TIMEOUT_SCALE (default 1.0,
// floor 1.0): shared clusters export a larger value (e.g. 3) instead of
// editing per-driver deadline constants when waits flake under load. The
// scaled result is capped at maxE2ETimeout so the helper only ever loosens a
// deadline.
func e2eTimeout(base time.Duration) time.Duration {
	s := os.Getenv("RAY_GO_E2E_TIMEOUT_SCALE")
	if s == "" {
		return base
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 1.0 {
		return base
	}
	scaled := float64(base) * f
	if scaled > float64(maxE2ETimeout) {
		return maxE2ETimeout
	}
	return time.Duration(scaled)
}

// mustNotNilID fails the run when an expected non-nil ID is nil.
func mustNotNilID(label string, id interface{ IsNil() bool }) error {
	if id == nil || id.IsNil() {
		return fmt.Errorf("%s is nil, want a concrete ID", label)
	}
	return nil
}

// listed reports whether the given actor ID appears in a listing of ActorInfo.
func listed(id ids.ActorID, infos []api.ActorInfo) bool {
	for _, a := range infos {
		if a.ActorID == id {
			return true
		}
	}
	return false
}

// testDriverContext verifies RuntimeContext from the driver (non-actor)
// process. Per Java semantics the driver is not an actor, so
// WasCurrentActorRestarted() is false and GetCurrentActorHandle() is nil,
// while cluster-wide queries (GetAllNodeInfo / GetAllActorInfo) resolve.
func testDriverContext() error {
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	if ctx == nil {
		return fmt.Errorf("GetRuntimeContext returned nil context")
	}

	// Basic identity fields. A driver belongs to a job and a node; the task
	// ID is empty on the driver's main goroutine (no task in flight), which
	// matches Java's RuntimeContext.getCurrentTaskId() semantics.
	if err := mustNotNilID("job ID", ctx.JobID()); err != nil {
		return err
	}
	if err := mustNotNilID("node ID", ctx.NodeID()); err != nil {
		return err
	}
	// The native runtime always reports cluster mode.
	if ctx.IsLocalMode() {
		return fmt.Errorf("IsLocalMode() = true in cluster driver, want false")
	}
	// A driver initialized without an explicit namespace may legitimately
	// report an empty namespace (the raylet assigns the default). Just log it.
	fmt.Printf("  JobID=%s TaskID=%s NodeID=%s Namespace=%q IsLocalMode=%v\n",
		ctx.JobID().Hex(), ctx.TaskID().Hex(), ctx.NodeID().Hex(), ctx.Namespace(), ctx.IsLocalMode())

	// Driver is not an actor: restart flag must be false and current-actor
	// handle must be nil.
	if ctx.WasCurrentActorRestarted() {
		return fmt.Errorf("WasCurrentActorRestarted() = true in driver, want false")
	}
	fmt.Println("  WasCurrentActorRestarted() = false ✓ (driver not an actor)")
	if h := ctx.GetCurrentActorHandle(); h != nil {
		return fmt.Errorf("GetCurrentActorHandle() = %v in driver, want nil", h)
	}
	fmt.Println("  GetCurrentActorHandle() = nil ✓ (driver not an actor)")

	// Cluster-wide node listing: at least the head node must be visible.
	nodes := ctx.GetAllNodeInfo()
	if len(nodes) < 1 {
		return fmt.Errorf("GetAllNodeInfo() = %d nodes, want >= 1", len(nodes))
	}
	alive := 0
	for _, n := range nodes {
		if n.IsAlive {
			alive++
		}
		if n.NodeID.IsNil() {
			return fmt.Errorf("GetAllNodeInfo() returned a node with nil NodeID")
		}
	}
	fmt.Printf("  GetAllNodeInfo() = %d node(s), %d alive ✓\n", len(nodes), alive)
	for i, n := range nodes {
		if i >= 3 {
			break
		}
		fmt.Printf("    node[%d] id=%s address=%s alive=%v\n", i, n.NodeID.Hex(), n.NodeAddress, n.IsAlive)
	}

	// Resource / GPU APIs. The driver process is not a resource
	// holder: the raylet assigns resources to task/actor workers, not to the
	// driver, so GetAvailableResourceIds() may legitimately be empty here.
	// The meaningful assertion is that the calls succeed and return consistent
	// data; the actor-context test (testActorContext) asserts CPU is present on
	// a worker that actually holds resources.
	resources, err := api.GetAvailableResourceIds()
	if err != nil {
		return fmt.Errorf("GetAvailableResourceIds failed: %w", err)
	}
	var names []string
	for name := range resources {
		names = append(names, name)
	}
	sort.Strings(names)
	fmt.Printf("  GetAvailableResourceIds() = %d resource(s): %v\n", len(resources), names)

	gpuIDs := ctx.GetGpuIds()
	fmt.Printf("  GetGpuIds() = %v\n", gpuIDs)

	fmt.Println("  GetGpuIds / GetAvailableResourceIds invoked successfully ✓")
	return nil
}

// testActorContext creates a userfuncs.RuntimeContextProbe actor and reads the
// RuntimeContext values observed inside the actor method, asserting the
// actor-context semantics: a concrete actor ID, a usable current-actor handle,
// and cluster-wide queries resolving from the worker.
func testActorContext() error {
	handle, err := api.Actor[*userfuncs.RuntimeContextProbe]((*userfuncs.RuntimeContextProbe)(nil)).Create()
	if err != nil {
		return fmt.Errorf("create RuntimeContextProbe actor failed: %w", err)
	}
	if handle == nil || handle.ID().IsNil() {
		return fmt.Errorf("expected a non-nil actor handle")
	}
	defer func() { _ = api.KillActor(handle, true) }()

	typed := api.NewActorHandleImpl[map[string]string](handle.ID())
	ref, err := typed.Task((*userfuncs.RuntimeContextProbe).Probe).Remote()
	if err != nil {
		return fmt.Errorf("RuntimeContextProbe.Probe() failed: %w", err)
	}
	probe, err := api.Get(ref)
	if err != nil {
		return fmt.Errorf("get probe result failed: %w", err)
	}

	// The actor context must expose the concrete actor ID.
	actorID := probe["actor_id"]
	if actorID == "" || actorID == ids.NilActorID().Hex() {
		return fmt.Errorf("probe actor_id = %q, want concrete actor ID", actorID)
	}
	if actorID != handle.ID().Hex() {
		return fmt.Errorf("probe actor_id = %s, want %s (created handle)", actorID, handle.ID().Hex())
	}
	fmt.Printf("  ActorID = %s ✓ (matches created handle)\n", actorID)

	// The current-actor handle must be resolvable inside the actor.
	if probe["actor_handle_present"] != "true" {
		return fmt.Errorf("probe actor_handle_present = %q, want \"true\"", probe["actor_handle_present"])
	}
	if probe["actor_handle_id"] != handle.ID().Hex() {
		return fmt.Errorf("probe actor_handle_id = %q, want %s", probe["actor_handle_id"], handle.ID().Hex())
	}
	fmt.Println("  GetCurrentActorHandle() non-nil with matching ID ✓")

	// A freshly created actor is not a restarted one.
	if probe["was_actor_restarted"] != "false" {
		return fmt.Errorf("probe was_actor_restarted = %q, want \"false\" for a fresh actor", probe["was_actor_restarted"])
	}
	fmt.Println("  WasCurrentActorRestarted() = false ✓ (fresh actor)")

	// Job and node identity must resolve inside the actor too.
	if probe["job_id"] == "" || probe["job_id"] == ids.NilJobID().Hex() {
		return fmt.Errorf("probe job_id = %q, want concrete job ID", probe["job_id"])
	}
	fmt.Printf("  JobID = %s ✓\n", probe["job_id"])

	if probe["node_id"] == "" || probe["node_id"] == ids.NilNodeID().Hex() {
		return fmt.Errorf("probe node_id = %q, want concrete node ID", probe["node_id"])
	}
	fmt.Printf("  NodeID = %s ✓\n", probe["node_id"])

	// Cluster-wide queries resolve from the actor worker.
	nodeCount, err := strconv.Atoi(probe["node_count"])
	if err != nil || nodeCount < 1 {
		return fmt.Errorf("probe node_count = %q, want >= 1", probe["node_count"])
	}
	fmt.Printf("  GetAllNodeInfo() from actor = %d node(s) ✓\n", nodeCount)

	actorCount, err := strconv.Atoi(probe["actor_count"])
	if err != nil || actorCount < 1 {
		return fmt.Errorf("probe actor_count = %q, want >= 1 (at least the probe actor)", probe["actor_count"])
	}
	fmt.Printf("  GetAllActorInfo() from actor = %d actor(s) ✓\n", actorCount)

	// GPU / resource APIs resolve from the actor worker.
	if probe["gpu_ids"] == "" && probe["resource_names"] == "" {
		// No GPU and no resource info is a suspicious outcome; CPU must be
		// listed even on a GPU-less machine.
		return fmt.Errorf("probe resource_names = %q, want at least CPU", probe["resource_names"])
	}
	fmt.Printf("  gpu_ids = [%s], resources = [%s] ✓\n", probe["gpu_ids"], probe["resource_names"])
	if !strings.Contains(probe["resource_names"], "CPU") {
		return fmt.Errorf("probe resource_names = %q, missing CPU", probe["resource_names"])
	}

	fmt.Println("  Actor-context RuntimeContext verified ✓")
	return nil
}

// testActorInfoListing creates a named Counter actor and verifies the actor
// appears in GetAllActorInfo (by job filter and by alive state), exercising
// GetAllActorInfoByFilter, the Java-parity filtered overload.
func testActorInfoListing() error {
	counter, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(0)
	if err != nil {
		return fmt.Errorf("create Counter actor failed: %w", err)
	}
	defer func() { _ = api.KillActor(counter, true) }()
	counterID := counter.ID()

	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}

	// Actor registration in GCS is asynchronous: the core worker publishes the
	// actor record shortly after creation. GO workers are started on demand
	// (RAY_ENABLE_GO_SETUP_WORKER=1), so the actor's constructor may take
	// ~200ms+ to run after its record appears. Poll the alive-filtered query
	// (not just the unfiltered list) so the listing below observes an actor
	// that is both registered AND alive, mirroring how a real client waits for
	// its actor to become available before querying cluster state.
	deadline := time.Now().Add(e2eTimeout(10 * time.Second))
	for {
		if listed(counterID, ctx.GetAllActorInfoByFilter(ctx.JobID(), api.ActorStateAlive)) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("Counter actor %s never appeared ALIVE in GetAllActorInfoByFilter within 10s", counterID.Hex())
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("  GetAllActorInfo() lists created Counter actor %s ✓\n", counterID.Hex())

	// Filtered by the current job: the Counter actor belongs to this job.
	byJob := ctx.GetAllActorInfoByFilter(ctx.JobID(), api.ActorStateAny)
	if !listed(counterID, byJob) {
		return fmt.Errorf("GetAllActorInfoByFilter(jobID, Any) missing Counter actor %s", counterID.Hex())
	}
	fmt.Printf("  GetAllActorInfoByFilter(job, Any) lists Counter actor ✓ (%d actors)\n", len(byJob))

	// Filtered by alive state: a live actor must be listed as alive.
	aliveActors := ctx.GetAllActorInfoByFilter(ctx.JobID(), api.ActorStateAlive)
	if !listed(counterID, aliveActors) {
		return fmt.Errorf("GetAllActorInfoByFilter(job, Alive) missing live Counter actor %s", counterID.Hex())
	}
	fmt.Printf("  GetAllActorInfoByFilter(job, Alive) lists live Counter actor ✓\n")

	// Filtering by a dead state must exclude the live actor.
	deadActors := ctx.GetAllActorInfoByFilter(ctx.JobID(), api.ActorStateDead)
	if listed(counterID, deadActors) {
		return fmt.Errorf("GetAllActorInfoByFilter(job, Dead) included live Counter actor %s", counterID.Hex())
	}
	fmt.Println("  GetAllActorInfoByFilter(job, Dead) excludes live actor ✓")

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := initRay(); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestDriverContext ===")
	if err := testDriverContext(); err != nil {
		log.Fatalf("TestDriverContext FAILED: %v", err)
	}
	fmt.Println("=== TestDriverContext PASSED ===\n")

	fmt.Println("=== TestActorContext ===")
	if err := testActorContext(); err != nil {
		log.Fatalf("TestActorContext FAILED: %v", err)
	}
	fmt.Println("=== TestActorContext PASSED ===\n")

	fmt.Println("=== TestActorInfoListing ===")
	if err := testActorInfoListing(); err != nil {
		log.Fatalf("TestActorInfoListing FAILED: %v", err)
	}
	fmt.Println("=== TestActorInfoListing PASSED ===\n")

	fmt.Println("All RuntimeContext / resource-ID checks passed.")
}
