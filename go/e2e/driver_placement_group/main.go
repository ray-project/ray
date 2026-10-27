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

// driver_placement_group is a Ray Go driver that verifies the Placement Group
// user API end to end on a real cluster (and in local mode). It exercises the
// surfaces where the Go API previously trailed Java and C++:
//
//	Placement Group user API: was completely missing (only a placeholder).
//	Placement Group strategy: was not exposed.
//	Task-level scheduling strategy: was merged into the placement group
//	    strategy (PACK/SPREAD/STRICT_PACK/STRICT_SPREAD).
//
// Cluster mode (--run-mode=driver, ray job submit) covers:
//   - PG lifecycle: Create -> Wait -> Get(id) / GetByName(name, ns) /
//     GetAll -> Remove -> Get(id) returns (nil, nil)
//   - PG task placement: Remote(...).WithPlacementGroup(pg, bundleIndex) runs
//     on a cluster node (call.go taskOptions[T] main path)
//   - PG actor placement: Actor(...).WithPlacementGroup(pg, bundleIndex) actor
//     method calls succeed (call.go actorOptions[T] main path)
//   - Validation: invalid strategy / empty name / nil options / empty bundles
//     are rejected before reaching the submitter
//
// Local mode (--run-mode=local) covers:
//   - create / remove / wait succeed via the in-process map simulation
//   - get / getByName / getAll resolve through the in-process store; an
//     unknown group is a not-found ((nil, nil) / (empty, nil)) rather than an
//     error, because the GCS client factory is not registered and the facade
//     falls back to the submitter's local store
//
// Build & submit as a Ray job:
//
//	bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
//	bazel build //go/e2e/driver_placement_group
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
//	cp bazel-bin/go/e2e/driver_placement_group_/driver_placement_group job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./driver_placement_group
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ray-project/ray/go/e2e/userfuncs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/local"
)

var runModeFlag = flag.String("run-mode", "driver", "Run mode: driver or local")

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
// userfuncs.so code search path, so PG-bound remote tasks and actors can be
// resolved by the Go worker. The GCS address is read from the RAY_ADDRESS
// environment variable (set by `ray job submit`).
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

// newPGName builds a unique placement group name for a test.
func newPGName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// pgListContainsID reports whether a placement group with the given hex id
// appears in the list.
func pgListContainsID(list []*api.PlacementGroup, idHex string) bool {
	for _, pg := range list {
		if pg != nil && pg.ID().Hex() == idHex {
			return true
		}
	}
	return false
}

// clusterNodeQuery returns the set of hex node ids currently in the cluster,
// keyed by node id.
type clusterNodeQuery func(context.Context) (map[string]bool, error)

// assertNodeInCluster verifies that nodeID is a current cluster node,
// re-querying once after retryDelay to tolerate autoscaler/GCS registration
// lag. Unlike a snapshot taken once after PG creation, querying live tolerates
// nodes that join or leave while the test runs: on an autoscaling cluster a PG
// bundle may be rescheduled onto a newly added node, so a PG-bound task/actor
// that follows the bundle lands on a node absent from any earlier snapshot.
func assertNodeInCluster(ctx context.Context, nodeID string, query clusterNodeQuery, retryDelay time.Duration) error {
	has := func() (bool, error) {
		nodes, err := query(ctx)
		if err != nil {
			return false, err
		}
		if len(nodes) == 0 {
			return false, fmt.Errorf("cluster reports no nodes")
		}
		return nodes[nodeID], nil
	}
	ok, err := has()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if retryDelay > 0 {
		time.Sleep(retryDelay)
		ok, err = has()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("node %q is not a current cluster node", nodeID)
}

// testPGLifecycle verifies the full placement group lifecycle in cluster mode:
// create returns a PlacementGroup whose accessors
// match the creation options, Wait reports ready, the group is discoverable by
// id, by name (empty namespace falls back to the current namespace, and an
// explicit namespace also works), and by listing; after Remove the by-id query
// returns (nil, nil).
func testPGLifecycle(ctx context.Context) error {
	name := newPGName("pg-lifecycle")
	opts, err := api.NewPlacementGroupCreationOptionsBuilder().
		WithName(name).
		WithBundles([]map[string]float64{{"CPU": 1}}).
		WithStrategy(api.PlacementStrategyPack).
		Build()
	if err != nil {
		return fmt.Errorf("Build PG options failed: %w", err)
	}

	// Create.
	pg, err := api.CreatePlacementGroup(ctx, opts)
	if err != nil {
		return fmt.Errorf("CreatePlacementGroup failed: %w", err)
	}
	if pg == nil {
		return fmt.Errorf("CreatePlacementGroup returned nil")
	}
	if pg.ID().IsNil() {
		return fmt.Errorf("created placement group has a nil id")
	}
	defer func() { _ = api.RemovePlacementGroup(ctx, pg.ID()) }()
	if pg.Name() != name {
		return fmt.Errorf("pg.Name() = %q, want %q", pg.Name(), name)
	}
	if len(pg.Bundles()) != 1 || pg.Bundles()[0]["CPU"] != 1 {
		return fmt.Errorf("pg.Bundles() = %v, want [{CPU:1}]", pg.Bundles())
	}
	if pg.Strategy() != api.PlacementStrategyPack {
		return fmt.Errorf("pg.Strategy() = %v, want PACK", pg.Strategy())
	}
	fmt.Printf("  created %s (id=%s) ✓\n", pg.Name(), pg.ID().Hex())

	// Wait until ready.
	ready, err := pg.Wait(60)
	if err != nil {
		return fmt.Errorf("pg.Wait failed: %w", err)
	}
	if !ready {
		return fmt.Errorf("pg.Wait returned not-ready within 60s")
	}
	fmt.Println("  pg.Wait(60) -> ready ✓")

	// Query by id.
	got, err := api.GetPlacementGroup(ctx, pg.ID())
	if err != nil {
		return fmt.Errorf("GetPlacementGroup(id) failed: %w", err)
	}
	if got == nil {
		return fmt.Errorf("GetPlacementGroup(id) returned nil for a live PG")
	}
	if !got.ID().Equal(pg.ID()) || got.Name() != name {
		return fmt.Errorf("GetPlacementGroup(id) = {%s %s}, want {%s %s}",
			got.ID().Hex(), got.Name(), pg.ID().Hex(), name)
	}
	fmt.Printf("  GetPlacementGroup(id) = %s ✓\n", got.ID().Hex())

	// Query by name with an empty namespace (falls back to current).
	rc, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	byName, err := api.GetPlacementGroupByName(ctx, name, "")
	if err != nil {
		return fmt.Errorf("GetPlacementGroupByName(name, \"\") failed: %w", err)
	}
	if byName == nil || !byName.ID().Equal(pg.ID()) {
		return fmt.Errorf("GetPlacementGroupByName(name, \"\") = %+v, want id %s", byName, pg.ID().Hex())
	}
	fmt.Println("  GetPlacementGroupByName(name, \"\") matches ✓")

	// Query by name with the explicit current namespace.
	rc, err = api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	byNameNS, err := api.GetPlacementGroupByName(ctx, name, rc.Namespace())
	if err != nil {
		return fmt.Errorf("GetPlacementGroupByName(name, ns) failed: %w", err)
	}
	if byNameNS == nil || !byNameNS.ID().Equal(pg.ID()) {
		return fmt.Errorf("GetPlacementGroupByName(name, ns=%q) = %+v, want id %s",
			rc.Namespace(), byNameNS, pg.ID().Hex())
	}
	fmt.Printf("  GetPlacementGroupByName(name, current namespace %q) matches ✓\n", rc.Namespace())

	// Listing contains the group.
	all, err := api.GetAllPlacementGroups(ctx)
	if err != nil {
		return fmt.Errorf("GetAllPlacementGroups failed: %w", err)
	}
	if !pgListContainsID(all, pg.ID().Hex()) {
		return fmt.Errorf("GetAllPlacementGroups() missing %s (got %d groups)", pg.ID().Hex(), len(all))
	}
	fmt.Printf("  GetAllPlacementGroups() contains %s ✓\n", pg.ID().Hex())

	// Remove, then the PG must transition to the Removed state. GCS keeps
	// the table entry with state=REMOVED (state diagram: Created -> Removed),
	// so poll until the by-id query reports Removed (or the entry disappears).
	if err := api.RemovePlacementGroup(ctx, pg.ID()); err != nil {
		return fmt.Errorf("RemovePlacementGroup failed: %w", err)
	}
	fmt.Println("  RemovePlacementGroup -> nil ✓")
	deadline := time.Now().Add(e2eTimeout(15 * time.Second))
	for {
		gone, err := api.GetPlacementGroup(ctx, pg.ID())
		if err != nil {
			return fmt.Errorf("GetPlacementGroup after remove failed: %w", err)
		}
		if gone == nil || gone.State() == api.PlacementGroupStateRemoved {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("placement group %s not Removed 15s after Remove (state=%v)",
				pg.ID().Hex(), gone.State())
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("  GetPlacementGroup after Remove -> Removed/nil ✓")

	return nil
}

// testPGBinding verifies that tasks and actors bound to a placement group
// bundle through WithPlacementGroup actually run on a cluster node, and that a
// task bound to a removed placement group fails (proving the binding is
// transmitted to and enforced by the scheduler). A placement group with two
// CPU bundles is used so both bundle index 0 and bundle index 1 are exercised.
func testPGBinding(ctx context.Context) error {
	name := newPGName("pg-bind")
	pg, err := api.CreatePlacementGroup(ctx, &api.PlacementGroupCreationOptions{
		Name:     name,
		Bundles:  []map[string]float64{{"CPU": 1}, {"CPU": 1}},
		Strategy: api.PlacementStrategyPack,
	})
	if err != nil {
		return fmt.Errorf("CreatePlacementGroup (binding) failed: %w", err)
	}
	defer func() { _ = api.RemovePlacementGroup(ctx, pg.ID()) }()
	ready, err := pg.Wait(60)
	if err != nil {
		return fmt.Errorf("pg.Wait (binding) failed: %w", err)
	}
	if !ready {
		return fmt.Errorf("binding PG not ready within 60s (cluster needs >=2 CPUs for 2 bundles)")
	}
	fmt.Printf("  created binding PG %s (2 bundles) and it is ready ✓\n", pg.ID().Hex())

	// Build a live cluster-node query. Assertions below query it (with one
	// retry) instead of relying on a snapshot, so a PG-bound task/actor that
	// follows a bundle rescheduled onto a newly added node (autoscaling) is
	// not misreported as running on an unknown node.
	query := func(ctx context.Context) (map[string]bool, error) {
		rc, err := api.GetRuntimeContext()
		if err != nil {
			return nil, fmt.Errorf("GetRuntimeContext failed: %w", err)
		}
		nodes := rc.GetAllNodeInfo()
		set := make(map[string]bool, len(nodes))
		for _, n := range nodes {
			set[n.NodeID.Hex()] = true
		}
		return set, nil
	}
	if initial, err := query(ctx); err != nil {
		return err
	} else {
		fmt.Printf("  cluster nodes: %d\n", len(initial))
	}

	// Task bound to bundle 0 (taskOptions[T].WithPlacementGroup main path).
	t0, err := api.Remote[string](userfuncs.GetReportNodeID()).
		WithPlacementGroup(pg, 0).
		Call()
	if err != nil {
		return fmt.Errorf("PG-bound task (bundle 0) submission failed: %w", err)
	}
	node0, err := t0.Get()
	if err != nil {
		return fmt.Errorf("PG-bound task (bundle 0) execution failed: %w", err)
	}
	if node0 == "" {
		return fmt.Errorf("PG-bound task (bundle 0) reported an empty node")
	}
	if err := assertNodeInCluster(ctx, node0, query, 2*time.Second); err != nil {
		return fmt.Errorf("PG-bound task (bundle 0): %w", err)
	}
	fmt.Printf("  PG-bound task (bundle 0) ran on node %s ✓\n", node0)

	// Task bound to bundle 1 (exercises a non-zero bundle index).
	t1, err := api.Remote[string](userfuncs.GetReportNodeID()).
		WithPlacementGroup(pg, 1).
		Call()
	if err != nil {
		return fmt.Errorf("PG-bound task (bundle 1) submission failed: %w", err)
	}
	node1, err := t1.Get()
	if err != nil {
		return fmt.Errorf("PG-bound task (bundle 1) execution failed: %w", err)
	}
	if node1 == "" {
		return fmt.Errorf("PG-bound task (bundle 1) reported an empty node")
	}
	if err := assertNodeInCluster(ctx, node1, query, 2*time.Second); err != nil {
		return fmt.Errorf("PG-bound task (bundle 1): %w", err)
	}
	fmt.Printf("  PG-bound task (bundle 1) ran on node %s ✓\n", node1)

	// Actor bound to bundle 0 (actorOptions[T].WithPlacementGroup main path).
	probe, err := api.Actor[*userfuncs.RuntimeContextProbe]((*userfuncs.RuntimeContextProbe)(nil)).
		WithPlacementGroup(pg, 0).
		Create()
	if err != nil {
		return fmt.Errorf("PG-bound actor creation failed: %w", err)
	}
	if probe == nil || probe.ID().IsNil() {
		return fmt.Errorf("PG-bound actor returned a nil handle")
	}
	defer func() { _ = api.KillActor(probe, true, true) }()
	typed := api.NewActorHandleImpl[map[string]string](probe.ID())
	ref, err := typed.Task((*userfuncs.RuntimeContextProbe).Probe).Remote()
	if err != nil {
		return fmt.Errorf("PG-bound actor Probe submission failed: %w", err)
	}
	info, err := ref.Get()
	if err != nil {
		return fmt.Errorf("PG-bound actor Probe execution failed: %w", err)
	}
	actorNode, ok := info["node_id"]
	if !ok || actorNode == "" {
		return fmt.Errorf("PG-bound actor reported no node (info=%v)", info)
	}
	if err := assertNodeInCluster(ctx, actorNode, query, 2*time.Second); err != nil {
		return fmt.Errorf("PG-bound actor: %w (info=%v)", err, info)
	}
	fmt.Printf("  PG-bound actor ran on node %s ✓\n", actorNode)

	return nil
}

// testPGValidation verifies that invalid creation options are rejected before
// reaching the submitter (aligned with Java PlacementGroupCreationOptions
// construction validation): the builder Build() validates name/bundles, and
// the CreatePlacementGroup facade validates name/bundles/strategy/nil via
// submitter.PlacementGroupCreationOptions.Validate (the single source of
// truth).
func testPGValidation(ctx context.Context) error {
	// Invalid strategy: Build() does not validate the strategy (only
	// name/bundles), so it is rejected by the facade's Validate.
	badStrategy, err := api.NewPlacementGroupCreationOptionsBuilder().
		WithName(newPGName("pg-bad-strategy")).
		WithBundles([]map[string]float64{{"CPU": 1}}).
		WithStrategy(api.PlacementStrategy(99)).
		Build()
	if err != nil {
		return fmt.Errorf("Build with invalid strategy should succeed, got %v", err)
	}
	if _, err := api.CreatePlacementGroup(ctx, badStrategy); err == nil {
		return fmt.Errorf("CreatePlacementGroup with invalid strategy should error")
	}
	fmt.Println("  invalid strategy rejected ✓")

	// Empty name: the name is optional (anonymous placement groups are legal),
	// so Build() succeeds and the facade creates the group. The created group is
	// removed immediately so the cluster is left clean.
	anon, err := api.NewPlacementGroupCreationOptionsBuilder().
		WithBundles([]map[string]float64{{"CPU": 1}}).
		Build()
	if err != nil {
		return fmt.Errorf("Build with empty name should succeed (anonymous PG), got %v", err)
	}
	fmt.Println("  Build with empty name succeeds (anonymous PG) ✓")
	anonPG, err := api.CreatePlacementGroup(ctx, anon)
	if err != nil {
		return fmt.Errorf("CreatePlacementGroup with empty name should succeed, got %v", err)
	}
	if anonPG == nil || anonPG.ID().IsNil() {
		return fmt.Errorf("CreatePlacementGroup with empty name returned a nil id")
	}
	if err := api.RemovePlacementGroup(ctx, anonPG.ID()); err != nil {
		return fmt.Errorf("RemovePlacementGroup (anonymous PG cleanup) failed: %w", err)
	}
	fmt.Println("  CreatePlacementGroup with empty name succeeds ✓")

	// Empty bundles: Build() rejects it, and the facade rejects a directly-built
	// options struct too.
	if _, err := api.NewPlacementGroupCreationOptionsBuilder().
		WithName(newPGName("pg-no-bundles")).
		Build(); err == nil {
		return fmt.Errorf("Build with empty bundles should error")
	}
	fmt.Println("  Build with empty bundles rejected ✓")
	if _, err := api.CreatePlacementGroup(ctx, &api.PlacementGroupCreationOptions{
		Name:     newPGName("pg-no-bundles"),
		Strategy: api.PlacementStrategyPack,
	}); err == nil {
		return fmt.Errorf("CreatePlacementGroup with empty bundles should error")
	}
	fmt.Println("  CreatePlacementGroup with empty bundles rejected ✓")

	// Nil options.
	if _, err := api.CreatePlacementGroup(ctx, nil); err == nil {
		return fmt.Errorf("CreatePlacementGroup with nil options should error")
	}
	fmt.Println("  nil options rejected ✓")

	return nil
}

// testLocalPGChange verifies the local-mode placement group change simulation:
// create generates an in-process id, wait succeeds immediately, and remove
// deletes the group.
func testLocalPGChange() error {
	pg, err := api.CreatePlacementGroup(context.Background(), &api.PlacementGroupCreationOptions{
		Name:     newPGName("local-pg"),
		Bundles:  []map[string]float64{{"CPU": 1}},
		Strategy: api.PlacementStrategyPack,
	})
	if err != nil {
		return fmt.Errorf("local CreatePlacementGroup failed: %w", err)
	}
	if pg == nil || pg.ID().IsNil() {
		return fmt.Errorf("local CreatePlacementGroup returned a nil id")
	}
	defer func() { _ = api.RemovePlacementGroup(context.Background(), pg.ID()) }()
	fmt.Printf("  local create -> id %s ✓\n", pg.ID().Hex())

	ready, err := pg.Wait(1)
	if err != nil {
		return fmt.Errorf("local pg.Wait failed: %w", err)
	}
	if !ready {
		return fmt.Errorf("local pg.Wait should be immediately ready")
	}
	fmt.Println("  local pg.Wait -> (true, nil) ✓")

	if err := api.RemovePlacementGroup(context.Background(), pg.ID()); err != nil {
		return fmt.Errorf("local RemovePlacementGroup failed: %w", err)
	}
	fmt.Println("  local remove -> nil ✓")
	return nil
}

// testLocalPGQueryMissing verifies that placement group queries in local mode
// resolve through the in-process store: an unknown group is reported as not
// found ((nil, nil) / (empty, nil)) rather than an error, because local mode
// keeps its placement groups in a submitter-side map (the GCS client factory
// is not registered, so the facade falls back to the local store).
func testLocalPGQueryMissing() error {
	ctx := context.Background()
	bogusID := ids.NilPlacementGroupID()

	pg, err := api.GetPlacementGroup(ctx, bogusID)
	if err != nil {
		return fmt.Errorf("local GetPlacementGroup should be a not-found (nil, nil), got error: %v", err)
	}
	if pg != nil {
		return fmt.Errorf("local GetPlacementGroup(bogus) = %+v, want nil", pg)
	}
	fmt.Println("  local GetPlacementGroup(bogus) -> (nil, nil) ✓")

	byName, err := api.GetPlacementGroupByName(ctx, "pg", "")
	if err != nil {
		return fmt.Errorf("local GetPlacementGroupByName should be a not-found (nil, nil), got error: %v", err)
	}
	if byName != nil {
		return fmt.Errorf("local GetPlacementGroupByName(bogus) = %+v, want nil", byName)
	}
	fmt.Println("  local GetPlacementGroupByName(bogus) -> (nil, nil) ✓")

	all, err := api.GetAllPlacementGroups(ctx)
	if err != nil {
		return fmt.Errorf("local GetAllPlacementGroups should return the local list, got error: %v", err)
	}
	if len(all) != 0 {
		return fmt.Errorf("local GetAllPlacementGroups after cleanup = %+v, want empty list", all)
	}
	fmt.Println("  local GetAllPlacementGroups -> (empty, nil) ✓")
	return nil
}

func runDriver() {
	if err := initRay(); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	ctx := context.Background()

	fmt.Println("=== TestPGLifecycle ===")
	if err := testPGLifecycle(ctx); err != nil {
		log.Fatalf("TestPGLifecycle FAILED: %v", err)
	}
	fmt.Println("=== TestPGLifecycle PASSED ===\n")

	fmt.Println("=== TestPGBinding ===")
	if err := testPGBinding(ctx); err != nil {
		log.Fatalf("TestPGBinding FAILED: %v", err)
	}
	fmt.Println("=== TestPGBinding PASSED ===\n")

	fmt.Println("=== TestPGValidation ===")
	if err := testPGValidation(ctx); err != nil {
		log.Fatalf("TestPGValidation FAILED: %v", err)
	}
	fmt.Println("=== TestPGValidation PASSED ===\n")

	fmt.Println("All placement group cluster checks passed.")
}

func runLocal() {
	local.Enable()
	if err := api.InitLocal(); err != nil {
		log.Fatalf("local init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestLocalPGChange ===")
	if err := testLocalPGChange(); err != nil {
		log.Fatalf("TestLocalPGChange FAILED: %v", err)
	}
	fmt.Println("=== TestLocalPGChange PASSED ===\n")

	fmt.Println("=== TestLocalPGQueryMissing ===")
	if err := testLocalPGQueryMissing(); err != nil {
		log.Fatalf("TestLocalPGQueryMissing FAILED: %v", err)
	}
	fmt.Println("=== TestLocalPGQueryMissing PASSED ===\n")

	fmt.Println("All placement group local-mode checks passed.")
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	switch *runModeFlag {
	case "driver":
		runDriver()
	case "local":
		runLocal()
	default:
		log.Fatalf("unknown run-mode %q (want driver or local)", *runModeFlag)
	}
}
