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

// driver_gcs_client is a Ray Go driver that verifies the user-facing GCSClient
// interface extension end to end on a real cluster (and the local-mode
// not-supported behavior). It exercises the surfaces where the Go API
// previously trailed Java and C++:
//
//	GCSClient interface extension: the user-facing GCSClient interface was
//	    too narrow (only 4 methods) and had no user entry point. This driver
//	    exercises the 6 added query methods (GetPlacementGroupInfo /
//	    GetPlacementGroupInfoByName / GetAllPlacementGroupInfo /
//	    GetInternalKV / GetAllNodeInfo / GetAllActorInfo) through
//	    api.GetGCSClient(), which reuses the gcsClientCache.
//	Placement Group name-based query: Placement Group had no name-based query
//	    in the GCS client layer. This driver asserts
//	    GetPlacementGroupInfoByName resolves a PG created through the
//	    submitter path.
//
// Cluster mode (--run-mode=driver, ray job submit) covers:
//   - api.GetGCSClient() resolves to a working client
//   - GetPlacementGroupInfo(id) / GetPlacementGroupInfoByName(name, ns) /
//     GetAllPlacementGroupInfo() against a live PG, including (nil, nil) after
//     removal
//   - GetInternalKV round trip: put via the internal gcs client, read back via
//     the api GCS client
//   - GetAllNodeInfo() returns the cluster nodes
//   - GetAllActorInfo() returns created actors, including an ALIVE state
//     filter
//
// Local mode (--run-mode=local) covers the not-supported behavior: the GCS
// client factory is not registered in local mode, so GetGCSClient() returns
// ErrGCSClientFactoryNotRegistered.
//
// Build & submit as a Ray job:
//
//	bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
//	bazel build //go/e2e/driver_gcs_client
//	mkdir -p job_dir
//	cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
//	cp bazel-bin/go/e2e/driver_gcs_client_/driver_gcs_client job_dir/
//	cd job_dir && ray job submit --working-dir . -- ./driver_gcs_client
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ray-project/ray/go/e2e/userfuncs"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/pkg/runtime/local"
	"github.com/ray-project/ray/go/proto"
)

var runModeFlag = flag.String("run-mode", "driver", "Run mode: driver or local")

// initRay initializes the Ray runtime with a JobConfig that carries the
// userfuncs.so code search path. The GCS address is read from the RAY_ADDRESS
// environment variable (set by `ray job submit`).
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

// newPGName builds a unique placement group name for a test.
func newPGName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// getAPIGCSClient returns the api GCS client, failing the test if the user
// entry point does not resolve.
func getAPIGCSClient() (api.GCSClient, error) {
	client, err := api.GetGCSClient()
	if err != nil {
		return nil, fmt.Errorf("api.GetGCSClient() failed: %w", err)
	}
	if client == nil {
		return nil, fmt.Errorf("api.GetGCSClient() returned nil")
	}
	return client, nil
}

// internalClient returns the internal gcs.Client singleton, which
// api.GetGCSClient() registers as a side effect (ConnectClient -> gcs.SetClient).
func internalClient() (gcs.Client, error) {
	client, err := gcs.GetClient()
	if err != nil {
		return nil, fmt.Errorf("gcs.GetClient() failed (internal singleton not registered): %w", err)
	}
	if client == nil {
		return nil, fmt.Errorf("gcs.GetClient() returned nil")
	}
	return client, nil
}

// waitPGGone polls until the placement group is no longer usable in GCS after
// removal: the by-id query returns nil or a REMOVED-state entry (GCS keeps the
// table entry with state=REMOVED; state diagram Created -> Removed).
func waitPGGone(ctx context.Context, client api.GCSClient, id ids.PlacementGroupID) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := client.GetPlacementGroupInfo(ctx, id)
		if err != nil {
			return fmt.Errorf("GetPlacementGroupInfo failed: %w", err)
		}
		if got == nil || got.State == proto.PlacementGroupTableData_REMOVED {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("placement group %s not Removed 15s after Remove (state=%v)",
				id.Hex(), got.State)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// testPGQueries verifies the three placement group query methods on the api
// GCSClient: by id, by name, and the full listing, against a PG created through
// the submitter path, including (nil, nil) after removal.
func testPGQueries(ctx context.Context) error {
	client, err := getAPIGCSClient()
	if err != nil {
		return err
	}

	name := newPGName("gcs-pg")
	pg, err := api.CreatePlacementGroup(ctx, &api.PlacementGroupCreationOptions{
		Name:     name,
		Bundles:  []map[string]float64{{"CPU": 1}},
		Strategy: api.PlacementStrategyStrictSpread,
	})
	if err != nil {
		return fmt.Errorf("CreatePlacementGroup failed: %w", err)
	}
	if ready, err := pg.Wait(60); err != nil || !ready {
		return fmt.Errorf("pg.Wait failed (err=%v, ready=%v)", err, ready)
	}
	fmt.Printf("  created PG %s (name=%q) ✓\n", pg.ID().Hex(), name)

	// GetPlacementGroupInfo by id.
	got, err := client.GetPlacementGroupInfo(ctx, pg.ID())
	if err != nil {
		return fmt.Errorf("GetPlacementGroupInfo(id) failed: %w", err)
	}
	if got == nil {
		return fmt.Errorf("GetPlacementGroupInfo(id) returned nil for a live PG")
	}
	if got.Name != name {
		return fmt.Errorf("GetPlacementGroupInfo(id).Name = %q, want %q", got.Name, name)
	}
	if got.Strategy != proto.PlacementStrategy_STRICT_SPREAD {
		return fmt.Errorf("GetPlacementGroupInfo(id).Strategy = %v, want STRICT_SPREAD", got.Strategy)
	}
	fmt.Printf("  GetPlacementGroupInfo(id) -> %x (%s) ✓\n", got.GetPlacementGroupId(), got.Name)

	// GetPlacementGroupInfoByName with the current namespace.
	rc, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	byName, err := client.GetPlacementGroupInfoByName(ctx, name, rc.Namespace())
	if err != nil {
		return fmt.Errorf("GetPlacementGroupInfoByName(name, ns) failed: %w", err)
	}
	if byName == nil || string(byName.GetPlacementGroupId()) != string(pg.ID().Binary()) {
		return fmt.Errorf("GetPlacementGroupInfoByName(name, ns) = %+v, want id %s", byName, pg.ID().Hex())
	}
	fmt.Println("  GetPlacementGroupInfoByName(name, current namespace) matches ✓")

	// GetAllPlacementGroupInfo contains the group.
	all, err := client.GetAllPlacementGroupInfo(ctx)
	if err != nil {
		return fmt.Errorf("GetAllPlacementGroupInfo failed: %w", err)
	}
	found := false
	for _, d := range all {
		if d != nil && string(d.GetPlacementGroupId()) == string(pg.ID().Binary()) {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("GetAllPlacementGroupInfo() missing %s (got %d groups)", pg.ID().Hex(), len(all))
	}
	fmt.Printf("  GetAllPlacementGroupInfo() contains %s ✓\n", pg.ID().Hex())

	// After Remove, the by-id query must observe the removal: a nil entry or a
	// REMOVED-state entry (Java GcsClient.getPlacementGroupInfo semantics after
	// removal).
	if err := api.RemovePlacementGroup(ctx, pg.ID()); err != nil {
		return fmt.Errorf("RemovePlacementGroup failed: %w", err)
	}
	if err := waitPGGone(ctx, client, pg.ID()); err != nil {
		return err
	}
	gone, err := client.GetPlacementGroupInfo(ctx, pg.ID())
	if err != nil {
		return fmt.Errorf("GetPlacementGroupInfo after remove failed: %w", err)
	}
	if gone != nil && gone.State != proto.PlacementGroupTableData_REMOVED {
		return fmt.Errorf("GetPlacementGroupInfo after remove = state %v, want REMOVED or nil", gone.State)
	}
	fmt.Println("  GetPlacementGroupInfo after Remove -> REMOVED/nil ✓")

	return nil
}

// testInternalKV verifies the GetInternalKV method: the value written
// through the internal gcs client is read back through the api GCS client.
func testInternalKV(ctx context.Context) error {
	apiClient, err := getAPIGCSClient()
	if err != nil {
		return err
	}
	internal, err := internalClient()
	if err != nil {
		return err
	}

	ns := api.DefaultInternalKVNamespace
	key := fmt.Sprintf("gcs_e2e_%d", time.Now().UnixNano())
	value := []byte("gcs-client-e2e-value")

	// Clean up any pre-existing key (idempotent overwrite).
	if _, err := internal.Put(ctx, ns, key, value, true); err != nil {
		return fmt.Errorf("internal Put failed: %w", err)
	}
	got, err := apiClient.GetInternalKV(ctx, ns, key)
	if err != nil {
		return fmt.Errorf("GetInternalKV failed: %w", err)
	}
	if string(got) != string(value) {
		return fmt.Errorf("GetInternalKV(%q) = %q, want %q", key, got, value)
	}
	fmt.Printf("  GetInternalKV(%s) round trip matches ✓\n", key)

	// GetInternalKV on a missing key must not panic (Java GcsClient returns
	// null; Go returns a nil slice or an error depending on the bridge).
	if _, err := apiClient.GetInternalKV(ctx, ns, key+"-missing"); err == nil {
		// Accept both: nil value (treated as missing) or a key-not-found error.
		fmt.Println("  GetInternalKV(missing) returned without error (nil value) ✓")
	} else {
		fmt.Printf("  GetInternalKV(missing) returned error: %v ✓\n", err)
	}

	if _, err := internal.Del(ctx, ns, key, false); err != nil {
		return fmt.Errorf("internal Del cleanup failed: %w", err)
	}
	return nil
}

// testNodeInfo verifies GetAllNodeInfo returns the cluster nodes.
func testNodeInfo(ctx context.Context) error {
	client, err := getAPIGCSClient()
	if err != nil {
		return err
	}
	nodes, err := client.GetAllNodeInfo(ctx)
	if err != nil {
		return fmt.Errorf("GetAllNodeInfo failed: %w", err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("GetAllNodeInfo returned no nodes")
	}
	for id, n := range nodes {
		if n == nil {
			return fmt.Errorf("GetAllNodeInfo returned a nil node for %s", id.Hex())
		}
	}
	fmt.Printf("  GetAllNodeInfo() returned %d nodes ✓\n", len(nodes))
	return nil
}

// actorListed reports whether an actor with the given id appears in the list.
func actorListed(actorID ids.ActorID, infos []*proto.ActorTableData) bool {
	for _, a := range infos {
		if a == nil || len(a.ActorId) == 0 {
			continue
		}
		if string(a.ActorId) == string(actorID.Binary()) {
			return true
		}
	}
	return false
}

// testActorInfo verifies GetAllActorInfo returns created actors, including the
// ALIVE state filter.
func testActorInfo(ctx context.Context) error {
	client, err := getAPIGCSClient()
	if err != nil {
		return err
	}

	// Create an actor so the listing has a known entry.
	handle, err := api.Actor[*userfuncs.Counter]((*userfuncs.Counter)(nil)).Create(7)
	if err != nil {
		return fmt.Errorf("create Counter actor failed: %w", err)
	}
	actorID := handle.ID()
	fmt.Printf("  created Counter actor %s ✓\n", actorID.Hex())

	rc, err := api.GetRuntimeContext()
	if err != nil {
		return fmt.Errorf("GetRuntimeContext failed: %w", err)
	}
	jobID := rc.JobID()

	// Unfiltered listing must include the actor (poll until GCS observes it).
	aliveState := gcs.ActorStateNameAlive
	deadline := time.Now().Add(30 * time.Second)
	var all []*proto.ActorTableData
	for {
		all, err = client.GetAllActorInfo(ctx, nil, nil)
		if err != nil {
			return fmt.Errorf("GetAllActorInfo(nil, nil) failed: %w", err)
		}
		if actorListed(actorID, all) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("actor %s never appeared in GetAllActorInfo within 30s", actorID.Hex())
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("  GetAllActorInfo(nil, nil) contains actor %s ✓\n", actorID.Hex())

	// Job-filtered listing must include the actor.
	byJob, err := client.GetAllActorInfo(ctx, &jobID, nil)
	if err != nil {
		return fmt.Errorf("GetAllActorInfo(jobID, nil) failed: %w", err)
	}
	if !actorListed(actorID, byJob) {
		return fmt.Errorf("GetAllActorInfo(jobID, nil) missing actor %s", actorID.Hex())
	}
	fmt.Printf("  GetAllActorInfo(jobID, nil) contains actor %s ✓\n", actorID.Hex())

	// ALIVE state filter must include the actor (poll until GCS reports ALIVE).
	deadline = time.Now().Add(30 * time.Second)
	var alive []*proto.ActorTableData
	for {
		alive, err = client.GetAllActorInfo(ctx, &jobID, &aliveState)
		if err != nil {
			return fmt.Errorf("GetAllActorInfo(jobID, ALIVE) failed: %w", err)
		}
		if actorListed(actorID, alive) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("GetAllActorInfo(jobID, ALIVE) missing live actor %s within 30s", actorID.Hex())
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, a := range alive {
		if a != nil && string(a.ActorId) == string(actorID.Binary()) && a.State != proto.ActorTableData_ALIVE {
			return fmt.Errorf("GetAllActorInfo(jobID, ALIVE) returned actor %s in state %v", actorID.Hex(), a.State)
		}
	}
	fmt.Printf("  GetAllActorInfo(jobID, ALIVE) contains actor %s ✓\n", actorID.Hex())

	return nil
}

// testLocalGCSClientNotSupported verifies that in local mode the GCS client
// factory is not registered and GetGCSClient returns a not-supported error.
func testLocalGCSClientNotSupported() error {
	client, err := api.GetGCSClient()
	if err == nil {
		return fmt.Errorf("local GetGCSClient() should error, got %v", client)
	}
	fmt.Printf("  local GetGCSClient() -> error: %v ✓\n", err)
	return nil
}

func runDriver() {
	if err := initRay(); err != nil {
		log.Fatalf("init failed: %v", err)
	}
	defer api.Shutdown()

	ctx := context.Background()

	fmt.Println("=== TestPGQueries ===")
	if err := testPGQueries(ctx); err != nil {
		log.Fatalf("TestPGQueries FAILED: %v", err)
	}
	fmt.Println("=== TestPGQueries PASSED ===\n")

	fmt.Println("=== TestInternalKV ===")
	if err := testInternalKV(ctx); err != nil {
		log.Fatalf("TestInternalKV FAILED: %v", err)
	}
	fmt.Println("=== TestInternalKV PASSED ===\n")

	fmt.Println("=== TestNodeInfo ===")
	if err := testNodeInfo(ctx); err != nil {
		log.Fatalf("TestNodeInfo FAILED: %v", err)
	}
	fmt.Println("=== TestNodeInfo PASSED ===\n")

	fmt.Println("=== TestActorInfo ===")
	if err := testActorInfo(ctx); err != nil {
		log.Fatalf("TestActorInfo FAILED: %v", err)
	}
	fmt.Println("=== TestActorInfo PASSED ===\n")

	fmt.Println("All GCS client checks passed.")
}

func runLocal() {
	local.Enable()
	if err := api.InitLocal(); err != nil {
		log.Fatalf("local init failed: %v", err)
	}
	defer api.Shutdown()

	fmt.Println("=== TestLocalGCSClientNotSupported ===")
	if err := testLocalGCSClientNotSupported(); err != nil {
		log.Fatalf("TestLocalGCSClientNotSupported FAILED: %v", err)
	}
	fmt.Println("=== TestLocalGCSClientNotSupported PASSED ===\n")

	fmt.Println("All GCS client local-mode checks passed.")
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
