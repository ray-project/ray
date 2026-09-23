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

// Package integration holds end-to-end tests for the native GCS client. They
// need a running Ray cluster: TestMain starts and stops one, and every test
// skips when the environment is not ready. The target is opt-in
// (RAY_GCS_INTEGRATION_TESTS=1 plus the manual Bazel tag), because it takes
// over the machine's Ray cluster.
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/internal/gcs/testenv"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Package-level variables used by TestMain to manage the cluster lifetime.
var (
	rayAddr   string
	clusterID ids.ClusterID
	cleanup   func()
	// skipReason is non-empty when the cluster environment is unavailable. The
	// tests skip on it to keep failures visible, instead of exiting the whole
	// test package silently with os.Exit(0).
	skipReason string
)

// TestMain manages the Ray cluster lifetime at package level so that all tests
// share one cluster.
func TestMain(m *testing.M) {
	// The tests need a real Ray cluster; skip (rather than pass silently) when
	// the environment is unavailable.
	if reason := testenv.CheckEnv(); reason != "" {
		skipReason = reason
		fmt.Fprintln(os.Stderr, skipReason)
		os.Exit(m.Run())
	}

	// Start a single Ray cluster shared by every test.
	rayAddr, clusterID, cleanup = startRayClusterForTestMain()
	if rayAddr == "" {
		// Skip the tests instead of failing them when the cluster cannot start.
		skipReason = "failed to start Ray cluster, skipping GCS integration tests"
		fmt.Fprintln(os.Stderr, skipReason)
		os.Exit(m.Run())
	}
	code := m.Run()
	if cleanup != nil {
		cleanup()
	}
	os.Exit(code)
}

// requireCluster skips the current test unless the cluster environment is
// available, which keeps failures visible instead of hiding them.
func requireCluster(t *testing.T) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

// startRayClusterForTestMain starts a Ray cluster and returns the GCS address
// and the ClusterID. It returns an empty address when the cluster cannot be
// started, leaving the skip decision to the caller.
func startRayClusterForTestMain() (string, ids.ClusterID, func()) {
	// Start the Ray head node (with a random ClusterID).
	cmd := exec.Command("ray", "start", "--head", "--port=6379", "--num-cpus=2")
	err := cmd.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start Ray head node, skipping integration tests: %v\n", err)
		return "", ids.NilClusterID(), nil
	}

	// Wait for Ray to come up.
	time.Sleep(10 * time.Second)

	addr := "127.0.0.1:6379"

	// Read the actual ClusterID through the Python API: Ray generates a random
	// ClusterID on startup.
	pythonCode := `
import ray
ray.init(address='auto')
ctx = ray._private.worker.global_worker
cluster_and_job = ctx.current_cluster_and_job
print(str(cluster_and_job[0]))
`
	pythonCmd := exec.Command("python3", "-c", pythonCode)
	output, err := pythonCmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read ClusterID from Python, skipping integration tests: %v\n", err)
		stopRayCluster(cmd)
		return "", ids.NilClusterID(), nil
	}

	// Parse the ClusterID, formatted as "ClusterID(hex_string)", and keep the
	// hex string inside the parentheses.
	clusterIDStr := strings.TrimSpace(string(output))
	startIdx := strings.Index(clusterIDStr, "(")
	endIdx := strings.Index(clusterIDStr, ")")
	if startIdx == -1 || endIdx == -1 || startIdx >= endIdx {
		fmt.Fprintf(os.Stderr, "invalid ClusterID format %q, skipping integration tests\n", clusterIDStr)
		stopRayCluster(cmd)
		return "", ids.NilClusterID(), nil
	}
	hexStr := clusterIDStr[startIdx+1 : endIdx]

	clusterID, err := ids.ClusterIDFromHex(hexStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse ClusterID %q, skipping integration tests: %v\n", hexStr, err)
		stopRayCluster(cmd)
		return "", ids.NilClusterID(), nil
	}

	cleanup := func() {
		stopRayCluster(cmd)
	}

	return addr, clusterID, cleanup
}

// stopRayCluster kills the head node started for the tests and waits for Ray to
// stop completely.
func stopRayCluster(cmd *exec.Cmd) {
	if cmd.Process != nil {
		cmd.Process.Kill()
		cmd.Wait()
	}
	stopCmd := exec.Command("ray", "stop")
	stopCmd.Run()
	time.Sleep(2 * time.Second)
}

// TestGcsClientE2E connects to GCS and exercises the basic KV operations.
func TestGcsClientE2E(t *testing.T) {
	requireCluster(t)
	t.Logf("=== TestGcsClientE2E start ===")
	t.Logf("GCS address: %s", rayAddr)
	t.Logf("ClusterID: %s", clusterID.Hex())

	client, err := native.ConnectClient(gcs.ClientOptions{
		Address:   rayAddr,
		ClusterID: clusterID,
		TimeoutMs: 10000,
	})
	require.NoError(t, err, "Failed to connect to GCS")
	defer client.Close()

	t.Logf("GCS connected")
	t.Logf("Client ClusterID: %s", client.ClusterID().Hex())
	assert.False(t, client.ClusterID().IsNil(), "ClusterID should not be nil")
	assert.Equal(t, rayAddr, client.Address())

	ctx := context.Background()

	success, err := client.Put(ctx, "test_ns", "test_key", []byte("test_value"), true)
	assert.NoError(t, err)
	assert.True(t, success)

	value, err := client.Get(ctx, "test_ns", "test_key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("test_value"), value)

	exists, err := client.Exists(ctx, "test_ns", "test_key")
	assert.NoError(t, err)
	assert.True(t, exists)

	count, err := client.Del(ctx, "test_ns", "test_key", false)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, count, 1)
}

// TestClusterStateE2E queries the cluster state through GCS.
func TestClusterStateE2E(t *testing.T) {
	requireCluster(t)
	client, err := native.ConnectClient(gcs.ClientOptions{
		Address:   rayAddr,
		ClusterID: clusterID,
		TimeoutMs: 10000,
	})
	require.NoError(t, err, "Failed to connect to GCS")
	defer client.Close()

	ctx := context.Background()

	// Nodes
	nodes, err := client.GetAll(ctx, nil)
	assert.NoError(t, err, "GetAll Nodes failed")
	t.Logf("Found %d nodes", len(nodes))
	require.Greater(t, len(nodes), 0, "Should have at least one node")

	// NodeResources
	for nodeID := range nodes {
		resources, err := client.GetAvailableResources(ctx, nodeID)
		assert.NoError(t, err, "GetAvailableResources failed")
		if resources != nil {
			t.Logf("Node %s has available resources: CPU=%f, memory=%f",
				nodeID.Hex(),
				resources.ResourcesAvailable["CPU"],
				resources.ResourcesAvailable["memory"])
		}

		totalResources, err := client.GetTotalResources(ctx, nodeID)
		assert.NoError(t, err, "GetTotalResources failed")
		if totalResources != nil {
			t.Logf("Node %s has total resources: CPU=%f, memory=%f",
				nodeID.Hex(),
				totalResources.ResourcesTotal["CPU"],
				totalResources.ResourcesTotal["memory"])
		}
	}

	// Jobs
	jobs, err := client.ListJobs(ctx)
	assert.NoError(t, err, "ListJobs failed")
	t.Logf("Found %d jobs", len(jobs))
}

// TestWorkloadQueryE2E queries the workload state through GCS.
func TestWorkloadQueryE2E(t *testing.T) {
	requireCluster(t)
	client, err := native.ConnectClient(gcs.ClientOptions{
		Address:   rayAddr,
		ClusterID: clusterID,
		TimeoutMs: 10000,
	})
	require.NoError(t, err, "Failed to connect to GCS")
	defer client.Close()

	ctx := context.Background()

	// Actors - a fresh cluster has no actor.
	actors, err := client.ListActors(ctx, nil)
	assert.NoError(t, err, "ListActors failed")
	t.Logf("Initial: Found %d actors", len(actors))

	// Workers - the head node always has at least one.
	workers, err := client.ListWorkers(ctx)
	assert.NoError(t, err, "ListWorkers failed")
	t.Logf("Initial: Found %d workers", len(workers))
	require.Greater(t, len(workers), 0, "Should have at least one worker")

	// PlacementGroups - a fresh cluster has none.
	pgs, err := client.ListPlacementGroups(ctx)
	assert.NoError(t, err, "ListPlacementGroups failed")
	t.Logf("Initial: Found %d placement groups", len(pgs))

	// Create an actor and a placement group through Python.
	t.Logf("Creating Actor and PlacementGroup via Python...")
	pythonCode := `
import ray
ray.init(address='auto')

@ray.remote
class Counter:
    def __init__(self):
        self.value = 0
    def increment(self):
        self.value += 1
        return self.value

# Create the actor.
counter = Counter.remote()
counter.increment.remote()
print("Actor created")

# Create the placement group.
pg = ray.util.placement_group(
    bundles=[{"CPU": 1}, {"CPU": 1}],
    strategy="PACK",
    name="test_pg"
)
ray.get(pg.ready())
print("PlacementGroup created")

# Give GCS a moment to sync.
import time
time.sleep(2)
print("Done")
`
	pythonCmd := exec.Command("python3", "-c", pythonCode)
	pythonCmd.Env = append(os.Environ(), "RAY_ADDRESS="+rayAddr)
	output, err := pythonCmd.CombinedOutput()
	t.Logf("Python output: %s", string(output))
	require.NoError(t, err, "Failed to create Actor and PlacementGroup")

	// Wait for GCS to sync.
	time.Sleep(3 * time.Second)

	// Query the actors again - the new actor should be visible.
	actors, err = client.ListActors(ctx, nil)
	assert.NoError(t, err, "ListActors after create failed")
	t.Logf("After create: Found %d actors", len(actors))

	// Query the placement groups again - the new one should be visible.
	pgs, err = client.ListPlacementGroups(ctx)
	assert.NoError(t, err, "ListPlacementGroups after create failed")
	t.Logf("After create: Found %d placement groups", len(pgs))
	require.GreaterOrEqual(t, len(pgs), 1, "Should have at least one placement group")

	// Check the placement group info.
	if len(pgs) > 0 {
		pg := pgs[0]
		t.Logf("PlacementGroup name: %s, state: %s", pg.Name, pg.State.String())
		assert.NotEmpty(t, pg.PlacementGroupId, "PlacementGroupID should not be empty")
	}
}
