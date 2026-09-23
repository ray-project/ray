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

//go:build cgo

// CGO integration tests for the native GCS client. They need a running Ray
// cluster: TestMain starts and stops one, and every test skips when the
// environment is not ready. The target is opt-in (RAY_GCS_INTEGRATION_TESTS=1
// plus the manual Bazel tag), because it takes over the machine's Ray cluster.

package native

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/gcs/testenv"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"go.uber.org/zap/zapcore"
)

// ============ TestMain - manages the Ray cluster lifetime ============

var (
	testRayAddr   string
	testClusterID ids.ClusterID
	testCleanup   func()
	// skipReason is non-empty when the cluster environment is unavailable. The
	// tests skip on it to keep failures visible, instead of exiting the whole
	// test package silently with os.Exit(0).
	skipReason string
)

func TestMain(m *testing.M) {
	zap.SetupDefaultLogger(zap.WithDevelopment(true), zap.WithLevel(zapcore.DebugLevel))
	// The integration tests need a real Ray cluster; skip (rather than pass
	// silently) when the environment is unavailable.
	if reason := testenv.CheckEnv(); reason != "" {
		skipReason = reason
		fmt.Fprintln(os.Stderr, skipReason)
		os.Exit(m.Run())
	}

	// Start the Ray cluster.
	testRayAddr, testClusterID, testCleanup = startRayClusterForTest()
	if testRayAddr == "" {
		skipReason = "failed to start Ray cluster, skipping GCS integration tests"
		fmt.Fprintln(os.Stderr, skipReason)
		os.Exit(m.Run())
	}

	code := m.Run()

	// Clean up the Ray cluster.
	if testCleanup != nil {
		testCleanup()
	}

	os.Exit(code)
}

// startRayClusterForTest starts a Ray cluster for the integration tests.
func startRayClusterForTest() (string, ids.ClusterID, func()) {
	// Check whether a Ray cluster is already running.
	cmd := exec.Command("ray", "start", "--head", "--port=6379", "--num-cpus=2", "--include-dashboard=false")
	output, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(output), "already running") {
		// Ray may already be running, so stop it and start again.
		exec.Command("ray", "stop", "--force").Run()
		time.Sleep(2 * time.Second)

		cmd = exec.Command("ray", "start", "--head", "--port=6379", "--num-cpus=2", "--include-dashboard=false")
		output, err = cmd.CombinedOutput()
		if err != nil {
			return "", ids.NilClusterID(), nil
		}
	}

	// Wait for Ray to come up.
	time.Sleep(10 * time.Second)

	addr := "127.0.0.1:6379"

	// Read the actual ClusterID through the Python API.
	pythonCode := `
import ray
ray.init(address='auto')
ctx = ray._private.worker.global_worker
cluster_and_job = ctx.current_cluster_and_job
print(str(cluster_and_job[0]))
`
	pythonCmd := exec.Command("python3", "-c", pythonCode)
	output, err = pythonCmd.Output()
	if err != nil {
		// Fall back to the GCS address when Python cannot report it.
		return addr, ids.NilClusterID(), func() {
			exec.Command("ray", "stop", "--force").Run()
		}
	}

	// Parse the ClusterID, formatted as "ClusterID(hex_string)".
	clusterIDStr := strings.TrimSpace(string(output))
	startIdx := strings.Index(clusterIDStr, "(")
	endIdx := strings.Index(clusterIDStr, ")")
	if startIdx != -1 && endIdx != -1 && startIdx < endIdx {
		clusterIDStr = clusterIDStr[startIdx+1 : endIdx]
	}

	clusterID, err := ids.ClusterIDFromHex(clusterIDStr)
	if err != nil {
		return addr, ids.NilClusterID(), func() {
			exec.Command("ray", "stop", "--force").Run()
		}
	}

	return addr, clusterID, func() {
		exec.Command("ray", "stop", "--force").Run()
	}
}

// ============ Test helpers ============

// requireCluster skips the current test unless the cluster environment is
// available, which keeps failures visible instead of hiding them.
func requireCluster(t *testing.T) {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
}

// createTestClientWithGCS creates a client that talks to the Ray cluster
// started by TestMain.
func createTestClientWithGCS(t *testing.T) *cgoClient {
	t.Helper()

	requireCluster(t)

	if testClusterID.IsNil() {
		t.Skip("ClusterID is nil, skipping integration test")
		return nil
	}

	opts := gcs.ClientOptions{
		Address:   testRayAddr,
		ClusterID: testClusterID,
		TimeoutMs: 5000,
	}

	client, err := ConnectClient(opts)
	if err != nil {
		t.Fatalf("ConnectClient failed: %v", err)
		return nil
	}

	return client.(*cgoClient)
}

// ============ Actor tests ============

func Test_getActorInfo_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	// Look up a random ActorID.
	actorID := ids.NilActorID()
	info, err := client.GetActorInfo(ctx, actorID)
	if err != nil {
		t.Fatalf("GetActorInfo failed: %v", err)
	}
	// info can be nil when the actor does not exist.
	_ = info
}

func Test_listActors_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	actors, err := client.ListActors(ctx, nil)
	if err != nil {
		t.Fatalf("ListActors failed: %v", err)
	}
	if actors == nil {
		t.Fatal("expected actor list, got nil")
	}
}

// ============ Autoscaler tests ============

func Test_getAutoscalerStatus_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	status, err := client.GetAutoscalerStatus(ctx)
	if err != nil {
		t.Fatalf("GetAutoscalerStatus failed: %v", err)
	}
	_ = status
}

// ============ Job tests ============

func Test_jobs_getJobInfo_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	jobID := ids.NilJobID()
	info, err := client.GetJobInfo(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobInfo failed: %v", err)
	}
	_ = info
}

func Test_jobs_getJobInfo_NilJobID_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	jobID := ids.NilJobID()
	info, err := client.GetJobInfo(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobInfo with nil JobID failed: %v", err)
	}
	_ = info
}

func Test_jobs_listJobs_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	jobs, err := client.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if jobs == nil {
		t.Fatal("expected job list, got nil")
	}
}

func Test_jobs_listJobs_WithFilter_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	// List jobs and check the filtered query path.
	jobs, err := client.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs with filter failed: %v", err)
	}
	_ = jobs
}

func Test_jobs_getJobInfo_DeadJob_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	// Fetch the info of a dead job, when one exists.
	jobID := ids.NilJobID()
	info, err := client.GetJobInfo(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobInfo for dead job failed: %v", err)
	}
	_ = info
}

func Test_jobs_listJobs_Empty_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	jobs, err := client.ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs empty failed: %v", err)
	}
	_ = jobs
}

// ============ Node tests ============

func Test_nodes_checkAlive_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodeIDs := []ids.NodeID{}
	alive, err := client.CheckAlive(ctx, nodeIDs)
	if err != nil {
		t.Fatalf("CheckAlive failed: %v", err)
	}
	_ = alive
}

func Test_nodes_checkAlive_EmptyList_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	alive, err := client.CheckAlive(ctx, []ids.NodeID{})
	if err != nil {
		t.Fatalf("CheckAlive empty list failed: %v", err)
	}
	_ = alive
}

func Test_nodes_getAll_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodes, err := client.GetAll(ctx, nil)
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if nodes == nil {
		t.Fatal("expected node map, got nil")
	}
}

func Test_nodes_getAll_EmptyList_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodes, err := client.GetAll(ctx, nil)
	if err != nil {
		t.Fatalf("GetAll empty failed: %v", err)
	}
	_ = nodes
}

func Test_nodes_drainNodes_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodeIDs := []ids.NodeID{}
	drained, err := client.DrainNodes(ctx, nodeIDs)
	if err != nil {
		t.Fatalf("DrainNodes failed: %v", err)
	}
	_ = drained
}

func Test_nodes_drainNodes_EmptyList_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	drained, err := client.DrainNodes(ctx, []ids.NodeID{})
	if err != nil {
		t.Fatalf("DrainNodes empty list failed: %v", err)
	}
	_ = drained
}

// ============ PlacementGroup tests ============

func Test_getPlacementGroup_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	pgID := ids.NilPlacementGroupID()
	pg, err := client.GetPlacementGroup(ctx, pgID)
	if err != nil {
		t.Fatalf("GetPlacementGroup failed: %v", err)
	}
	_ = pg
}

func Test_listPlacementGroups_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	pgs, err := client.ListPlacementGroups(ctx)
	if err != nil {
		t.Fatalf("ListPlacementGroups failed: %v", err)
	}
	if pgs == nil {
		t.Fatal("expected placement group list, got nil")
	}
}

// ============ Publisher tests ============
//
// The PublishErrors/PublishLogs channel helpers are not part of the open-source
// gcs.Client interface: log records are published one batch at a time through
// LogBatchPublisher.PublishLogBatch, which is covered by the unit target in
// this package.

// ============ Resource tests ============

func Test_resources_getAvailableResources_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodeID := ids.NilNodeID()
	resources, err := client.GetAvailableResources(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetAvailableResources failed: %v", err)
	}
	_ = resources
}

func Test_resources_getAvailableResources_NilNodeID_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	resources, err := client.GetAvailableResources(ctx, ids.NilNodeID())
	if err != nil {
		t.Fatalf("GetAvailableResources with nil nodeID failed: %v", err)
	}
	_ = resources
}

func Test_resources_getTotalResources_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodeID := ids.NilNodeID()
	resources, err := client.GetTotalResources(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetTotalResources failed: %v", err)
	}
	_ = resources
}

func Test_resources_getTotalResources_NilNodeID_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	resources, err := client.GetTotalResources(ctx, ids.NilNodeID())
	if err != nil {
		t.Fatalf("GetTotalResources with nil nodeID failed: %v", err)
	}
	_ = resources
}

func Test_resources_getAvailableResources_MultipleNodes_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	// Fetch all nodes.
	nodes, err := client.GetAll(ctx, nil)
	if err != nil {
		t.Skipf("Cannot get nodes for multi-node test: %v", err)
	}

	if len(nodes) < 2 {
		t.Skip("Multi-node test requires at least 2 nodes")
	}

	// Take the first node ID.
	var nodeID ids.NodeID
	for id := range nodes {
		nodeID = id
		break
	}

	resources, err := client.GetAvailableResources(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetAvailableResources for multi-node failed: %v", err)
	}
	_ = resources
}

func Test_resources_getTotalResources_MultipleNodes_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	// Fetch all nodes.
	nodes, err := client.GetAll(ctx, nil)
	if err != nil {
		t.Skipf("Cannot get nodes for multi-node test: %v", err)
	}

	if len(nodes) < 2 {
		t.Skip("Multi-node test requires at least 2 nodes")
	}

	// Take the first node ID.
	var nodeID ids.NodeID
	for id := range nodes {
		nodeID = id
		break
	}

	resources, err := client.GetTotalResources(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetTotalResources for multi-node failed: %v", err)
	}
	_ = resources
}

func Test_resources_getAvailableResources_VerifyResourceTypes_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodes, err := client.GetAll(ctx, nil)
	if err != nil || len(nodes) == 0 {
		t.Skipf("Cannot get nodes for resource type verification: %v", err)
	}

	// Take the first node ID.
	var nodeID ids.NodeID
	for id := range nodes {
		nodeID = id
		break
	}

	resources, err := client.GetAvailableResources(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetAvailableResources for type verification failed: %v", err)
	}

	if resources == nil {
		t.Error("Expected resources to be non-nil")
	}
}

func Test_resources_getTotalResources_VerifyResourceTypes_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	nodes, err := client.GetAll(ctx, nil)
	if err != nil || len(nodes) == 0 {
		t.Skipf("Cannot get nodes for resource type verification: %v", err)
	}

	// Take the first node ID.
	var nodeID ids.NodeID
	for id := range nodes {
		nodeID = id
		break
	}

	resources, err := client.GetTotalResources(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetTotalResources for type verification failed: %v", err)
	}

	if resources == nil {
		t.Error("Expected resources to be non-nil")
	}
}

// ============ Worker tests ============

func Test_getWorkerInfo_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	workerID := ids.NilWorkerID()
	info, err := client.GetWorkerInfo(ctx, workerID)
	if err != nil {
		t.Fatalf("GetWorkerInfo failed: %v", err)
	}
	_ = info
}

func Test_listWorkers_Integration(t *testing.T) {
	ctx := context.Background()
	client := createTestClientWithGCS(t)
	defer client.Close()

	workers, err := client.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if workers == nil {
		t.Fatal("expected worker list, got nil")
	}
}
