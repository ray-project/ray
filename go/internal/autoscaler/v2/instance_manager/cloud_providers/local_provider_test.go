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

package cloud_providers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// newLocalProviderForTest creates a LocalProvider against a fresh RAY_TMPDIR
// so tests never touch the real cluster state file.
func newLocalProviderForTest(t *testing.T, workerIPs ...string) *LocalProvider {
	t.Helper()
	t.Setenv("RAY_TMPDIR", t.TempDir())
	providerConfig := map[string]interface{}{
		"head_ip":    "127.0.0.1",
		"worker_ips": toInterfaceSlice(workerIPs),
	}
	provider, err := NewLocalProvider("test-cluster", providerConfig)
	assert.NoError(t, err)
	return provider
}

func toInterfaceSlice(values []string) []interface{} {
	result := make([]interface{}, 0, len(values))
	for _, v := range values {
		result = append(result, v)
	}
	return result
}

// TestNewLocalProvider_InitialState seeds the state file from the provider
// config: all nodes (head + workers) start terminated, so no cloud instance
// is visible before the head state is recorded or workers are launched.
func TestNewLocalProvider_InitialState(t *testing.T) {
	provider := newLocalProviderForTest(t, "127.0.0.2", "127.0.0.3")

	instances, err := provider.GetNonTerminated()
	assert.NoError(t, err)
	assert.Empty(t, instances)
}

// TestNewLocalProvider_MissingHeadIP rejects a provider config without head_ip.
func TestNewLocalProvider_MissingHeadIP(t *testing.T) {
	_, err := NewLocalProvider("test-cluster", map[string]interface{}{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "head_ip")
}

// TestClusterState_HeadIPChangedResetsState recreates the cluster when the
// head ip of an existing state file changed (aligned with Python).
func TestClusterState_HeadIPChangedResetsState(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "cluster-test-cluster.state")
	// State file of a previous cluster: old head running + old worker running.
	oldState := `{
		"10.0.0.1": {"tags": {"ray-node-type": "head"}, "state": "running"},
		"10.0.0.2": {"tags": {"ray-node-type": "worker"}, "state": "running"}
	}`
	assert.NoError(t, os.WriteFile(statePath, []byte(oldState), 0o644))

	state, err := NewClusterState(statePath, "127.0.0.1", []string{"127.0.0.2"})
	assert.NoError(t, err)

	workers, err := state.get()
	assert.NoError(t, err)
	// Old nodes are dropped; only the new head + worker remain, both terminated.
	assert.Len(t, workers, 2)
	assert.Equal(t, nodeStateTerminated, workers["127.0.0.1"].State)
	assert.Equal(t, nodeKindTagHead, workers["127.0.0.1"].Tags[tagRayNodeKind])
	assert.Equal(t, nodeStateTerminated, workers["127.0.0.2"].State)
	assert.Equal(t, nodeKindTagWorker, workers["127.0.0.2"].Tags[tagRayNodeKind])
}

// TestClusterState_RemovedWorkersPruned drops nodes that are no longer
// present in the provider config (user reduced the number of workers).
func TestClusterState_RemovedWorkersPruned(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "cluster-test-cluster.state")
	oldState := `{
		"127.0.0.1": {"tags": {"ray-node-type": "head"}, "state": "running"},
		"127.0.0.2": {"tags": {"ray-node-type": "worker"}, "state": "running"},
		"127.0.0.3": {"tags": {"ray-node-type": "worker"}, "state": "terminated"}
	}`
	assert.NoError(t, os.WriteFile(statePath, []byte(oldState), 0o644))

	state, err := NewClusterState(statePath, "127.0.0.1", []string{"127.0.0.2"})
	assert.NoError(t, err)

	workers, err := state.get()
	assert.NoError(t, err)
	assert.Len(t, workers, 2)
	assert.NotContains(t, workers, "127.0.0.3")
}

// TestLaunch_FlipsTerminatedWorkers launches nodes: terminated workers flip
// to running with launch tags, visible via GetNonTerminated with worker
// kind, node type and request id taken from the tags.
func TestLaunch_FlipsTerminatedWorkers(t *testing.T) {
	provider := newLocalProviderForTest(t, "127.0.0.2", "127.0.0.3")

	assert.NoError(t, provider.Launch(map[string]int{"local.cluster.node": 2}, "req-1"))

	instances, err := provider.GetNonTerminated()
	assert.NoError(t, err)
	assert.Len(t, instances, 2)
	for ip, instance := range instances {
		assert.Equal(t, ip, instance.CloudInstanceId)
		assert.Equal(t, proto.NodeKind_WORKER, instance.NodeKind)
		assert.Equal(t, "local.cluster.node", instance.NodeType)
		assert.True(t, instance.IsRunning)
		assert.Equal(t, "req-1", instance.RequestId)
	}
}

// TestLaunch_PartialWhenInsufficient launches at most the available number
// of nodes (aligned with Python create_node: min(count, available)).
func TestLaunch_PartialWhenInsufficient(t *testing.T) {
	provider := newLocalProviderForTest(t, "127.0.0.2")

	assert.NoError(t, provider.Launch(map[string]int{"local.cluster.node": 3}, "req-1"))

	instances, err := provider.GetNonTerminated()
	assert.NoError(t, err)
	assert.Len(t, instances, 1)
}

// TestLaunch_DuplicateRequestNoOp: a launch request id that was already
// applied does not flip further nodes.
func TestLaunch_DuplicateRequestNoOp(t *testing.T) {
	provider := newLocalProviderForTest(t, "127.0.0.2", "127.0.0.3")

	assert.NoError(t, provider.Launch(map[string]int{"local.cluster.node": 1}, "req-1"))
	// Same request id again: no additional node is flipped.
	assert.NoError(t, provider.Launch(map[string]int{"local.cluster.node": 1}, "req-1"))

	instances, err := provider.GetNonTerminated()
	assert.NoError(t, err)
	assert.Len(t, instances, 1)
}

// TestTerminate_RemovesFromNonTerminated flips terminated nodes out of the
// non-terminated view.
func TestTerminate_RemovesFromNonTerminated(t *testing.T) {
	provider := newLocalProviderForTest(t, "127.0.0.2", "127.0.0.3")
	assert.NoError(t, provider.Launch(map[string]int{"local.cluster.node": 2}, "req-1"))

	assert.NoError(t, provider.Terminate([]string{"127.0.0.2"}, "req-2"))

	instances, err := provider.GetNonTerminated()
	assert.NoError(t, err)
	assert.NotContains(t, instances, "127.0.0.2")
	assert.Contains(t, instances, "127.0.0.3")
}

// TestRecordLocalHeadStateIfNeeded marks the head node as running with head
// tags and is idempotent.
func TestRecordLocalHeadStateIfNeeded(t *testing.T) {
	provider := newLocalProviderForTest(t, "127.0.0.2")

	assert.NoError(t, provider.RecordLocalHeadStateIfNeeded())
	instances, err := provider.GetNonTerminated()
	assert.NoError(t, err)
	head, ok := instances["127.0.0.1"]
	assert.True(t, ok)
	assert.Equal(t, proto.NodeKind_HEAD, head.NodeKind)
	assert.Equal(t, "local.cluster.node", head.NodeType)
	assert.True(t, head.IsRunning)

	// Recording again keeps the head running.
	assert.NoError(t, provider.RecordLocalHeadStateIfNeeded())
	instances, err = provider.GetNonTerminated()
	assert.NoError(t, err)
	assert.Contains(t, instances, "127.0.0.1")
}

// TestStatePersistence_Reload: a provider recreated from the same state file
// sees the previously launched workers still running.
func TestStatePersistence_Reload(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("RAY_TMPDIR", tmpDir)
	providerConfig := map[string]interface{}{
		"head_ip":    "127.0.0.1",
		"worker_ips": toInterfaceSlice([]string{"127.0.0.2"}),
	}

	provider, err := NewLocalProvider("test-cluster", providerConfig)
	assert.NoError(t, err)
	assert.NoError(t, provider.Launch(map[string]int{"local.cluster.node": 1}, "req-1"))

	// Recreate from the same state file.
	reloaded, err := NewLocalProvider("test-cluster", providerConfig)
	assert.NoError(t, err)
	instances, err := reloaded.GetNonTerminated()
	assert.NoError(t, err)
	assert.Len(t, instances, 1)
	assert.Equal(t, "req-1", instances["127.0.0.2"].RequestId)
}

// TestPollErrors_ReturnsAndClears drains the accumulated errors.
func TestPollErrors_ReturnsAndClears(t *testing.T) {
	provider := newLocalProviderForTest(t)
	provider.addError(assert.AnError)
	provider.addError(assert.AnError)

	errs := provider.PollErrors()
	assert.Len(t, errs, 2)
	assert.Empty(t, provider.PollErrors())
}
