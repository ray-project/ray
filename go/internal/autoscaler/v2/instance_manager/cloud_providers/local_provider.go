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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/local"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// Node states persisted in the local cluster state file.
// Aligned with python/ray/autoscaler/_private/local/node_provider.py.
const (
	nodeStateRunning    = "running"
	nodeStateTerminated = "terminated"
)

// Tag keys/values used in the local cluster state file.
// Aligned with python/ray/autoscaler/tags.py. For legacy reasons the kind
// tag key says "type" while meaning "kind" (head/worker/unmanaged).
const (
	tagRayNodeKind      = "ray-node-type"
	tagRayUserNodeType  = "ray-user-node-type"
	tagRayNodeName      = "ray-node-name"
	tagRayNodeStatus    = "ray-node-status"
	tagRayLaunchRequest = "ray-launch-request"

	nodeKindTagHead   = "head"
	nodeKindTagWorker = "worker"

	statusUninitialized = "uninitialized"
	statusUpToDate      = "up-to-date"
)

// localNodeInfo is one node entry of the local cluster state file,
// mirroring the Python {"tags": {...}, "state": "running"/"terminated"} layout.
type localNodeInfo struct {
	Tags  map[string]string `json:"tags"`
	State string            `json:"state"`
}

// ClusterState manages the local cluster state file
// ($RAY_TMPDIR/cluster-<cluster_name>.state), corresponding to Python
// local/node_provider.py::ClusterState: the node set is derived from the
// provider config (head_ip + worker_ips), node liveness is persisted in the
// state file. Concurrency is guarded by an in-process mutex; the Python-side
// file lock (multi-process, shared with the ray CLI) is not implemented yet.
type ClusterState struct {
	mu        sync.Mutex
	savePath  string
	headIP    string
	workerIPs []string
}

// NewClusterState loads or initializes the cluster state file:
//   - an existing state file is loaded; if the head ip changed (missing or
//     not tagged head), the state is discarded and the cluster recreated;
//   - ips from the provider config missing in the file are seeded as
//     terminated nodes (head/workers tagged by node kind);
//   - nodes removed from the provider config are dropped;
//   - the result is written back to disk.
func NewClusterState(savePath, headIP string, workerIPs []string) (*ClusterState, error) {
	if err := os.MkdirAll(filepath.Dir(savePath), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cluster state dir: %w", err)
	}

	state := &ClusterState{savePath: savePath, headIP: headIP, workerIPs: workerIPs}

	workers := make(map[string]localNodeInfo)
	if data, err := os.ReadFile(savePath); err == nil {
		if err := json.Unmarshal(data, &workers); err != nil {
			return nil, fmt.Errorf("failed to parse cluster state file %s: %w", savePath, err)
		}
		if head, ok := workers[headIP]; !ok || head.Tags[tagRayNodeKind] != nodeKindTagHead {
			workers = make(map[string]localNodeInfo)
			log.Log.Info("Head IP changed - recreating cluster.")
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read cluster state file %s: %w", savePath, err)
	}
	log.Log.V(1).Info("ClusterState: loaded cluster state.", "path", savePath, "numNodes", len(workers))

	for _, workerIP := range workerIPs {
		if info, ok := workers[workerIP]; ok {
			if info.Tags[tagRayNodeKind] != nodeKindTagWorker {
				return nil, fmt.Errorf("node %s in cluster state file is not tagged as a worker", workerIP)
			}
			continue
		}
		workers[workerIP] = localNodeInfo{
			Tags:  map[string]string{tagRayNodeKind: nodeKindTagWorker},
			State: nodeStateTerminated,
		}
	}
	if info, ok := workers[headIP]; ok {
		if info.Tags[tagRayNodeKind] != nodeKindTagHead {
			return nil, fmt.Errorf("head node %s in cluster state file is not tagged as head", headIP)
		}
	} else {
		workers[headIP] = localNodeInfo{
			Tags:  map[string]string{tagRayNodeKind: nodeKindTagHead},
			State: nodeStateTerminated,
		}
	}

	// Drop nodes no longer present in the provider config
	// (e.g. the user reduced the number of workers).
	for nodeIP := range workers {
		if nodeIP != headIP && !containsString(workerIPs, nodeIP) {
			delete(workers, nodeIP)
		}
	}

	if err := state.persist(workers); err != nil {
		return nil, err
	}
	return state, nil
}

// get returns a snapshot of the whole state, re-reading the file to stay
// consistent with external writers (aligned with Python ClusterState.get).
func (s *ClusterState) get() (map[string]localNodeInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.read()
}

// update applies fn to the state in a single read-modify-write cycle.
func (s *ClusterState) update(fn func(workers map[string]localNodeInfo)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	workers, err := s.read()
	if err != nil {
		return err
	}
	fn(workers)
	return s.persist(workers)
}

// read loads the state file; the caller must hold s.mu.
func (s *ClusterState) read() (map[string]localNodeInfo, error) {
	data, err := os.ReadFile(s.savePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster state file %s: %w", s.savePath, err)
	}
	workers := make(map[string]localNodeInfo)
	if err := json.Unmarshal(data, &workers); err != nil {
		return nil, fmt.Errorf("failed to parse cluster state file %s: %w", s.savePath, err)
	}
	return workers, nil
}

// persist writes the state file; the caller must hold s.mu.
func (s *ClusterState) persist(workers map[string]localNodeInfo) error {
	data, err := json.Marshal(workers)
	if err != nil {
		return fmt.Errorf("failed to serialize cluster state: %w", err)
	}
	if err := os.WriteFile(s.savePath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write cluster state file %s: %w", s.savePath, err)
	}
	return nil
}

func containsString(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

// LocalProvider manages a fixed set of on-premise nodes (head_ip +
// worker_ips from the provider config) through the local cluster state file.
// It natively implements ICloudInstanceProvider: on the Python side the v1
// LocalNodeProvider is wrapped in a NodeProviderAdapter, but with only the
// local provider to support, the adapter layer (batched launch executors,
// launch config hash, v1 tag protocol) is unnecessary, so the state file is
// managed directly (see go/docs/autoscaler_v2_cloud_provider_implementation.md).
type LocalProvider struct {
	clusterName string
	headIP      string
	state       *ClusterState

	mu       sync.Mutex
	errors   []error
	requests map[string]struct{} // launch request ids already applied
}

// NewLocalProvider creates the local provider from the provider config
// ("head_ip" and "worker_ips" are required; they are validated by
// local.PrepareLocal before reaching the autoscaler).
func NewLocalProvider(clusterName string, providerConfig map[string]interface{}) (*LocalProvider, error) {
	headIP, ok := providerConfig["head_ip"].(string)
	if !ok || headIP == "" {
		return nil, fmt.Errorf("local provider config requires a `head_ip`")
	}
	rawWorkerIPs, _ := providerConfig["worker_ips"].([]interface{})
	workerIPs := make([]string, 0, len(rawWorkerIPs))
	for _, rawIP := range rawWorkerIPs {
		if ip, ok := rawIP.(string); ok {
			workerIPs = append(workerIPs, ip)
		}
	}

	state, err := NewClusterState(localStatePath(clusterName), headIP, workerIPs)
	if err != nil {
		return nil, fmt.Errorf("failed to init local cluster state: %w", err)
	}
	return &LocalProvider{
		clusterName: clusterName,
		headIP:      headIP,
		state:       state,
		requests:    make(map[string]struct{}),
	}, nil
}

// localStatePath returns $RAY_TMPDIR/cluster-<cluster_name>.state,
// aligned with python local/config.py::get_state_path
// (ray temp dir defaults to /tmp/ray).
func localStatePath(clusterName string) string {
	tmpDir := os.Getenv("RAY_TMPDIR")
	if tmpDir == "" {
		tmpDir = "/tmp/ray"
	}
	return filepath.Join(tmpDir, fmt.Sprintf("cluster-%s.state", clusterName))
}

// GetNonTerminated returns the non-terminated nodes as cloud instances.
// The node kind comes from the node kind tag (unmanaged nodes are filtered
// out); node type and launch request id come from the node tags, aligned
// with Python NodeProviderAdapter.get_non_terminated.
func (p *LocalProvider) GetNonTerminated() (map[string]autoscaler.CloudInstance, error) {
	workers, err := p.state.get()
	if err != nil {
		return nil, err
	}

	instances := make(map[string]autoscaler.CloudInstance)
	for nodeIP, info := range workers {
		if info.State == nodeStateTerminated {
			continue
		}
		var nodeKind proto.NodeKind
		switch info.Tags[tagRayNodeKind] {
		case nodeKindTagHead:
			nodeKind = proto.NodeKind_HEAD
		case nodeKindTagWorker:
			nodeKind = proto.NodeKind_WORKER
		default:
			// Filter out unmanaged nodes.
			continue
		}
		instances[nodeIP] = autoscaler.CloudInstance{
			CloudInstanceId: nodeIP,
			NodeKind:        nodeKind,
			NodeType:        info.Tags[tagRayUserNodeType],
			IsRunning:       info.State == nodeStateRunning,
			RequestId:       info.Tags[tagRayLaunchRequest],
		}
	}
	return instances, nil
}

// Launch flips terminated worker nodes to running, aligned with the Python
// v1 LocalNodeProvider.create_node semantics: min(count, currently
// available) nodes are created and tagged. A request id that was already
// applied is a no-op. Operations are synchronous file updates; IO failures
// are reported through the error queue (see PollErrors), matching the async
// semantics of the Python adapter where launch errors surface via
// poll_errors instead of the return value.
func (p *LocalProvider) Launch(shape map[string]int, requestId string) error {
	p.mu.Lock()
	if _, ok := p.requests[requestId]; ok {
		p.mu.Unlock()
		return nil
	}
	p.requests[requestId] = struct{}{}
	p.mu.Unlock()

	err := p.state.update(func(workers map[string]localNodeInfo) {
		for nodeType, count := range shape {
			if count <= 0 {
				continue
			}
			tags := map[string]string{
				tagRayNodeName:      fmt.Sprintf("ray-%s-worker", p.clusterName),
				tagRayNodeKind:      nodeKindTagWorker,
				tagRayNodeStatus:    statusUninitialized,
				tagRayLaunchRequest: requestId,
				tagRayUserNodeType:  nodeType,
			}
			for nodeIP, info := range workers {
				if count == 0 {
					break
				}
				if info.State == nodeStateTerminated && info.Tags[tagRayNodeKind] == nodeKindTagWorker {
					workers[nodeIP] = localNodeInfo{Tags: tags, State: nodeStateRunning}
					count--
				}
			}
		}
	})
	if err != nil {
		p.addError(fmt.Errorf("failed to launch nodes (request %s): %w", requestId, err))
	}
	return nil
}

// Terminate flips the given nodes to terminated. The caller (cloud instance
// updater) passes a fresh request id per call, so no request dedup applies;
// flipping an already-terminated node is idempotent.
func (p *LocalProvider) Terminate(ids []string, requestId string) error {
	err := p.state.update(func(workers map[string]localNodeInfo) {
		for _, id := range ids {
			if info, ok := workers[id]; ok {
				info.State = nodeStateTerminated
				workers[id] = info
			}
		}
	})
	if err != nil {
		p.addError(fmt.Errorf("failed to terminate nodes %v (request %s): %w", ids, requestId, err))
	}
	return nil
}

// PollErrors returns and clears the accumulated asynchronous errors.
func (p *LocalProvider) PollErrors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	errs := p.errors
	p.errors = nil
	return errs
}

// RecordLocalHeadStateIfNeeded marks the head node as running in the cluster
// state file if it is not yet recorded, aligned with Python
// record_local_head_state_if_needed: the head state is recorded on the
// cluster-launching machine by `ray up`, so the head where the autoscaler
// runs must record its own existence.
func (p *LocalProvider) RecordLocalHeadStateIfNeeded() error {
	workers, err := p.state.get()
	if err != nil {
		return err
	}
	if head, ok := workers[p.headIP]; ok && head.State != nodeStateTerminated {
		return nil
	}

	headTags := map[string]string{
		tagRayNodeName:     fmt.Sprintf("ray-%s-head", p.clusterName),
		tagRayNodeKind:     nodeKindTagHead,
		tagRayUserNodeType: local.LOCAL_CLUSTER_NODE_TYPE,
		tagRayNodeStatus:   statusUpToDate,
	}
	return p.state.update(func(workers map[string]localNodeInfo) {
		workers[p.headIP] = localNodeInfo{Tags: headTags, State: nodeStateRunning}
	})
}

func (p *LocalProvider) addError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errors = append(p.errors, err)
}
