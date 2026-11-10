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

package job

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// fakeGCSClient is an in-memory gcs.Client used by the unit tests in this
// package. Only the internal KV surface is meaningful; the remaining methods
// return zero values because the agent job tests do not exercise them.
// mockgcs does not implement the KV interface, hence the hand-rolled fake.
type fakeGCSClient struct {
	mu   sync.Mutex
	kv   map[string][]byte
	jobs []*proto.JobTableData
}

var _ gcs.Client = (*fakeGCSClient)(nil)

func newFakeGCSClient() *fakeGCSClient {
	return &fakeGCSClient{kv: map[string][]byte{}}
}

func kvKey(ns, key string) string { return ns + "/" + key }

func (f *fakeGCSClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.kv[kvKey(ns, key)]; ok {
		return v, nil
	}
	return nil, gcs.ErrKeyNotFound
}

func (f *fakeGCSClient) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]byte{}
	for _, key := range keys {
		if v, ok := f.kv[kvKey(ns, key)]; ok {
			out[key] = v
		}
	}
	return out, nil
}

func (f *fakeGCSClient) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := kvKey(ns, key)
	if _, exists := f.kv[k]; exists && !overwrite {
		return false, nil
	}
	f.kv[k] = value
	return true, nil
}

func (f *fakeGCSClient) Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if delByPrefix {
		prefix := kvKey(ns, key)
		deleted := 0
		for k := range f.kv {
			if strings.HasPrefix(k, prefix) {
				delete(f.kv, k)
				deleted++
			}
		}
		return deleted, nil
	}
	if _, ok := f.kv[kvKey(ns, key)]; ok {
		delete(f.kv, kvKey(ns, key))
		return 1, nil
	}
	return 0, nil
}

func (f *fakeGCSClient) Keys(ctx context.Context, ns, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	full := kvKey(ns, prefix)
	var out []string
	for k := range f.kv {
		if strings.HasPrefix(k, full) {
			out = append(out, strings.TrimPrefix(k, ns+"/"))
		}
	}
	sort.Strings(out)
	return out, nil
}

func (f *fakeGCSClient) Exists(ctx context.Context, ns, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.kv[kvKey(ns, key)]
	return ok, nil
}

func (f *fakeGCSClient) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, nil
}

func (f *fakeGCSClient) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	return nil, nil
}

func (f *fakeGCSClient) DrainNode(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
	return false, "", nil
}

func (f *fakeGCSClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) ListJobsWithFilter(ctx context.Context, jobOrSubmissionID string, skipSubmissionJobInfoField, skipIsRunningTasksField bool) ([]*proto.JobTableData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobs, nil
}

func (f *fakeGCSClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	return ids.JobID{}, nil
}

func (f *fakeGCSClient) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetPlacementGroupByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, nil
}

func (f *fakeGCSClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	return nil, nil
}

func (f *fakeGCSClient) ReportAutoscalingState(autoscalingState string) error {
	return nil
}

func (f *fakeGCSClient) Address() string { return "fake" }

func (f *fakeGCSClient) ClusterID() ids.ClusterID { return ids.ClusterID{} }

func (f *fakeGCSClient) Close() error { return nil }

func (f *fakeGCSClient) IsClosed() bool { return false }
