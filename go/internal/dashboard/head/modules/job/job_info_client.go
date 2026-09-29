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

package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
)

// KV constants aligned with ray/_private/ray_constants.py and
// python/ray/dashboard/modules/job/common.py.
const (
	// rayInternalNamespacePrefix is RAY_INTERNAL_NAMESPACE_PREFIX from
	// ray/_raylet.pyx.
	rayInternalNamespacePrefix = "_ray_internal_"
	// kvNamespaceJob is KV_NAMESPACE_JOB.
	kvNamespaceJob = "job"
	// jobDataKeyPrefix is JobInfoStorageClient.JOB_DATA_KEY_PREFIX: the
	// internal KV key prefix under which submission job infos are stored.
	jobDataKeyPrefix = rayInternalNamespacePrefix + "job_info_"
	// kvHeadNodeIDKey is KV_HEAD_NODE_ID_KEY.
	kvHeadNodeIDKey = "head_node_id"
	// kvNamespaceDashboard is KV_NAMESPACE_DASHBOARD.
	kvNamespaceDashboard = "dashboard"
	// dashboardAgentAddrNodeIDPrefix is DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX
	// from python/ray/dashboard/consts.py.
	dashboardAgentAddrNodeIDPrefix = "DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:"
)

// jobIDMetadataKey is JOB_ID_METADATA_KEY from job/common.py. When a job's
// JobConfig metadata contains this key, the job is the driver of a submission
// job whose submission id is the metadata value.
const jobIDMetadataKey = "job_submission_id"

// JobInfoStorageClient manages submission job info in the GCS internal KV
// store, aligned with the Python JobInfoStorageClient (job/common.py).
type JobInfoStorageClient struct {
	client *head.GCSClient
}

// NewJobInfoStorageClient creates a JobInfoStorageClient backed by the GCS
// client.
func NewJobInfoStorageClient(client *head.GCSClient) *JobInfoStorageClient {
	return &JobInfoStorageClient{client: client}
}

// jobDataKey returns the internal KV key for a submission id.
func jobDataKey(submissionID string) string {
	return jobDataKeyPrefix + submissionID
}

// PutInfo stores the job info under JOB_DATA_KEY for the submission id.
// It returns whether a new key was added.
func (c *JobInfoStorageClient) PutInfo(ctx context.Context, submissionID string, info *JobInfo, overwrite bool) (bool, error) {
	payload, err := json.Marshal(info)
	if err != nil {
		return false, fmt.Errorf("marshal job info: %w", err)
	}
	return c.client.InternalKVPut(ctx, kvNamespaceJob, jobDataKey(submissionID), payload, overwrite)
}

// GetInfo reads the job info for a submission id, or nil when absent.
func (c *JobInfoStorageClient) GetInfo(ctx context.Context, submissionID string) (*JobInfo, error) {
	reply, err := c.client.InternalKVGet(ctx, kvNamespaceJob, jobDataKey(submissionID))
	if err != nil {
		return nil, err
	}
	if len(reply.Value) == 0 {
		return nil, nil
	}
	info := &JobInfo{}
	if err := json.Unmarshal(reply.Value, info); err != nil {
		return nil, fmt.Errorf("unmarshal job info for %s: %w", submissionID, err)
	}
	return info, nil
}

// DeleteInfo removes the job info for a submission id.
func (c *JobInfoStorageClient) DeleteInfo(ctx context.Context, submissionID string) error {
	_, err := c.client.InternalKVDel(ctx, kvNamespaceJob, jobDataKey(submissionID), false)
	return err
}

// OrderedJob is a submission job paired with its submission id, in the GCS
// internal KV scan order returned by InternalKVKeys.
type OrderedJob struct {
	SubmissionID string
	Info         *JobInfo
}

// GetAllJobsOrdered returns every submission job in the GCS internal KV scan
// order, each paired with its submission id, aligned with
// JobInfoStorageClient.get_all_jobs(): it lists the keys under
// JOB_DATA_KEY_PREFIX and fetches each one. The scan order is preserved so the
// /api/jobs/ list matches the Python dashboard, which iterates the dict built
// from async_internal_kv_keys in order.
func (c *JobInfoStorageClient) GetAllJobsOrdered(ctx context.Context) ([]OrderedJob, error) {
	keys, err := c.client.InternalKVKeys(ctx, kvNamespaceJob, jobDataKeyPrefix)
	if err != nil {
		return nil, err
	}
	jobs := make([]OrderedJob, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, jobDataKeyPrefix) {
			continue
		}
		submissionID := key[len(jobDataKeyPrefix):]
		info, err := c.GetInfo(ctx, submissionID)
		if err != nil {
			return nil, err
		}
		if info != nil {
			jobs = append(jobs, OrderedJob{SubmissionID: submissionID, Info: info})
		}
	}
	return jobs, nil
}

// GetAllJobs returns every submission job info keyed by submission id. The
// iteration order of the returned map is unspecified; callers that need the
// GCS KV scan order should use GetAllJobsOrdered.
func (c *JobInfoStorageClient) GetAllJobs(ctx context.Context) (map[string]*JobInfo, error) {
	jobs, err := c.GetAllJobsOrdered(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]*JobInfo, len(jobs))
	for _, j := range jobs {
		m[j.SubmissionID] = j.Info
	}
	return m, nil
}

// GetHeadNodeID reads the head node id from the GCS internal KV (namespace
// "job"), aligned with get_head_node_id in job/utils.py. Returns "" when not
// yet persisted.
func (c *JobInfoStorageClient) GetHeadNodeID(ctx context.Context) (string, error) {
	reply, err := c.client.InternalKVGet(ctx, kvNamespaceJob, kvHeadNodeIDKey)
	if err != nil {
		return "", err
	}
	return string(reply.Value), nil
}

// FetchAgentInfo reads the dashboard agent address of a node from the GCS
// internal KV (namespace "dashboard"), aligned with
// JobHead._fetch_agent_info. The value is a JSON array [ip, http_port,
// grpc_port]. It returns an error when the info is missing.
func (c *JobInfoStorageClient) FetchAgentInfo(ctx context.Context, nodeIDHex string) (ip string, httpPort int, err error) {
	key := dashboardAgentAddrNodeIDPrefix + nodeIDHex
	reply, err := c.client.InternalKVGet(ctx, kvNamespaceDashboard, key)
	if err != nil {
		return "", 0, err
	}
	if len(reply.Value) == 0 {
		return "", 0, fmt.Errorf("agent info not found in internal KV for node %s", nodeIDHex)
	}
	var info [3]json.RawMessage
	if err := json.Unmarshal(reply.Value, &info); err != nil {
		return "", 0, fmt.Errorf("unmarshal agent info for node %s: %w", nodeIDHex, err)
	}
	var ipStr string
	if err := json.Unmarshal(info[0], &ipStr); err != nil {
		return "", 0, fmt.Errorf("unmarshal agent ip for node %s: %w", nodeIDHex, err)
	}
	var port int
	if err := json.Unmarshal(info[1], &port); err != nil {
		return "", 0, fmt.Errorf("unmarshal agent http port for node %s: %w", nodeIDHex, err)
	}
	return ipStr, port, nil
}

// PinRuntimeEnvURI pins a runtime_env URI in the GCS for expiration_s seconds
// via the RuntimeEnvGcsService, aligned with
// gcs_client.pin_runtime_env_uri. A non-positive expiration is a no-op.
func (c *JobInfoStorageClient) PinRuntimeEnvURI(ctx context.Context, uri string, expirationS int32) error {
	if expirationS <= 0 {
		return nil
	}
	svc := c.client.RuntimeEnv()
	_, err := svc.PinRuntimeEnvURI(ctx, &proto.PinRuntimeEnvURIRequest{Uri: uri, ExpirationS: expirationS})
	return err
}
