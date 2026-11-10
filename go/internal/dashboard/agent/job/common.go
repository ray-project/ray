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

// Package job implements the dashboard agent's job module: it stores job
// infos in the GCS internal KV, keeps the driver logs on disk, drives Python
// job supervisors, and serves the job agent REST API. The behavior mirrors
// python/ray/dashboard/modules/job/common.py, job_manager.py and
// job_supervisor.py.
package job

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/jobutil"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
)

// KV layout aligned with python/ray/dashboard/modules/job/common.py.
const (
	// kvNamespaceJob is KV_NAMESPACE_JOB.
	kvNamespaceJob = "job"
	// kvHeadNodeIDKey is KV_HEAD_NODE_ID_KEY.
	kvHeadNodeIDKey = "head_node_id"
	// jobDataKeyPrefix is JobInfoStorageClient.JOB_DATA_KEY_PREFIX, shared with
	// the dashboard head through jobutil so both sides address the same keys.
	jobDataKeyPrefix = jobutil.RayInternalNamespacePrefix + "job_info_"
)

const (
	// JobNameMetadataKey is KV_JOB_NAME_METADATA_KEY.
	JobNameMetadataKey = "job_name"
	// JobActorNameTemplate is the supervisor actor name template.
	JobActorNameTemplate = jobutil.RayInternalNamespacePrefix + "job_actor_{job_id}"
	// SupervisorActorRayNamespace is the Ray namespace the supervisors live in.
	SupervisorActorRayNamespace = "SUPERVISOR_ACTOR_RAY_NAMESPACE"
	// JobLogsPathTemplate is the driver log file name template.
	JobLogsPathTemplate = "job-driver-{submission_id}.log"
)

func jobActorName(jobID string) string {
	return strings.ReplaceAll(JobActorNameTemplate, "{job_id}", jobID)
}

func jobLogsPath(submissionID string) string {
	return strings.ReplaceAll(JobLogsPathTemplate, "{submission_id}", submissionID)
}

// JobStatus is the status of a submission job, aligned with the Python
// JobStatus enum.
type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusStopped   JobStatus = "STOPPED"
	JobStatusSucceeded JobStatus = "SUCCEEDED"
	JobStatusFailed    JobStatus = "FAILED"
)

// IsTerminal reports whether no further status transition is allowed.
func (s JobStatus) IsTerminal() bool {
	return s == JobStatusStopped || s == JobStatusSucceeded || s == JobStatusFailed
}

// JobErrorType classifies why a job ended up in a failure state.
type JobErrorType string

const (
	JobErrorTypeRuntimeEnvSetupFailure           JobErrorType = "RUNTIME_ENV_SETUP_FAILURE"
	JobErrorTypeJobSupervisorActorStartTimeout   JobErrorType = "JOB_SUPERVISOR_ACTOR_START_TIMEOUT"
	JobErrorTypeJobSupervisorActorStartFailure   JobErrorType = "JOB_SUPERVISOR_ACTOR_START_FAILURE"
	JobErrorTypeJobSupervisorActorUnschedulable  JobErrorType = "JOB_SUPERVISOR_ACTOR_UNSCHEDULABLE"
	JobErrorTypeJobSupervisorActorUnknownFailure JobErrorType = "JOB_SUPERVISOR_ACTOR_UNKNOWN_FAILURE"
	JobErrorTypeJobSupervisorActorDied           JobErrorType = "JOB_SUPERVISOR_ACTOR_DIED"
	JobErrorTypeJobEntrypointCommandStartError   JobErrorType = "JOB_ENTRYPOINT_COMMAND_START_ERROR"
	JobErrorTypeJobEntrypointCommandError        JobErrorType = "JOB_ENTRYPOINT_COMMAND_ERROR"
)

// JobType distinguishes a driver job (started by `ray start` or a script) from
// a job submitted through the job API.
type JobType string

const (
	JobTypeSubmission JobType = "SUBMISSION"
	JobTypeDriver     JobType = "DRIVER"
)

// DriverInfo describes the driver process of a job.
type DriverInfo struct {
	ID            string `json:"id"`
	NodeIPAddress string `json:"node_ip_address"`
	PID           string `json:"pid"`
}

// JobDetails is the job view returned by the job APIs, aligned with the Python
// JobDetails dataclass.
type JobDetails struct {
	Type                   JobType                `json:"type"`
	JobID                  *string                `json:"job_id"`
	SubmissionID           *string                `json:"submission_id"`
	DriverInfo             *DriverInfo            `json:"driver_info"`
	Status                 JobStatus              `json:"status"`
	Entrypoint             string                 `json:"entrypoint"`
	Message                *string                `json:"message"`
	ErrorType              *string                `json:"error_type"`
	StartTime              *int64                 `json:"start_time"`
	EndTime                *int64                 `json:"end_time"`
	Metadata               map[string]string      `json:"metadata"`
	RuntimeEnv             map[string]interface{} `json:"runtime_env"`
	DriverAgentHTTPAddress *string                `json:"driver_agent_http_address"`
	DriverNodeID           *string                `json:"driver_node_id"`
	DriverExitCode         *int32                 `json:"driver_exit_code"`
}

// JobInfo is the record persisted in the internal KV for a submission job.
type JobInfo struct {
	Status                 JobStatus          `json:"status"`
	Entrypoint             string             `json:"entrypoint"`
	Message                *string            `json:"message"`
	ErrorType              *string            `json:"error_type"`
	StartTime              *int64             `json:"start_time"`
	EndTime                *int64             `json:"end_time"`
	Metadata               map[string]string  `json:"metadata"`
	RuntimeEnvJSON         *string            `json:"runtime_env_json,omitempty"`
	EntrypointNumCPUs      *float64           `json:"entrypoint_num_cpus"`
	EntrypointNumGPUs      *float64           `json:"entrypoint_num_gpus"`
	EntrypointMemory       *uint64            `json:"entrypoint_memory"`
	EntrypointResources    map[string]float64 `json:"entrypoint_resources"`
	DriverAgentHTTPAddress *string            `json:"driver_agent_http_address"`
	DriverNodeID           *string            `json:"driver_node_id"`
	DriverExitCode         *int32             `json:"driver_exit_code"`
}

// JobSubmitRequest is the parsed body of POST /api/job_agent/jobs.
type JobSubmitRequest struct {
	Entrypoint          string                 `json:"entrypoint"`
	SubmissionID        *string                `json:"submission_id"`
	JobID               *string                `json:"job_id"`
	RuntimeEnv          map[string]interface{} `json:"runtime_env"`
	Metadata            map[string]string      `json:"metadata"`
	EntrypointNumCPUs   *float64               `json:"entrypoint_num_cpus"`
	EntrypointNumGPUs   *float64               `json:"entrypoint_num_gpus"`
	EntrypointMemory    *uint64                `json:"entrypoint_memory"`
	EntrypointResources map[string]float64     `json:"entrypoint_resources"`
}

type JobSubmitResponse struct {
	JobID        string `json:"job_id"`
	SubmissionID string `json:"submission_id"`
}

type JobStopResponse struct {
	Stopped bool `json:"stopped"`
}

type JobDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

type JobLogsResponse struct {
	Logs string `json:"logs"`
}

func ptrString(s string) *string { return &s }

func ptrInt64(v int64) *int64 { return &v }

func ptrFloat64(v float64) *float64 { return &v }

func ptrUint64(v uint64) *uint64 { return &v }

// JobInfoStorageClient persists JobInfo records in the GCS internal KV under
// the "job" namespace.
type JobInfoStorageClient struct {
	client gcs.Client
}

func NewJobInfoStorageClient(client gcs.Client) *JobInfoStorageClient {
	return &JobInfoStorageClient{client: client}
}

// jobDataKeyFor returns the internal KV key holding the JobInfo of a
// submission id.
func jobDataKeyFor(submissionID string) string {
	return jobDataKeyPrefix + submissionID
}

// PutInfo writes the job info. With overwrite false an existing record is left
// untouched and the returned bool is false.
func (c *JobInfoStorageClient) PutInfo(ctx context.Context, submissionID string, info *JobInfo, overwrite bool) (bool, error) {
	payload, err := json.Marshal(info)
	if err != nil {
		return false, fmt.Errorf("marshal job info: %w", err)
	}
	return c.client.Put(ctx, kvNamespaceJob, jobDataKeyFor(submissionID), payload, overwrite)
}

// GetInfo reads the job info, returning (nil, nil) when the record is absent.
func (c *JobInfoStorageClient) GetInfo(ctx context.Context, submissionID string) (*JobInfo, error) {
	value, err := c.client.Get(ctx, kvNamespaceJob, jobDataKeyFor(submissionID))
	if err == gcs.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}
	info := &JobInfo{}
	if err := json.Unmarshal(value, info); err != nil {
		return nil, fmt.Errorf("unmarshal job info for %s: %w", submissionID, err)
	}
	return info, nil
}

// DeleteInfo removes the job info. Deleting an absent record is not an error.
func (c *JobInfoStorageClient) DeleteInfo(ctx context.Context, submissionID string) error {
	_, err := c.client.Del(ctx, kvNamespaceJob, jobDataKeyFor(submissionID), false)
	return err
}

// PutStatus updates the status fields of a job info, creating the record when
// it does not exist yet. A job in a terminal state cannot leave it; the update
// then fails instead of overwriting the terminal status.
func (c *JobInfoStorageClient) PutStatus(ctx context.Context, jobID string, status JobStatus, message *string, driverExitCode *int32, errorType *JobErrorType) error {
	oldInfo, err := c.GetInfo(ctx, jobID)
	if err != nil {
		return err
	}

	var newInfo *JobInfo
	if oldInfo != nil {
		if status != oldInfo.Status && oldInfo.Status.IsTerminal() {
			return fmt.Errorf("attempted to change job status from a terminal state")
		}
		newInfo = oldInfo
	} else {
		ep := "Entrypoint not found."
		newInfo = &JobInfo{Entrypoint: ep}
	}

	newInfo.Status = status
	newInfo.Message = message
	newInfo.DriverExitCode = driverExitCode
	if errorType != nil {
		et := string(*errorType)
		newInfo.ErrorType = &et
	} else {
		newInfo.ErrorType = nil
	}

	if newInfo.Message == nil {
		newInfo.Message = defaultMessage(newInfo, status)
	}

	if status.IsTerminal() {
		now := time.Now().UnixMilli()
		newInfo.EndTime = &now
	}

	_, err = c.PutInfo(ctx, jobID, newInfo, true)
	return err
}

// defaultMessage returns the status message shown when the caller supplies
// none, aligned with JobInfo.get_default_message.
func defaultMessage(info *JobInfo, status JobStatus) *string {
	switch status {
	case JobStatusPending:
		msg := "Job has not started yet."
		resourcesSpecified := (info.EntrypointNumCPUs != nil && *info.EntrypointNumCPUs > 0) ||
			(info.EntrypointNumGPUs != nil && *info.EntrypointNumGPUs > 0) ||
			(info.EntrypointMemory != nil && *info.EntrypointMemory > 0) ||
			len(info.EntrypointResources) > 0
		if resourcesSpecified {
			msg += " It may be waiting for resources (CPUs, GPUs, memory, custom resources) to become available."
		}
		if info.RuntimeEnvJSON != nil && *info.RuntimeEnvJSON != "" && *info.RuntimeEnvJSON != "{}" {
			msg += " It may be waiting for the runtime environment to be set up."
		}
		return &msg
	case JobStatusRunning:
		return ptrString("Job is currently running.")
	case JobStatusStopped:
		return ptrString("Job was intentionally stopped.")
	case JobStatusSucceeded:
		return ptrString("Job finished successfully.")
	case JobStatusFailed:
		return ptrString("Job failed.")
	default:
		return nil
	}
}

// GetStatus reads only the status, returning nil when the job info is absent.
func (c *JobInfoStorageClient) GetStatus(ctx context.Context, jobID string) (*JobStatus, error) {
	info, err := c.GetInfo(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	return &info.Status, nil
}

// GetAllJobs returns every stored job info keyed by submission id.
func (c *JobInfoStorageClient) GetAllJobs(ctx context.Context) (map[string]*JobInfo, error) {
	keys, err := c.client.Keys(ctx, kvNamespaceJob, jobDataKeyPrefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*JobInfo, len(keys))
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
			out[submissionID] = info
		}
	}
	return out, nil
}

// GetHeadNodeID returns the id of the head node as published in the internal
// KV, or an empty string when the key is absent.
//
// The value is validated as a node id hex string before it is returned: it is
// the only input the agent feeds to a strict native decoder, and the C++ side
// reacts to a malformed id by throwing out of an extern "C" boundary or
// aborting on a length check, which kills the agent. A value that does not
// decode is treated like an absent key, so the caller falls back to default
// scheduling instead.
func (c *JobInfoStorageClient) GetHeadNodeID(ctx context.Context) (string, error) {
	value, err := c.client.Get(ctx, kvNamespaceJob, kvHeadNodeIDKey)
	if err == gcs.ErrKeyNotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	headNodeID := strings.TrimSpace(string(value))
	if headNodeID == "" {
		return "", nil
	}
	if _, err := ids.NodeIDFromHex(headNodeID); err != nil {
		log.Log.Error(err, "ignoring a malformed head node id in the internal KV; using default scheduling for the job driver",
			"value", headNodeID)
		return "", nil
	}
	return headNodeID, nil
}
