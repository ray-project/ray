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

// Package job implements the JobHead dashboard module that exposes the Ray Jobs
// API (submit/stop/delete/list/logs + log tailing WebSocket) and the
// component-activity endpoint, aligned with the Python JobHead in
// python/ray/dashboard/modules/job/job_head.py.
package job

// JobStatus mirrors the Python JobStatus enum (job/common.py). The terminal
// statuses are STOPPED, SUCCEEDED and FAILED.
type JobStatus string

const (
	// JobStatusPending is JobStatus.PENDING.
	JobStatusPending JobStatus = "PENDING"
	// JobStatusRunning is JobStatus.RUNNING.
	JobStatusRunning JobStatus = "RUNNING"
	// JobStatusStopped is JobStatus.STOPPED.
	JobStatusStopped JobStatus = "STOPPED"
	// JobStatusSucceeded is JobStatus.SUCCEEDED.
	JobStatusSucceeded JobStatus = "SUCCEEDED"
	// JobStatusFailed is JobStatus.FAILED.
	JobStatusFailed JobStatus = "FAILED"
)

// IsTerminal reports whether the status is terminal (STOPPED/SUCCEEDED/FAILED),
// aligned with JobStatus.is_terminal().
func (s JobStatus) IsTerminal() bool {
	return s == JobStatusStopped || s == JobStatusSucceeded || s == JobStatusFailed
}

// JobType mirrors the Python JobType enum (job/pydantic_models.py).
type JobType string

const (
	// JobTypeSubmission is JobType.SUBMISSION.
	JobTypeSubmission JobType = "SUBMISSION"
	// JobTypeDriver is JobType.DRIVER.
	JobTypeDriver JobType = "DRIVER"
)

// DriverInfo mirrors the Python DriverInfo pydantic model.
type DriverInfo struct {
	ID            string `json:"id"`
	NodeIPAddress string `json:"node_ip_address"`
	PID           string `json:"pid"`
}

// JobDetails mirrors the Python JobDetails pydantic model. The JSON field
// names match pydantic's field names exactly (snake_case).
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

// JobInfo mirrors the Python JobInfo dataclass stored in the GCS internal KV
// under the JOB_DATA_KEY. JSON field names match JobInfo.to_json().
type JobInfo struct {
	Status                 JobStatus          `json:"status"`
	Entrypoint             string             `json:"entrypoint"`
	Message                *string            `json:"message"`
	ErrorType              *string            `json:"error_type"`
	StartTime              *int64             `json:"start_time"`
	EndTime                *int64             `json:"end_time"`
	Metadata               map[string]string  `json:"metadata"`
	RuntimeEnvJSON         *string            `json:"runtime_env_json"`
	EntrypointNumCPUs      *float64           `json:"entrypoint_num_cpus"`
	EntrypointNumGPUs      *float64           `json:"entrypoint_num_gpus"`
	EntrypointMemory       *uint64            `json:"entrypoint_memory"`
	EntrypointResources    map[string]float64 `json:"entrypoint_resources"`
	DriverAgentHTTPAddress *string            `json:"driver_agent_http_address"`
	DriverNodeID           *string            `json:"driver_node_id"`
	DriverExitCode         *int32             `json:"driver_exit_code"`
}

// JobSubmitRequest mirrors the Python JobSubmitRequest dataclass.
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

// JobSubmitResponse mirrors the Python JobSubmitResponse dataclass.
type JobSubmitResponse struct {
	JobID        string `json:"job_id"`
	SubmissionID string `json:"submission_id"`
}

// JobStopResponse mirrors the Python JobStopResponse dataclass.
type JobStopResponse struct {
	Stopped bool `json:"stopped"`
}

// JobDeleteResponse mirrors the Python JobDeleteResponse dataclass.
type JobDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

// JobLogsResponse mirrors the Python JobLogsResponse dataclass.
type JobLogsResponse struct {
	Logs string `json:"logs"`
}

// VersionResponse mirrors the Python VersionResponse dataclass
// (python/ray/dashboard/modules/version.py).
type VersionResponse struct {
	Version     string `json:"version"`
	RayVersion  string `json:"ray_version"`
	RayCommit   string `json:"ray_commit"`
	SessionName string `json:"session_name"`
}

// RayActivityStatus mirrors the Python RayActivityStatus enum.
type RayActivityStatus string

const (
	// RayActivityActive is RayActivityStatus.ACTIVE.
	RayActivityActive RayActivityStatus = "ACTIVE"
	// RayActivityInactive is RayActivityStatus.INACTIVE.
	RayActivityInactive RayActivityStatus = "INACTIVE"
	// RayActivityError is RayActivityStatus.ERROR.
	RayActivityError RayActivityStatus = "ERROR"
)

// RayActivityResponse mirrors the Python RayActivityResponse pydantic model.
type RayActivityResponse struct {
	IsActive       RayActivityStatus `json:"is_active"`
	Reason         *string           `json:"reason"`
	Timestamp      float64           `json:"timestamp"`
	LastActivityAt *float64          `json:"last_activity_at"`
}
