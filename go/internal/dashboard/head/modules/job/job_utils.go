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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
)

// buildAddress formats a host and port into "host:port", wrapping IPv6
// addresses in brackets, aligned with the C++ BuildAddress
// (src/ray/util/network_util.cc).
func buildAddress(host string, port int) string {
	if strings.Contains(host, ":") {
		return fmt.Sprintf("[%s]:%d", host, port)
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// ptrString returns a pointer to a copy of s.
func ptrString(s string) *string { return &s }

// runtimeEnvToDict converts the serialized runtime_env JSON string into the
// dict form exposed by RuntimeEnv.to_dict() (python/ray/runtime_env/
// runtime_env.py). An empty serialized env yields an empty dict.
func runtimeEnvToDict(serialized string) map[string]interface{} {
	out := map[string]interface{}{}
	if serialized == "" {
		return out
	}
	_ = json.Unmarshal([]byte(serialized), &out)
	return out
}

// getDriverJobs queries all driver jobs and returns (driverJobs, submissionDrivers),
// aligned with get_driver_jobs in job/utils.py.
//
// driverJobs is keyed by job id and holds every non-internal driver job;
// submissionDrivers is keyed by the submission id of the job whose driver is
// running, holding only the last driver of each submission job.
func GetDriverJobs(ctx context.Context, client *head.GCSClient, jobOrSubmissionID string) (map[string]*JobDetails, map[string]*DriverInfo, error) {
	req := &proto.GetAllJobInfoRequest{
		SkipSubmissionJobInfoField: boolPtr(true),
		SkipIsRunningTasksField:    boolPtr(true),
	}
	if jobOrSubmissionID != "" {
		req.JobOrSubmissionId = ptrString(jobOrSubmissionID)
	}
	reply, err := client.GetAllJobInfo(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("get all job info: %w", err)
	}

	sorted := make([]*proto.JobTableData, len(reply.JobInfoList))
	copy(sorted, reply.JobInfoList)
	// Sort by job id hex to return only the last driver of a submission job,
	// aligned with the Python sorting convention.
	sort.Slice(sorted, func(i, j int) bool {
		return hex.EncodeToString(sorted[i].JobId) < hex.EncodeToString(sorted[j].JobId)
	})

	jobs := map[string]*JobDetails{}
	submissionDrivers := map[string]*DriverInfo{}
	for _, entry := range sorted {
		if entry.Config == nil || strings.HasPrefix(entry.Config.RayNamespace, rayInternalNamespacePrefix) {
			// Skip jobs in any _ray_internal_ namespace.
			continue
		}
		jobID := hex.EncodeToString(entry.JobId)
		metadata := entry.Config.Metadata
		submissionID := metadata[jobIDMetadataKey]
		if submissionID == "" {
			driver := &DriverInfo{
				ID:            jobID,
				NodeIPAddress: driverIP(entry),
				PID:           strconv.FormatInt(entry.DriverPid, 10),
			}
			status := JobStatusRunning
			if entry.IsDead {
				status = JobStatusSucceeded
			}
			// Python passes dict(job_table_entry.config.metadata), which is
			// always a dict; normalize a nil proto map to an empty dict so the
			// JSON output is {} rather than null.
			if metadata == nil {
				metadata = map[string]string{}
			}
			jobs[jobID] = &JobDetails{
				Type:       JobTypeDriver,
				JobID:      ptrString(jobID),
				Status:     status,
				Entrypoint: entry.Entrypoint,
				StartTime:  int64Ptr(int64(entry.StartTime)),
				EndTime:    int64Ptr(int64(entry.EndTime)),
				Metadata:   metadata,
				RuntimeEnv: runtimeEnvToDict(runtimeEnvSerialized(entry)),
				DriverInfo: driver,
			}
		} else {
			submissionDrivers[submissionID] = &DriverInfo{
				ID:            jobID,
				NodeIPAddress: driverIP(entry),
				PID:           strconv.FormatInt(entry.DriverPid, 10),
			}
		}
	}
	return jobs, submissionDrivers, nil
}

// boolPtr returns a pointer to a copy of b.
func boolPtr(b bool) *bool { return &b }

// int64Ptr returns a pointer to a copy of v.
func int64Ptr(v int64) *int64 { return &v }

// driverIP returns the driver node IP address, preferring the structured
// driver_address field (aligned with the Python driver_address.ip_address).
func driverIP(entry *proto.JobTableData) string {
	if entry.DriverAddress != nil && entry.DriverAddress.IpAddress != "" {
		return entry.DriverAddress.IpAddress
	}
	return entry.DriverIpAddress
}

// runtimeEnvSerialized extracts the serialized runtime env from the job
// config, handling a nil RuntimeEnvInfo.
func runtimeEnvSerialized(entry *proto.JobTableData) string {
	if entry.Config == nil || entry.Config.RuntimeEnvInfo == nil {
		return ""
	}
	return entry.Config.RuntimeEnvInfo.SerializedRuntimeEnv
}

// findJobByIDs finds a job by either its job id or its submission id, aligned
// with find_job_by_ids in job/utils.py. It first tries the job id (driver
// job), then a driver of a submission job, then the submission id in the KV.
func findJobByIDs(ctx context.Context, client *head.GCSClient, infoClient *JobInfoStorageClient, jobOrSubmissionID string) (*JobDetails, error) {
	driverJobs, submissionDrivers, err := GetDriverJobs(ctx, client, jobOrSubmissionID)
	if err != nil {
		return nil, err
	}
	if job := driverJobs[jobOrSubmissionID]; job != nil {
		return job, nil
	}
	submissionID := ""
	for id, driver := range submissionDrivers {
		if driver.ID == jobOrSubmissionID {
			submissionID = id
			break
		}
	}
	if submissionID == "" {
		submissionID = jobOrSubmissionID
	}
	info, err := infoClient.GetInfo(ctx, submissionID)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	var driver *DriverInfo
	if d := submissionDrivers[submissionID]; d != nil {
		driver = d
	}
	return SubmissionJobDetails(submissionID, info, driver), nil
}

// submissionJobDetails assembles a submission JobDetails from the KV job info
// and the driver, aligned with the JobDetails(**asdict(job_info), ...)
// construction in the Python find_job_by_ids.
func SubmissionJobDetails(submissionID string, info *JobInfo, driver *DriverInfo) *JobDetails {
	var jobID *string
	if driver != nil {
		jobID = ptrString(driver.ID)
	}
	return &JobDetails{
		Type:                   JobTypeSubmission,
		JobID:                  jobID,
		SubmissionID:           ptrString(submissionID),
		DriverInfo:             driver,
		Status:                 info.Status,
		Entrypoint:             info.Entrypoint,
		Message:                info.Message,
		ErrorType:              info.ErrorType,
		StartTime:              info.StartTime,
		EndTime:                info.EndTime,
		Metadata:               info.Metadata,
		RuntimeEnv:             runtimeEnvFromJSON(info.RuntimeEnvJSON),
		DriverAgentHTTPAddress: info.DriverAgentHTTPAddress,
		DriverNodeID:           info.DriverNodeID,
		DriverExitCode:         info.DriverExitCode,
	}
}

// runtimeEnvFromJSON converts the runtime_env_json string stored in the KV
// into a dict, aligned with JobInfo.from_json(): when the stored info has no
// runtime_env_json field the runtime_env stays None, otherwise it is parsed.
func runtimeEnvFromJSON(serialized *string) map[string]interface{} {
	if serialized == nil {
		return nil
	}
	out := map[string]interface{}{}
	_ = json.Unmarshal([]byte(*serialized), &out)
	return out
}

// BuildListJobsDetails queries all driver and submission jobs and returns them
// as a single JobDetails list, aligned with list_jobs in
// python/ray/dashboard/modules/job/job_head.py: the submission jobs first (in
// the GCS KV scan order passed in), then the driver jobs. submissionJobs may be
// empty.
func BuildListJobsDetails(ctx context.Context, client *head.GCSClient, submissionJobs []OrderedJob) ([]*JobDetails, error) {
	driverJobs, submissionDrivers, err := GetDriverJobs(ctx, client, "")
	if err != nil {
		return nil, err
	}
	items := make([]*JobDetails, 0, len(submissionJobs)+len(driverJobs))
	for _, sj := range submissionJobs {
		items = append(items, SubmissionJobDetails(sj.SubmissionID, sj.Info, submissionDrivers[sj.SubmissionID]))
	}
	for _, job := range driverJobs {
		items = append(items, job)
	}
	return items, nil
}

// JobDetailsToDict converts a JobDetails into the dict form returned by the
// state API, aligned with pydantic's JobDetails.dict(). Pointer fields are
// emitted only when non-nil; the runtime_env is emitted as the parsed dict.
func JobDetailsToDict(jd *JobDetails) map[string]interface{} {
	out := map[string]interface{}{
		"type":       jd.Type,
		"status":     jd.Status,
		"entrypoint": jd.Entrypoint,
	}
	if jd.JobID != nil {
		out["job_id"] = *jd.JobID
	}
	if jd.SubmissionID != nil {
		out["submission_id"] = *jd.SubmissionID
	}
	if jd.Message != nil {
		out["message"] = *jd.Message
	}
	if jd.ErrorType != nil {
		out["error_type"] = *jd.ErrorType
	}
	if jd.StartTime != nil {
		out["start_time"] = *jd.StartTime
	}
	if jd.EndTime != nil {
		out["end_time"] = *jd.EndTime
	}
	if jd.Metadata != nil {
		out["metadata"] = jd.Metadata
	} else {
		out["metadata"] = map[string]string{}
	}
	if jd.RuntimeEnv != nil {
		out["runtime_env"] = jd.RuntimeEnv
	}
	if jd.DriverInfo != nil {
		out["driver_info"] = map[string]interface{}{
			"id":              jd.DriverInfo.ID,
			"node_ip_address": jd.DriverInfo.NodeIPAddress,
			"pid":             jd.DriverInfo.PID,
		}
	}
	if jd.DriverAgentHTTPAddress != nil {
		out["driver_agent_http_address"] = *jd.DriverAgentHTTPAddress
	}
	if jd.DriverNodeID != nil {
		out["driver_node_id"] = *jd.DriverNodeID
	}
	if jd.DriverExitCode != nil {
		out["driver_exit_code"] = *jd.DriverExitCode
	}
	return out
}
