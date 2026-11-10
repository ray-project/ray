// Package jobutil holds the job table conversions shared by the dashboard
// head and agent job modules. The helpers mirror the Python job/common.py and
// job/utils.py semantics.
package jobutil

import (
	"encoding/json"

	"github.com/ray-project/ray/go/proto"
)

// RayInternalNamespacePrefix is RAY_INTERNAL_NAMESPACE_PREFIX from
// job/common.py: jobs in a namespace with this prefix are dashboard-internal
// and are hidden from the job APIs.
const RayInternalNamespacePrefix = "_ray_internal_"

// JobIDMetadataKey is JOB_ID_METADATA_KEY from job/common.py. When a job's
// config metadata carries it, the value is the id of the submission job the
// driver belongs to.
const JobIDMetadataKey = "job_submission_id"

// PtrString returns a pointer to a copy of s.
func PtrString(s string) *string { return &s }

// Int64Ptr returns a pointer to a copy of v.
func Int64Ptr(v int64) *int64 { return &v }

// RuntimeEnvToDict converts the serialized runtime_env JSON string into the
// dict form exposed by RuntimeEnv.to_dict() (python/ray/runtime_env/
// runtime_env.py). An empty serialized env yields an empty dict.
func RuntimeEnvToDict(serialized string) map[string]interface{} {
	out := map[string]interface{}{}
	if serialized == "" {
		return out
	}
	_ = json.Unmarshal([]byte(serialized), &out)
	return out
}

// RuntimeEnvSerialized extracts the serialized runtime env from the job
// config, handling a nil RuntimeEnvInfo. The entry itself must be non-nil: it
// is always a row returned by a job listing.
func RuntimeEnvSerialized(entry *proto.JobTableData) string {
	if entry.Config == nil || entry.Config.RuntimeEnvInfo == nil {
		return ""
	}
	return entry.Config.RuntimeEnvInfo.SerializedRuntimeEnv
}

// RuntimeEnvFromJSON converts the runtime_env_json string stored in the KV
// into a dict, aligned with JobInfo.from_json(): when the stored info has no
// runtime_env_json field the runtime_env stays None, otherwise it is parsed.
func RuntimeEnvFromJSON(serialized *string) map[string]interface{} {
	if serialized == nil {
		return nil
	}
	out := map[string]interface{}{}
	_ = json.Unmarshal([]byte(*serialized), &out)
	return out
}

// DriverIP returns the driver node IP address, preferring the structured
// driver_address field (aligned with the Python driver_address.ip_address).
func DriverIP(entry *proto.JobTableData) string {
	if entry.DriverAddress != nil && entry.DriverAddress.IpAddress != "" {
		return entry.DriverAddress.IpAddress
	}
	return entry.DriverIpAddress
}
