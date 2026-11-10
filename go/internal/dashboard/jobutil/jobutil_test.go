package jobutil

import (
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// TestConstantsDriftGuard pins the two exported constants to the Python values
// they mirror: RAY_INTERNAL_NAMESPACE_PREFIX and JOB_ID_METADATA_KEY from
// python/ray/dashboard/modules/job/common.py. The dashboard agent job module
// (go/internal/dashboard/agent/job) composes its internal KV keys from these
// constants, so a change here silently rewrites the cross-language key layout.
// The agent-side TestJobutilConstantsDriftGuard asserts the same values from
// the consuming side.
func TestConstantsDriftGuard(t *testing.T) {
	assert.Equal(t, "_ray_internal_", RayInternalNamespacePrefix)
	assert.Equal(t, "job_submission_id", JobIDMetadataKey)
}

func TestPtrString(t *testing.T) {
	assert.Equal(t, "x", *PtrString("x"))
}

func TestInt64Ptr(t *testing.T) {
	assert.Equal(t, int64(7), *Int64Ptr(7))
}

func TestRuntimeEnvToDict(t *testing.T) {
	assert.Equal(t, map[string]interface{}{}, RuntimeEnvToDict(""))
	assert.Equal(t, map[string]interface{}{}, RuntimeEnvToDict("not-json"))
	got := RuntimeEnvToDict(`{"pip":["x"],"n":2}`)
	assert.Equal(t, map[string]interface{}{"pip": []interface{}{"x"}, "n": 2.0}, got)
}

func TestRuntimeEnvSerialized(t *testing.T) {
	entry := &proto.JobTableData{
		Config: &proto.JobConfig{
			RuntimeEnvInfo: &proto.RuntimeEnvInfo{SerializedRuntimeEnv: `{"a":1}`},
		},
	}
	assert.Equal(t, `{"a":1}`, RuntimeEnvSerialized(entry))

	// A missing job config, a missing runtime env info, and an empty serialized
	// env all yield the empty string.
	assert.Equal(t, "", RuntimeEnvSerialized(&proto.JobTableData{}))
	assert.Equal(t, "", RuntimeEnvSerialized(&proto.JobTableData{Config: &proto.JobConfig{}}))
	assert.Equal(t, "", RuntimeEnvSerialized(&proto.JobTableData{
		Config: &proto.JobConfig{RuntimeEnvInfo: &proto.RuntimeEnvInfo{}},
	}))

	// The entry itself must be non-nil: every caller passes a row returned by a
	// job listing, so the helper keeps the non-nil contract of the Python
	// counterpart instead of silently accepting nil.
	assert.Panics(t, func() { RuntimeEnvSerialized(nil) })
}

func TestRuntimeEnvFromJSON(t *testing.T) {
	assert.Nil(t, RuntimeEnvFromJSON(nil))
	empty := "{}"
	assert.Equal(t, map[string]interface{}{}, RuntimeEnvFromJSON(&empty))
	val := `{"k":"v"}`
	got := RuntimeEnvFromJSON(&val)
	assert.Equal(t, map[string]interface{}{"k": "v"}, got)
}

func TestDriverIP(t *testing.T) {
	// DriverAddress takes precedence.
	entry := &proto.JobTableData{
		DriverIpAddress: "1.1.1.1",
		DriverAddress:   &proto.Address{IpAddress: "2.2.2.2"},
	}
	assert.Equal(t, "2.2.2.2", DriverIP(entry))

	// Falls back to DriverIpAddress when DriverAddress is empty/nil.
	entry2 := &proto.JobTableData{DriverIpAddress: "3.3.3.3"}
	assert.Equal(t, "3.3.3.3", DriverIP(entry2))

	entry3 := &proto.JobTableData{DriverAddress: &proto.Address{}}
	assert.Equal(t, "", DriverIP(entry3))

	assert.Equal(t, "", DriverIP(&proto.JobTableData{}))
}
