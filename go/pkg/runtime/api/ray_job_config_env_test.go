// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	protolib "google.golang.org/protobuf/proto"
)

// decodeJobConfig base64-decodes and unmarshals the serialized JobConfig that
// buildJobOptionsFromBuilder produced.
func decodeJobConfig(t *testing.T, jobOpts options.JobOptions) *proto.JobConfig {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(jobOpts.JobConfig)
	if err != nil {
		t.Fatalf("jobOpts.JobConfig is not valid base64: %v", err)
	}
	var config proto.JobConfig
	if err := protolib.Unmarshal(decoded, &config); err != nil {
		t.Fatalf("jobOpts.JobConfig is not a valid JobConfig protobuf: %v", err)
	}
	return &config
}

// TestBuildJobOptionsFromBuilderMergesJobRuntimeEnv proves that a builder
// passed to the explicit-job-config initialization path picks up the
// job-injected runtime_env carried in RAY_JOB_CONFIG_JSON_ENV_VAR. Without the
// merge, the driver's job config has an empty runtime_env_info, so task and
// actor workers on other nodes receive no working_dir or env_vars.
func TestBuildJobOptionsFromBuilderMergesJobRuntimeEnv(t *testing.T) {
	const jobRuntimeEnv = `{"working_dir":"gcs://_ray_pkg_test.zip","env_vars":{"FROM_JOB":"1"}}`
	t.Setenv(options.JobConfigJSONEnvVar,
		`{"runtime_env":`+jobRuntimeEnv+`,"metadata":{"job_key":"job_value"}}`)

	builder := options.NewJobConfigBuilder().
		WithCodeSearchPath("/tmp/ray/userfuncs.so")
	jobOpts, err := buildJobOptionsFromBuilder(builder)
	if err != nil {
		t.Fatalf("buildJobOptionsFromBuilder() error = %v", err)
	}

	config := decodeJobConfig(t, jobOpts)

	if len(config.CodeSearchPath) != 1 || config.CodeSearchPath[0] != "/tmp/ray/userfuncs.so" {
		t.Fatalf("CodeSearchPath = %v, want the caller-provided path preserved", config.CodeSearchPath)
	}
	if config.RuntimeEnvInfo == nil {
		t.Fatalf("RuntimeEnvInfo is nil, want the job-injected runtime_env merged in")
	}

	var re map[string]interface{}
	if err := json.Unmarshal([]byte(config.RuntimeEnvInfo.SerializedRuntimeEnv), &re); err != nil {
		t.Fatalf("SerializedRuntimeEnv is not valid JSON: %v", err)
	}
	if got := re["working_dir"]; got != "gcs://_ray_pkg_test.zip" {
		t.Fatalf("runtime_env working_dir = %v, want the job-injected working_dir", got)
	}
	envVars, ok := re["env_vars"].(map[string]interface{})
	if !ok || envVars["FROM_JOB"] != "1" {
		t.Fatalf("runtime_env env_vars = %v, want the job-injected env var present", re["env_vars"])
	}
	if config.Metadata["job_key"] != "job_value" {
		t.Fatalf("Metadata = %v, want the job-injected metadata merged in", config.Metadata)
	}
}

// TestBuildJobOptionsFromBuilderMergesCallerAndJobRuntimeEnv proves the merge
// is additive: a caller-provided runtime_env and the job-injected one are
// combined, with the caller's env_vars keys retained.
func TestBuildJobOptionsFromBuilderMergesCallerAndJobRuntimeEnv(t *testing.T) {
	t.Setenv(options.JobConfigJSONEnvVar,
		`{"runtime_env":{"working_dir":"gcs://_ray_pkg_test.zip","env_vars":{"FROM_JOB":"1"}}}`)

	builder := options.NewJobConfigBuilder().
		WithRuntimeEnv(`{"env_vars":{"FROM_CALLER":"2"}}`)
	jobOpts, err := buildJobOptionsFromBuilder(builder)
	if err != nil {
		t.Fatalf("buildJobOptionsFromBuilder() error = %v", err)
	}

	config := decodeJobConfig(t, jobOpts)
	if config.RuntimeEnvInfo == nil {
		t.Fatalf("RuntimeEnvInfo is nil, want a merged runtime_env")
	}

	var re map[string]interface{}
	if err := json.Unmarshal([]byte(config.RuntimeEnvInfo.SerializedRuntimeEnv), &re); err != nil {
		t.Fatalf("SerializedRuntimeEnv is not valid JSON: %v", err)
	}
	if got := re["working_dir"]; got != "gcs://_ray_pkg_test.zip" {
		t.Fatalf("runtime_env working_dir = %v, want the job-injected working_dir merged in", got)
	}
	envVars, ok := re["env_vars"].(map[string]interface{})
	if !ok {
		t.Fatalf("runtime_env env_vars = %v, want a merged env_vars map", re["env_vars"])
	}
	if envVars["FROM_JOB"] != "1" || envVars["FROM_CALLER"] != "2" {
		t.Fatalf("runtime_env env_vars = %v, want both caller and job env vars present", envVars)
	}
}

// TestBuildJobOptionsFromBuilderIsRepeatable proves the merge does not write the
// job-injected configuration back into the caller's builder. A caller that
// retries initialization with the same builder - after a transient failure, for
// example - must not be told it has a runtime_env conflict it never created.
// Before the merge was moved onto a copy, the first call stored the
// job-injected runtime_env in the builder and the second merged that value with
// itself, and checkConflict reports a shared key as a conflict whatever the
// values are.
func TestBuildJobOptionsFromBuilderIsRepeatable(t *testing.T) {
	t.Setenv(options.JobConfigJSONEnvVar,
		`{"runtime_env":{"working_dir":"gcs://_ray_pkg_test.zip","env_vars":{"FROM_JOB":"1"}},`+
			`"metadata":{"job_key":"job_value"}}`)

	builder := options.NewJobConfigBuilder().
		WithCodeSearchPath("/tmp/ray/userfuncs.so").
		// An explicit namespace keeps the generated default (a fresh UUID per
		// build) out of the comparison below.
		WithNamespace("repeatable-test")

	first, err := buildJobOptionsFromBuilder(builder)
	if err != nil {
		t.Fatalf("first buildJobOptionsFromBuilder() error = %v", err)
	}
	second, err := buildJobOptionsFromBuilder(builder)
	if err != nil {
		t.Fatalf("second buildJobOptionsFromBuilder() on the same builder error = %v", err)
	}
	if !reflect.DeepEqual(decodeJobConfig(t, first), decodeJobConfig(t, second)) {
		t.Fatalf("retrying with the same builder produced a different JobConfig:\nfirst  = %v\nsecond = %v",
			decodeJobConfig(t, first), decodeJobConfig(t, second))
	}
	if got := builder.RuntimeEnvJSON(); got != "" {
		t.Fatalf("builder.RuntimeEnvJSON() = %q after the call, want the caller's builder left untouched", got)
	}
}

// TestBuildJobOptionsFromBuilder_NilBuilder proves a nil builder is rejected
// with an error instead of panicking. Clone() dereferences its receiver, so a
// nil builder used to crash the caller; the guard lets InitializeWithJobConfig
// report it through its error return.
func TestBuildJobOptionsFromBuilder_NilBuilder(t *testing.T) {
	_, err := buildJobOptionsFromBuilder(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be nil")
}
