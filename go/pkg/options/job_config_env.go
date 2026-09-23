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

package options

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	// JobConfigJSONEnvVar is the environment variable injected by the job
	// supervisor. Its value is a JSON object containing job-level
	// configuration such as runtime_env and metadata.
	JobConfigJSONEnvVar = "RAY_JOB_CONFIG_JSON_ENV_VAR"

	// OverrideJobRuntimeEnvVar is the environment variable that, when set to
	// "1", makes the user-provided runtime_env take priority over the
	// job-injected runtime_env.
	OverrideJobRuntimeEnvVar = "RAY_OVERRIDE_JOB_RUNTIME_ENV"
)

// JobConfigEnvData holds the runtime_env and metadata parsed from the
// JobConfigJSONEnvVar environment variable.
//
// This is a neutral intermediate structure that avoids coupling the
// pkg/options package to any internal types. Callers are responsible for
// mapping it onto their own configuration structures.
type JobConfigEnvData struct {
	// RuntimeEnvJSON is the serialized runtime_env object injected by the job.
	// Empty if the environment variable does not contain a valid runtime_env.
	RuntimeEnvJSON string

	// Metadata is the job metadata injected by the job. Nil if not present.
	Metadata map[string]string
}

// ParseJobConfigFromEnv reads and parses the JobConfigJSONEnvVar environment
// variable into a neutral JobConfigEnvData structure.
//
// This is the single source of truth for parsing the job config JSON injected
// by the supervisor, shared by both the driver (api) and worker (internal)
// packages, so that pkg/options never needs to depend on internal types.
//
// Returns an empty JobConfigEnvData (no error) when the environment variable
// is not set. Returns an error when the JSON is malformed.
func ParseJobConfigFromEnv() (*JobConfigEnvData, error) {
	jobConfigJSON := os.Getenv(JobConfigJSONEnvVar)
	if jobConfigJSON == "" {
		return &JobConfigEnvData{}, nil
	}

	var jobConfigData map[string]interface{}
	if err := json.Unmarshal([]byte(jobConfigJSON), &jobConfigData); err != nil {
		return nil, fmt.Errorf("failed to parse job config JSON from environment: %w", err)
	}

	data := &JobConfigEnvData{}

	if runtimeEnvRaw, ok := jobConfigData["runtime_env"]; ok {
		if runtimeEnvMap, ok := runtimeEnvRaw.(map[string]interface{}); ok {
			if runtimeEnvJSON, err := json.Marshal(runtimeEnvMap); err == nil {
				data.RuntimeEnvJSON = string(runtimeEnvJSON)
			}
		}
	}

	if metadataRaw, ok := jobConfigData["metadata"]; ok {
		if metadataMap, ok := metadataRaw.(map[string]interface{}); ok {
			data.Metadata = make(map[string]string, len(metadataMap))
			for k, v := range metadataMap {
				if strVal, ok := v.(string); ok {
					data.Metadata[k] = strVal
				}
			}
		}
	}

	return data, nil
}

// MergeFromEnv reads the job config injected via the JobConfigJSONEnvVar
// environment variable and merges its runtime_env and metadata into the
// builder.
//
// The merge follows these rules, matching the Python runtime's
// _merge_runtime_env (python/ray/runtime_env/runtime_env.py):
//   - If only one side provides a runtime_env, it is used as-is.
//   - Otherwise, the job-injected and user-provided runtime_env are merged per
//     top-level key, and env_vars are merged per key. The user-provided value
//     wins on conflicting keys.
//   - By default, conflicting top-level fields or env_vars keys return an
//     error. Setting OverrideJobRuntimeEnvVar=1 suppresses the error and lets
//     the user-provided value win on conflicts (non-conflicting job-injected
//     fields are still merged).
//   - Metadata is always merged from the job config (job-injected takes
//     priority over user-provided metadata).
func (b *JobConfigBuilder) MergeFromEnv() error {
	data, err := ParseJobConfigFromEnv()
	if err != nil {
		return err
	}

	if data.RuntimeEnvJSON != "" {
		if b.runtimeEnvJSON == "" {
			// User didn't set runtime_env: use the job-injected one as-is.
			b.WithRuntimeEnv(data.RuntimeEnvJSON)
		} else {
			override := os.Getenv(OverrideJobRuntimeEnvVar) == "1"
			merged, err := mergeRuntimeEnv(data.RuntimeEnvJSON, b.runtimeEnvJSON, override)
			if err != nil {
				return err
			}
			b.WithRuntimeEnv(merged)
		}
	}

	for k, v := range data.Metadata {
		b.WithMetadata(k, v)
	}
	return nil
}

// mergeRuntimeEnv merges a job-injected runtime_env with a user-provided one,
// matching the Python runtime's _merge_runtime_env semantics.
//
// Merging happens per top-level key, while env_vars are merged per env var
// key, with the user-provided value winning on conflicts. When override is
// false, conflicting top-level fields or env_vars keys return an error to
// signal a conflict (the caller should instruct the user to set
// OverrideJobRuntimeEnvVar=1). When override is true, conflicts are resolved
// in favor of the user-provided value without error.
func mergeRuntimeEnv(jobJSON, userJSON string, override bool) (string, error) {
	job := map[string]interface{}{}
	if jobJSON != "" {
		if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
			return "", fmt.Errorf("failed to parse job-injected runtime_env: %w", err)
		}
	}
	user := map[string]interface{}{}
	if userJSON != "" {
		if err := json.Unmarshal([]byte(userJSON), &user); err != nil {
			return "", fmt.Errorf("failed to parse user-provided runtime_env: %w", err)
		}
	}

	jobEnvVars := takeEnvVars(job)
	userEnvVars := takeEnvVars(user)

	if !override {
		if err := checkConflict("fields", job, user); err != nil {
			return "", err
		}
		if err := checkConflict("env_vars", jobEnvVars, userEnvVars); err != nil {
			return "", err
		}
	}

	// Merge top-level fields: user-provided overwrites job-injected.
	for k, v := range user {
		job[k] = v
	}
	// Merge env_vars per key: user-provided overwrites job-injected.
	for k, v := range userEnvVars {
		jobEnvVars[k] = v
	}
	if len(jobEnvVars) > 0 {
		job["env_vars"] = jobEnvVars
	}

	merged, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("failed to marshal merged runtime_env: %w", err)
	}
	return string(merged), nil
}

// takeEnvVars removes the "env_vars" field from m (when it is a JSON object)
// and returns it. Unlike other top-level fields, env_vars must be merged per
// key rather than replaced wholesale.
func takeEnvVars(m map[string]interface{}) map[string]interface{} {
	raw, ok := m["env_vars"]
	if !ok {
		return map[string]interface{}{}
	}
	envVars, ok := raw.(map[string]interface{})
	if !ok {
		// Malformed (non-object) env_vars: leave it as a regular field.
		return map[string]interface{}{}
	}
	delete(m, "env_vars")
	return envVars
}

// intersectKeys returns the keys present in both a and b.
func intersectKeys(a, b map[string]interface{}) []string {
	var keys []string
	for k := range a {
		if _, ok := b[k]; ok {
			keys = append(keys, k)
		}
	}
	return keys
}

// checkConflict returns an error if a and b share any key. The kind argument
// labels the conflicting entity (e.g., "fields" or "env_vars") in the error.
func checkConflict(kind string, a, b map[string]interface{}) error {
	keys := intersectKeys(a, b)
	if len(keys) == 0 {
		return nil
	}
	return fmt.Errorf(
		"failed to merge the job's runtime_env with the user's runtime_env: "+
			"conflicting %s %v are not allowed; set %s=1 to let the "+
			"user-provided runtime_env override the job-injected one",
		kind, keys, OverrideJobRuntimeEnvVar)
}
