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
	"reflect"
	"testing"
)

func TestParseJobConfigFromEnv(t *testing.T) {
	tests := []struct {
		name               string
		envJSON            string
		expectedRuntimeEnv string
		expectedMetadata   map[string]string
		expectError        bool
	}{
		{
			name:               "Unset",
			envJSON:            "",
			expectedRuntimeEnv: "",
			expectedMetadata:   nil,
			expectError:        false,
		},
		{
			name:               "EmptyObject",
			envJSON:            `{}`,
			expectedRuntimeEnv: "",
			expectedMetadata:   nil,
			expectError:        false,
		},
		{
			name:               "RuntimeEnvAndMetadata",
			envJSON:            `{"runtime_env": {"working_dir": "gcs://_ray_pkg_abc123.zip"}, "metadata": {"job_name": "test-job"}}`,
			expectedRuntimeEnv: `{"working_dir":"gcs://_ray_pkg_abc123.zip"}`,
			expectedMetadata:   map[string]string{"job_name": "test-job"},
			expectError:        false,
		},
		{
			name:               "MetadataOnly",
			envJSON:            `{"metadata": {"a": "1", "b": "2"}}`,
			expectedRuntimeEnv: "",
			expectedMetadata:   map[string]string{"a": "1", "b": "2"},
			expectError:        false,
		},
		{
			name:               "NonStringMetadataIgnored",
			envJSON:            `{"metadata": {"n": 123, "s": "ok"}}`,
			expectedRuntimeEnv: "",
			expectedMetadata:   map[string]string{"s": "ok"},
			expectError:        false,
		},
		{
			name:               "NonMapRuntimeEnvIgnored",
			envJSON:            `{"runtime_env": "not-a-map"}`,
			expectedRuntimeEnv: "",
			expectedMetadata:   nil,
			expectError:        false,
		},
		{
			name:        "MalformedJSON",
			envJSON:     `{invalid`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(JobConfigJSONEnvVar, tt.envJSON)

			data, err := ParseJobConfigFromEnv()
			if (err != nil) != tt.expectError {
				t.Fatalf("ParseJobConfigFromEnv() error = %v, expectError = %v", err, tt.expectError)
			}
			if tt.expectError {
				return
			}

			if data.RuntimeEnvJSON != tt.expectedRuntimeEnv {
				t.Errorf("RuntimeEnvJSON = %q, want %q", data.RuntimeEnvJSON, tt.expectedRuntimeEnv)
			}
			if !reflect.DeepEqual(data.Metadata, tt.expectedMetadata) {
				t.Errorf("Metadata = %v, want %v", data.Metadata, tt.expectedMetadata)
			}
		})
	}
}

func TestJobConfigBuilder_MergeFromEnv(t *testing.T) {
	tests := []struct {
		name               string
		envJSON            string
		userRuntimeEnv     string
		overrideEnv        string
		expectedRuntimeEnv string
		expectedMetadata   map[string]string
		expectError        bool
		description        string
	}{
		{
			name:               "EmptyEnv",
			envJSON:            "",
			userRuntimeEnv:     "",
			expectedRuntimeEnv: "",
			expectedMetadata:   nil,
			expectError:        false,
			description:        "When RAY_JOB_CONFIG_JSON_ENV_VAR is not set, no merging occurs",
		},
		{
			name:               "JobInjectedOnly",
			envJSON:            `{"runtime_env": {"working_dir": "gcs://_ray_pkg_abc123.zip"}, "metadata": {"job_name": "test-job"}}`,
			userRuntimeEnv:     "",
			expectedRuntimeEnv: `{"working_dir":"gcs://_ray_pkg_abc123.zip"}`,
			expectedMetadata:   map[string]string{"job_name": "test-job"},
			expectError:        false,
			description:        "When user didn't provide runtime_env, job-injected is used as-is",
		},
		{
			name:               "UserOnlyNoJobInjected",
			envJSON:            `{}`,
			userRuntimeEnv:     `{"env_vars": {"FOO": "bar"}}`,
			expectedRuntimeEnv: `{"env_vars":{"FOO":"bar"}}`,
			expectedMetadata:   nil,
			expectError:        false,
			description:        "When job didn't inject runtime_env, user-provided is kept",
		},
		{
			name:               "MergeNonOverlapping",
			envJSON:            `{"runtime_env": {"working_dir": "gcs://_ray_pkg_abc123.zip"}}`,
			userRuntimeEnv:     `{"pip": ["requests"]}`,
			expectedRuntimeEnv: `{"pip":["requests"],"working_dir":"gcs://_ray_pkg_abc123.zip"}`,
			expectedMetadata:   nil,
			expectError:        false,
			description:        "Non-overlapping top-level fields are merged",
		},
		{
			name:               "EnvVarsUnion",
			envJSON:            `{"runtime_env": {"working_dir": "gcs://_ray_pkg_abc123.zip", "env_vars": {"A": "1"}}}`,
			userRuntimeEnv:     `{"pip": ["requests"], "env_vars": {"B": "2"}}`,
			expectedRuntimeEnv: `{"env_vars":{"A":"1","B":"2"},"pip":["requests"],"working_dir":"gcs://_ray_pkg_abc123.zip"}`,
			expectedMetadata:   nil,
			expectError:        false,
			description:        "env_vars are merged per key (union)",
		},
		{
			name:               "TopLevelConflict",
			envJSON:            `{"runtime_env": {"working_dir": "gcs://_ray_pkg_abc123.zip", "pip": ["requests"]}, "metadata": {"job_name": "test-job"}}`,
			userRuntimeEnv:     `{"working_dir": "file:///local/path", "env_vars": {"FOO": "bar"}}`,
			expectedRuntimeEnv: "",
			expectedMetadata:   nil,
			expectError:        true,
			description:        "Conflicting top-level fields return an error unless overridden",
		},
		{
			name:               "EnvVarsConflict",
			envJSON:            `{"runtime_env": {"env_vars": {"FOO": "bar"}}}`,
			userRuntimeEnv:     `{"env_vars": {"FOO": "baz"}}`,
			expectedRuntimeEnv: "",
			expectedMetadata:   nil,
			expectError:        true,
			description:        "Conflicting env_vars keys return an error unless overridden",
		},
		{
			name:               "OverrideMode",
			envJSON:            `{"runtime_env": {"working_dir": "gcs://_ray_pkg_abc123.zip"}, "metadata": {"job_name": "test-job"}}`,
			userRuntimeEnv:     `{"working_dir": "file:///local/path", "env_vars": {"FOO": "bar"}}`,
			overrideEnv:        "1",
			expectedRuntimeEnv: `{"env_vars":{"FOO":"bar"},"working_dir":"file:///local/path"}`,
			expectedMetadata:   map[string]string{"job_name": "test-job"},
			expectError:        false,
			description:        "With override, user-provided fields win on conflict without error",
		},
		{
			name:               "OverrideUnion",
			envJSON:            `{"runtime_env": {"working_dir": "gcs://_ray_pkg_abc123.zip", "pip": ["requests"]}}`,
			userRuntimeEnv:     `{"env_vars": {"FOO": "bar"}}`,
			overrideEnv:        "1",
			expectedRuntimeEnv: `{"env_vars":{"FOO":"bar"},"pip":["requests"],"working_dir":"gcs://_ray_pkg_abc123.zip"}`,
			expectedMetadata:   nil,
			expectError:        false,
			description:        "With override, non-conflicting job-injected fields are still merged",
		},
		{
			name:               "MetadataMerged",
			envJSON:            `{"runtime_env": {}, "metadata": {"job_name": "test-job", "version": "1.0"}}`,
			userRuntimeEnv:     "",
			expectedRuntimeEnv: `{}`,
			expectedMetadata:   map[string]string{"job_name": "test-job", "version": "1.0"},
			expectError:        false,
			description:        "Metadata from job config is always merged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment
			t.Setenv(JobConfigJSONEnvVar, tt.envJSON)
			if tt.overrideEnv != "" {
				t.Setenv(OverrideJobRuntimeEnvVar, tt.overrideEnv)
			}

			// Create builder with user-provided runtime_env
			builder := NewJobConfigBuilder()
			if tt.userRuntimeEnv != "" {
				builder.WithRuntimeEnv(tt.userRuntimeEnv)
			}

			err := builder.MergeFromEnv()
			if (err != nil) != tt.expectError {
				t.Fatalf("%s: MergeFromEnv() error = %v, expectError = %v", tt.description, err, tt.expectError)
			}
			if tt.expectError {
				return
			}

			// Verify runtime_env
			actualRuntimeEnv := builder.RuntimeEnvJSON()
			if tt.expectedRuntimeEnv == "" {
				if actualRuntimeEnv != "" {
					t.Errorf("%s: expected empty runtime_env, got %q", tt.description, actualRuntimeEnv)
				}
			} else {
				assertJSONEqual(t, tt.expectedRuntimeEnv, actualRuntimeEnv, tt.description)
			}

			// Verify metadata
			if tt.expectedMetadata == nil {
				if len(builder.metadata) != 0 {
					t.Errorf("%s: expected empty metadata, got %v", tt.description, builder.metadata)
				}
			} else if !reflect.DeepEqual(builder.metadata, tt.expectedMetadata) {
				t.Errorf("%s: metadata = %v, want %v", tt.description, builder.metadata, tt.expectedMetadata)
			}
		})
	}
}

// assertJSONEqual compares two JSON strings semantically (ignoring key order).
func assertJSONEqual(t *testing.T, expected, actual, description string) {
	t.Helper()

	var expectedMap, actualMap map[string]interface{}
	if err := json.Unmarshal([]byte(expected), &expectedMap); err != nil {
		t.Fatalf("%s: failed to unmarshal expected runtime_env: %v", description, err)
	}
	if err := json.Unmarshal([]byte(actual), &actualMap); err != nil {
		t.Fatalf("%s: failed to unmarshal actual runtime_env: %v", description, err)
	}

	if !reflect.DeepEqual(expectedMap, actualMap) {
		t.Errorf("%s: runtime_env mismatch: expected %v, got %v", description, expectedMap, actualMap)
	}
}
