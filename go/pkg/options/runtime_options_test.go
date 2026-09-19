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
	"encoding/base64"
	"testing"

	rayproto "github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

func TestWorkerType_String(t *testing.T) {
	tests := []struct {
		name       string
		workerType WorkerType
		expected   string
	}{
		{"Driver", WorkerTypeDriver, "driver"},
		{"Worker", WorkerTypeWorker, "worker"},
		{"Local", WorkerTypeLocal, "local"},
		{"Unknown", WorkerType(-1), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.workerType.String()
			if result != tt.expected {
				t.Errorf("WorkerType.String() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestParseWorkerType(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    WorkerType
		expectError bool
	}{
		{"ParseDriver", "driver", WorkerTypeDriver, false},
		{"ParseWorker", "worker", WorkerTypeWorker, false},
		{"ParseLocal", "local", WorkerTypeLocal, false},
		{"ParseUnknown", "unknown", 0, true},
		{"ParseEmpty", "", 0, true},
		{"ParseInvalid", "invalid_type", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseWorkerType(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("ParseWorkerType() error = %v, expectError = %v", err, tt.expectError)
			}
			if result != tt.expected {
				t.Errorf("ParseWorkerType() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestJobConfigBuilder_Empty(t *testing.T) {
	builder := NewJobConfigBuilder()

	configStr, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() unexpected error: %v", err)
	}

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(configStr)
	if err != nil {
		t.Fatalf("Build() returned invalid base64: %v", err)
	}

	// Parse protobuf
	var config rayproto.JobConfig
	if err := protolib.Unmarshal(decoded, &config); err != nil {
		t.Fatalf("Build() returned invalid protobuf: %v", err)
	}

	// Verify default_actor_lifetime is set
	if config.DefaultActorLifetime != rayproto.JobConfig_NON_DETACHED {
		t.Errorf("default_actor_lifetime = %v, expected NON_DETACHED", config.DefaultActorLifetime)
	}
}

func TestJobConfigBuilder_WithCodeSearchPath(t *testing.T) {
	tests := []struct {
		name          string
		paths         []string
		expectedLen   int
		expectedFirst string
		expectedLast  string
	}{
		{
			name:          "SinglePath",
			paths:         []string{"./userfuncs.so"},
			expectedLen:   1,
			expectedFirst: "./userfuncs.so",
			expectedLast:  "./userfuncs.so",
		},
		{
			name:          "MultiplePaths",
			paths:         []string{"./userfuncs.so", "./plugins/myplugin.so", "/absolute/path.so"},
			expectedLen:   3,
			expectedFirst: "./userfuncs.so",
			expectedLast:  "/absolute/path.so",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewJobConfigBuilder()
			builder.WithCodeSearchPath(tt.paths...)

			configStr, err := builder.Build()
			if err != nil {
				t.Fatalf("Build() unexpected error: %v", err)
			}

			// Decode base64
			decoded, err := base64.StdEncoding.DecodeString(configStr)
			if err != nil {
				t.Fatalf("Build() returned invalid base64: %v", err)
			}

			// Parse protobuf
			var config rayproto.JobConfig
			if err := protolib.Unmarshal(decoded, &config); err != nil {
				t.Fatalf("Build() returned invalid protobuf: %v", err)
			}

			if len(config.CodeSearchPath) != tt.expectedLen {
				t.Errorf("code_search_path length = %d, expected %d", len(config.CodeSearchPath), tt.expectedLen)
			}

			if config.CodeSearchPath[0] != tt.expectedFirst {
				t.Errorf("first path = %v, expected %v", config.CodeSearchPath[0], tt.expectedFirst)
			}

			if config.CodeSearchPath[len(config.CodeSearchPath)-1] != tt.expectedLast {
				t.Errorf("last path = %v, expected %v", config.CodeSearchPath[len(config.CodeSearchPath)-1], tt.expectedLast)
			}
		})
	}
}

func TestJobConfigBuilder_WithNamespace(t *testing.T) {
	builder := NewJobConfigBuilder()
	builder.WithNamespace("test-namespace")

	configStr, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() unexpected error: %v", err)
	}

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(configStr)
	if err != nil {
		t.Fatalf("Build() returned invalid base64: %v", err)
	}

	// Parse protobuf
	var config rayproto.JobConfig
	if err := protolib.Unmarshal(decoded, &config); err != nil {
		t.Fatalf("Build() returned invalid protobuf: %v", err)
	}

	if config.RayNamespace != "test-namespace" {
		t.Errorf("ray_namespace = %v, expected test-namespace", config.RayNamespace)
	}
}

func TestJobConfigBuilder_BuildToJobOptions(t *testing.T) {
	builder := NewJobConfigBuilder().
		WithCodeSearchPath("./userfuncs.so", "./plugins/plugin.so").
		WithNamespace("production").
		WithDefaultActorLifetime(JobConfigActorLifetimeDetached)

	jobOpts, err := builder.BuildToJobOptions()
	if err != nil {
		t.Fatalf("BuildToJobOptions() unexpected error: %v", err)
	}

	if jobOpts.JobConfig == "" {
		t.Fatalf("JobConfig is empty")
	}

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(jobOpts.JobConfig)
	if err != nil {
		t.Fatalf("JobConfig is not valid base64: %v", err)
	}

	// Parse protobuf
	var config rayproto.JobConfig
	if err := protolib.Unmarshal(decoded, &config); err != nil {
		t.Fatalf("JobConfig is not valid protobuf: %v", err)
	}

	// Verify fields
	if len(config.CodeSearchPath) != 2 {
		t.Errorf("code_search_path = %v, expected 2 paths", config.CodeSearchPath)
	}

	if config.RayNamespace != "production" {
		t.Errorf("ray_namespace = %v, expected production", config.RayNamespace)
	}

	if config.DefaultActorLifetime != rayproto.JobConfig_DETACHED {
		t.Errorf("default_actor_lifetime = %v, expected DETACHED", config.DefaultActorLifetime)
	}
}

func TestJobConfigBuilder_WithMetadata(t *testing.T) {
	builder := NewJobConfigBuilder()
	builder.WithMetadata("version", "1.0.0").
		WithMetadata("owner", "test-team")

	configStr, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() unexpected error: %v", err)
	}

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(configStr)
	if err != nil {
		t.Fatalf("Build() returned invalid base64: %v", err)
	}

	// Parse protobuf
	var config rayproto.JobConfig
	if err := protolib.Unmarshal(decoded, &config); err != nil {
		t.Fatalf("Build() returned invalid protobuf: %v", err)
	}

	metadataVersion, ok := config.Metadata["version"]
	if !ok || metadataVersion != "1.0.0" {
		t.Errorf("metadata[version] = %v, expected 1.0.0", metadataVersion)
	}

	metadataOwner, ok := config.Metadata["owner"]
	if !ok || metadataOwner != "test-team" {
		t.Errorf("metadata[owner] = %v, expected test-team", metadataOwner)
	}
}

func TestJobConfigActorLifetime_Values(t *testing.T) {
	tests := []struct {
		name     JobConfigActorLifetime
		expected rayproto.JobConfig_ActorLifetime
	}{
		{JobConfigActorLifetimeDetached, rayproto.JobConfig_DETACHED},
		{JobConfigActorLifetimeNonDetached, rayproto.JobConfig_NON_DETACHED},
	}

	for _, tt := range tests {
		t.Run(tt.name.String(), func(t *testing.T) {
			builder := NewJobConfigBuilder()
			builder.defaultActorLifetime = tt.name

			configStr, err := builder.Build()
			if err != nil {
				t.Fatalf("Build() unexpected error: %v", err)
			}

			// Decode base64
			decoded, err := base64.StdEncoding.DecodeString(configStr)
			if err != nil {
				t.Fatalf("Build() returned invalid base64: %v", err)
			}

			// Parse protobuf
			var config rayproto.JobConfig
			if err := protolib.Unmarshal(decoded, &config); err != nil {
				t.Fatalf("Build() returned invalid protobuf: %v", err)
			}

			if config.DefaultActorLifetime != tt.expected {
				t.Errorf("default_actor_lifetime = %v, expected %v", config.DefaultActorLifetime, tt.expected)
			}
		})
	}
}
