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

// Package options provides common type definitions for Ray Go Runtime.
// This package is designed to avoid circular dependencies and provide
// a single source of truth for shared types.
//
// The package name "options" reflects its primary purpose: defining
// configuration option types (RuntimeOptions, NetworkOptions, JobOptions,
// InitializeOptions) used throughout the Ray Go Runtime.
package options

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	rayproto "github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// WorkerType represents the type of worker.
// Note: SPILL_WORKER and RESTORE_WORKER are internal IO workers managed by Raylet,
// not exposed to Go users.
// Note: Values must match CNativeRuntimeType enum in native_runtime.h and
// ray::rpc::WorkerType in protobuf (common.proto) for correct cross-language type conversion.
type WorkerType int

const (
	// WorkerTypeWorker represents a regular worker.
	// Must match NATIVE_RUNTIME_TYPE_WORKER (0) and ray::rpc::WorkerType::WORKER (0)
	WorkerTypeWorker WorkerType = iota
	// WorkerTypeDriver represents a driver-type worker.
	// Must match NATIVE_RUNTIME_TYPE_DRIVER (1) and ray::rpc::WorkerType::DRIVER (1)
	WorkerTypeDriver
	// WorkerTypeLocal represents local mode for testing/debugging without full Ray cluster.
	// Internal use only, not passed to C layer.
	WorkerTypeLocal
)

// String returns the string representation of WorkerType.
func (t WorkerType) String() string {
	switch t {
	case WorkerTypeDriver:
		return "driver"
	case WorkerTypeWorker:
		return "worker"
	case WorkerTypeLocal:
		return "local"
	default:
		return "unknown"
	}
}

// ParseWorkerType parses a string into WorkerType.
func ParseWorkerType(s string) (WorkerType, error) {
	switch s {
	case "driver":
		return WorkerTypeDriver, nil
	case "worker":
		return WorkerTypeWorker, nil
	case "local":
		return WorkerTypeLocal, nil
	default:
		return 0, fmt.Errorf("unknown worker type: %s", s)
	}
}

// ToInt32 converts WorkerType to int32 for serialization.
func (t WorkerType) ToInt32() int32 {
	return int32(t)
}

// FromInt32 creates WorkerType from int32.
func FromInt32(v int32) WorkerType {
	return WorkerType(v)
}

// RuntimeOptions contains runtime-related configuration options.
type RuntimeOptions struct {
	StoreSocket    string // Object store socket path
	RayletSocket   string // Raylet socket path
	LogDir         string // Log directory
	StartupToken   int32  // Worker startup token
	RuntimeEnvHash int32  // Runtime environment hash
	WorkerIDHex    string // Worker ID (hex) assigned by the raylet; empty for drivers
}

// NetworkOptions contains network-related configuration options.
type NetworkOptions struct {
	NodeIPAddress   string // Node IP address
	NodeManagerPort int32  // Node manager port
	GcsAddress      string // GCS service address (format: "host:port")
}

// JobOptions contains job-related configuration options.
type JobOptions struct {
	JobID     string // Hex-encoded JobID string
	ClusterID string // Cluster ID (hex-encoded string, optional)
	JobConfig string // Base64-encoded protobuf JobConfig message
}

// InitializeOptions contains configuration options for Go Runtime initialization.
// This struct is shared between plugin and worker to ensure type consistency.
//
// Design notes:
// 1. Uses type-safe WorkerType enum
// 2. JobID uses hex-encoded string for easy cross-process transmission
// 3. JobConfig uses JSON format string for easy serialization
// 4. Options are grouped by category for better organization
type InitializeOptions struct {
	WorkerType WorkerType     // Type-safe worker type enum
	Network    NetworkOptions // Network configuration
	Job        JobOptions     // Job configuration
	Runtime    RuntimeOptions // Runtime configuration
}

// JobConfigActorLifetime specifies the default lifetime for actors.
// Values match ray::rpc::JobConfig_ActorLifetime enum in common.proto:
// - DETACHED (0): Actor lives independently of the job
// - NON_DETACHED (1): Actor dies when the job ends
type JobConfigActorLifetime int

const (
	// JobConfigActorLifetimeDetached - detached actor lifetime (corresponds to DETACHED in proto)
	JobConfigActorLifetimeDetached JobConfigActorLifetime = 0
	// JobConfigActorLifetimeNonDetached - non-detached actor lifetime (corresponds to NON_DETACHED in proto)
	JobConfigActorLifetimeNonDetached JobConfigActorLifetime = 1
)

// String returns the string representation of JobConfigActorLifetime.
func (l JobConfigActorLifetime) String() string {
	switch l {
	case JobConfigActorLifetimeDetached:
		return "DETACHED"
	case JobConfigActorLifetimeNonDetached:
		return "NON_DETACHED"
	default:
		return "UNKNOWN"
	}
}

// JobConfigBuilder provides a fluent API for building JobConfig JSON.
// The JSON format matches the proto message ray::rpc::JobConfig.
//
// Usage:
//
//	jobConfig, err := NewJobConfigBuilder().
//	    WithCodeSearchPath("./userfuncs.so").
//	    WithNamespace("my-job").
//	    BuildToJobOptions()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	opts := &InitializeOptions{Job: jobConfig}
type JobConfigBuilder struct {
	// codeSearchPath specifies paths to search for user code plugins.
	// For Go workers, these are .so files that will be loaded via plugin.Open().
	codeSearchPath []string

	// namespace is the job namespace for isolation (ray_namespace in proto).
	namespace string

	// jvmOptions are JVM options for Java workers.
	jvmOptions []string

	// runtimeEnvJSON is the serialized RuntimeEnvInfo proto message.
	runtimeEnvJSON string

	// defaultActorLifetime specifies the default lifetime for actors.
	defaultActorLifetime JobConfigActorLifetime

	// metadata contains additional job metadata.
	metadata map[string]string
}

// NewJobConfigBuilder creates a new JobConfigBuilder with default values.
func NewJobConfigBuilder() *JobConfigBuilder {
	return &JobConfigBuilder{
		codeSearchPath:       []string{},
		jvmOptions:           []string{},
		metadata:             make(map[string]string),
		defaultActorLifetime: JobConfigActorLifetimeNonDetached,
	}
}

// WithCodeSearchPath sets the code search path for user plugins.
// Multiple paths can be specified; they will be joined with ':' on the worker side.
func (b *JobConfigBuilder) WithCodeSearchPath(paths ...string) *JobConfigBuilder {
	b.codeSearchPath = append(b.codeSearchPath, paths...)
	return b
}

// WithNamespace sets the job namespace for isolation.
func (b *JobConfigBuilder) WithNamespace(ns string) *JobConfigBuilder {
	b.namespace = ns
	return b
}

// WithJvmOptions sets JVM options for Java workers.
func (b *JobConfigBuilder) WithJvmOptions(opts ...string) *JobConfigBuilder {
	b.jvmOptions = append(b.jvmOptions, opts...)
	return b
}

// WithRuntimeEnv sets the runtime environment as a serialized JSON string.
// The JSON should match the RuntimeEnvInfo proto message structure.
func (b *JobConfigBuilder) WithRuntimeEnv(runtimeEnvJSON string) *JobConfigBuilder {
	b.runtimeEnvJSON = runtimeEnvJSON
	return b
}

// WithDefaultActorLifetime sets the default lifetime for actors in this job.
func (b *JobConfigBuilder) WithDefaultActorLifetime(lifetime JobConfigActorLifetime) *JobConfigBuilder {
	b.defaultActorLifetime = lifetime
	return b
}

// WithMetadata sets job metadata (key-value pairs).
func (b *JobConfigBuilder) WithMetadata(key, value string) *JobConfigBuilder {
	b.metadata[key] = value
	return b
}

// Build serializes the JobConfig to protobuf binary format, then base64-encodes it.
// The returned string can be used in options.JobOptions.JobConfig.
//
// This matches the behavior of Java and CPP:
// - Java: JobConfig.newBuilder()...build().toByteArray()
// - CPP: JobConfig protobuf message
//
// The base64 encoding is necessary because:
// 1. Go's C.CString() creates null-terminated C strings
// 2. Protobuf binary data may contain null bytes (0x00)
// 3. C++ std::string(c_str) would truncate at first null byte
// 4. Base64 encoding ensures safe transport across CGO boundary
//
// C++ side will base64-decode and then ParseFromString().
func (b *JobConfigBuilder) Build() (string, error) {
	// Build protobuf JobConfig message
	jobConfig := &rayproto.JobConfig{}

	// Add non-empty fields
	if len(b.codeSearchPath) > 0 {
		jobConfig.CodeSearchPath = append(jobConfig.CodeSearchPath, b.codeSearchPath...)
	}
	if b.namespace != "" {
		jobConfig.RayNamespace = b.namespace
	}
	if len(b.jvmOptions) > 0 {
		jobConfig.JvmOptions = append(jobConfig.JvmOptions, b.jvmOptions...)
	}

	serEnv := b.runtimeEnvJSON
	if serEnv == "" {
		serEnv = "{}"
	} else if !json.Valid([]byte(serEnv)) {
		return "", fmt.Errorf("invalid runtime_env JSON %q: not a valid JSON document", b.runtimeEnvJSON)
	}
	jobConfig.RuntimeEnvInfo = &rayproto.RuntimeEnvInfo{
		SerializedRuntimeEnv: serEnv,
	}
	// Set default_actor_lifetime (convert Go enum to proto enum)
	if b.defaultActorLifetime == JobConfigActorLifetimeDetached {
		jobConfig.DefaultActorLifetime = rayproto.JobConfig_DETACHED
	} else {
		jobConfig.DefaultActorLifetime = rayproto.JobConfig_NON_DETACHED
	}
	if len(b.metadata) > 0 {
		jobConfig.Metadata = make(map[string]string)
		for k, v := range b.metadata {
			jobConfig.Metadata[k] = v
		}
	}

	// Marshal to protobuf binary format
	data, err := protolib.Marshal(jobConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JobConfig protobuf: %w", err)
	}

	// Base64-encode for safe CGO transport
	encoded := base64.StdEncoding.EncodeToString(data)
	return encoded, nil
}

// BuildToJobOptions builds the JobConfig and returns JobOptions.
// This is a convenience method for use with InitializeOptions.
func (b *JobConfigBuilder) BuildToJobOptions() (JobOptions, error) {
	configBase64, err := b.Build()
	if err != nil {
		return JobOptions{}, err
	}
	return JobOptions{
		JobConfig: configBase64,
	}, nil
}
