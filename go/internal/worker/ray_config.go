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

// Package worker provides Ray worker process configuration management:
// it parses and stores the various configuration parameters of Ray startup.
package worker

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	contract "github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/spf13/cobra"
)

// Command-line flag names (corresponds to C++ FLAGS_ray_* in config_internal.cc)
const (
	// Ray cluster connection flags (corresponds to C++ FLAGS_ray_*)
	GcsAddress      = "gcs-address"
	CodeSearchPath  = "code-search-path"
	RedisUsername   = "redis-username"
	RedisPassword   = "redis-password"
	JobID           = "job-id"
	ClusterID       = "cluster-id"
	NodeManagerPort = "node-manager-port"
	RayletName      = "raylet-name"
	PlasmaStoreName = "plasma-store-name"

	// Session and logging flags
	SessionDir = "session-dir"
	LogsDir    = "logs-dir"

	// Node configuration flags
	NodeIpAddress = "node-ip-address"
	HeadArgs      = "head-args"

	// Worker authentication and lifecycle flags
	StartupToken         = "startup-token"
	WorkerIDFlag         = "worker-id"
	DefaultActorLifetime = "default-actor-lifetime"

	// Runtime environment flags
	RuntimeEnvFlag     = "runtime-env"
	RuntimeEnvHashFlag = "runtime-env-hash"

	// Job configuration flags
	JobNamespace = "job-namespace"
)

// ActorLifetime represents the lifecycle type of an actor.
type ActorLifetime int

const (
	// NonDetached is a non-detached actor that exits with the driver.
	NonDetached ActorLifetime = iota
	// Detached is a detached actor that is independent of the driver lifecycle.
	Detached
)

// String returns the string representation of ActorLifetime.
func (a ActorLifetime) String() string {
	switch a {
	case NonDetached:
		return "NON_DETACHED"
	case Detached:
		return "DETACHED"
	default:
		return "UNKNOWN"
	}
}

// JobConfigMetadata holds job configuration metadata.
type JobConfigMetadata map[string]string

// RuntimeEnv holds runtime environment configuration.
type RuntimeEnv struct {
	// RawJSON is the serialized runtime environment JSON data.
	RawJSON string
}

// Deserialize unmarshals a JSON string into a RuntimeEnv.
func Deserialize(jsonStr string) (*RuntimeEnv, error) {
	if jsonStr == "" {
		return nil, nil
	}
	// Validate the JSON format.
	var tmp interface{}
	if err := json.Unmarshal([]byte(jsonStr), &tmp); err != nil {
		return nil, fmt.Errorf("invalid runtime env JSON: %w", err)
	}
	return &RuntimeEnv{RawJSON: jsonStr}, nil
}

// RayConfig holds the internal Ray configuration singleton.
type RayConfig struct {
	// RunMode is either SINGLE_PROCESS (single-process debug) or CLUSTER.
	// contract.RunMode is the authoritative definition.
	RunMode contract.RunMode

	// WorkerType is either WORKER or DRIVER.
	// options.WorkerType is the authoritative definition.
	WorkerType options.WorkerType

	// BootstrapIP is the Ray cluster bootstrap address, e.g. "127.0.0.1".
	BootstrapIP   string
	BootstrapPort int

	// CodeSearchPath is used to locate user dynamic libraries.
	CodeSearchPath []string

	// Redis authentication information (not recommended).
	RedisUsername string
	RedisPassword string

	// HeadArgs are appended to the ray start command.
	HeadArgs []string

	// DefaultActorLifetime is the default actor lifecycle.
	DefaultActorLifetime ActorLifetime

	// RuntimeEnv is the runtime environment configuration.
	RuntimeEnv string

	// JobID is the job identifier.
	JobID string

	// NodeManagerPort is the node manager port.
	NodeManagerPort int32

	// RayletName is the raylet socket name.
	RayletName string

	// PlasmaStoreName is the plasma store socket name.
	PlasmaStoreName string

	// SessionDir is the session directory.
	SessionDir string

	// LogsDir is the log directory.
	LogsDir string

	// NodeIPAddress is the node IP address.
	NodeIPAddress string

	// StartupToken is used for worker authentication.
	StartupToken int

	// WorkerIDHex is the worker ID (hex) assigned by the raylet. Empty for
	// driver processes; required for worker processes so the worker registers
	// back with the raylet under the ID the raylet assigned.
	WorkerIDHex string

	// RayNamespace is the job namespace.
	RayNamespace string

	// RuntimeEnvHash is the hash of the runtime environment.
	RuntimeEnvHash int

	// JobConfigMetadata holds the job configuration metadata.
	JobConfigMetadata JobConfigMetadata

	ClusterID string
}

// SetBootstrapAddress sets the bootstrap address from an "ip:port" string.
func (c *RayConfig) SetBootstrapAddress(bootstrapAddress string) error {
	ip, port, err := common.ValidateHostPort(bootstrapAddress)
	if err != nil {
		return fmt.Errorf("failed to parse bootstrap address: %w", err)
	}
	c.BootstrapIP = ip
	c.BootstrapPort = port
	log.Log.V(1).Info("Bootstrap address set", "ip", ip, "port", port)
	return nil
}

// UpdateSessionDir updates the session and log directories.
func (c *RayConfig) UpdateSessionDir(dir string) {
	if c.SessionDir == "" {
		c.SessionDir = dir
	}
	if c.LogsDir == "" {
		c.LogsDir = dir + "/logs"
	}
}

// ParseDefaultActorLifetimeType parses the default actor lifetime type.
// defaultActorLifetime is either "detached" or "non_detached".
func ParseDefaultActorLifetimeType(defaultActorLifetime string) (ActorLifetime, error) {
	// Convert to lowercase.
	lifetime := strings.ToLower(defaultActorLifetime)

	// Validate and return the matching lifetime type.
	if lifetime == "non_detached" {
		return NonDetached, nil
	} else if lifetime == "detached" {
		return Detached, nil
	}

	return NonDetached, fmt.Errorf("invalid default_actor_lifetime: %s, must be 'detached' or 'non_detached'", defaultActorLifetime)
}

func NewRayConfig(cmd *cobra.Command, localMode bool, isWorker bool) (*RayConfig, error) {
	if cmd == nil {
		return nil, errors.New("cmd is nil.")
	}

	config := initRayConfig(localMode, isWorker)

	if err := parseClusterFlags(cmd, config); err != nil {
		return nil, err
	}

	if err := parseNodeConfig(cmd, config); err != nil {
		return nil, err
	}

	if err := parseRuntimeFlags(cmd, config); err != nil {
		return nil, err
	}

	if err := parseDriverConfig(cmd, config); err != nil {
		return nil, err
	}

	if err := parseJobConfigFromEnv(config); err != nil {
		return nil, err
	}

	return config, nil
}

// initRayConfig initializes the base RayConfig.
func initRayConfig(localMode bool, isWorker bool) *RayConfig {
	config := &RayConfig{
		RunMode:              contract.RunModeCluster,
		WorkerType:           options.WorkerTypeWorker,
		BootstrapIP:          "",
		BootstrapPort:        6379,
		RedisUsername:        common.REDIS_DEFAULT_USERNAME,
		RedisPassword:        common.REDIS_DEFAULT_PASSWORD,
		NodeManagerPort:      0,
		CodeSearchPath:       nil,
		PlasmaStoreName:      "",
		RayletName:           "",
		SessionDir:           "",
		JobID:                "",
		LogsDir:              "",
		NodeIPAddress:        "",
		StartupToken:         -1,
		HeadArgs:             nil,
		RuntimeEnv:           "",
		RuntimeEnvHash:       0,
		DefaultActorLifetime: NonDetached,
		JobConfigMetadata:    make(map[string]string),
		RayNamespace:         "",
	}
	if localMode {
		config.RunMode = contract.RunModeLocal
	} else {
		config.RunMode = contract.RunModeCluster
	}
	if isWorker {
		config.WorkerType = options.WorkerTypeWorker
	} else {
		config.WorkerType = options.WorkerTypeDriver
	}
	return config
}

// parseClusterFlags parses the cluster-related flags.
func parseClusterFlags(cmd *cobra.Command, config *RayConfig) error {
	address, err := cmd.Flags().GetString(GcsAddress)
	if err != nil {
		return err
	}
	if address != "" {
		if err := config.SetBootstrapAddress(address); err != nil {
			log.Log.V(1).Error(err, "Failed to set bootstrap address from command line")
			return err
		}
	}

	searchPath, err := cmd.Flags().GetString(CodeSearchPath)
	if err != nil {
		return err
	}
	if searchPath != "" {
		// Multiple paths separated by ':'.
		paths := strings.Split(searchPath, ":")
		config.CodeSearchPath = append(config.CodeSearchPath, paths...)
		log.Log.V(1).Info("Code search path set from command line", "paths", config.CodeSearchPath)
	}

	if username, err := cmd.Flags().GetString(RedisUsername); err == nil && username != Dummy_Default_Value {
		config.RedisUsername = username
	} else {
		return err
	}

	if password, err := cmd.Flags().GetString(RedisPassword); err == nil && password != Dummy_Default_Value {
		config.RedisPassword = password
	} else {
		return err
	}

	jobID, err := cmd.Flags().GetString(JobID)
	if err != nil {
		return err
	}
	if jobID != "" {
		config.JobID = jobID
	}

	if clusterID, err := cmd.Flags().GetString(ClusterID); err == nil && clusterID != "" {
		config.ClusterID = clusterID
	}

	return nil
}

// parseNodeConfig parses the node configuration flags.
func parseNodeConfig(cmd *cobra.Command, config *RayConfig) error {
	nodeManagerPort, err := cmd.Flags().GetInt(NodeManagerPort)
	if err != nil {
		return fmt.Errorf("failed to get flag '%s': %v", NodeManagerPort, err)
	}
	if nodeManagerPort <= 0 || nodeManagerPort > 65535 {
		return fmt.Errorf("invalid nodeManager port: %d, must be between 1 and 65535", nodeManagerPort)
	}
	config.NodeManagerPort = int32(nodeManagerPort)

	rayletName, err := cmd.Flags().GetString(RayletName)
	if err != nil {
		return err
	}
	if rayletName != "" {
		config.RayletName = rayletName
	}

	plasmaStoreName, err := cmd.Flags().GetString(PlasmaStoreName)
	if err != nil {
		return err
	}
	if plasmaStoreName != "" {
		config.PlasmaStoreName = plasmaStoreName
	}

	sessionDir, err := cmd.Flags().GetString(SessionDir)
	if err != nil {
		return err
	}
	if sessionDir != "" {
		config.SessionDir = sessionDir
	}

	logsDir, err := cmd.Flags().GetString(LogsDir)
	if err != nil {
		return err
	}
	if logsDir != "" {
		config.LogsDir = logsDir
	}

	nodeIP, err := cmd.Flags().GetString(NodeIpAddress)
	if err != nil {
		return err
	}
	if nodeIP != "" {
		config.NodeIPAddress = nodeIP
	}

	headArgs, err := cmd.Flags().GetString(HeadArgs)
	if err != nil {
		return err
	}
	if headArgs != "" {
		args := strings.Fields(headArgs)
		config.HeadArgs = append(config.HeadArgs, args...)
	}

	startupToken, err := cmd.Flags().GetInt(StartupToken)
	if err != nil {
		return fmt.Errorf("failed to get flag '%s': %v", StartupToken, err)
	}
	config.StartupToken = startupToken

	config.WorkerIDHex, _ = cmd.Flags().GetString(WorkerIDFlag)

	lifetimeStr, err := cmd.Flags().GetString(DefaultActorLifetime)
	if err != nil {
		return err
	}
	if lifetimeStr != "" {
		lifetime, err := ParseDefaultActorLifetimeType(lifetimeStr)
		if err != nil {
			log.Log.V(1).Error(err, "Failed to parse default actor lifetime")
			return err
		}
		config.DefaultActorLifetime = lifetime
		log.Log.V(1).Info("Default actor lifetime set", "lifetime", lifetime)
	}

	return nil
}

// parseRuntimeFlags parses the runtime environment flags.
func parseRuntimeFlags(cmd *cobra.Command, config *RayConfig) error {
	runtimeEnvStr, err := cmd.Flags().GetString(RuntimeEnvFlag)
	if err != nil {
		return err
	}
	if runtimeEnvStr != "" {
		_, err := Deserialize(runtimeEnvStr)
		if err != nil {
			log.Log.V(1).Error(err, "Failed to deserialize runtime env")
			return err
		}
		config.RuntimeEnv = runtimeEnvStr
	}

	runtimeEnvHash, err := cmd.Flags().GetInt(RuntimeEnvHashFlag)
	if err != nil {
		return fmt.Errorf("failed to get flag '%s': %v", RuntimeEnvHashFlag, err)
	}
	config.RuntimeEnvHash = runtimeEnvHash

	return nil
}

// parseDriverConfig parses the driver-specific flags: the RAY_ADDRESS
// environment variable, code search path handling, and job_namespace.
func parseDriverConfig(cmd *cobra.Command, config *RayConfig) error {
	if config.WorkerType == options.WorkerTypeDriver && config.RunMode == contract.RunModeCluster {
		// Read the cluster address from the RAY_ADDRESS environment variable.
		if config.BootstrapIP == "" {
			if rayAddress := os.Getenv("RAY_ADDRESS"); rayAddress != "" {
				log.Log.V(1).Info("Initialize Ray cluster address from environment variable", "address", rayAddress)
				if err := config.SetBootstrapAddress(rayAddress); err != nil {
					log.Log.V(1).Error(err, "Failed to set bootstrap address from environment")
					return err
				}
			}
		}

		// Handle the code search path: default to the program directory.
		if len(config.CodeSearchPath) == 0 {
			exePath, err := os.Executable()
			if err != nil {
				return err
			}
			exeDir := filepath.Dir(exePath)
			config.CodeSearchPath = append(config.CodeSearchPath, exeDir)
			log.Log.V(1).Info("No code search path found yet. The program location path will be added as default.", "path", exeDir)
		} else {
			absolutePaths := make([]string, 0, len(config.CodeSearchPath))
			for _, path := range config.CodeSearchPath {
				if absPath, err := toAbsolutePath(path); err == nil {
					absolutePaths = append(absolutePaths, absPath)
				} else {
					absolutePaths = append(absolutePaths, path)
				}
			}
			config.CodeSearchPath = absolutePaths
		}
	}

	// Parse job_namespace (driver only).
	if config.WorkerType == options.WorkerTypeDriver {
		namespace, err := cmd.Flags().GetString(JobNamespace)
		if err != nil {
			return err
		}
		if namespace != "" {
			config.RayNamespace = namespace
		}
		if config.RayNamespace == "" {
			config.RayNamespace = generateRandomNamespace()
		}
	}

	return nil
}

func parseJobConfigFromEnv(config *RayConfig) error {
	// Parse the RAY_JOB_ID environment variable.
	// This is set by Raylet when starting worker processes.
	if jobID := os.Getenv("RAY_JOB_ID"); jobID != "" && config.JobID == "" {
		config.JobID = jobID
		log.Log.V(1).Info("parsed RAY_JOB_ID from environment", "job_id", jobID)
	}

	jobConfigJSON := os.Getenv("RAY_JOB_CONFIG_JSON_ENV_VAR")
	if jobConfigJSON == "" {
		return nil
	}

	var jobConfigData map[string]interface{}
	if err := json.Unmarshal([]byte(jobConfigJSON), &jobConfigData); err != nil {
		log.Log.V(1).Error(err, "Failed to parse job config JSON from environment")
		return err
	}

	// Parse runtime_env.
	if runtimeEnvRaw, ok := jobConfigData["runtime_env"]; ok {
		if runtimeEnvJSON, ok := runtimeEnvRaw.(string); ok && runtimeEnvJSON != "" {
			if _, err := Deserialize(runtimeEnvJSON); err == nil {
				config.RuntimeEnv = runtimeEnvJSON
			}
		}
	}

	// Parse metadata.
	if metadataRaw, ok := jobConfigData["metadata"]; ok {
		if metadataMap, ok := metadataRaw.(map[string]interface{}); ok {
			config.JobConfigMetadata = make(JobConfigMetadata)
			for k, v := range metadataMap {
				if strVal, ok := v.(string); ok {
					config.JobConfigMetadata[k] = strVal
				}
			}
		}
	}

	return nil
}

func toAbsolutePath(path string) (string, error) {
	absPath, err := os.Getwd()
	if err != nil {
		return path, err
	}
	if !strings.HasPrefix(path, "/") {
		absPath = absPath + "/" + path
	} else {
		absPath = path
	}
	cleanPath := filepath.Clean(absPath)
	return cleanPath, nil
}

func generateRandomNamespace() string {
	id, err := uuid.NewRandom()
	if err == nil {
		return id.String()
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("ns-%d", os.Getpid())
	}
	return hex.EncodeToString(bytes) + "-" + strconv.Itoa(os.Getpid())
}

func (c *RayConfig) GetBootstrapAddress() string {
	if c.BootstrapIP == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", c.BootstrapIP, c.BootstrapPort)
}

func (c *RayConfig) IsWorker() bool {
	return c.WorkerType == options.WorkerTypeWorker
}

func (c *RayConfig) IsDriver() bool {
	return c.WorkerType == options.WorkerTypeDriver
}

func (c *RayConfig) IsClusterMode() bool {
	return c.RunMode == contract.RunModeCluster
}

func (c *RayConfig) IsLocalMode() bool {
	return c.RunMode == contract.RunModeLocal
}
