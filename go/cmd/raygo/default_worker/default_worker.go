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

// Package default_worker provides the worker subcommand for Ray Go CLI, used to start Ray worker processes.
//
// **Design Notes**:
//
// This implementation uses the **Plugin pattern** (mirroring Java's DefaultWorker + RayNativeRuntime combination):
// - Java: loads libcore_worker_library_java.so via System.load()
// - Go: loads go_runtime.so via plugin.Open()
//
// Usage:
//
//	raygo defaultworker --node-ip-address=192.168.1.100 --node-manager-port=6379 \
//	             --store-socket=/tmp/ray/plasma --raylet-socket=/tmp/ray/raylet \
//	             --gcs-address=192.168.1.100:6379
package default_worker

import (
	"fmt"
	"os"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/worker" // Use worker package (Plugin mode)
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/spf13/cobra"
)

// buildJobConfigFromEnv builds a JobConfig from environment variables.
// This matches Java/CPP behavior where JobConfig is built from driver settings.
func buildJobConfigFromEnv() string {
	codeSearchPath := os.Getenv("RAY_CODE_SEARCH_PATH")
	if codeSearchPath == "" {
		return ""
	}

	// Build JobConfig using protobuf serialization (matches Java/CPP)
	builder := &options.JobConfigBuilder{}
	if codeSearchPath != "" {
		builder.WithCodeSearchPath(codeSearchPath)
	}

	configStr, err := builder.Build()
	if err != nil {
		log.Log.Error(err, "Failed to build JobConfig", "code_search_path", codeSearchPath)
		return ""
	}

	log.Log.Info("Built JobConfig from environment", "code_search_path", codeSearchPath)
	return configStr
}

// WorkerCmd represents the worker subcommand
//
// **Design Notes**:
//
// This implementation uses the **Plugin pattern**, mirroring Java's DefaultWorker + RayNativeRuntime:
// - Java: DefaultWorker.main() -> Ray.init() -> RayNativeRuntime.run()
// - Go: worker.New(opts) -> worker.Run() (dynamically loads go_runtime.so)
var WorkerCmd = &cobra.Command{
	Use:   "defaultworker",
	Short: "Start a Ray worker process",
	Long: `Start a Ray worker process for Go language runtime.

The worker process connects to an existing Ray cluster and processes
tasks assigned by the raylet. It can be used to run Go-based Ray components
such as log_monitor, dashboard, etc.

**Design**: Uses Go plugin API to dynamically load go_runtime.so at runtime,
similar to how Java loads libcore_worker_library_java.so via System.load().

Usage:
  raygo defaultworker [flags]

Examples:
  raygo defaultworker --node-ip-address=192.168.1.100 --node-manager-port=6379 \
                --store-socket=/tmp/ray/plasma --raylet-socket=/tmp/ray/raylet \
                --gcs-address=192.168.1.100:6379`,
	RunE: runWorker,
}

func init() {
	// Ray cluster connection flags (corresponds to C++ FLAGS_ray_*)
	WorkerCmd.Flags().String(worker.GcsAddress, "", "The address of the Ray cluster to connect to (--gcs-address).")
	// Note: --gcs-address is an alias for --ray-address, for compatibility with Python's build_go_setup_worker_command
	WorkerCmd.Flags().String(worker.CodeSearchPath, "", "A list of directories or files of dynamic libraries that specify the search path for user code. ':' is used as the separator (--code-search-path).")
	WorkerCmd.Flags().String(worker.RedisUsername, common.REDIS_DEFAULT_USERNAME, "Prevents external clients without the username from connecting to Redis if provided (--redis-username).")
	WorkerCmd.Flags().String(worker.RedisPassword, common.REDIS_DEFAULT_PASSWORD, "Prevents external clients without the password from connecting to Redis if provided (--redis-password).")
	WorkerCmd.Flags().String(worker.JobID, "", "Assigned job id (--job-id).")
	WorkerCmd.Flags().String(worker.ClusterID, "", "Cluster ID in hex format (--cluster-id).")
	WorkerCmd.Flags().Int(worker.NodeManagerPort, 0, "The port to use for the node manager (--node-manager-port).")
	WorkerCmd.Flags().String(worker.RayletName, "", "It will specify the socket name used by the raylet if provided (--raylet-name).")
	WorkerCmd.Flags().String(worker.PlasmaStoreName, "", "It will specify the socket name used by the plasma store if provided (--plasma-store-name).")

	// Session and logging flags
	WorkerCmd.Flags().String(worker.SessionDir, "", "The path of this session (--session-dir).")
	WorkerCmd.Flags().String(worker.LogsDir, "", "Logs dir for workers (--logs-dir).")

	// Node configuration flags
	WorkerCmd.Flags().String(worker.NodeIpAddress, "", "The ip address for this node (--node-ip-address).")
	WorkerCmd.Flags().String(worker.HeadArgs, "", "The command line args to be appended as parameters of the `ray start` command (--head-args).")

	// Worker authentication and lifecycle flags
	WorkerCmd.Flags().Int(worker.StartupToken, -1, "The startup token assigned to this worker process by the raylet (--startup-token).")
	WorkerCmd.Flags().String(worker.WorkerIDFlag, "", "The worker ID assigned to this worker process by the raylet (--worker-id).")
	WorkerCmd.Flags().String(worker.DefaultActorLifetime, "", "The default actor lifetime type, `detached` or `non_detached` (--default-actor-lifetime).")

	// Runtime environment flags
	WorkerCmd.Flags().String(worker.RuntimeEnvFlag, "", "The serialized runtime env (--runtime-env).")
	WorkerCmd.Flags().Int(worker.RuntimeEnvHashFlag, -1, "The computed hash of the runtime env for this worker (--runtime-env-hash).")

	// Job configuration flags
	WorkerCmd.Flags().String(worker.JobNamespace, "", "The namespace of job. If not set, a unique value will be randomly generated (--job-namespace).")
}

// main is the entry point for the defaultworker executable
func main() {
	if err := WorkerCmd.Execute(); err != nil {
		log.Log.Error(err, "failed to execute worker command")
		os.Exit(1)
	}
}

func GetDefaultWorkerCmd() *cobra.Command {
	return WorkerCmd
}

func runWorker(cmd *cobra.Command, args []string) error {
	// Handle --gcs-address as an alias for --ray-address
	// This is for compatibility with Python's build_go_setup_worker_command
	gcsAddress, _ := cmd.Flags().GetString("gcs-address")
	if gcsAddress != "" {
		// Set ray-address flag value from gcs-address
		cmd.Flags().Set(worker.GcsAddress, gcsAddress)
	}

	// 1. Parse configuration - mirroring Java: RayConfig parsing
	config, err := worker.NewRayConfig(cmd, false, true)
	if err != nil {
		return fmt.Errorf("failed to initialize config: %w", err)
	}

	// For Worker mode, set RAY_JOB_ID environment variable.
	// C++ core_worker reads JobID from RAY_JOB_ID env var for workers (not from options).
	// This matches the behavior where Raylet sets RAY_JOB_ID when starting worker processes.
	if config.WorkerType == options.WorkerTypeWorker && config.JobID != "" {
		if err := os.Setenv("RAY_JOB_ID", config.JobID); err != nil {
			return fmt.Errorf("failed to set RAY_JOB_ID: %w", err)
		}
		log.Log.Info("set RAY_JOB_ID environment variable", "job_id", config.JobID)
	}

	// 2. Build Plugin-mode Worker options
	// Mirroring Java: new RayNativeRuntime(rayConfig)

	// Build JobConfig from environment (populated by Raylet from driver's JobConfig)
	jobConfigStr := buildJobConfigFromEnv()

	opts := worker.NewOptions(
		config.WorkerType, // use options.WorkerType directly
		options.NetworkOptions{
			NodeIPAddress:   config.NodeIPAddress,
			NodeManagerPort: config.NodeManagerPort,
			GcsAddress:      config.GetBootstrapAddress(),
		},
		options.JobOptions{
			JobID:     config.JobID,
			ClusterID: config.ClusterID,
			JobConfig: jobConfigStr, // Use base64-encoded protobuf JobConfig
		},
		options.RuntimeOptions{
			StoreSocket:    config.PlasmaStoreName,
			RayletSocket:   config.RayletName,
			LogDir:         config.LogsDir,
			StartupToken:   int32(config.StartupToken),
			RuntimeEnvHash: int32(config.RuntimeEnvHash),
			WorkerIDHex:    config.WorkerIDHex,
		},
		worker.WithCodeSearchPath(config.CodeSearchPath),
	)

	// 3. Create Worker instance (Plugin mode)
	// Mirroring Java: DefaultWorker.main() -> Ray.init()
	w := worker.New(opts)

	// 4. Run Worker (will dynamically load go_runtime.so)
	// Mirroring Java: RayNativeRuntime.run() -> nativeRunTaskExecutor(taskExecutor)
	// This is a blocking call, with C++ core_worker driving task execution
	if err := w.Run(); err != nil {
		return fmt.Errorf("worker run failed: %w", err)
	}

	// 5. Shutdown Worker (optional, automatically shuts down on process exit)
	// Mirroring Java: RayNativeRuntime.shutdown()
	w.Shutdown()

	return nil
}
