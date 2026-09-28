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

// Package runtime_env_agent implements the raygo runtime-env-agent subcommand,
// which runs the node-level runtime environment agent HTTP server.
package runtime_env_agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/internal/runtime_env"
	"github.com/ray-project/ray/go/internal/runtime_env/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"github.com/spf13/cobra"
)

// runtimeGOOS allows tests to override the platform behavior.
var runtimeGOOS = runtime.GOOS

// connectGCSClient is the factory used to create GCS client connections; tests
// can replace it with a fake.
var connectGCSClient = func(opts gcs.ClientOptions) (gcs.Client, error) {
	return native.ConnectClient(opts)
}

// Flags for the runtime-env-agent command.
var (
	// Required flags.
	nodeIPAddress       string
	runtimeEnvAgentPort int
	gcsAddress          string
	clusterIDHex        string
	runtimeEnvDir       string
	logDir              string
	tempDir             string
	pythonExecutable    string

	// Port persistence flags: the raylet discovers this agent by waiting for
	// the bound port to be written into the session directory, so with an
	// OS-assigned port (0) these two must be present.
	sessionDir string
	nodeID     string

	// Logging configuration flags.
	loggingLevel             string
	loggingFormat            string
	loggingFilename          string
	loggingRotateBytes       int
	loggingRotateBackupCount int

	// stdout/stderr redirection flags.
	stdoutFilepath string
	stderrFilepath string
)

var RuntimeEnvAgentCmd = &cobra.Command{
	Use:   "runtime-env-agent",
	Short: "Ray Runtime Env Agent",
	Long:  `Ray Runtime Env Agent manages runtime environments at node level.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// Validate the logging-level flag.
		if !isValidLogLevel(loggingLevel) {
			return fmt.Errorf("invalid logging-level %q, must be one of: %v", loggingLevel, common.LoggerLevelChoices)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRuntimeEnvAgent(cmd.Context())
	},
}

// init registers the runtime-env-agent command flags.
func init() {
	// Disable flag sorting so flags are shown in declaration order.
	RuntimeEnvAgentCmd.Flags().SortFlags = false

	// Required flags.
	RuntimeEnvAgentCmd.Flags().StringVar(&nodeIPAddress, "node-ip-address", "", "the IP address of this node.")
	RuntimeEnvAgentCmd.Flags().IntVar(&runtimeEnvAgentPort, "runtime-env-agent-port", 0, "The port on which the runtime env agent will receive HTTP requests.")
	RuntimeEnvAgentCmd.Flags().StringVar(&gcsAddress, "gcs-address", "", "The address (ip:port) of GCS.")
	RuntimeEnvAgentCmd.Flags().StringVar(&clusterIDHex, "cluster-id-hex", "", "The cluster id in hex.")
	RuntimeEnvAgentCmd.Flags().StringVar(&runtimeEnvDir, "runtime-env-dir", "", "Specify the path of the resource directory used by runtime_env.")
	RuntimeEnvAgentCmd.Flags().StringVar(&pythonExecutable, "python-executable", "", "Specify the Python executable path.")
	RuntimeEnvAgentCmd.Flags().StringVar(&sessionDir, "session-dir", "", "Session directory where the bound port is persisted for the raylet to discover.")
	RuntimeEnvAgentCmd.Flags().StringVar(&nodeID, "node-id", "", "Hex node ID used in the persisted port file name.")
	RuntimeEnvAgentCmd.Flags().IntVar(&loggingRotateBytes, "logging-rotate-bytes", 0, "Specify the max bytes for rotating log file")
	RuntimeEnvAgentCmd.Flags().IntVar(&loggingRotateBackupCount, "logging-rotate-backup-count", 0, "Specify the backup count of rotated log file")
	RuntimeEnvAgentCmd.Flags().StringVar(&logDir, "log-dir", "", "Specify the path of log directory.")
	RuntimeEnvAgentCmd.Flags().StringVar(&tempDir, "temp-dir", "", "Specify the path of the temporary directory use by Ray process.")

	// Optional logging flags.
	RuntimeEnvAgentCmd.Flags().StringVar(&loggingLevel, "logging-level", common.LoggerLevel, fmt.Sprintf("The logging level. One of: %s", strings.Join(common.LoggerLevelChoices, ", ")))
	RuntimeEnvAgentCmd.Flags().StringVar(&loggingFormat, "logging-format", common.LoggerFormat, common.LoggerFormatHelp)
	RuntimeEnvAgentCmd.Flags().StringVar(&loggingFilename, "logging-filename", agent.RuntimeEnvAgentLogFilename, "Specify the name of log file, log to stdout if set empty")
	RuntimeEnvAgentCmd.Flags().StringVar(&stdoutFilepath, "stdout-filepath", "", "The filepath to dump runtime env agent stdout.")
	RuntimeEnvAgentCmd.Flags().StringVar(&stderrFilepath, "stderr-filepath", "", "The filepath to dump runtime env agent stderr.")

	// Mark the required flags.
	RuntimeEnvAgentCmd.MarkFlagRequired("node-ip-address")
	RuntimeEnvAgentCmd.MarkFlagRequired("runtime-env-agent-port")
	RuntimeEnvAgentCmd.MarkFlagRequired("gcs-address")
	RuntimeEnvAgentCmd.MarkFlagRequired("cluster-id-hex")
	RuntimeEnvAgentCmd.MarkFlagRequired("runtime-env-dir")
	RuntimeEnvAgentCmd.MarkFlagRequired("python-executable")
	RuntimeEnvAgentCmd.MarkFlagRequired("logging-rotate-bytes")
	RuntimeEnvAgentCmd.MarkFlagRequired("logging-rotate-backup-count")
	RuntimeEnvAgentCmd.MarkFlagRequired("log-dir")
	RuntimeEnvAgentCmd.MarkFlagRequired("temp-dir")
}

// GetRuntimeEnvAgentCmd returns the runtime-env-agent command.
func GetRuntimeEnvAgentCmd() *cobra.Command {
	return RuntimeEnvAgentCmd
}

// runRuntimeEnvAgent runs the runtime env agent.
func runRuntimeEnvAgent(ctx context.Context) error {
	if pythonExecutable != "" {
		runtime_env.PythonExecutable = pythonExecutable
	}

	// 1. Configure the logging parameters (mirrors the Python implementation).
	isWindows := runtimeGOOS == "windows"
	loggingRotationBytes := loggingRotateBytes
	loggingRotationBackupCount := loggingRotateBackupCount
	if isWindows {
		loggingRotationBytes = 0
		loggingRotationBackupCount = 1
	}

	// Setup stdout/stderr redirect files if redirection enabled.
	// This mirrors the Python code: logging_utils.redirect_stdout_stderr_if_needed(...)
	if err := runtime_env.RedirectStdoutStderrIfNeeded(
		stdoutFilepath,
		stderrFilepath,
		int64(loggingRotationBytes),
		loggingRotationBackupCount,
	); err != nil {
		return fmt.Errorf("failed to redirect stdout/stderr: %w", err)
	}

	loggingZapcoreLevel, err := common.ParseLogLevel(loggingLevel)
	if err != nil {
		return err
	}

	var outputPaths []string
	if loggingFilename != "" {
		if logDir != "" {
			outputPaths = []string{filepath.Join(logDir, loggingFilename)}
		} else {
			outputPaths = []string{loggingFilename}
		}
	}

	loggerOpts := runtime_env.BuildLoggingOptions(
		loggingZapcoreLevel,
		loggingFormat,
		outputPaths,
		loggingRotationBytes,
		loggingRotationBackupCount,
	)

	if err := zap.SetupDefaultLogger(loggerOpts...); err != nil {
		return fmt.Errorf("failed to setup default logger: %w", err)
	}

	// Parse the cluster ID.
	clusterID, err := ids.ClusterIDFromHex(clusterIDHex)
	if err != nil {
		return fmt.Errorf("failed to parse cluster ID from hex: %v", err)
	}

	// Build the GCS client connection options.
	opts := gcs.ClientOptions{
		Address:   gcsAddress,
		ClusterID: clusterID,
		TimeoutMs: 60000, // 60s timeout.
	}

	// Create the GCS client via ConnectClient.
	gcsClient, err := connectGCSClient(opts)
	if err != nil {
		return fmt.Errorf("failed to connect to GCS client: %v", err)
	}

	// 2. Create the agent service.
	config := &agent.RuntimeEnvAgentConfig{
		TempDir:             tempDir,
		RuntimeEnvDir:       runtimeEnvDir,
		Address:             nodeIPAddress,
		RuntimeEnvAgentPort: runtimeEnvAgentPort,
		GcsClient:           gcsClient,
		LoggingParams: &agent.LoggingConfig{
			Level:               loggingZapcoreLevel,
			Format:              loggingFormat,
			Filename:            loggingFilename,
			RotationBytes:       loggingRotationBytes,
			RotationBackupCount: loggingRotationBackupCount,
			LogsDir:             logDir,
			StdoutFilepath:      stdoutFilepath,
			StderrFilepath:      stderrFilepath,
		},
	}

	agentService, err := agent.NewRuntimeEnvAgentService(config)
	if err != nil {
		return fmt.Errorf("failed to create runtime env agent service: %w", err)
	}

	// 3. Create the HTTP server. When the port is OS-assigned (0), the bound
	// port is persisted to the session directory so the raylet's
	// WaitForPersistedPort finds it; without a persisted file the raylet
	// times out and FATALs.
	addr := fmt.Sprintf("%s:%d", nodeIPAddress, runtimeEnvAgentPort)
	server := agent.NewHTTPServer(addr, agentService)
	if sessionDir != "" && nodeID != "" {
		portFileName := fmt.Sprintf("runtime_env_agent_port_%s", nodeID)
		server.OnBound = func(port int) {
			portFile := filepath.Join(sessionDir, portFileName)
			if err := os.WriteFile(portFile, []byte(strconv.Itoa(port)), 0644); err != nil {
				log.Log.Error(err, "Failed to persist runtime env agent port", "file", portFile)
				return
			}
			log.Log.Info("Runtime env agent port persisted", "port", port, "file", portFile)
		}
	}

	// 4. Start the HTTP server (blocks until ctx is cancelled).
	log.Log.Info("Starting HTTP server", "address", addr)
	if err := server.Start(ctx); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	log.Log.Info("Shutting down runtime env agent")

	return nil
}

// isValidLogLevel reports whether the given log level is valid.
func isValidLogLevel(level string) bool {
	for _, validLevel := range common.LoggerLevelChoices {
		if level == validLevel {
			return true
		}
	}
	return false
}
