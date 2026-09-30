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

package dashboard_agent

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/internal/dashboard/healthz"
	dashboardlog "github.com/ray-project/ray/go/internal/dashboard/log"
	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/spf13/cobra"
)

// dashboardAgentLogFilename is the default log file name used when
// --logging-filename is not provided.
const dashboardAgentLogFilename = "dashboard_agent.log"

// CLI flags, package-level so cobra can bind them and tests can drive them
// through cmd.SetArgs.
var (
	// Required parameters.
	nodeIPAddress   string
	grpcPort        int
	listenPort      int
	nodeManagerPort string
	gcsAddress      string
	clusterIDHex    string
	nodeIDHex       string
	logDir          string
	tempDir         string
	sessionDir      string
	sessionName     string
	objectStoreName string
	rayletName      string
	stdoutFilepath  string
	stderrFilepath  string

	// Logging configuration.
	loggingLevel             string
	loggingFormat            string
	loggingFilename          string
	loggingRotateBytes       int
	loggingRotateBackupCount int

	// Optional behavior flags.
	disableMetricsCollection bool
	eventsExportAddr         string

	// Reserved flags (P0 deferred).
	minimal bool
)

// DashboardAgentCmd is the `dashboard-agent` subcommand of raygo.
var DashboardAgentCmd = &cobra.Command{
	Use:   "dashboard-agent",
	Short: "Ray Dashboard Agent",
	Long: `Ray Dashboard Agent runs on every node and hosts dashboard modules
(healthz, log, event, aggregator, reporter, job) behind an HTTP/gRPC server.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if minimal {
			return fmt.Errorf("dashboard agent --minimal not supported yet")
		}
		if !isValidLogLevel(loggingLevel) {
			return fmt.Errorf("invalid logging-level %q, must be one of: %v", loggingLevel, common.LoggerLevelChoices)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDashboardAgent(cmd.Context())
	},
}

// init registers the dashboard-agent CLI flags.
func init() {
	DashboardAgentCmd.Flags().SortFlags = false

	// Required parameters.
	DashboardAgentCmd.Flags().StringVar(&nodeIPAddress, "node-ip-address", "", "the IP address of this node.")
	DashboardAgentCmd.Flags().IntVar(&grpcPort, "grpc-port", 0, "The port on which the dashboard agent gRPC server listens.")
	DashboardAgentCmd.Flags().IntVar(&listenPort, "listen-port", 0, "The port on which the dashboard agent HTTP server listens.")
	DashboardAgentCmd.Flags().StringVar(&nodeManagerPort, "node-manager-port", "", "The port of the node manager on this node, or the raylet placeholder literal.")
	DashboardAgentCmd.Flags().StringVar(&gcsAddress, "gcs-address", "", "The address (ip:port) of GCS.")
	DashboardAgentCmd.Flags().StringVar(&clusterIDHex, "cluster-id-hex", "", "The cluster id in hex.")
	DashboardAgentCmd.Flags().StringVar(&nodeIDHex, "node-id-hex", "", "The node id in hex; defaults to a freshly generated id if empty.")
	DashboardAgentCmd.Flags().StringVar(&logDir, "log-dir", "", "Specify the path of log directory.")
	DashboardAgentCmd.Flags().StringVar(&tempDir, "temp-dir", "", "Specify the path of the temporary directory used by Ray process.")
	DashboardAgentCmd.Flags().StringVar(&sessionDir, "session-dir", "", "Specify the path of the session directory of the current Ray runtime.")
	DashboardAgentCmd.Flags().StringVar(&sessionName, "session-name", "", "Specify the name of the current Ray session.")
	DashboardAgentCmd.Flags().StringVar(&objectStoreName, "object-store-name", "", "Specify the name of the object store.")
	DashboardAgentCmd.Flags().StringVar(&rayletName, "raylet-name", "", "Specify the name of the raylet.")
	DashboardAgentCmd.Flags().StringVar(&stdoutFilepath, "stdout-filepath", "", "The filepath to dump dashboard agent stdout.")
	DashboardAgentCmd.Flags().StringVar(&stderrFilepath, "stderr-filepath", "", "The filepath to dump dashboard agent stderr.")

	// Logging configuration.
	DashboardAgentCmd.Flags().StringVar(&loggingLevel, "logging-level", common.LoggerLevel, fmt.Sprintf("The logging level. One of: %s", strings.Join(common.LoggerLevelChoices, ", ")))
	DashboardAgentCmd.Flags().StringVar(&loggingFormat, "logging-format", common.LoggerFormat, common.LoggerFormatHelp)
	DashboardAgentCmd.Flags().StringVar(&loggingFilename, "logging-filename", "", fmt.Sprintf("Specify the name of log file, default is %s", dashboardAgentLogFilename))
	DashboardAgentCmd.Flags().IntVar(&loggingRotateBytes, "logging-rotate-bytes", 0, "Specify the max bytes for rotating log file")
	DashboardAgentCmd.Flags().IntVar(&loggingRotateBackupCount, "logging-rotate-backup-count", 0, "Specify the backup count of rotated log file")

	// Optional behavior flags.
	DashboardAgentCmd.Flags().BoolVar(&disableMetricsCollection, "disable-metrics-collection", false, "Disable metrics collection for this agent.")
	DashboardAgentCmd.Flags().StringVar(&eventsExportAddr, "events-export-addr", "", "The address events are exported to, e.g. an external statsd endpoint.")

	// Reserved flags.
	DashboardAgentCmd.Flags().BoolVar(&minimal, "minimal", false, "Run a minimal dashboard agent (not supported yet).")

	// Required flags.
	DashboardAgentCmd.MarkFlagRequired("node-ip-address")
	DashboardAgentCmd.MarkFlagRequired("grpc-port")
	DashboardAgentCmd.MarkFlagRequired("listen-port")
	DashboardAgentCmd.MarkFlagRequired("node-manager-port")
	DashboardAgentCmd.MarkFlagRequired("gcs-address")
	DashboardAgentCmd.MarkFlagRequired("cluster-id-hex")
	DashboardAgentCmd.MarkFlagRequired("log-dir")
	DashboardAgentCmd.MarkFlagRequired("temp-dir")
	DashboardAgentCmd.MarkFlagRequired("session-dir")
	DashboardAgentCmd.MarkFlagRequired("session-name")
	DashboardAgentCmd.MarkFlagRequired("object-store-name")
	DashboardAgentCmd.MarkFlagRequired("raylet-name")
	DashboardAgentCmd.MarkFlagRequired("stdout-filepath")
	DashboardAgentCmd.MarkFlagRequired("stderr-filepath")
}

// GetDashboardAgentCmd returns the dashboard-agent subcommand for registration
// in the raygo root command.
func GetDashboardAgentCmd() *cobra.Command {
	return DashboardAgentCmd
}

// buildConfig assembles the framework agent.Config from the parsed CLI flags.
func buildConfig() (*agent.Config, error) {
	clusterID, err := ids.ClusterIDFromHex(clusterIDHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluster id from hex: %w", err)
	}

	nodeID := ids.NewNodeID()
	if nodeIDHex != "" {
		parsed, err := ids.NodeIDFromHex(nodeIDHex)
		if err != nil {
			return nil, fmt.Errorf("failed to parse node id from hex: %w", err)
		}
		nodeID = parsed
	}

	// raylet passes the literal RAY_NODE_MANAGER_PORT_PLACEHOLDER string in
	// the dashboard_agent_command, exactly as it does for the Python agent.
	// The agent does not dial the node manager, so a non-numeric placeholder is
	// tolerated as a zero port rather than rejected at flag-parse time.
	nodeManagerPortInt := 0
	if n, err := strconv.Atoi(nodeManagerPort); err == nil {
		nodeManagerPortInt = n
	}

	return &agent.Config{
		NodeIP:                    nodeIPAddress,
		NodeID:                    nodeID,
		GCSAddress:                gcsAddress,
		ClusterID:                 clusterID,
		LogDir:                    logDir,
		TempDir:                   tempDir,
		SessionDir:                sessionDir,
		SessionName:               sessionName,
		GRPCPort:                  grpcPort,
		ListenPort:                listenPort,
		NodeManagerPort:           nodeManagerPortInt,
		MetricsCollectionDisabled: disableMetricsCollection,
		EventsExportAddr:          eventsExportAddr,
	}, nil
}

// is_head lookup retry parameters, mirroring the Python agent's
// call_with_retry(description="get self node info", max_attempts=30,
// max_backoff_s=1): both RPC errors and a node missing from the node table
// are retried, tolerating the startup race where the agent comes up before
// raylet has registered the node.
const (
	isHeadRetryAttempts = 30
	isHeadRetryInterval = time.Second
)

// resolveIsHead reports whether this node is the head node by looking up its
// own node info in GCS, mirroring the Python agent
// (python/ray/dashboard/agent.py: _fetch_node_info sets is_head from
// GcsNodeInfo.is_head_node). A nil node id or a lookup failure leaves isHead
// false so the agent errs on the side of not probing GCS on non-head nodes.
func resolveIsHead(ctx context.Context, gcsClient gcs.Client, nodeID ids.NodeID) (bool, error) {
	return resolveIsHeadRetry(ctx, gcsClient, nodeID, isHeadRetryAttempts, isHeadRetryInterval)
}

// resolveIsHeadRetry is resolveIsHead with injectable retry parameters. Like
// the Python call_with_retry wrapper, it retries both RPC errors and a node
// missing from the node table (a not-found is not an RPC error, so the
// underlying RPC-level retry cannot cover it).
func resolveIsHeadRetry(ctx context.Context, gcsClient gcs.Client, nodeID ids.NodeID, attempts int, interval time.Duration) (bool, error) {
	// The Python agent derives is_head from its own node id; a freshly
	// generated id (no --node-id-hex) can never be found, so we only look up
	// an explicitly provided node id.
	if nodeID.IsNil() {
		return false, nil
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		nodes, err := gcsClient.GetAll(ctx, []ids.NodeID{nodeID})
		if err == nil {
			if nodeInfo, ok := nodes[nodeID]; ok {
				return nodeInfo.GetIsHeadNode(), nil
			}
			lastErr = fmt.Errorf("node %s not found in node table, cannot determine is_head", nodeID.Hex())
		} else {
			lastErr = fmt.Errorf("failed to fetch node info for is_head: %w", err)
		}
		if attempt < attempts-1 {
			select {
			case <-ctx.Done():
				return false, lastErr
			case <-time.After(interval):
			}
		}
	}
	return false, lastErr
}

// runDashboardAgent sets up logging, connects to GCS, builds the agent
// Config and delegates to agent.Run.
func runDashboardAgent(ctx context.Context) error {
	logFilename := loggingFilename
	if logFilename == "" {
		logFilename = dashboardAgentLogFilename
	}

	if err := common.NewLogOption(
		common.WithLoggingLevel(loggingLevel),
		common.WithLoggingFormat(loggingFormat),
		common.WithLoggingFilename(logFilename),
		common.WithLogsDir(logDir),
		common.WithLoggingRotateBytes(loggingRotateBytes),
		common.WithLoggingRotateBackupCount(loggingRotateBackupCount),
		common.WithStdoutFilepath(stdoutFilepath),
		common.WithStderrFilepath(stderrFilepath),
	).SetupComponentLogger(); err != nil {
		return fmt.Errorf("failed to setup default logger: %w", err)
	}

	clusterID, err := ids.ClusterIDFromHex(clusterIDHex)
	if err != nil {
		return fmt.Errorf("failed to parse cluster id from hex: %w", err)
	}

	// 10s connect timeout mirrors the Python dashboard agent defaults.
	gcsClient, err := native.ConnectClient(gcs.ClientOptions{
		Address:   gcsAddress,
		ClusterID: clusterID,
		TimeoutMs: 10_000,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to GCS client: %w", err)
	}
	defer gcsClient.Close()

	cfg, err := buildConfig()
	if err != nil {
		return err
	}
	cfg.GCS = gcsClient

	isHead, err := resolveIsHead(ctx, gcsClient, cfg.NodeID)
	if err != nil {
		return fmt.Errorf("resolve is_head: %w", err)
	}
	cfg.IsHead = isHead

	log.Log.Info("starting dashboard agent",
		"node_ip", cfg.NodeIP,
		"grpc_port", cfg.GRPCPort,
		"listen_port", cfg.ListenPort,
		"is_head", cfg.IsHead,
	)

	mods, err := buildModules(*cfg)
	if err != nil {
		return err
	}
	return agent.Run(ctx, *cfg, mods)
}

// buildModules assembles the modules the dashboard agent runs with. The log
// module verifies its log directory up front, so a misconfigured --log-dir
// surfaces before any server starts.
func buildModules(cfg agent.Config) ([]agent.Module, error) {
	logAgent, err := dashboardlog.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create log module: %w", err)
	}
	healthzAgent, err := healthz.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create healthz module: %w", err)
	}
	return []agent.Module{logAgent, healthzAgent}, nil
}

// isValidLogLevel reports whether level is one of common.LoggerLevelChoices.
func isValidLogLevel(level string) bool {
	for _, validLevel := range common.LoggerLevelChoices {
		if level == validLevel {
			return true
		}
	}
	return false
}
