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

// Package dashboard implements the raygo `dashboard` subcommand, which starts
// the Ray dashboard head process (the Go migration of the Python head).
package dashboard

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/data"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/event"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/flow"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/insight"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/job"
	logmod "github.com/ray-project/ray/go/internal/dashboard/head/modules/log"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/metrics"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/node"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/reporter"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/serve"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/state"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/train"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/usage_stats"
	"github.com/ray-project/ray/go/internal/runtime_env"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/log/zap"
	"github.com/spf13/cobra"
)

// CLI flags, package-level so cobra can bind them. The flag set mirrors the
// Python dashboard.py argparse so the head accepts an identical command line
// from ray/_private/services.py (start_api_server).
var (
	host                     string
	port                     int
	portRetries              int
	gcsAddress               string
	clusterIDHex             string
	nodeIP                   string
	logDir                   string
	sessionDir               string
	tempDir                  string
	minimal                  bool
	disableFrontend          bool
	loggingLevel             string
	loggingFormat            string
	loggingFilename          string
	loggingRotateBytes       int
	loggingRotateBackupCount int
	modulesToLoad            string
	stdoutFilepath           string
	stderrFilepath           string
)

// DashboardCmd is the raygo dashboard subcommand.
var DashboardCmd = &cobra.Command{
	Use:   "dashboard",
	Short: "Ray Dashboard Head",
	Long:  "Ray Dashboard Head serves the Ray dashboard UI and state APIs (default port 8265).",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Align --logging-level with the Python dashboard.py argparse default
		// (ray_constants.LOGGER_LEVEL = os.environ.get("RAY_LOGGER_LEVEL",
		// "info")): only when the flag was not explicitly provided do we fall
		// back to the RAY_LOGGER_LEVEL env var and then "info".
		if !cmd.Flags().Changed("logging-level") {
			if v := os.Getenv("RAY_LOGGER_LEVEL"); v != "" {
				loggingLevel = v
			} else {
				loggingLevel = "info"
			}
		}
		return runDashboard(cmd.Context())
	},
}

// init registers the dashboard CLI flags. The set mirrors the Python
// dashboard.py argparse so start_api_server can launch the Go head with an
// identical command line.
func init() {
	DashboardCmd.Flags().StringVar(&host, "host", "127.0.0.1", "The host to use for the HTTP server.")
	DashboardCmd.Flags().IntVar(&port, "port", 8265, "The port to use for the HTTP server.")
	// Default 0 aligns with the Python dashboard.py --port-retries default.
	DashboardCmd.Flags().IntVar(&portRetries, "port-retries", 0, "The maximum number of retries to bind to the HTTP server port.")
	DashboardCmd.Flags().StringVar(&gcsAddress, "gcs-address", "", "The address (ip:port) of GCS.")
	DashboardCmd.Flags().StringVar(&clusterIDHex, "cluster-id-hex", "", "The cluster ID in hex.")
	DashboardCmd.Flags().StringVar(&nodeIP, "node-ip-address", "", "The IP address of the node where this is running.")
	DashboardCmd.Flags().StringVar(&logDir, "log-dir", "", "The directory for logs.")
	DashboardCmd.Flags().StringVar(&sessionDir, "session-dir", "", "The session directory.")
	DashboardCmd.Flags().StringVar(&tempDir, "temp-dir", "", "The temporary directory.")
	DashboardCmd.Flags().BoolVar(&minimal, "minimal", false, "Run a minimal dashboard (only usage stats).")
	DashboardCmd.Flags().BoolVar(&disableFrontend, "disable-frontend", false, "Do not serve frontend HTML.")
	// Default "info" aligns with ray_constants.LOGGER_LEVEL; runDashboard
	// additionally falls back to the RAY_LOGGER_LEVEL env var when the flag is
	// not explicitly set (mirroring the Python argparse default).
	DashboardCmd.Flags().StringVar(&loggingLevel, "logging-level", "info", "The logging level.")
	DashboardCmd.Flags().StringVar(&loggingFormat, "logging-format", "", "The logging format. Default is a prefix of time, pid, filename and message.")
	DashboardCmd.Flags().StringVar(&loggingFilename, "logging-filename", "dashboard.log", "The log file to log to (relative to --log-dir). Matches Python dashboard_consts.DASHBOARD_LOG_FILENAME.")
	DashboardCmd.Flags().IntVar(&loggingRotateBytes, "logging-rotate-bytes", 512*1024*1024, "Specify the max bytes for rotating log file.")
	DashboardCmd.Flags().IntVar(&loggingRotateBackupCount, "logging-rotate-backup-count", 5, "Specify the backup count of rotated log file.")
	DashboardCmd.Flags().StringVar(&modulesToLoad, "modules-to-load", "", "The list of dashboard modules to load. If empty, load all modules.")
	DashboardCmd.Flags().StringVar(&stdoutFilepath, "stdout-filepath", "", "The path to write stdout to.")
	DashboardCmd.Flags().StringVar(&stderrFilepath, "stderr-filepath", "", "The path to write stderr to.")
	_ = DashboardCmd.MarkFlagRequired("gcs-address")
	_ = DashboardCmd.MarkFlagRequired("cluster-id-hex")
	_ = DashboardCmd.MarkFlagRequired("node-ip-address")
}

// GetDashboardCmd returns the dashboard subcommand for registration in raygo.
func GetDashboardCmd() *cobra.Command { return DashboardCmd }

// runDashboard assembles the config and modules, connects to GCS and runs the
// head main flow.
func runDashboard(ctx context.Context) error {
	args := []string{"--gcs-address=" + gcsAddress, "--cluster-id-hex=" + clusterIDHex,
		"--node-ip-address=" + nodeIP, "--log-dir=" + logDir, "--session-dir=" + sessionDir,
		"--temp-dir=" + tempDir,
		"--logging-level=" + loggingLevel, "--logging-format=" + loggingFormat,
		"--logging-filename=" + loggingFilename,
		"--logging-rotate-bytes=" + fmt.Sprintf("%d", loggingRotateBytes),
		"--logging-rotate-backup-count=" + fmt.Sprintf("%d", loggingRotateBackupCount),
		"--modules-to-load=" + modulesToLoad,
		"--stdout-filepath=" + stdoutFilepath, "--stderr-filepath=" + stderrFilepath,
		"--port-retries=" + fmt.Sprintf("%d", portRetries),
	}
	args = append(args, "--host="+host, "--port="+fmt.Sprintf("%d", port))

	cfg, err := head.ParseConfigFromFlags(args)
	if err != nil {
		return err
	}
	if minimal {
		cfg.Minimal = true
	}
	if disableFrontend {
		cfg.ServeFrontend = false
	}
	// Apply the logging configuration (level, file output with rotation) and
	// stdout/stderr redirection, aligned with the Python dashboard's
	// setup_component_logger + redirect_stdout_stderr_if_needed. This must run
	// before initGoRuntime so the go runtime and all modules log to the same
	// configured sink.
	if err := setupLogging(cfg); err != nil {
		return err
	}

	client, err := head.NewGCSClient(ctx, gcsAddress, head.WithClusterID(clusterIDHex))
	if err != nil {
		return fmt.Errorf("connect GCS: %w", err)
	}
	defer client.Close()

	// Initialize the Go runtime as a driver through the static base.Initialize
	// path (same as the worker process), so the dashboard head can call Python
	// actors across languages (e.g. GetPythonActorWithNamespace in the
	// flow/serve/train/data modules). Mirrors the Python dashboard, which
	// lazily connects as a driver in the internal dashboard namespace. The
	// JobID is allocated fresh from GCS (per-process, like Python).
	// Sockets/ports are auto-filled from GCS in driver mode, so only
	// GcsAddress and NodeIPAddress are required.
	//
	// A failure here is NOT fatal: the Python dashboard starts its HTTP server
	// and GCS-based modules without a driver connection (it connects lazily),
	// and during `ray start` the dashboard launches right after GCS/raylet so
	// the driver's JobID allocation may race GCS readiness. Degrade to running
	// without the cross-language runtime; the GCS-based modules (state, jobs,
	// logs, node, ...) still serve, and the flow/serve/train/data modules log
	// their own errors until GCS is ready.
	if err := initGoRuntime(cfg); err != nil {
		log.Log.Error(err, "failed to init go runtime for dashboard head; running without cross-language runtime")
	} else {
		defer head.ShutdownRuntime()
	}

	// Register the implemented dashboard head modules. Minimal mode loads only
	// the usage_stats module (UsageStatsHead.is_minimal_module()==True in the
	// Python dashboard), so /usage_stats_enabled and /cluster_id remain
	// available; all other modules are skipped in minimal mode. When
	// --modules-to-load is set, only the named modules are loaded (matched by
	// HeadModule Name(), which equals the Python module class name). Modules
	// listed in the RAY_DASHBOARD_DISABLED_MODULES env var (comma-separated)
	// are never loaded, aligned with DashboardHeadModule.is_enabled in
	// subprocesses/module.py.
	disabled := disabledModules()
	mods := []head.HeadModule{}
	allMods := []head.HeadModule{
		state.New(cfg, client),
		node.New(cfg, client),
		reporter.New(cfg, client),
		logmod.New(cfg, client),
		event.New(cfg, client),
		metrics.New(cfg, client),
		usage_stats.New(cfg, client),
		job.New(cfg, client),
		flow.New(cfg, client),
		insight.New(cfg, client),
		serve.New(cfg, client),
		train.New(cfg, client),
		data.New(cfg, client),
	}
	if cfg.Minimal {
		// Aligned with the Python dashboard: minimal mode loads the
		// DashboardHeadModule usage_stats only, not the subprocess modules.
		for _, m := range allMods {
			if m.Name() == "UsageStatsHead" {
				mods = append(mods, m)
			}
		}
	} else if len(cfg.ModulesToLoad) == 0 {
		for _, m := range allMods {
			if !disabled[m.Name()] {
				mods = append(mods, m)
			}
		}
	} else {
		load := make(map[string]bool, len(cfg.ModulesToLoad))
		for _, n := range cfg.ModulesToLoad {
			load[n] = true
		}
		for _, m := range allMods {
			if load[m.Name()] && !disabled[m.Name()] {
				mods = append(mods, m)
			}
		}
	}

	log.Log.Info("starting dashboard head", "port", cfg.HTTPPort, "gcs", cfg.GCSAddress)
	return head.New(cfg, client, mods).Run(ctx)
}

// disabledModules parses the RAY_DASHBOARD_DISABLED_MODULES env var
// (comma-separated module names) into a set, aligned with
// DashboardHeadModule.is_enabled in subprocesses/module.py. An empty/unset
// env var yields an empty set (all modules enabled).
func disabledModules() map[string]bool {
	out := map[string]bool{}
	raw := os.Getenv("RAY_DASHBOARD_DISABLED_MODULES")
	if raw == "" {
		return out
	}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			out[name] = true
		}
	}
	return out
}

// setupLogging configures the global logger (level, optional file output with
// rotation) and redirects stdout/stderr to files when requested. It mirrors the
// Python dashboard's setup_component_logger + redirect_stdout_stderr_if_needed
// and reuses the shared helpers in go/internal/runtime_env (same as the other
// raygo subcommands runtime_env_agent / log_monitor).
//
// Logging filename semantics (aligned with Python): when LoggingFilename is
// empty the logger writes to stderr; when non-empty it writes to
// {LogDir}/{LoggingFilename} with rotation. The cobra default "dashboard.log"
// matches dashboard_consts.DASHBOARD_LOG_FILENAME, and start_api_server passes
// an explicit --logging-filename= (empty) when it wants logs on stderr.
func setupLogging(cfg *head.HeadConfig) error {
	// Redirect stdout/stderr to files with rotation when requested.
	if err := runtime_env.RedirectStdoutStderrIfNeeded(
		cfg.StdoutFilepath,
		cfg.StderrFilepath,
		int64(cfg.LoggingRotateBytes),
		cfg.LoggingRotateBackup,
	); err != nil {
		return fmt.Errorf("failed to redirect stdout/stderr: %w", err)
	}

	// Python passes uppercase logging levels (e.g. "INFO"); ParseLogLevel
	// expects lowercase, so normalize first.
	level, err := common.ParseLogLevel(strings.ToLower(cfg.LoggingLevel))
	if err != nil {
		return err
	}

	var outputPaths []string
	if cfg.LoggingFilename != "" {
		if cfg.LogDir != "" {
			outputPaths = []string{filepath.Join(cfg.LogDir, cfg.LoggingFilename)}
		} else {
			outputPaths = []string{cfg.LoggingFilename}
		}
	}

	loggerOpts := runtime_env.BuildLoggingOptions(
		level,
		cfg.LoggingFormat,
		outputPaths,
		cfg.LoggingRotateBytes,
		cfg.LoggingRotateBackup,
	)

	if err := zap.SetupDefaultLogger(loggerOpts...); err != nil {
		return fmt.Errorf("failed to setup dashboard logger: %w", err)
	}
	return nil
}

// initGoRuntime initializes the Go runtime as a driver so the dashboard head
// can call Python actors across languages. It is called once at startup (where
// a failure degrades to running without the cross-language runtime, matching
// the Python dashboard's lazy connection), and the same initWithConfig is
// reused lazily by the cross-language modules via head.EnsureInitialized when
// the GCS becomes ready.
func initGoRuntime(cfg *head.HeadConfig) error {
	if err := head.EnsureInitialized(cfg); err != nil {
		return err
	}
	log.Log.Info("go runtime initialized for dashboard head", "gcs", cfg.GCSAddress, "node", cfg.NodeIPAddress)
	return nil
}
