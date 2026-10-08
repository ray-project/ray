// Copyright 2026 The Ray Authors.
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

package v2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/constant"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/scheduler"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/common/usage"
	"github.com/ray-project/ray/go/internal/event"
	"github.com/ray-project/ray/go/internal/gcs/native"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

var globalWorker = &worker{}

type worker struct {
	workerID ids.WorkerID
	mode     proto.WorkerType
}

func (gw *worker) init() {
	gw.workerID = ids.NewWorkerID()
	gw.mode = proto.WorkerType_DRIVER
}

// AUTOSCALER_METRIC_PORT / AUTOSCALER_UPDATE_INTERVAL_S live in the constant
// package: the defaults match Python (44217 / 5 seconds) and both can be
// overridden through same-named environment variables.

// Retry parameters for the Prometheus metrics server. The expected failure is
// a transient port conflict with the previous monitor process during cluster
// restarts (~1-2s), so retry a bounded number of times before giving up
// instead of retrying forever when the port is permanently occupied.
const (
	metricsServerRetryInterval = 5 * time.Second
	metricsServerMaxRetries    = 12
)

// CreateConfigReader creates the matching config reader for the given config.
func CreateConfigReader(config *MonitorV2Config) (instance_manager.IConfigReader, error) {
	// Which config reader to build depends on whether a config file is provided.
	if config.AutoscalingConfig == "" {
		// No config file: use the read-only node provider.
		log.Log.V(1).Info("No autoscaling config provided: use read only node provider.")
		return instance_manager.NewReadOnlyProviderConfigReader(config.GcsAddress)
	} else {
		// A config file is provided: use the file config reader.
		log.Log.V(1).Info("Using autoscaling config file", "path", config.AutoscalingConfig)
		autoscalingConfig, err := common.ExpandUser(config.AutoscalingConfig)
		if err != nil {
			return nil, err
		}
		return instance_manager.NewFileConfigReader(autoscalingConfig, true)
	}
}

// RecordAutoscalerV2Usage sends a usage tag to GCS marking that the cluster is
// running the v2 autoscaler.
func RecordAutoscalerV2Usage(gcsClient gcs.Client) {
	if err := usage.RecordExtraUsageTag(constant.Ctx, proto.TagKey_AUTOSCALER_VERSION, "v2", gcsClient); err != nil {
		log.Log.V(1).Error(err, "Error recording usage for autoscaler v2.")
		return
	}
	return
}

// eventLoggerAdapter adapts an AutoscalerEventLogger to the scheduler.EventLogger
// interface. The two LogClusterSchedulingUpdate signatures differ in parameter
// order (the AutoscalerEventLogger starts with clusterResources and returns an
// error), so the arguments are reordered here to match the scheduler interface.
type eventLoggerAdapter struct {
	logger *AutoscalerEventLogger
}

func (a *eventLoggerAdapter) LogClusterSchedulingUpdate(
	launchRequests []*proto.LaunchRequest,
	terminateRequests []*proto.TerminationRequest,
	infeasibleRequests []*proto.ResourceRequest,
	infeasibleGangRequests []*proto.GangResourceRequest,
	infeasibleClusterResourceConstraints []*proto.ClusterResourceConstraint,
	clusterResources map[string]float64,
) {
	if a == nil || a.logger == nil {
		return
	}
	_ = a.logger.LogClusterSchedulingUpdate(
		clusterResources,
		launchRequests,
		terminateRequests,
		infeasibleRequests,
		infeasibleGangRequests,
		infeasibleClusterResourceConstraints,
	)
}

// AutoscalerMonitor is the main class of the Ray autoscaler monitor.
type AutoscalerMonitor struct {
	gcsAddress     string
	gcsClient      gcs.Client
	configReader   instance_manager.IConfigReader
	logDir         string // Log directory used for the event log
	monitorIP      string
	sessionName    string
	eventLogger    *AutoscalerEventLogger
	metricReporter *AutoscalerMetricsReporter
	autoscaler     *Autoscaler // The autoscaler assembling all long-lived collaborators (see autoscaler.go)
	metricsServer  *http.Server
	workerID       ids.WorkerID // Worker ID of the driver process
}

// NewAutoscalerMonitor creates and initializes an AutoscalerMonitor instance.
func NewAutoscalerMonitor(address string, configReader instance_manager.IConfigReader, logDir string, monitorIP string) (*AutoscalerMonitor, error) {
	gcsClient, err := gcs.GetClient()
	if err != nil || gcsClient == nil {
		gcsClient, err = native.ConnectClient(gcs.ClientOptions{
			Address:   address,
			ClusterID: ids.NilClusterID(),
			TimeoutMs: 10000,
		})
		if err != nil || gcsClient == nil {
			return nil, fmt.Errorf("failed to connect to GCS at '%s': %w", address, err)
		}
	}
	// Record the v2 usage.
	RecordAutoscalerV2Usage(gcsClient)

	monitor := &AutoscalerMonitor{}
	monitor.gcsAddress = address
	monitor.gcsClient = gcsClient

	globalWorker.init()

	// Register the metrics server address in GCS.
	// A non-empty monitorIP means the Prometheus metrics endpoint must be exposed.
	if monitorIP != "" {
		// Build the metrics server address "monitorIP:port" (port comes from
		// constant.AUTOSCALER_METRIC_PORT).
		monitorAddr, err := common.BuildAddress(monitorIP, constant.AUTOSCALER_METRIC_PORT)
		if err != nil {
			return nil, err
		}

		// Store the metrics address in the GCS key-value store:
		// key: "AutoscalerMetricsAddress"
		// value: "monitorIP:port"
		putRes, err := monitor.gcsClient.Put(constant.Ctx, "", "AutoscalerMetricsAddress", []byte(monitorAddr), true)
		if err != nil {
			log.Log.V(1).Error(err, "Failed to register metrics address to GCS")
			return nil, err
		}
		if !putRes {
			// A GCS backend can report "not written" without an error even for an
			// overwriting put (the cgo client maps the C++ success flag this way).
			// Log a warning and continue: returning (nil, err) with a nil error
			// made the caller start Run() on a nil monitor and panic.
			log.Log.V(1).Info("Metrics address was not registered in GCS (no error reported).", "address", monitorAddr)
		}
	}
	// Fetch the name of the current Ray session.
	sessionName, err := getSessionName(gcsClient)
	if err != nil {
		log.Log.V(1).Error(err, "Failed to get session name from GCS")
		return nil, err
	}
	monitor.sessionName = sessionName
	log.Log.V(1).Info("Session name retrieved", "sessionName", sessionName)

	headNodeIP, _, err := common.ValidateHostPort(monitor.gcsAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid GCS address '%s': %w", address, err)
	}
	// Initialize the event logger. A failure here must leave the event logger
	// nil: wrapping a nil logger into the adapter would nil-deref on the first
	// scheduling update. On any failure the monitor keeps running without event
	// logging and the scheduler's nil-guard skips it.
	if logDir != "" {
		rayEventLogger, err := event.GetEventLogger(proto.Event_AUTOSCALER, logDir)
		if err != nil {
			log.Log.V(1).Error(err, "failed to get event logger")
		} else {
			monitor.eventLogger, err = NewAutoscalerEventLogger(rayEventLogger)
			if err != nil {
				log.Log.V(1).Error(err, "failed to create AutoscalerEventLogger")
				monitor.eventLogger = nil
			}
		}
	} else {
		monitor.eventLogger = nil
	}

	// Initialize the metrics reporter and the Prometheus metrics server.
	promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics(monitor.sessionName)
	if err != nil {
		return nil, err
	}
	metricsReporter, err := NewAutoscalerMetricsReporter(promMetrics)
	if err != nil {
		return nil, err
	}
	monitor.metricReporter = metricsReporter

	// Start the Prometheus HTTP server.
	if monitorIP != "" {
		log.Log.V(1).Info(fmt.Sprintf("Starting autoscaler metrics server on port %d", constant.AUTOSCALER_METRIC_PORT))

		var bindAddr string
		if headNodeIP == "127.0.0.1" {
			bindAddr = "127.0.0.1"
			log.Log.V(1).Info("Binding metrics server to localhost only (head node is local)")
		} else {
			// An empty string means "0.0.0.0" for http.ListenAndServe.
			bindAddr = ""
			log.Log.V(1).Info("Binding metrics server to all interfaces")
		}

		// Start the HTTP server listening on the port asynchronously.
		// ListenAndServe failures are retried a bounded number of times at a fixed
		// interval: during a cluster restart the previous monitor process may still
		// hold the port briefly ("address already in use"); without retrying, this
		// session would permanently lose its metrics endpoint. Give up and log a
		// terminal message once the retry budget is exhausted.
		go func() {
			for attempt := 0; ; attempt++ {
				server, err := monitor.createHTTPServer(constant.AUTOSCALER_METRIC_PORT, promMetrics.Registry, bindAddr)
				if err != nil {
					log.Log.Error(err, "An exception occurred while starting the metrics server",
						"port", constant.AUTOSCALER_METRIC_PORT, "bindAddr", bindAddr)
					return
				}

				log.Log.V(1).Info("Starting Prometheus metrics server",
					"address", bindAddr, "port", constant.AUTOSCALER_METRIC_PORT)
				// Block in the background until the service is shut down; log and
				// retry on an abnormal shutdown error.
				if err := server.ListenAndServe(); err != http.ErrServerClosed {
					if attempt >= metricsServerMaxRetries {
						log.Log.Error(err, "Prometheus metrics server keeps failing, giving up",
							"address", bindAddr, "attempts", attempt+1)
						return
					}
					log.Log.Error(err, "Prometheus metrics server stopped unexpectedly, retrying",
						"address", bindAddr, "port", constant.AUTOSCALER_METRIC_PORT,
						"attempt", attempt+1, "maxAttempts", metricsServerMaxRetries+1)
					time.Sleep(metricsServerRetryInterval)
					continue
				}
				return
			}
		}()
	}

	// Create the core Autoscaler: it assembles all the long-lived collaborators
	// (error queues / cloud provider / instance manager and subscribers /
	// scheduler / reconciler), corresponding to the Python Autoscaler.__init__ +
	// _init_cloud_instance_provider + _init_instance_manager.
	// Pass a nil interface when the event log is nil; the scheduler skips it.
	var schedEventLogger scheduler.EventLogger
	if monitor.eventLogger != nil {
		schedEventLogger = &eventLoggerAdapter{logger: monitor.eventLogger}
	}
	monitor.autoscaler, err = NewAutoscaler(
		gcsClient,
		configReader,
		sessionName,
		schedEventLogger,
		monitor.metricReporter,
	)
	if err != nil {
		return nil, err
	}

	monitor.configReader = configReader
	monitor.logDir = logDir
	monitor.monitorIP = monitorIP
	return monitor, nil
}

// getSessionName fetches the name of the current Ray session from GCS.
// Reads the session name from the "session" KV namespace: the head node
// writes it there (mirroring Python ray_constants.KV_NAMESPACE_SESSION).
// If the key is absent, the session name is considered empty and the monitor
// keeps running, matching Python where a missing session name only means the
// reported metrics lack the session label.
func getSessionName(gcsClient gcs.Client) (string, error) {
	ctx, cancel := context.WithTimeout(constant.Ctx, 10*time.Second)
	defer cancel()

	sessionNameBytes, err := gcsClient.Get(ctx, constant.KV_NAMESPACE_SESSION, "session_name")
	if err != nil {
		if errors.Is(err, gcs.ErrKeyNotFound) {
			log.Log.V(1).Info("Session name not found in GCS, continue without it.")
			return "", nil
		}
		return "", fmt.Errorf("failed to get session name from GCS: %w", err)
	}

	if len(sessionNameBytes) == 0 {
		return "", nil
	}

	return string(sessionNameBytes), nil
}

// reportAutoscalingState reports the autoscaling state to GCS.
// It corresponds to the Python _report_autoscaling_state: the state is binary
// serialized and reported through gcs.Client.ReportAutoscalingState; a report or
// serialization failure is only logged and never interrupts the monitor loop.
func (am *AutoscalerMonitor) reportAutoscalingState(autoscalingState *proto.AutoscalingState) {
	serialized, err := protolib.Marshal(autoscalingState)
	if err != nil {
		log.Log.Error(err, "Error serializing autoscaling state.")
		return
	}
	if err := am.gcsClient.ReportAutoscalingState(string(serialized)); err != nil {
		log.Log.Error(err, "Error reporting autoscaling state to GCS.")
	}
}

// run runs the monitor loop.
// It corresponds to the Python AutoscalerMonitor._run() and each round performs:
//  1. autoscaler.UpdateAutoscalingState to reconcile the cluster once; on error
//     the loop terminates and the error propagates (matching the Python
//     exception propagation / process termination semantics)
//  2. when the state is non-empty, report it to GCS through
//     reportAutoscalingState (failures are only logged, the loop continues)
//  3. when the state is empty, print a warning log and keep looping
//  4. sleep AUTOSCALER_UPDATE_INTERVAL_S (5 seconds by default, overridable via
//     the environment variable) before the next round
//
// Difference from the Python side: the Python time.sleep is uninterruptible,
// while this loop supports graceful shutdown through ctx; the production path
// uses constant.Ctx from Run(), which is never canceled, matching Python.
func (am *AutoscalerMonitor) run(ctx context.Context) error {
	for {
		autoscalingState, err := am.autoscaler.UpdateAutoscalingState()
		if err != nil {
			return err
		}

		if autoscalingState != nil {
			// Report the autoscaling state; failures are only logged, the loop
			// continues.
			am.reportAutoscalingState(autoscalingState)
		} else {
			log.Log.Info("No autoscaling state to report.")
		}

		// Wait for one update interval before handling the next round.
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Duration(constant.AUTOSCALER_UPDATE_INTERVAL_S) * time.Second):
		}
	}
}

// Run is the entry method that starts the monitor loop.
// It corresponds to the Python AutoscalerMonitor.run(): when the loop fails the
// error is logged and returned (the Python side logs and re-raises; the Go side
// propagates through the error return value).
func (am *AutoscalerMonitor) Run() error {
	err := am.run(constant.Ctx)
	if err != nil {
		log.Log.Error(err, "Error in monitor loop")
		return err
	}
	return nil
}

func (monitor *AutoscalerMonitor) createHTTPServer(port int, registry *prometheus.Registry, bindAddr string) (*http.Server, error) {
	if registry == nil {
		log.Log.V(1).Error(nil, "Registry is nil, metrics server will not be started.")
		return nil, fmt.Errorf("registry is nil, metrics server will not be started")
	}

	addr, err := common.BuildAddress(bindAddr, port)
	if err != nil {
		return nil, err
	}

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})

	// Register on a server-private mux instead of the global DefaultServeMux:
	// the latter panics on route conflicts when a server is created twice in the
	// same process.
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second, // Read timeout
		WriteTimeout: 10 * time.Second, // Write timeout
		IdleTimeout:  60 * time.Second, // Idle connection timeout
	}
	monitor.metricsServer = server
	return server, nil
}

// CustomPanicHook captures a panic and reports it to GCS.
func CustomPanicHook(r interface{}) {
	if r != nil {
		stack := debug.Stack()
		errorMsg := fmt.Sprintf("%s", stack)
		doCustomExceptHook(errorMsg)
	}
}

// CustomErrorHook captures an error and invokes the custom exception handling.
func CustomErrorHook(err error) {
	if err != nil {
		errorMsg := fmt.Sprintf("%v\n", err)
		doCustomExceptHook(errorMsg)
	}
}

// doCustomExceptHook runs the custom exception handling logic.
// It reports the exception information to the GCS worker table.
func doCustomExceptHook(errorMsg string) {
	if globalWorker.mode == proto.WorkerType_DRIVER && !globalWorker.workerID.IsNil() {
		if _, err := common.State.ConnectAndGetAccessor(); err != nil {
			errorMsg = fmt.Sprintf("%s\n%v\n", errorMsg, err)
		}
		workerInfo := map[string]string{
			"exception": errorMsg,
		}
		common.State.AddWorker(globalWorker.workerID, globalWorker.mode, workerInfo)
	}

	log.Log.Error(nil, fmt.Sprintf("Unhandled exception in Driver process: %s", errorMsg))
}
