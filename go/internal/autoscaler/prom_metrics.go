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

package autoscaler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/pkg/log"
)

// AutoscalerPrometheusMetrics defines the autoscaler Prometheus metrics.
type AutoscalerPrometheusMetrics struct {
	SessionName string
	Registry    *prometheus.Registry

	// Histograms.
	WorkerCreateNodeTimeVec *prometheus.HistogramVec
	WorkerCreateNodeTime    prometheus.Observer
	WorkerUpdateTimeVec     *prometheus.HistogramVec
	WorkerUpdateTime        prometheus.Observer
	UpdateTimeVec           *prometheus.HistogramVec
	UpdateTime              prometheus.Observer

	// Gauges.
	PendingNodesVec *prometheus.GaugeVec
	//// todo: check whether caching is needed.
	//PendingNodes prometheus.Gauge
	ActiveNodesVec *prometheus.GaugeVec
	//// todo: check whether caching is needed.
	//ActiveNodes prometheus.Gauge
	RecentlyFailedNodesVec *prometheus.GaugeVec
	//// todo: check whether caching is needed.
	//RecentlyFailedNodes prometheus.Gauge
	UpdatingNodesVec   *prometheus.GaugeVec
	UpdatingNodes      prometheus.Gauge
	RecoveringNodesVec *prometheus.GaugeVec
	RecoveringNodes    prometheus.Gauge
	RunningWorkersVec  *prometheus.GaugeVec
	RunningWorkers     prometheus.Gauge

	ClusterResourcesVec *prometheus.GaugeVec
	PendingResourcesVec *prometheus.GaugeVec

	// Counters.
	StartedNodesVec               *prometheus.CounterVec
	StartedNodes                  prometheus.Counter
	StoppedNodesVec               *prometheus.CounterVec
	StoppedNodes                  prometheus.Counter
	FailedCreateNodesVec          *prometheus.CounterVec
	FailedCreateNodes             prometheus.Counter
	FailedUpdatesVec              *prometheus.CounterVec
	FailedUpdates                 prometheus.Counter
	SuccessfulUpdatesVec          *prometheus.CounterVec
	SuccessfulUpdates             prometheus.Counter
	FailedRecoveriesVec           *prometheus.CounterVec
	FailedRecoveries              prometheus.Counter
	SuccessfulRecoveriesVec       *prometheus.CounterVec
	SuccessfulRecoveries          prometheus.Counter
	UpdateLoopExceptionsVec       *prometheus.CounterVec
	UpdateLoopExceptions          prometheus.Counter
	NodeLaunchExceptionsVec       *prometheus.CounterVec
	NodeLaunchExceptions          prometheus.Counter
	ResetExceptionsVec            *prometheus.CounterVec
	ResetExceptions               prometheus.Counter
	ConfigValidationExceptionsVec *prometheus.CounterVec
	ConfigValidationExceptions    prometheus.Counter
	DrainNodeExceptionsVec        *prometheus.CounterVec
	DrainNodeExceptions           prometheus.Counter
}

// MetricsOption is a functional option that configures AutoscalerPrometheusMetrics.
type MetricsOption func(*AutoscalerPrometheusMetrics)

// NewAutoscalerPrometheusMetrics creates the Prometheus registry and metrics.
func NewAutoscalerPrometheusMetrics(sessionName string, opts ...MetricsOption) (*AutoscalerPrometheusMetrics, error) {
	registry := prometheus.NewRegistry()

	metrics := &AutoscalerPrometheusMetrics{
		SessionName: sessionName,
		Registry:    registry,
	}

	for _, opt := range opts {
		opt(metrics)
	}

	if metrics.Registry == nil {
		metrics.Registry = registry
	}

	// Histogram buckets: 5 seconds to 30 minutes, used for the worker launch
	// time and worker update time.
	histogramBuckets := []float64{
		5, 10, 20, 30, 45, 60, 90, 120, 180, 240, 300, 360, 480, 600, 720, 900, 1200, 1500, 1800,
	}

	// Update time histogram buckets: 0.01 seconds to 1000 seconds.
	updateTimeBuckets := []float64{0.01, 0.1, 1, 10, 100, 1000}

	// workerCreateNodeTime: worker launch time.
	workerCreateNodeTimeVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "autoscaler",
			Name:      "worker_create_node_time_seconds",
			Help: "Worker launch time. This is the time it takes for a call to " +
				"a node provider's create_node method to return. Note that " +
				"when nodes are launched in batches, the launch time for that " +
				"batch will be observed once for *each* node in that batch. " +
				"For example, if 8 nodes are launched in 3 minutes, a launch " +
				"time of 3 minutes will be observed 8 times. (Unit: seconds)",
			Buckets: histogramBuckets,
		},
		[]string{"SessionName"},
	)
	if err := metrics.Registry.Register(workerCreateNodeTimeVec); err != nil {
		log.Log.Error(err, "Warning: failed to register worker_create_node_time_seconds")
	}
	metrics.WorkerCreateNodeTime = workerCreateNodeTimeVec.With(prometheus.Labels{
		"SessionName": sessionName,
	})
	metrics.WorkerCreateNodeTimeVec = workerCreateNodeTimeVec

	// workerUpdateTime: worker update time.
	workerUpdateTimeVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "autoscaler",
			Name:      "worker_update_time_seconds",
			Help: "Worker update time. This is the time between when an updater " +
				"thread begins executing and when it exits successfully. This " +
				"metric only observes times for successful updates. (Unit: seconds)",
			Buckets: histogramBuckets,
		},
		[]string{"SessionName"},
	)
	if err := metrics.Registry.Register(workerUpdateTimeVec); err != nil {
		log.Log.Error(err, "Warning: failed to register worker_update_time_seconds")
		return nil, err
	}
	metrics.WorkerUpdateTime = workerUpdateTimeVec.With(prometheus.Labels{
		"SessionName": sessionName,
	})
	metrics.WorkerUpdateTimeVec = workerUpdateTimeVec

	// updateTime: autoscaler update iteration time.
	updateTimeVec := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "autoscaler",
			Name:      "update_time",
			Help:      "Autoscaler update time. This is the time for an autoscaler update iteration to complete. (Unit: seconds)",
			Buckets:   updateTimeBuckets,
		},
		[]string{"SessionName"},
	)
	if err := metrics.Registry.Register(updateTimeVec); err != nil {
		log.Log.Error(err, "Warning: failed to register update_time")
	}
	metrics.UpdateTime = updateTimeVec.With(prometheus.Labels{
		"SessionName": sessionName,
	})
	metrics.UpdateTimeVec = updateTimeVec

	// pendingNodes: number of nodes pending launch.
	pendingNodesVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "pending_nodes",
			Help:      "Number of nodes pending to be started. (Unit: nodes)",
		},
		[]string{
			"NodeType",
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(pendingNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register pending_nodes")
	}
	metrics.PendingNodesVec = pendingNodesVec

	// activeNodes: number of active nodes.
	activeNodesVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "active_nodes",
			Help:      "Number of nodes in the cluster. (Unit: nodes)",
		},
		[]string{
			"NodeType",
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(activeNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register active_nodes")
	}
	metrics.ActiveNodesVec = activeNodesVec

	// recentlyFailedNodes: number of recently failed nodes.
	recentlyFailedNodesVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "recently_failed_nodes",
			Help:      "The number of recently failed nodes. This count could reset at undefined times. (unit: nodes)",
		},
		[]string{
			"NodeType",
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(recentlyFailedNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register recently_failed_nodes")
	}
	metrics.RecentlyFailedNodesVec = recentlyFailedNodesVec

	// startedNodes: number of started nodes.
	startedNodesVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "started_nodes",
			Help:      "Number of nodes started. (Unit: nodes)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(startedNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register started_nodes")
	}
	metrics.StartedNodesVec = startedNodesVec
	metrics.StartedNodes = startedNodesVec.WithLabelValues(sessionName)

	// stopped_nodes: number of stopped nodes.
	stoppedNodesVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "stopped_nodes",
			Help:      "Number of nodes stopped. (Unit: nodes)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(stoppedNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register stopped_nodes")
	}
	metrics.StoppedNodesVec = stoppedNodesVec
	metrics.StoppedNodes = stoppedNodesVec.WithLabelValues(sessionName)

	// updatingNodes: number of nodes being updated.
	updatingNodesVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "updating_nodes",
			Help:      "Number of nodes in the process of updating. (Unit: nodes)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(updatingNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register updating_nodes")
	}
	metrics.UpdatingNodesVec = updatingNodesVec
	metrics.UpdatingNodes = updatingNodesVec.WithLabelValues(sessionName)

	// recoveringNodes: number of nodes being recovered.
	recoveringNodesVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "recovering_nodes",
			Help:      "Number of nodes in the process of recovering. (Unit: nodes)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(recoveringNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register recovering_nodes")
	}
	metrics.RecoveringNodesVec = recoveringNodesVec
	metrics.RecoveringNodes = recoveringNodesVec.WithLabelValues(sessionName)

	// runningWorkers: number of running worker nodes.
	runningWorkersVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "running_workers",
			Help:      "Number of worker nodes running. (Unit: nodes)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(runningWorkersVec); err != nil {
		log.Log.Error(err, "Warning: failed to register running_workers")
	}
	metrics.RunningWorkersVec = runningWorkersVec
	metrics.RunningWorkers = runningWorkersVec.WithLabelValues(sessionName)

	// failedCreateNodes: number of nodes that failed to be created.
	failedCreateNodesVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "failed_create_nodes",
			Help:      "Number of nodes that failed to be created due to an exception in the node provider's create_node method. (Unit: nodes)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(failedCreateNodesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register failed_create_nodes")
	}
	metrics.FailedCreateNodesVec = failedCreateNodesVec
	metrics.FailedCreateNodes = failedCreateNodesVec.WithLabelValues(sessionName)

	// failedUpdates: number of failed updates.
	failedUpdatesVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "failed_updates",
			Help:      "Number of failed worker node updates. (Unit: updates)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(failedUpdatesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register failed_updates")
	}
	metrics.FailedUpdatesVec = failedUpdatesVec
	metrics.FailedUpdates = failedUpdatesVec.WithLabelValues(sessionName)

	// successfulUpdates: number of successful updates.
	successfulUpdatesVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "successful_updates",
			Help:      "Number of succesfful worker node updates. (Unit: updates)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(successfulUpdatesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register successful_updates")
	}
	metrics.SuccessfulUpdatesVec = successfulUpdatesVec
	metrics.SuccessfulUpdates = successfulUpdatesVec.WithLabelValues(sessionName)

	// failedRecoveries: number of failed recoveries.
	failedRecoveriesVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "failed_recoveries",
			Help:      "Number of failed node recoveries. (Unit: recoveries)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(failedRecoveriesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register failed_recoveries")
	}
	metrics.FailedRecoveriesVec = failedRecoveriesVec
	metrics.FailedRecoveries = failedRecoveriesVec.WithLabelValues(sessionName)

	// successfulRecoveries: number of successful recoveries.
	successfulRecoveriesVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "successful_recoveries",
			Help:      "Number of successful node recoveries. (Unit: recoveries)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(successfulRecoveriesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register successful_recoveries")
	}
	metrics.SuccessfulRecoveriesVec = successfulRecoveriesVec
	metrics.SuccessfulRecoveries = successfulRecoveriesVec.WithLabelValues(sessionName)

	// updateLoopExceptions: number of update loop exceptions.
	updateLoopExceptionsVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "update_loop_exceptions",
			Help:      "Number of exceptions raised in the update loop of the autoscaler. (Unit: exceptions)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(updateLoopExceptionsVec); err != nil {
		log.Log.Error(err, "Warning: failed to register update_loop_exceptions")
	}
	metrics.UpdateLoopExceptionsVec = updateLoopExceptionsVec
	metrics.UpdateLoopExceptions = updateLoopExceptionsVec.WithLabelValues(sessionName)

	// nodeLaunchExceptions: number of node launch exceptions.
	nodeLaunchExceptionsVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "node_launch_exceptions",
			Help:      "Number of exceptions raised while launching nodes. (Unit: exceptions)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(nodeLaunchExceptionsVec); err != nil {
		log.Log.Error(err, "Warning: failed to register node_launch_exceptions")
	}
	metrics.NodeLaunchExceptionsVec = nodeLaunchExceptionsVec
	metrics.NodeLaunchExceptions = nodeLaunchExceptionsVec.WithLabelValues(sessionName)

	// resetExceptions: number of reset exceptions.
	resetExceptionsVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "reset_exceptions",
			Help:      "Number of exceptions raised while resetting the autoscaler. (Unit: exceptions)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(resetExceptionsVec); err != nil {
		log.Log.Error(err, "Warning: failed to register reset_exceptions")
	}
	metrics.ResetExceptionsVec = resetExceptionsVec
	metrics.ResetExceptions = resetExceptionsVec.WithLabelValues(sessionName)

	// configValidationExceptions: number of config validation exceptions.
	configValidationExceptionsVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "config_validation_exceptions",
			Help:      "Number of exceptions raised while validating the config during a reset. (Unit: exceptions)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(configValidationExceptionsVec); err != nil {
		log.Log.Error(err, "Warning: failed to register config_validation_exceptions")
	}
	metrics.ConfigValidationExceptionsVec = configValidationExceptionsVec
	metrics.ConfigValidationExceptions = configValidationExceptionsVec.WithLabelValues(sessionName)

	// drainNodeExceptions: number of DrainNode RPC exceptions.
	drainNodeExceptionsVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "autoscaler",
			Name:      "drain_node_exceptions",
			Help:      "Number of exceptions raised when making a DrainNode rpc prior to node termination. (Unit: exceptions)",
		},
		[]string{
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(drainNodeExceptionsVec); err != nil {
		log.Log.Error(err, "Warning: failed to register drain_node_exceptions")
	}
	metrics.DrainNodeExceptionsVec = drainNodeExceptionsVec
	metrics.DrainNodeExceptions = drainNodeExceptionsVec.WithLabelValues(sessionName)

	// clusterResources: total logical resources of the cluster.
	clusterResourcesVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "cluster_resources",
			Help:      "Total logical resources in the cluster. (Unit: resources)",
		},
		[]string{
			"resource",
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(clusterResourcesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register cluster_resources")
	}
	metrics.ClusterResourcesVec = clusterResourcesVec

	// pendingResources: pending logical resources.
	pendingResourcesVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "autoscaler",
			Name:      "pending_resources",
			Help:      "Pending logical resources in the cluster. (Unit: resources)",
		},
		[]string{
			"resource",
			"SessionName",
		},
	)
	if err := metrics.Registry.Register(pendingResourcesVec); err != nil {
		log.Log.Error(err, "Warning: failed to register pending_resources")
	}
	metrics.PendingResourcesVec = pendingResourcesVec

	return metrics, nil
}
