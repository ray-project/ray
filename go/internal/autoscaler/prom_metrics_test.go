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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestNewAutoscalerPrometheusMetrics tests metrics creation.
func TestNewAutoscalerPrometheusMetrics(t *testing.T) {
	t.Run("CreateMetrics", func(t *testing.T) {
		sessionName := "test-session"
		metrics, err := NewAutoscalerPrometheusMetrics(sessionName)

		if err != nil {
			t.Fatalf("failed to create metrics: %v", err)
		}

		if metrics == nil {
			t.Fatal("expected metrics to be non-nil")
		}

		if metrics.SessionName != sessionName {
			t.Errorf("expected SessionName to be '%s', got '%s'", sessionName, metrics.SessionName)
		}

		if metrics.Registry == nil {
			t.Error("expected Registry to be non-nil")
		}
	})

	t.Run("EmptySessionName", func(t *testing.T) {
		metrics, err := NewAutoscalerPrometheusMetrics("")

		if err != nil {
			t.Fatalf("failed to create metrics: %v", err)
		}

		if metrics.SessionName != "" {
			t.Errorf("expected SessionName to be empty, got '%s'", metrics.SessionName)
		}
	})
}

// TestHistogramMetrics tests the histogram metrics.
func TestHistogramMetrics(t *testing.T) {
	sessionName := "test-histogram"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	t.Run("WorkerCreateNodeTimeVec", func(t *testing.T) {
		if metrics.WorkerCreateNodeTimeVec == nil {
			t.Error("expected WorkerCreateNodeTimeVec to be non-nil")
		}

		metrics.WorkerCreateNodeTime.Observe(10.5)

		var metric dto.Metric
		err := metrics.WorkerCreateNodeTime.(prometheus.Metric).Write(&metric)
		if err != nil {
			t.Fatalf("failed to write the metric: %v", err)
		}

		if metric.Histogram == nil {
			t.Error("expected Histogram to be non-nil")
		}
	})

	t.Run("WorkerUpdateTimeVec", func(t *testing.T) {
		if metrics.WorkerUpdateTimeVec == nil {
			t.Error("expected WorkerUpdateTimeVec to be non-nil")
		}

		metrics.WorkerUpdateTime.Observe(20.3)
	})

	t.Run("UpdateTimeVec", func(t *testing.T) {
		if metrics.UpdateTimeVec == nil {
			t.Error("expected UpdateTimeVec to be non-nil")
		}

		metrics.UpdateTime.Observe(0.5)
	})
}

// TestGaugeMetrics tests the gauge metrics.
func TestGaugeMetrics(t *testing.T) {
	sessionName := "test-gauge"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	t.Run("PendingNodesVec", func(t *testing.T) {
		if metrics.PendingNodesVec == nil {
			t.Error("expected PendingNodesVec to be non-nil")
		}

		metrics.PendingNodesVec.WithLabelValues("worker-node", sessionName).Set(5.0)
	})

	t.Run("ActiveNodesVec", func(t *testing.T) {
		if metrics.ActiveNodesVec == nil {
			t.Error("expected ActiveNodesVec to be non-nil")
		}

		metrics.ActiveNodesVec.WithLabelValues("head-node", sessionName).Set(1.0)
	})

	t.Run("RecentlyFailedNodesVec", func(t *testing.T) {
		if metrics.RecentlyFailedNodesVec == nil {
			t.Error("expected RecentlyFailedNodesVec to be non-nil")
		}

		metrics.RecentlyFailedNodesVec.WithLabelValues("worker-node", sessionName).Set(2.0)
	})

	t.Run("UpdatingNodes", func(t *testing.T) {
		if metrics.UpdatingNodesVec == nil {
			t.Error("expected UpdatingNodesVec to be non-nil")
		}

		if metrics.UpdatingNodes == nil {
			t.Error("expected UpdatingNodes to be non-nil")
		}

		metrics.UpdatingNodes.Set(3.0)
	})

	t.Run("RecoveringNodes", func(t *testing.T) {
		if metrics.RecoveringNodesVec == nil {
			t.Error("expected RecoveringNodesVec to be non-nil")
		}

		if metrics.RecoveringNodes == nil {
			t.Error("expected RecoveringNodes to be non-nil")
		}

		metrics.RecoveringNodes.Set(1.0)
	})

	t.Run("RunningWorkers", func(t *testing.T) {
		if metrics.RunningWorkersVec == nil {
			t.Error("expected RunningWorkersVec to be non-nil")
		}

		if metrics.RunningWorkers == nil {
			t.Error("expected RunningWorkers to be non-nil")
		}

		metrics.RunningWorkers.Set(10.0)
	})

	t.Run("ClusterResourcesVec", func(t *testing.T) {
		if metrics.ClusterResourcesVec == nil {
			t.Error("expected ClusterResourcesVec to be non-nil")
		}

		// Set the CPU resource.
		metrics.ClusterResourcesVec.WithLabelValues("CPU", sessionName).Set(64.0)
		// Set the memory resource.
		metrics.ClusterResourcesVec.WithLabelValues("memory", sessionName).Set(256.0)
	})

	t.Run("PendingResourcesVec", func(t *testing.T) {
		if metrics.PendingResourcesVec == nil {
			t.Error("expected PendingResourcesVec to be non-nil")
		}

		metrics.PendingResourcesVec.WithLabelValues("GPU", sessionName).Set(4.0)
	})
}

// TestCounterMetrics tests the counter metrics.
func TestCounterMetrics(t *testing.T) {
	sessionName := "test-counter"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	t.Run("StartedNodes", func(t *testing.T) {
		if metrics.StartedNodesVec == nil {
			t.Error("expected StartedNodesVec to be non-nil")
		}

		if metrics.StartedNodes == nil {
			t.Error("expected StartedNodes to be non-nil")
		}

		metrics.StartedNodes.Inc()
		metrics.StartedNodes.Add(5.0)
	})

	t.Run("StoppedNodes", func(t *testing.T) {
		if metrics.StoppedNodesVec == nil {
			t.Error("expected StoppedNodesVec to be non-nil")
		}

		if metrics.StoppedNodes == nil {
			t.Error("expected StoppedNodes to be non-nil")
		}

		metrics.StoppedNodes.Inc()
	})

	t.Run("FailedCreateNodes", func(t *testing.T) {
		if metrics.FailedCreateNodesVec == nil {
			t.Error("expected FailedCreateNodesVec to be non-nil")
		}

		if metrics.FailedCreateNodes == nil {
			t.Error("expected FailedCreateNodes to be non-nil")
		}

		metrics.FailedCreateNodes.Add(2.0)
	})

	t.Run("FailedUpdates", func(t *testing.T) {
		if metrics.FailedUpdatesVec == nil {
			t.Error("expected FailedUpdatesVec to be non-nil")
		}

		if metrics.FailedUpdates == nil {
			t.Error("expected FailedUpdates to be non-nil")
		}

		metrics.FailedUpdates.Inc()
	})

	t.Run("SuccessfulUpdates", func(t *testing.T) {
		if metrics.SuccessfulUpdatesVec == nil {
			t.Error("expected SuccessfulUpdatesVec to be non-nil")
		}

		if metrics.SuccessfulUpdates == nil {
			t.Error("expected SuccessfulUpdates to be non-nil")
		}

		metrics.SuccessfulUpdates.Add(10.0)
	})

	t.Run("FailedRecoveries", func(t *testing.T) {
		if metrics.FailedRecoveriesVec == nil {
			t.Error("expected FailedRecoveriesVec to be non-nil")
		}

		if metrics.FailedRecoveries == nil {
			t.Error("expected FailedRecoveries to be non-nil")
		}

		metrics.FailedRecoveries.Inc()
	})

	t.Run("SuccessfulRecoveries", func(t *testing.T) {
		if metrics.SuccessfulRecoveriesVec == nil {
			t.Error("expected SuccessfulRecoveriesVec to be non-nil")
		}

		if metrics.SuccessfulRecoveries == nil {
			t.Error("expected SuccessfulRecoveries to be non-nil")
		}

		metrics.SuccessfulRecoveries.Add(8.0)
	})

	t.Run("UpdateLoopExceptions", func(t *testing.T) {
		if metrics.UpdateLoopExceptionsVec == nil {
			t.Error("expected UpdateLoopExceptionsVec to be non-nil")
		}

		if metrics.UpdateLoopExceptions == nil {
			t.Error("expected UpdateLoopExceptions to be non-nil")
		}

		metrics.UpdateLoopExceptions.Inc()
	})

	t.Run("NodeLaunchExceptions", func(t *testing.T) {
		if metrics.NodeLaunchExceptionsVec == nil {
			t.Error("expected NodeLaunchExceptionsVec to be non-nil")
		}

		if metrics.NodeLaunchExceptions == nil {
			t.Error("expected NodeLaunchExceptions to be non-nil")
		}

		metrics.NodeLaunchExceptions.Add(3.0)
	})

	t.Run("ResetExceptions", func(t *testing.T) {
		if metrics.ResetExceptionsVec == nil {
			t.Error("expected ResetExceptionsVec to be non-nil")
		}

		if metrics.ResetExceptions == nil {
			t.Error("expected ResetExceptions to be non-nil")
		}

		metrics.ResetExceptions.Inc()
	})

	t.Run("ConfigValidationExceptions", func(t *testing.T) {
		if metrics.ConfigValidationExceptionsVec == nil {
			t.Error("expected ConfigValidationExceptionsVec to be non-nil")
		}

		if metrics.ConfigValidationExceptions == nil {
			t.Error("expected ConfigValidationExceptions to be non-nil")
		}

		metrics.ConfigValidationExceptions.Inc()
	})

	t.Run("DrainNodeExceptions", func(t *testing.T) {
		if metrics.DrainNodeExceptionsVec == nil {
			t.Error("expected DrainNodeExceptionsVec to be non-nil")
		}

		if metrics.DrainNodeExceptions == nil {
			t.Error("expected DrainNodeExceptions to be non-nil")
		}

		metrics.DrainNodeExceptions.Add(1.0)
	})
}

// TestMetricsOption tests the MetricsOption behavior.
func TestMetricsOption(t *testing.T) {
	t.Run("CustomRegistry", func(t *testing.T) {
		customRegistry := prometheus.NewRegistry()
		sessionName := "test-option"

		option := func(m *AutoscalerPrometheusMetrics) {
			m.Registry = customRegistry
		}

		metrics, err := NewAutoscalerPrometheusMetrics(sessionName, option)
		if err != nil {
			t.Fatalf("failed to create metrics: %v", err)
		}

		if metrics.Registry != customRegistry {
			t.Error("expected the custom Registry to be used")
		}
	})

	t.Run("MultipleOptions", func(t *testing.T) {
		customRegistry := prometheus.NewRegistry()
		sessionName := "test-multi-options"

		option1 := func(m *AutoscalerPrometheusMetrics) {
			m.Registry = customRegistry
		}

		option2 := func(m *AutoscalerPrometheusMetrics) {
			// Room for extra configuration.
		}

		metrics, err := NewAutoscalerPrometheusMetrics(sessionName, option1, option2)
		if err != nil {
			t.Fatalf("failed to create metrics: %v", err)
		}

		if metrics.Registry != customRegistry {
			t.Error("expected the custom Registry to be used")
		}
	})
}

// TestMetricRegistration tests metric registration.
func TestMetricRegistration(t *testing.T) {
	sessionName := "test-registration"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	// Touch every metric (by setting values) to trigger registration.
	metrics.PendingNodesVec.WithLabelValues("worker", sessionName).Set(1.0)
	metrics.ActiveNodesVec.WithLabelValues("worker", sessionName).Set(1.0)
	metrics.RecentlyFailedNodesVec.WithLabelValues("worker", sessionName).Set(1.0)
	metrics.ClusterResourcesVec.WithLabelValues("CPU", sessionName).Set(64.0)
	metrics.PendingResourcesVec.WithLabelValues("CPU", sessionName).Set(16.0)

	// Gather all registered metrics.
	metricFamilies, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// There must be several metric families.
	if len(metricFamilies) == 0 {
		t.Error("expected at least one registered metric")
	}

	// Certain metrics must be present.
	expectedMetrics := []string{
		"autoscaler_worker_create_node_time_seconds",
		"autoscaler_worker_update_time_seconds",
		"autoscaler_update_time",
		"autoscaler_pending_nodes",
		"autoscaler_active_nodes",
		"autoscaler_recently_failed_nodes",
		"autoscaler_started_nodes",
		"autoscaler_stopped_nodes",
		"autoscaler_updating_nodes",
		"autoscaler_recovering_nodes",
		"autoscaler_running_workers",
		"autoscaler_failed_create_nodes",
		"autoscaler_failed_updates",
		"autoscaler_successful_updates",
		"autoscaler_failed_recoveries",
		"autoscaler_successful_recoveries",
		"autoscaler_update_loop_exceptions",
		"autoscaler_node_launch_exceptions",
		"autoscaler_reset_exceptions",
		"autoscaler_config_validation_exceptions",
		"autoscaler_drain_node_exceptions",
		"autoscaler_cluster_resources",
		"autoscaler_pending_resources",
	}

	registeredNames := make(map[string]bool)
	for _, mf := range metricFamilies {
		registeredNames[mf.GetName()] = true
	}

	for _, expected := range expectedMetrics {
		if !registeredNames[expected] {
			t.Errorf("expected the registered metric '%s' but it was not found", expected)
		}
	}
}

// TestMetricLabels tests the metric labels.
func TestMetricLabels(t *testing.T) {
	sessionName := "test-labels"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	t.Run("WorkerCreateNodeTimeLabels", func(t *testing.T) {
		// WorkerCreateNodeTime must carry the SessionName label.
		metrics.WorkerCreateNodeTime.Observe(5.0)

		var metric dto.Metric
		err := metrics.WorkerCreateNodeTime.(prometheus.Metric).Write(&metric)
		if err != nil {
			t.Fatalf("failed to write the metric: %v", err)
		}

		foundSessionName := false
		for _, label := range metric.Label {
			if label.GetName() == "SessionName" && label.GetValue() == sessionName {
				foundSessionName = true
				break
			}
		}

		if !foundSessionName {
			t.Error("expected WorkerCreateNodeTime to carry the SessionName label")
		}
	})

	t.Run("PendingNodesVecLabels", func(t *testing.T) {
		nodeType := "worker-node"
		metrics.PendingNodesVec.WithLabelValues(nodeType, sessionName).Set(3.0)

		// Gather and verify the labels.
		metricFamilies, err := metrics.Registry.Gather()
		if err != nil {
			t.Fatalf("failed to gather metrics: %v", err)
		}

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "autoscaler_pending_nodes" {
				found = true
				if len(mf.Metric) > 0 {
					hasNodeType := false
					hasSessionName := false
					for _, label := range mf.Metric[0].Label {
						if label.GetName() == "NodeType" && label.GetValue() == nodeType {
							hasNodeType = true
						}
						if label.GetName() == "SessionName" && label.GetValue() == sessionName {
							hasSessionName = true
						}
					}
					if !hasNodeType || !hasSessionName {
						t.Error("expected PendingNodesVec to carry the NodeType and SessionName labels")
					}
				}
				break
			}
		}

		if !found {
			t.Error("expected to find the autoscaler_pending_nodes metric")
		}
	})

	t.Run("ClusterResourcesVecLabels", func(t *testing.T) {
		resourceType := "CPU"
		metrics.ClusterResourcesVec.WithLabelValues(resourceType, sessionName).Set(32.0)

		metricFamilies, err := metrics.Registry.Gather()
		if err != nil {
			t.Fatalf("failed to gather metrics: %v", err)
		}

		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == "autoscaler_cluster_resources" {
				found = true
				if len(mf.Metric) > 0 {
					hasResource := false
					hasSessionName := false
					for _, label := range mf.Metric[0].Label {
						if label.GetName() == "resource" && label.GetValue() == resourceType {
							hasResource = true
						}
						if label.GetName() == "SessionName" && label.GetValue() == sessionName {
							hasSessionName = true
						}
					}
					if !hasResource || !hasSessionName {
						t.Error("expected ClusterResourcesVec to carry the resource and SessionName labels")
					}
				}
				break
			}
		}

		if !found {
			t.Error("expected to find the autoscaler_cluster_resources metric")
		}
	})
}

// TestHistogramBuckets tests the histogram bucket configuration.
func TestHistogramBuckets(t *testing.T) {
	sessionName := "test-buckets"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	t.Run("WorkerCreateNodeTimeBuckets", func(t *testing.T) {
		// worker_create_node_time_seconds buckets span 5 seconds to 30 minutes.
		expectedBuckets := []float64{
			5, 10, 20, 30, 45, 60, 90, 120, 180, 240, 300, 360, 480, 600, 720, 900, 1200, 1500, 1800,
		}

		var metric dto.Metric
		err := metrics.WorkerCreateNodeTime.(prometheus.Metric).Write(&metric)
		if err != nil {
			t.Fatalf("failed to write the metric: %v", err)
		}

		if metric.Histogram == nil {
			t.Fatal("expected Histogram to be non-nil")
		}

		if len(metric.Histogram.Bucket) != len(expectedBuckets) {
			t.Errorf("expected %d buckets, got %d", len(expectedBuckets), len(metric.Histogram.Bucket))
		}
	})

	t.Run("UpdateTimeBuckets", func(t *testing.T) {
		// update_time buckets span 0.01 seconds to 1000 seconds.
		expectedBuckets := []float64{0.01, 0.1, 1, 10, 100, 1000}

		var metric dto.Metric
		err := metrics.UpdateTime.(prometheus.Metric).Write(&metric)
		if err != nil {
			t.Fatalf("failed to write the metric: %v", err)
		}

		if metric.Histogram == nil {
			t.Fatal("expected Histogram to be non-nil")
		}

		if len(metric.Histogram.Bucket) != len(expectedBuckets) {
			t.Errorf("expected %d buckets, got %d", len(expectedBuckets), len(metric.Histogram.Bucket))
		}
	})
}

// TestConcurrentAccess tests concurrent metric access.
func TestConcurrentAccess(t *testing.T) {
	sessionName := "test-concurrent"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	done := make(chan bool)

	// Several goroutines update the metrics concurrently.
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				metrics.StartedNodes.Inc()
				metrics.StoppedNodes.Inc()
				metrics.UpdatingNodes.Set(float64(id))
				metrics.RecoveringNodes.Set(float64(j))
				metrics.WorkerCreateNodeTime.Observe(float64(j))
				metrics.UpdateTime.Observe(float64(j) * 0.1)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines.
	for i := 0; i < 10; i++ {
		<-done
	}

	// The metrics must still be valid.
	metricFamilies, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	if len(metricFamilies) == 0 {
		t.Error("expected at least one registered metric")
	}
}

// TestMetricValues tests the metric value correctness.
func TestMetricValues(t *testing.T) {
	sessionName := "test-values"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	t.Run("CounterIncrement", func(t *testing.T) {
		initialValue := getCounterValue(metrics.StartedNodes)

		metrics.StartedNodes.Inc()
		metrics.StartedNodes.Inc()
		metrics.StartedNodes.Add(5.0)

		finalValue := getCounterValue(metrics.StartedNodes)

		if finalValue-initialValue != 7.0 {
			t.Errorf("expected the counter to increase by 7, got %v", finalValue-initialValue)
		}
	})

	t.Run("GaugeSet", func(t *testing.T) {
		metrics.UpdatingNodes.Set(10.0)
		value := getGaugeValue(metrics.UpdatingNodes)

		if value != 10.0 {
			t.Errorf("expected the gauge value to be 10.0, got %v", value)
		}

		metrics.UpdatingNodes.Set(5.0)
		value = getGaugeValue(metrics.UpdatingNodes)

		if value != 5.0 {
			t.Errorf("expected the gauge value to be 5.0, got %v", value)
		}
	})

	t.Run("HistogramObservations", func(t *testing.T) {
		// Observe several values.
		for i := 1; i <= 10; i++ {
			metrics.UpdateTime.Observe(float64(i))
		}

		var metric dto.Metric
		err := metrics.UpdateTime.(prometheus.Metric).Write(&metric)
		if err != nil {
			t.Fatalf("failed to write the metric: %v", err)
		}

		if metric.Histogram.GetSampleCount() != 10 {
			t.Errorf("expected the sample count to be 10, got %d", metric.Histogram.GetSampleCount())
		}

		expectedSum := 0.0
		for i := 1; i <= 10; i++ {
			expectedSum += float64(i)
		}

		if metric.Histogram.GetSampleSum() != expectedSum {
			t.Errorf("expected the sample sum to be %v, got %v", expectedSum, metric.Histogram.GetSampleSum())
		}
	})
}

// getCounterValue is a helper that reads a counter's value.
func getCounterValue(counter prometheus.Counter) float64 {
	var metric dto.Metric
	err := counter.Write(&metric)
	if err != nil {
		return 0.0
	}
	return metric.Counter.GetValue()
}

// getGaugeValue is a helper that reads a gauge's value.
func getGaugeValue(gauge prometheus.Gauge) float64 {
	var metric dto.Metric
	err := gauge.Write(&metric)
	if err != nil {
		return 0.0
	}
	return metric.Gauge.GetValue()
}

// TestMultipleSessions tests that sessions are isolated.
func TestMultipleSessions(t *testing.T) {
	session1 := "session-1"
	session2 := "session-2"

	metrics1, err := NewAutoscalerPrometheusMetrics(session1)
	if err != nil {
		t.Fatalf("failed to create metrics1: %v", err)
	}

	metrics2, err := NewAutoscalerPrometheusMetrics(session2)
	if err != nil {
		t.Fatalf("failed to create metrics2: %v", err)
	}

	// Set different values for the different sessions.
	metrics1.UpdatingNodes.Set(10.0)
	metrics2.UpdatingNodes.Set(20.0)

	// Verify the isolation.
	value1 := getGaugeValue(metrics1.UpdatingNodes)
	value2 := getGaugeValue(metrics2.UpdatingNodes)

	if value1 != 10.0 {
		t.Errorf("expected session1 value to be 10.0, got %v", value1)
	}

	if value2 != 20.0 {
		t.Errorf("expected session2 value to be 20.0, got %v", value2)
	}
}

// TestResourceMetrics tests the resource metrics.
func TestResourceMetrics(t *testing.T) {
	sessionName := "test-resources"
	metrics, err := NewAutoscalerPrometheusMetrics(sessionName)
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	t.Run("MultipleResourceTypes", func(t *testing.T) {
		// Set the cluster resources.
		metrics.ClusterResourcesVec.WithLabelValues("CPU", sessionName).Set(64.0)
		metrics.ClusterResourcesVec.WithLabelValues("memory", sessionName).Set(256.0)
		metrics.ClusterResourcesVec.WithLabelValues("GPU", sessionName).Set(8.0)

		// Set the pending resources.
		metrics.PendingResourcesVec.WithLabelValues("CPU", sessionName).Set(16.0)
		metrics.PendingResourcesVec.WithLabelValues("memory", sessionName).Set(64.0)
		metrics.PendingResourcesVec.WithLabelValues("GPU", sessionName).Set(2.0)

		// Gather and verify.
		metricFamilies, err := metrics.Registry.Gather()
		if err != nil {
			t.Fatalf("failed to gather metrics: %v", err)
		}

		clusterResourcesFound := false
		pendingResourcesFound := false

		for _, mf := range metricFamilies {
			if mf.GetName() == "autoscaler_cluster_resources" {
				clusterResourcesFound = true
				if len(mf.Metric) != 3 {
					t.Errorf("expected 3 cluster resource metrics, got %d", len(mf.Metric))
				}
			}
			if mf.GetName() == "autoscaler_pending_resources" {
				pendingResourcesFound = true
				if len(mf.Metric) != 3 {
					t.Errorf("expected 3 pending resource metrics, got %d", len(mf.Metric))
				}
			}
		}

		if !clusterResourcesFound {
			t.Error("expected to find the autoscaler_cluster_resources metric")
		}

		if !pendingResourcesFound {
			t.Error("expected to find the autoscaler_pending_resources metric")
		}
	})
}
