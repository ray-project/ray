/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// ReconcileTotal is a prometheus counter metrics which holds the total
	// number of reconciliations per controller. It has two labels. controller label refers
	// to the controller name and result label refers to the reconcile result i.e
	// success, error, requeue, requeue_after.
	ReconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "controller_runtime_reconcile_total",
		Help: "Total number of reconciliations per controller",
	}, []string{"controller", "result"})

	// ReconcileErrors is a prometheus counter metrics which holds the total
	// number of errors from the Reconciler.
	ReconcileErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "controller_runtime_reconcile_errors_total",
		Help: "Total number of reconciliation errors per controller",
	}, []string{"controller"})

	// ReconcileTime is a prometheus metric which keeps track of the duration
	// of reconciliations.
	ReconcileTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "controller_runtime_reconcile_time_seconds",
		Help: "Length of time per reconciliation per controller",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0,
			1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 40, 50, 60},
	}, []string{"controller"})

	// WorkerCount is a prometheus metric which holds the number of
	// concurrent reconciles per controller.
	WorkerCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "controller_runtime_max_concurrent_reconciles",
		Help: "Maximum number of concurrent reconciles per controller",
	}, []string{"controller"})

	// ActiveWorkers is a prometheus metric which holds the number
	// of active workers per controller.
	ActiveWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "controller_runtime_active_workers",
		Help: "Number of currently used workers per controller",
	}, []string{"controller"})
)

func init() {
	metrics.Registry.MustRegister(
		ReconcileTotal,
		ReconcileErrors,
		ReconcileTime,
		WorkerCount,
		ActiveWorkers,
		// expose process metrics like CPU, Memory, file descriptor usage etc.
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		// expose Go runtime metrics like GC stats, memory stats etc.
		collectors.NewGoCollector(),
	)
}
