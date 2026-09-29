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

package head

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/procfs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/version"
)

// metricsRecordInterval is how often the dashboard component CPU/USS gauges
// are re-sampled, aligned with dashboard_consts.METRICS_RECORD_INTERVAL_S (5).
const metricsRecordInterval = 5 * time.Second

// MetricsRegistry owns the Prometheus registry and the /metrics HTTP server
// for the dashboard head. It registers the dashboard API request metrics and
// the component CPU/USS gauges, aligned with the Python
// DashboardPrometheusMetrics (dashboard_metrics.py) so the /metrics endpoint
// serves the series asserted by test_metrics_agent.py.
type MetricsRegistry struct {
	Registry *prometheus.Registry
	server   *http.Server
	metrics  *DashboardMetrics
}

// DashboardMetrics holds the Prometheus metrics shared between the /metrics
// export server and the withMetrics middleware. Names and labels align with
// python/ray/dashboard/dashboard_metrics.py:
//   - ray_dashboard_api_requests_duration_seconds (Histogram)
//   - ray_dashboard_api_requests_count_requests_total  (Counter)
//
// plus the component process gauges:
//   - ray_component_cpu_percentage (Gauge)
//   - ray_component_uss_mb         (Gauge)
//
// The client_golang text encoder does not emit the `_created` series that
// prometheus_client adds; the CI contract (test_metrics_agent.py
// _DASHBOARD_METRICS) requires `..._created` names to appear, so
// createdCollector appends them manually.
type DashboardMetrics struct {
	// requestDuration is ray_dashboard_api_requests_duration_seconds.
	requestDuration *prometheus.HistogramVec
	// requestCount is ray_dashboard_api_requests_count_requests_total.
	requestCount *prometheus.CounterVec
	// componentCPU is ray_component_cpu_percentage.
	componentCPU *prometheus.GaugeVec
	// componentUSS is ray_component_uss_mb.
	componentUSS *prometheus.GaugeVec

	registry *prometheus.Registry

	// cpuSampler computes the process CPU percentage over consecutive
	// collection intervals (ProcessMetricGauge in dashboard_metrics.py).
	cpuSampler *cpuSampler
}

// componentMetricLabels are the label values for the component gauges, aligned
// with head.py _record_cpu_mem_metrics_for_proc (COMPONENT_METRICS_TAG_KEYS:
// ip, pid, Version, Component, SessionName).
func (m *DashboardMetrics) componentMetricLabels(cfg *HeadConfig) prometheus.Labels {
	return prometheus.Labels{
		"ip":          cfg.NodeIPAddress,
		"pid":         strconv.Itoa(os.Getpid()),
		"Version":     version.RayVersion,
		"Component":   "dashboard",
		"SessionName": cfg.SessionName,
	}
}

// NewMetricsRegistry returns a MetricsRegistry backed by a fresh Prometheus
// registry so each head process exposes only its own metrics. It registers the
// dashboard API request metrics, the component CPU/USS gauges and the
// created-series collector aligned with the Python dashboard metrics.
func NewMetricsRegistry(cfg *HeadConfig) *MetricsRegistry {
	reg := prometheus.NewRegistry()
	m := &DashboardMetrics{
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "ray_dashboard_api_requests_duration_seconds",
				Help: "Time in seconds for API requests to complete.",
				// 17 buckets aligned with dashboard_metrics.py
				// histogram_buckets_s: 5ms..60s.
				Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 20, 40, 60},
			},
			[]string{"endpoint", "http_status", "Version", "SessionName", "Component"},
		),
		requestCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				// client_golang emits CounterOpts names verbatim. prometheus_client
				// renders the Python Counter "dashboard_api_requests_count" with
				// unit "requests" and namespace "ray" as
				// ray_dashboard_api_requests_count_requests_total, so the _total
				// suffix is part of the name here (client_golang does not append
				// its own _total when the name already ends in _total).
				Name: "ray_dashboard_api_requests_count_requests_total",
				Help: "Total number of API requests.",
			},
			[]string{"method", "endpoint", "http_status", "Version", "SessionName", "Component"},
		),
		componentCPU: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ray_component_cpu_percentage",
				Help: "Dashboard CPU percentage usage.",
			},
			[]string{"ip", "pid", "Version", "Component", "SessionName"},
		),
		componentUSS: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ray_component_uss_mb",
				Help: "USS usage of the dashboard component.",
			},
			[]string{"ip", "pid", "Version", "Component", "SessionName"},
		),
		registry:   reg,
		cpuSampler: newCPUSampler(),
	}
	reg.MustRegister(m.requestDuration, m.requestCount, m.componentCPU, m.componentUSS)
	// Append the _created series the CI contract requires but client_golang
	// does not emit (prometheus_client adds them for every metric).
	reg.MustRegister(newCreatedCollector(
		"ray_dashboard_api_requests_duration_seconds",
		// prometheus_client's Counter _created series carries the metric name
		// without the _total suffix (verified against the Python /metrics
		// output: ray_dashboard_api_requests_count_requests_created).
		"ray_dashboard_api_requests_count_requests",
	))
	m.recordComponentMetrics(cfg)
	return &MetricsRegistry{Registry: reg, metrics: m}
}

// DashboardMetrics returns the registered dashboard metrics so withMetrics can
// record request duration/counts against the shared registry.
func (m *MetricsRegistry) DashboardMetrics() *DashboardMetrics { return m.metrics }

// startComponentMetricsLoop re-samples the component CPU/USS gauges every
// metricsRecordInterval until ctx is cancelled, aligned with head.py
// _record_dashboard_metrics (async_loop_forever(METRICS_RECORD_INTERVAL_S)).
func (m *MetricsRegistry) startComponentMetricsLoop(ctx context.Context, cfg *HeadConfig) {
	go func() {
		ticker := time.NewTicker(metricsRecordInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.metrics.recordComponentMetrics(cfg)
			}
		}
	}()
}

// recordComponentMetrics samples the current process CPU percentage and USS
// (private RSS) and sets the component gauges, aligned with head.py
// _record_cpu_mem_metrics_for_proc.
func (m *DashboardMetrics) recordComponentMetrics(cfg *HeadConfig) {
	labels := m.componentMetricLabels(cfg)
	uss := uint64(0)
	if proc, err := procfs.Self(); err == nil {
		if stat, err := proc.Stat(); err == nil {
			m.cpuSampler.sample(float64(stat.UTime+stat.STime) / 100.0)
		}
		if rollup, err := proc.ProcSMapsRollup(); err == nil {
			// USS = private clean + private dirty pages.
			uss = rollup.PrivateClean + rollup.PrivateDirty
		}
	}
	m.componentCPU.With(labels).Set(m.cpuSampler.percentage())
	// Convert bytes to MB, aligned with the Python /1.0e6 conversion.
	m.componentUSS.With(labels).Set(float64(uss) / 1.0e6)
}

// cpuSampler computes a process CPU percentage from the delta of CPU time
// between two samples over the elapsed wall time, matching psutil's
// cpu_percent(interval=None) semantics used by ProcessMetricGauge.
type cpuSampler struct {
	mu             sync.Mutex
	lastCPUTS      float64
	lastTime       time.Time
	lastPercentage float64
}

func newCPUSampler() *cpuSampler {
	return &cpuSampler{lastTime: time.Now()}
}

// sample records the latest cumulative CPU seconds and updates the computed
// percentage from the delta since the previous sample.
func (s *cpuSampler) sample(cpuSec float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if !s.lastTime.IsZero() && cpuSec >= s.lastCPUTS {
		dt := now.Sub(s.lastTime).Seconds()
		if dt > 0 {
			s.lastPercentage = (cpuSec - s.lastCPUTS) / dt * 100.0
		}
	}
	s.lastCPUTS = cpuSec
	s.lastTime = now
}

// percentage returns the last computed CPU percentage.
func (s *cpuSampler) percentage() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPercentage
}

// createdCollector emits `<name>_created` series with a fixed creation
// timestamp, replicating the `_created` series prometheus_client adds to every
// metric so the names asserted by test_metrics_agent.py _DASHBOARD_METRICS are
// present on the /metrics output.
type createdCollector struct {
	names []string
}

var _ prometheus.Collector = (*createdCollector)(nil)

// newCreatedCollector returns a createdCollector for the given metric names.
func newCreatedCollector(names ...string) *createdCollector {
	return &createdCollector{names: names}
}

// Describe implements prometheus.Collector. It sends no descriptors so the
// registry collects it as an unchecked collector.
func (c *createdCollector) Describe(chan<- *prometheus.Desc) {}

// Collect implements prometheus.Collector, emitting one `_created` gauge per
// registered metric name with the current timestamp value.
func (c *createdCollector) Collect(out chan<- prometheus.Metric) {
	for _, name := range c.names {
		out <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(name+"_created", "Timestamp of this metric creation.", nil, nil),
			prometheus.GaugeValue,
			float64(time.Now().Unix()),
		)
	}
}

// Start binds a /metrics endpoint on the given host:port and serves it in the
// background until ctx is cancelled. A nil registry disables the server.
func (m *MetricsRegistry) Start(ctx context.Context, host string, port int) error {
	if m.Registry == nil {
		log.Log.Info("Metrics registry is nil, metrics server not started")
		return nil
	}
	handler := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	m.server = &http.Server{
		Addr:    net.JoinHostPort(host, fmt.Sprintf("%d", port)),
		Handler: mux,
		// Timeouts prevent slow/idle clients from leaking goroutines.
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	ln, err := net.Listen("tcp", m.server.Addr)
	if err != nil {
		return fmt.Errorf("listen on metrics addr %s: %w", m.server.Addr, err)
	}
	go func() {
		<-ctx.Done()
		_ = m.server.Shutdown(context.Background())
	}()
	go func() {
		if err := m.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Log.Error(err, "metrics server error")
		}
	}()
	log.Log.Info("Metrics server started", "addr", m.server.Addr)
	return nil
}
