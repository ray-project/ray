package reporter

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// metricNamespace is the Prometheus namespace applied to exported metrics,
// aligned with the Python OpenCensus/OpenTelemetry Prometheus exporter which
// namespaces everything under "ray" (e.g. ray_component_cpu_percentage).
const metricNamespace = "ray"

// MetricsService converts opencensus proto.Metric values (the shape returned
// by ReportOCMetricsRequest.GetMetrics()) into Prometheus gauges registered on
// a module-local registry. It is the Go counterpart of the Python
// reporter_agent MetricsService.Export, but operates on the opencensus
// metric proto already used by the reporter pipeline instead of the
// OpenTelemetry OTLP proto.
//
// The proxied metrics are kept per reporting component and rendered at scrape
// time by openCensusProxyCollector, so a component that stops reporting stops
// being exported and the per-worker series can be aggregated into node level
// series. Conversion degrades gracefully: a malformed metric (nil descriptor,
// missing name, unsupported point type) is skipped rather than failing the
// whole batch or crashing.
type MetricsService struct {
	registry *prometheus.Registry
	gauges   map[string]*prometheus.GaugeVec
	proxy    *openCensusProxyCollector
	mu       sync.Mutex
}

// NewMetricsService returns a MetricsService that registers exported metrics
// on the given Prometheus registry. The registry is supplied by the module
// assembly so the Prometheus exporter can scrape the same registry.
func NewMetricsService(registry *prometheus.Registry) *MetricsService {
	proxy := newOpenCensusProxyCollector(componentTimeout())
	if registry != nil {
		// A registration failure (a conflicting collector) is not fatal: the
		// reporter's own gauges must keep being exported even when the proxied
		// component metrics cannot be.
		if err := registry.Register(proxy); err != nil {
			log.Log.V(1).Info("failed to register the opencensus proxy collector, proxied metrics will not be exported",
				"error", err.Error())
		}
	}
	return &MetricsService{
		registry: registry,
		gauges:   make(map[string]*prometheus.GaugeVec),
		proxy:    proxy,
	}
}

// Registry returns the underlying Prometheus registry the exported metrics are
// registered on, so a Prometheus exporter can scrape them.
func (s *MetricsService) Registry() *prometheus.Registry {
	return s.registry
}

// Collect implements prometheus.Collector by delegating to the underlying
// registry, enabling the Prometheus exporter to gather the exported metrics.
func (s *MetricsService) Collect(ch chan<- prometheus.Metric) {
	s.registry.Collect(ch)
}

// Describe implements prometheus.Collector by delegating to the underlying
// registry.
func (s *MetricsService) Describe(ch chan<- *prometheus.Desc) {
	s.registry.Describe(ch)
}

// RecordGauge sets a single gauge value on the module registry, creating the
// gauge on first use. It is the Go counterpart of the Python
// MetricsAgent.record_and_export path the reporter agent uses for its own
// process metrics. The label values are projected onto the gauge's declared
// label keys, so a tag the gauge does not declare is ignored and a declared
// label the record does not carry exports as an empty string -- exactly how the
// opencensus TagMap of a Python record is rendered against the view's columns.
func (s *MetricsService) RecordGauge(def gaugeDef, tags map[string]string, v float64) {
	gauge := s.getOrCreateGauge(def.Name, def.Help, def.LabelNames)
	if len(def.LabelNames) == 0 {
		gauge.WithLabelValues().Set(v)
		return
	}
	values := make([]string, len(def.LabelNames))
	for i, name := range def.LabelNames {
		values[i] = tags[name]
	}
	gauge.WithLabelValues(values...).Set(v)
}

// Export records the opencensus metrics reported by one component. workerID is
// the reporting worker id carried by the request; it is empty for the metrics
// pushed by the raylet or GCS, which are attributed to the global component.
// A malformed worker id is reported to the caller, mirroring the Python agent
// which lets the WorkerID constructor raise and drops the batch.
func (s *MetricsService) Export(_ context.Context, workerID []byte, metrics []*proto.Metric) error {
	workerIDHex, err := componentKeyFromWorkerID(workerID)
	if err != nil {
		return err
	}
	s.proxy.record(metrics, workerIDHex)
	return nil
}

// CleanStaleComponents drops the metrics of the components that stopped
// reporting, mirroring MetricsAgent.clean_all_dead_worker_metrics. The reporter
// calls it once per collection cycle so a dead worker's metrics stop being
// exported.
func (s *MetricsService) CleanStaleComponents() {
	s.proxy.cleanStaleComponents()
}

// getOrCreateGauge returns the GaugeVec for the given metric name, creating
// and registering it on the module registry on first use. A failed
// registration (e.g. a conflicting collector already registered) degrades to
// returning the unregistered GaugeVec, whose sets are dropped without
// crashing the exporter.
func (s *MetricsService) getOrCreateGauge(name, help string, labelNames []string) *prometheus.GaugeVec {
	s.mu.Lock()
	defer s.mu.Unlock()

	if g, ok := s.gauges[name]; ok {
		return g
	}
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: metricNamespace + "_" + name,
			Help: help,
		},
		labelNames,
	)
	if err := s.registry.Register(gauge); err == nil {
		s.gauges[name] = gauge
	}
	return gauge
}

// pointValue extracts the numeric value of a point, returning false for point
// types that cannot be represented as a gauge (distribution/summary).
func pointValue(point *proto.Point) (float64, bool) {
	switch v := point.GetValue().(type) {
	case *proto.Point_Int64Value:
		return float64(v.Int64Value), true
	case *proto.Point_DoubleValue:
		return v.DoubleValue, true
	default:
		return 0, false
	}
}
