package reporter

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
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
// Conversion degrades gracefully: a malformed metric (nil descriptor, missing
// name, unsupported point type) is skipped rather than failing the whole
// batch or crashing.
type MetricsService struct {
	registry *prometheus.Registry
	gauges   map[string]*prometheus.GaugeVec
	mu       sync.Mutex
}

// NewMetricsService returns a MetricsService that registers exported metrics
// on the given Prometheus registry. The registry is supplied by the module
// assembly so the Prometheus exporter can scrape the same registry.
func NewMetricsService(registry *prometheus.Registry) *MetricsService {
	return &MetricsService{
		registry: registry,
		gauges:   make(map[string]*prometheus.GaugeVec),
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

// Export converts the given opencensus metrics into Prometheus gauges
// registered on the module registry and sets their values. Invalid metrics are
// skipped; the batch never fails on a single bad metric.
func (s *MetricsService) Export(_ context.Context, metrics []*proto.Metric) error {
	for _, metric := range metrics {
		s.exportMetric(metric)
	}
	return nil
}

// exportMetric converts a single opencensus metric. Metrics that cannot be
// converted are skipped silently.
func (s *MetricsService) exportMetric(metric *proto.Metric) {
	if metric == nil || metric.GetMetricDescriptor() == nil {
		return
	}
	desc := metric.GetMetricDescriptor()
	if desc.GetName() == "" {
		return
	}

	// Keep the descriptor keys that carry a name and remember their indexes, so
	// the series label values can be read at exactly those indexes. Dropping an
	// empty key must drop the matching value too, or the values no longer line
	// up with the gauge's label names and WithLabelValues panics.
	keys := desc.GetLabelKeys()
	labelNames := make([]string, 0, len(keys))
	keptIndexes := make([]int, 0, len(keys))
	for i, lk := range keys {
		if lk != nil && lk.GetKey() != "" {
			labelNames = append(labelNames, lk.GetKey())
			keptIndexes = append(keptIndexes, i)
		}
	}

	gauge := s.getOrCreateGauge(desc.GetName(), desc.GetDescription(), labelNames)

	for _, ts := range metric.GetTimeseries() {
		if ts == nil {
			continue
		}
		labelValues, ok := labelValuesForKeys(ts, keptIndexes)
		if !ok {
			// The series does not carry a value for every retained key; skip it
			// rather than mislabel or panic on a cardinality mismatch.
			continue
		}

		for _, point := range ts.GetPoints() {
			if point == nil {
				continue
			}
			value, ok := pointValue(point)
			if !ok {
				continue
			}
			gauge.WithLabelValues(labelValues...).Set(value)
		}
	}
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

// labelValuesForKeys extracts the series label values at the retained key
// indexes, so the result is aligned one-to-one with the gauge's label names
// even when some descriptor keys were dropped. It returns false when the series
// is missing a value for a retained key.
func labelValuesForKeys(ts *proto.TimeSeries, keptIndexes []int) ([]string, bool) {
	raw := ts.GetLabelValues()
	values := make([]string, 0, len(keptIndexes))
	for _, i := range keptIndexes {
		if i >= len(raw) {
			return nil, false
		}
		lv := raw[i]
		if lv == nil || !lv.GetHasValue() {
			values = append(values, "")
		} else {
			values = append(values, lv.GetValue())
		}
	}
	return values, true
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
