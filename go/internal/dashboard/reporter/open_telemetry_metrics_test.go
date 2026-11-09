package reporter

// Tests for the OpenTelemetry OTLP metrics path in open_telemetry_metrics.go:
// the MetricsService servicer registered on the dashboard agent's gRPC server
// and the recorder that converts the reported metrics into Prometheus series.
//
// The unit tests drive Export with hand-built OTLP requests and assert on what
// a real scrape of the shared registry returns, so instrument registration,
// label projection, cardinality dropping and value translation are all
// exercised end to end. The integration test starts a real gRPC server, dials
// it with the generated OTLP client and asserts the reported metric reaches
// the registry, which is what proves the servicer is reachable over the wire.

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpmetricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	otlpresourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// stringAttr builds a string-valued OTLP attribute.
func stringAttr(key, value string) *otlpcommon.KeyValue {
	return &otlpcommon.KeyValue{
		Key:   key,
		Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: value}},
	}
}

// intAttr builds an int-valued OTLP attribute, used to check that non-string
// values follow the Python string_value read.
func intAttr(key string, value int64) *otlpcommon.KeyValue {
	return &otlpcommon.KeyValue{
		Key:   key,
		Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_IntValue{IntValue: value}},
	}
}

// exportRequest wraps OTLP metrics in the resource/scope nesting the OTLP
// protocol requires.
func exportRequest(metrics ...*otlpmetricsv1.Metric) *collectormetricsv1.ExportMetricsServiceRequest {
	return &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*otlpmetricsv1.ResourceMetrics{{
			Resource: &otlpresourcev1.Resource{},
			ScopeMetrics: []*otlpmetricsv1.ScopeMetrics{{
				Metrics: metrics,
			}},
		}},
	}
}

// numberPoint builds a number data point carrying the given attributes.
func numberPoint(value float64, attrs ...*otlpcommon.KeyValue) *otlpmetricsv1.NumberDataPoint {
	return &otlpmetricsv1.NumberDataPoint{
		Attributes: attrs,
		Value:      &otlpmetricsv1.NumberDataPoint_AsDouble{AsDouble: value},
	}
}

// gaugeMetric builds a gauge metric named name.
func gaugeMetric(name, description string, points ...*otlpmetricsv1.NumberDataPoint) *otlpmetricsv1.Metric {
	return &otlpmetricsv1.Metric{
		Name:        name,
		Description: description,
		Data:        &otlpmetricsv1.Metric_Gauge{Gauge: &otlpmetricsv1.Gauge{DataPoints: points}},
	}
}

// sumMetric builds a sum metric named name with the given monotonicity.
func sumMetric(name, description string, monotonic bool, points ...*otlpmetricsv1.NumberDataPoint) *otlpmetricsv1.Metric {
	return &otlpmetricsv1.Metric{
		Name:        name,
		Description: description,
		Data: &otlpmetricsv1.Metric_Sum{Sum: &otlpmetricsv1.Sum{
			IsMonotonic: monotonic,
			DataPoints:  points,
		}},
	}
}

// histogramMetric builds a histogram metric with the given explicit bounds and
// bucket counts.
func histogramMetric(name string, bounds []float64, counts []uint64, attrs ...*otlpcommon.KeyValue) *otlpmetricsv1.Metric {
	var total uint64
	for _, count := range counts {
		total += count
	}
	return &otlpmetricsv1.Metric{
		Name: name,
		Data: &otlpmetricsv1.Metric_Histogram{Histogram: &otlpmetricsv1.Histogram{
			DataPoints: []*otlpmetricsv1.HistogramDataPoint{{
				Attributes:     attrs,
				ExplicitBounds: bounds,
				BucketCounts:   counts,
				Count:          total,
			}},
		}},
	}
}

// gatherFamily returns the metric family with the given name, failing the test
// when the registry does not expose it.
func gatherFamily(t *testing.T, registry *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found; gathered %d families", name, len(families))
	return nil
}

// countSamples returns the number of samples the registry exposes for the
// named family, or 0 when the registry does not expose the family at all.
// Python's Prometheus reader behaves the same way: a family with no live
// observations is omitted from the scrape rather than reported empty, so a
// test that expects a cleared metric must assert on the count and not on the
// presence of the family.
func countSamples(t *testing.T, registry *prometheus.Registry, name string) int {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return len(family.GetMetric())
		}
	}
	return 0
}

// samplesOf returns the samples of a single-family metric, failing the test
// when the family is missing.
func samplesOf(t *testing.T, registry *prometheus.Registry, name string) []*dto.Metric {
	t.Helper()
	return gatherFamily(t, registry, name).GetMetric()
}

// labelValue returns the value of one label of a sample, or the empty string
// when the sample does not carry it.
func labelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

// counterValuesByLabel maps the value of each counter sample to the value of
// the given label, so a test can assert per-series accumulation.
func counterValuesByLabel(metrics []*dto.Metric, label string) map[string]float64 {
	values := make(map[string]float64, len(metrics))
	for _, metric := range metrics {
		values[labelValue(metric, label)] = metric.GetCounter().GetValue()
	}
	return values
}

// unsetEnv removes an environment variable for the duration of the test,
// restoring its previous value afterwards. t.Setenv cannot express "unset",
// which the default-value cases need.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	previous, existed := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unsetenv %s: %v", key, err)
	}
	t.Cleanup(func() {
		if !existed {
			_ = os.Unsetenv(key)
			return
		}
		_ = os.Setenv(key, previous)
	})
}

// TestOTelExportRegistersGauge verifies a reported gauge is published on the
// registry under the ray_ namespace with its attribute as a label and its
// reported value, mirroring the Python PrometheusMetricReader output.
func TestOTelExportRegistersGauge(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.25, stringAttr("ip", "1.2.3.4"))),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	family := gatherFamily(t, registry, "ray_node_cpu_usage")
	if got := family.GetType().String(); got != "GAUGE" {
		t.Errorf("type = %s, want GAUGE", got)
	}
	if got := family.GetHelp(); got != "cpu usage" {
		t.Errorf("help = %q, want %q", got, "cpu usage")
	}
	samples := family.GetMetric()
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if got := samples[0].GetGauge().GetValue(); got != 0.25 {
		t.Errorf("value = %v, want 0.25", got)
	}
	if got := labelValue(samples[0], "ip"); got != "1.2.3.4" {
		t.Errorf("label ip = %q, want %q", got, "1.2.3.4")
	}
}

// TestOTelExportMonotonicSumIsCounter verifies a monotonic sum becomes a
// Prometheus counter and accumulates across exports, matching the C++ side
// which exports deltas the agent is expected to accumulate.
func TestOTelExportMonotonicSumIsCounter(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	for _, value := range []float64{2, 3} {
		if _, err := service.Export(context.Background(), exportRequest(
			sumMetric("tasks", "num tasks", true, numberPoint(value, stringAttr("State", "RUNNING"))),
		)); err != nil {
			t.Fatalf("Export: %v", err)
		}
	}

	family := gatherFamily(t, registry, "ray_tasks")
	if got := family.GetType().String(); got != "COUNTER" {
		t.Errorf("type = %s, want COUNTER", got)
	}
	samples := family.GetMetric()
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if got := samples[0].GetCounter().GetValue(); got != 5 {
		t.Errorf("value = %v, want 5 (2 accumulated with 3)", got)
	}
}

// TestOTelExportNonMonotonicSumIsGauge verifies a non-monotonic sum (an
// up-down counter in OpenTelemetry) is modelled as a gauge holding the last
// reported value, since Prometheus has no up-down counter.
func TestOTelExportNonMonotonicSumIsGauge(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		sumMetric("active_objects", "active objects", false, numberPoint(7, stringAttr("ip", "10.0.0.1"))),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	family := gatherFamily(t, registry, "ray_active_objects")
	if got := family.GetType().String(); got != "GAUGE" {
		t.Errorf("type = %s, want GAUGE", got)
	}
	samples := family.GetMetric()
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if got := samples[0].GetGauge().GetValue(); got != 7 {
		t.Errorf("value = %v, want 7", got)
	}
}

// TestOTelExportGaugeIsClearedPerCollect verifies the observable-gauge
// clear-on-collect semantics: a series reported once appears in the scrape that
// follows it and is gone from the next scrape, because the collector consumed
// and cleared its pending observations. This is what removes the metrics of a
// component that stopped reporting.
// TestOTelExportGaugeSurvivesScrapesUntilTTL verifies a reported gauge value is
// served to every scrape until it ages out, instead of being consumed by the
// first one. A scrape interval shorter than the report interval would otherwise
// blank the series between two reports, which is the behaviour the Python
// recorder explicitly avoids.
func TestOTelExportGaugeSurvivesScrapesUntilTTL(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	// The first export registers the instrument; the clock is pinned afterwards
	// so the timestamp it records is the one the test controls.
	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5, stringAttr("ip", "1.2.3.4"))),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}
	base := time.Now()
	store := instrumentStore(t, service, "node_cpu_usage")
	store.now = func() time.Time { return base }
	store.ttl = 20 * time.Second
	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5, stringAttr("ip", "1.2.3.4"))),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	for scrape := 1; scrape <= 3; scrape++ {
		if got := countSamples(t, registry, "ray_node_cpu_usage"); got != 1 {
			t.Fatalf("scrape %d samples = %d, want 1 (retained for the ttl)", scrape, got)
		}
	}

	store.now = func() time.Time { return base.Add(21 * time.Second) }
	if got := countSamples(t, registry, "ray_node_cpu_usage"); got != 0 {
		t.Errorf("samples after the ttl = %d, want 0 (the observation aged out)", got)
	}
}

// TestOTelExportNonMonotonicSumAccumulates verifies the points of a
// non-monotonic sum add up across reports. Each point carries the increment
// since the previous report, so replacing the value would keep only the last
// one and undercount by as many reports as the scrape interval holds.
func TestOTelExportNonMonotonicSumAccumulates(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	for _, increment := range []float64{2, 3, 5} {
		if _, err := service.Export(context.Background(), exportRequest(
			sumMetric("worker_tasks", "tasks", false, numberPoint(increment, stringAttr("ip", "1.2.3.4"))),
		)); err != nil {
			t.Fatalf("Export: %v", err)
		}
	}

	samples := samplesOf(t, registry, "ray_worker_tasks")
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if got := samples[0].GetGauge().GetValue(); got != 10 {
		t.Errorf("value = %v, want 10 (2+3+5 across three reports)", got)
	}
}

// instrumentStore returns the observation store of a registered observable
// gauge so a test can control its clock and retention window.
func instrumentStore(t *testing.T, service *OpenTelemetryMetricsService, name string) *otelGaugeStore {
	t.Helper()
	instrument, ok := service.instruments[name]
	if !ok {
		t.Fatalf("instrument %q not registered", name)
	}
	if instrument.store == nil {
		t.Fatalf("instrument %q has no observation store", name)
	}
	return instrument.store
}

// TestOTelExportGaugeKeepsLatestValuePerSeries verifies an observable gauge
// holds the last value reported for a series within one collection window,
// mirroring the Python dict assignment keyed by the tag set.
func TestOTelExportGaugeKeepsLatestValuePerSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage",
			numberPoint(0.5, stringAttr("ip", "1.2.3.4")),
			numberPoint(0.9, stringAttr("ip", "1.2.3.4")),
		),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	samples := samplesOf(t, registry, "ray_node_cpu_usage")
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1 (one series, latest value wins)", len(samples))
	}
	if got := samples[0].GetGauge().GetValue(); got != 0.9 {
		t.Errorf("value = %v, want 0.9 (the latest reported value)", got)
	}
}

// TestOTelExportAggregatesHighCardinalityMetrics verifies the recommended
// cardinality level drops the WorkerId label for the tasks metric and sums the
// values of the series that collapse onto the same remaining label set,
// mirroring MetricCardinality.get_aggregation_function.
func TestOTelExportAggregatesHighCardinalityMetrics(t *testing.T) {
	t.Setenv(metricCardinalityLevelEnv, metricCardinalityRecommended)
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		sumMetric("tasks", "num tasks", true,
			numberPoint(2, stringAttr(workerIDTagKey, "w1"), stringAttr("State", "RUNNING")),
			numberPoint(3, stringAttr(workerIDTagKey, "w2"), stringAttr("State", "RUNNING")),
		),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	samples := samplesOf(t, registry, "ray_tasks")
	if len(samples) != 1 {
		t.Fatalf("series = %d, want 1 after WorkerId is dropped", len(samples))
	}
	if got := samples[0].GetCounter().GetValue(); got != 5 {
		t.Errorf("value = %v, want 5 (2 and 3 summed)", got)
	}
	if got := labelValue(samples[0], workerIDTagKey); got != "" {
		t.Errorf("label %s = %q, want it dropped at the recommended cardinality level", workerIDTagKey, got)
	}
}

// TestOTelExportGaugeAggregatesHighCardinality verifies the aggregation also
// applies to observable gauges, which is where the Python callback performs it.
func TestOTelExportGaugeAggregatesHighCardinality(t *testing.T) {
	t.Setenv(metricCardinalityLevelEnv, metricCardinalityRecommended)
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("actors", "num actors",
			numberPoint(4, stringAttr(workerIDTagKey, "w1"), stringAttr("State", "ALIVE")),
			numberPoint(6, stringAttr(workerIDTagKey, "w2"), stringAttr("State", "ALIVE")),
		),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	samples := samplesOf(t, registry, "ray_actors")
	if len(samples) != 1 {
		t.Fatalf("series = %d, want 1 after WorkerId is dropped", len(samples))
	}
	if got := samples[0].GetGauge().GetValue(); got != 10 {
		t.Errorf("value = %v, want 10 (4 and 6 summed)", got)
	}
}

// TestOTelExportLowCardinalityAlsoDropsName verifies the low level additionally
// drops the Name label for tasks and actors.
func TestOTelExportLowCardinalityAlsoDropsName(t *testing.T) {
	t.Setenv(metricCardinalityLevelEnv, metricCardinalityLow)
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		sumMetric("tasks", "num tasks", true,
			numberPoint(1, stringAttr(workerIDTagKey, "w1"), stringAttr(taskOrActorNameTagKey, "foo")),
			numberPoint(1, stringAttr(workerIDTagKey, "w2"), stringAttr(taskOrActorNameTagKey, "bar")),
		),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	samples := samplesOf(t, registry, "ray_tasks")
	if len(samples) != 1 {
		t.Fatalf("series = %d, want 1 after WorkerId and Name are dropped", len(samples))
	}
	if got := samples[0].GetCounter().GetValue(); got != 2 {
		t.Errorf("value = %v, want 2", got)
	}
	if got := labelValue(samples[0], taskOrActorNameTagKey); got != "" {
		t.Errorf("label %s = %q, want it dropped at the low cardinality level", taskOrActorNameTagKey, got)
	}
}

// TestOTelExportLegacyKeepsHighCardinalityLabels verifies the legacy level
// keeps every label, so distinct worker ids stay distinct series and are not
// aggregated.
func TestOTelExportLegacyKeepsHighCardinalityLabels(t *testing.T) {
	t.Setenv(metricCardinalityLevelEnv, metricCardinalityLegacy)
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		sumMetric("tasks", "num tasks", true,
			numberPoint(2, stringAttr(workerIDTagKey, "w1")),
			numberPoint(3, stringAttr(workerIDTagKey, "w2")),
		),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	samples := samplesOf(t, registry, "ray_tasks")
	if len(samples) != 2 {
		t.Fatalf("series = %d, want 2 (legacy keeps WorkerId)", len(samples))
	}
	values := counterValuesByLabel(samples, workerIDTagKey)
	if values["w1"] != 2 || values["w2"] != 3 {
		t.Errorf("values = %v, want w1=2 w2=3", values)
	}
}

// TestOTelExportHistogramReconstructsMidpoints verifies a reported histogram is
// replayed at the reconstructed bucket midpoints. With bounds [1, 5] and one
// count in the (1,5] bucket, the observation lands at (1+5)/2 = 3.
func TestOTelExportHistogramReconstructsMidpoints(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		histogramMetric("task_processing_time", []float64{1, 5}, []uint64{0, 1, 0}, stringAttr("ip", "1.2.3.4")),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	family := gatherFamily(t, registry, "ray_task_processing_time")
	if got := family.GetType().String(); got != "HISTOGRAM" {
		t.Errorf("type = %s, want HISTOGRAM", got)
	}
	samples := family.GetMetric()
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	histogram := samples[0].GetHistogram()
	if got := histogram.GetSampleCount(); got != 1 {
		t.Errorf("sample count = %d, want 1", got)
	}
	if got := histogram.GetSampleSum(); got != 3 {
		t.Errorf("sample sum = %v, want 3 (the (1,5] bucket midpoint)", got)
	}
}

// TestOTelExportHistogramBucketCountsAreReplayed verifies every count of a
// bucket produces one observation, so a bucket holding three samples
// contributes three times its midpoint to the sum.
func TestOTelExportHistogramBucketCountsAreReplayed(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		histogramMetric("task_processing_time", []float64{2, 4}, []uint64{0, 3, 0}),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	histogram := samplesOf(t, registry, "ray_task_processing_time")[0].GetHistogram()
	if got := histogram.GetSampleCount(); got != 3 {
		t.Errorf("sample count = %d, want 3", got)
	}
	// The (2,4] bucket midpoint is (2+4)/2 = 3, observed three times.
	if got := histogram.GetSampleSum(); got != 9 {
		t.Errorf("sample sum = %v, want 9", got)
	}
}

// TestReconstructBucketMidpoints verifies the midpoint reconstruction,
// including the implicit +Inf bucket, against the Python formula. The expected
// values are the ones the Python implementation produces for the same bounds.
func TestReconstructBucketMidpoints(t *testing.T) {
	cases := []struct {
		name    string
		buckets []float64
		want    []float64
	}{
		// First bound above zero: the lower edge is treated as 0, so the first
		// midpoint is 1/2 and the +Inf midpoint is 5*2.
		{name: "positive first bound", buckets: []float64{1, 5}, want: []float64{0.5, 3, 10}},
		// First bound at or below zero: the lower edge doubles it, giving
		// (-2*2 + -2)/2 = -3, and the +Inf midpoint is 4*2.
		{name: "non-positive first bound", buckets: []float64{-2, 4}, want: []float64{-3, 1, 8}},
		// A last bound at or below zero makes the +Inf midpoint 1, and the
		// first midpoint is (-4*2 + -4)/2 = -6.
		{name: "non-positive last bound", buckets: []float64{-4, 0}, want: []float64{-6, -2, 1}},
		{name: "no bounds", buckets: nil, want: []float64{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reconstructBucketMidpoints(tc.buckets)
			if len(got) != len(tc.want) {
				t.Fatalf("midpoints = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("midpoints = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestOTelExportHistogramSkipsCountZeroDataPoints verifies a data point with no
// observations is skipped entirely, matching the Python guard on
// data_point.count. A count-zero data point must contribute nothing at all: the
// family is registered but stays absent from the scrape, exactly as the Python
// reader omits a metric family that has no live observations.
func TestOTelExportHistogramSkipsCountZeroDataPoints(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		histogramMetric("task_processing_time", []float64{1}, []uint64{0, 0}),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if got := countSamples(t, registry, "ray_task_processing_time"); got != 0 {
		t.Errorf("samples after a count-zero data point = %d, want 0", got)
	}

	// A later data point with observations must still be recorded, proving the
	// guard skipped the empty data point rather than the whole metric.
	if _, err := service.Export(context.Background(), exportRequest(
		histogramMetric("task_processing_time", []float64{1}, []uint64{0, 3}),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	histogram := samplesOf(t, registry, "ray_task_processing_time")[0].GetHistogram()
	if got := histogram.GetSampleCount(); got != 3 {
		t.Errorf("sample count = %d, want 3", got)
	}
	// The (1,+Inf] bucket midpoint is 1*2 = 2, observed three times.
	if got := histogram.GetSampleSum(); got != 6 {
		t.Errorf("sample sum = %v, want 6", got)
	}
}

// TestOTelExportNonStringAttributeRendersEmpty verifies an attribute whose
// value is not a string renders as the empty label value, mirroring the Python
// read of tag.value.string_value on a non-string AnyValue.
func TestOTelExportNonStringAttributeRendersEmpty(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5, intAttr("IsRetry", 1))),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	samples := samplesOf(t, registry, "ray_node_cpu_usage")
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if got := labelValue(samples[0], "IsRetry"); got != "" {
		t.Errorf("label IsRetry = %q, want the empty string", got)
	}
}

// TestOTelExportFillsMissingLabelsPerSeries verifies a series that does not
// carry every declared attribute key renders the missing ones as the empty
// string, so the metric vector accepts the series instead of rejecting it.
func TestOTelExportFillsMissingLabelsPerSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage",
			numberPoint(0.5, stringAttr("ip", "1.2.3.4"), stringAttr("State", "RUNNING")),
			numberPoint(0.7, stringAttr("ip", "5.6.7.8")),
		),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	samples := samplesOf(t, registry, "ray_node_cpu_usage")
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want 2", len(samples))
	}
	for _, sample := range samples {
		if labelValue(sample, "ip") != "5.6.7.8" {
			continue
		}
		if got := labelValue(sample, "State"); got != "" {
			t.Errorf("label State = %q, want the empty string for a series without it", got)
		}
	}
}

// TestOTelExportRegistersMetricOnce verifies the same metric reported by
// several components registers one instrument: repeated exports of the same
// name must not register a duplicate collector, which the Prometheus registry
// would reject.
func TestOTelExportRegistersMetricOnce(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	for i := 0; i < 3; i++ {
		if _, err := service.Export(context.Background(), exportRequest(
			gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5, stringAttr("ip", "1.2.3.4"))),
		)); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	count := 0
	for _, family := range families {
		if family.GetName() == "ray_node_cpu_usage" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("ray_node_cpu_usage families = %d, want 1", count)
	}
}

// TestOTelExportDisabledRecordsNothing verifies the servicer acknowledges an
// export without recording when metrics collection is disabled.
func TestOTelExportDisabledRecordsNothing(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, true)

	response, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5, stringAttr("ip", "1.2.3.4"))),
	))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if response == nil {
		t.Fatal("Export returned a nil response")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(families) != 0 {
		t.Errorf("families = %d, want 0 when metrics collection is disabled", len(families))
	}
}

// TestOTelExportNilRegistryDegrades verifies a servicer built without a
// registry acknowledges exports instead of panicking.
func TestOTelExportNilRegistryDegrades(t *testing.T) {
	service := NewOpenTelemetryMetricsService(nil, false)
	if _, err := service.Export(context.Background(), exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5)),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

// TestOTelExportSkipsUnsupportedMetricType verifies a metric whose data is
// neither a gauge, a sum nor a histogram is skipped without failing the batch,
// so the supported metrics in the same request are still recorded.
func TestOTelExportSkipsUnsupportedMetricType(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	if _, err := service.Export(context.Background(), exportRequest(
		&otlpmetricsv1.Metric{Name: "some_summary", Data: &otlpmetricsv1.Metric_Summary{
			Summary: &otlpmetricsv1.Summary{},
		}},
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5, stringAttr("ip", "1.2.3.4"))),
	)); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if got := len(samplesOf(t, registry, "ray_node_cpu_usage")); got != 1 {
		t.Errorf("samples = %d, want 1 (the supported metric must still be recorded)", got)
	}
}

// TestCardinalityLevel verifies the environment variable parsing, including the
// default and the fallback for an unrecognised value.
func TestCardinalityLevel(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want string
	}{
		{name: "default", env: "", want: metricCardinalityRecommended},
		{name: "legacy", env: "legacy", want: metricCardinalityLegacy},
		{name: "recommended", env: "recommended", want: metricCardinalityRecommended},
		{name: "low", env: "low", want: metricCardinalityLow},
		{name: "uppercase", env: "LOW", want: metricCardinalityLow},
		{name: "unrecognised falls back to legacy", env: "bogus", want: metricCardinalityLegacy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env == "" {
				unsetEnv(t, metricCardinalityLevelEnv)
			} else {
				t.Setenv(metricCardinalityLevelEnv, tc.env)
			}
			if got := cardinalityLevel(); got != tc.want {
				t.Errorf("cardinalityLevel() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestOpenTelemetryEnabled verifies the RAY_enable_open_telemetry switch,
// which is enabled unless explicitly turned off.
func TestOpenTelemetryEnabled(t *testing.T) {
	unsetEnv(t, openTelemetryEnabledEnv)
	if !openTelemetryEnabled() {
		t.Error("open telemetry must be enabled when RAY_enable_open_telemetry is unset")
	}

	t.Setenv(openTelemetryEnabledEnv, "1")
	if !openTelemetryEnabled() {
		t.Error("open telemetry must be enabled when RAY_enable_open_telemetry=1")
	}

	t.Setenv(openTelemetryEnabledEnv, "false")
	if openTelemetryEnabled() {
		t.Error("open telemetry must be disabled when RAY_enable_open_telemetry=false")
	}
}

// TestOTelServicerOverGRPC verifies the servicer is reachable over the wire: a
// real gRPC server serving the MetricsService, dialed with the generated OTLP
// client, delivers a metric that shows up on the shared registry. This is the
// path the raylet, the workers and GCS use to report their metrics.
func TestOTelServicerOverGRPC(t *testing.T) {
	registry := prometheus.NewRegistry()
	service := NewOpenTelemetryMetricsService(registry, false)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	collectormetricsv1.RegisterMetricsServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	response, err := collectormetricsv1.NewMetricsServiceClient(conn).Export(ctx, exportRequest(
		gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.75, stringAttr("ip", "1.2.3.4"))),
	))
	if err != nil {
		t.Fatalf("Export over gRPC: %v", err)
	}
	if response == nil {
		t.Fatal("Export over gRPC returned a nil response")
	}

	samples := samplesOf(t, registry, "ray_node_cpu_usage")
	if len(samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(samples))
	}
	if got := samples[0].GetGauge().GetValue(); got != 0.75 {
		t.Errorf("value = %v, want 0.75", got)
	}
}

// TestReporterModuleRegistersOTelService verifies the reporter module registers
// the OTLP MetricsService on the agent's gRPC server when the OpenTelemetry
// backend is enabled, and leaves it unregistered when the backend is turned
// off. The check dials the server and calls Export, so it observes the actual
// service registry rather than trusting the registration call.
func TestReporterModuleRegistersOTelService(t *testing.T) {
	cases := []struct {
		name       string
		env        string
		unset      bool
		wantServed bool
	}{
		{name: "enabled by default", unset: true, wantServed: true},
		{name: "enabled explicitly", env: "true", wantServed: true},
		{name: "disabled", env: "false", wantServed: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.unset {
				unsetEnv(t, openTelemetryEnabledEnv)
			} else {
				t.Setenv(openTelemetryEnabledEnv, tc.env)
			}

			module, _ := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})
			if module.otelService == nil {
				t.Fatal("otelService was not assembled by New")
			}

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			server := grpc.NewServer()
			if err := module.RegisterGRPC(server); err != nil {
				t.Fatalf("RegisterGRPC: %v", err)
			}
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := grpc.DialContext(ctx, listener.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()

			_, err = collectormetricsv1.NewMetricsServiceClient(conn).Export(ctx, exportRequest(
				gaugeMetric("node_cpu_usage", "cpu usage", numberPoint(0.5, stringAttr("ip", "1.2.3.4"))),
			))
			if !tc.wantServed {
				if err == nil {
					t.Fatal("Export succeeded although the MetricsService must not be registered")
				}
				if !strings.Contains(err.Error(), "Unimplemented") {
					t.Fatalf("Export error = %v, want an Unimplemented error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Export: %v", err)
			}

			families, err := module.metricsService.Registry().Gather()
			if err != nil {
				t.Fatalf("gather: %v", err)
			}
			samples := 0
			for _, family := range families {
				if family.GetName() == "ray_node_cpu_usage" {
					samples = len(family.GetMetric())
				}
			}
			if samples != 1 {
				t.Errorf("ray_node_cpu_usage samples = %d, want 1", samples)
			}
		})
	}
}

// TestReporterModuleOTelServiceSharesRegistry verifies the OTLP servicer
// publishes onto the same registry the reporter's own gauges use, so one
// /metrics scrape returns both, matching the Python agent where both paths
// write to the global REGISTRY.
func TestReporterModuleOTelServiceSharesRegistry(t *testing.T) {
	module, _ := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})
	if module.otelService.registry != module.metricsService.Registry() {
		t.Fatal("otelService must publish on the reporter's registry")
	}
}
