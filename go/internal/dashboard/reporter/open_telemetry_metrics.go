package reporter

// This file ports the OpenTelemetry metrics path of the Python reporter agent
// (python/ray/dashboard/modules/reporter/reporter_agent.py: Export and
// _export_histogram_data / _export_number_data) together with the recorder it
// feeds (python/ray/_private/telemetry/open_telemetry_metric_recorder.py).
//
// Ray components running on the same node (raylet, core workers, GCS) push
// their metrics over OTLP to the dashboard agent's MetricsService gRPC
// endpoint. The Python agent registers that servicer when
// RAY_ENABLE_OPEN_TELEMETRY is set (the default), converts every reported
// metric into an OpenTelemetry instrument and exposes the result on its
// Prometheus endpoint through the OpenTelemetry PrometheusMetricReader.
//
// Go has no OpenTelemetry SDK in the agent, so the conversion targets the
// Prometheus registry directly: gauges become observable gauges (a collector
// that snapshots the pending observations per scrape), monotonic sums become
// counters, non-monotonic sums become up-down counters (modelled as gauges,
// since Prometheus has no up-down counter) and histograms become Prometheus
// histograms whose bucket counts are replayed at the reconstructed bucket
// midpoints.

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpmetricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// openTelemetryEnabledEnv mirrors RAY_enable_open_telemetry in
// python/ray/_private/ray_constants.py. The OpenTelemetry metrics backend is
// enabled unless the variable says otherwise.
const openTelemetryEnabledEnv = "RAY_enable_open_telemetry"

// metricCardinalityLevelEnv mirrors RAY_metric_cardinality_level in
// python/ray/_private/ray_constants.py.
const metricCardinalityLevelEnv = "RAY_metric_cardinality_level"

// Metric cardinality levels, mirroring the MetricCardinality enum in
// python/ray/_private/telemetry/metric_cardinality.py.
const (
	metricCardinalityLegacy      = "legacy"
	metricCardinalityRecommended = "recommended"
	metricCardinalityLow         = "low"

	// defaultMetricCardinalityLevel is the level assumed when the environment
	// variable is unset, matching ray_constants.py.
	defaultMetricCardinalityLevel = metricCardinalityRecommended
)

// High-cardinality tag keys, kept in sync with the WorkerIdKey / NameKey tag
// definitions in src/ray/stats/tag_defs.cc.
const (
	workerIDTagKey        = "WorkerId"
	taskOrActorNameTagKey = "Name"
)

// aggregatedMetrics are the high-cardinality metrics whose observations are
// summed when their high-cardinality labels collapse onto a single label set,
// mirroring HIGH_CARDINALITY_METRICS_TO_AGGREGATION. Every other metric keeps
// its observations separate, so each collapsed label set carries one value.
var aggregatedMetrics = map[string]bool{
	"tasks":  true,
	"actors": true,
}

// openTelemetryEnabled reports whether the OpenTelemetry metrics path is
// enabled, mirroring RAY_ENABLE_OPEN_TELEMETRY in the Python agent's run().
func openTelemetryEnabled() bool {
	return common.EnvBool(openTelemetryEnabledEnv, true)
}

// cardinalityLevel returns the configured metric cardinality level, defaulting
// to "recommended". An unrecognised value is treated as the legacy level, as
// MetricCardinality.get_cardinality_level does when the enum lookup fails.
func cardinalityLevel() string {
	level := os.Getenv(metricCardinalityLevelEnv)
	if level == "" {
		return defaultMetricCardinalityLevel
	}
	switch level = strings.ToLower(level); level {
	case metricCardinalityLegacy, metricCardinalityRecommended, metricCardinalityLow:
		return level
	default:
		return metricCardinalityLegacy
	}
}

// highCardinalityLabelsToDrop returns the tag keys the current cardinality
// level drops for the named metric, mirroring
// MetricCardinality.get_high_cardinality_labels_to_drop. An empty result means
// every tag is kept.
func highCardinalityLabelsToDrop(name string) map[string]bool {
	level := cardinalityLevel()
	if level == metricCardinalityLegacy || !aggregatedMetrics[name] {
		return nil
	}
	drop := map[string]bool{workerIDTagKey: true}
	if level == metricCardinalityLow {
		drop[taskOrActorNameTagKey] = true
	}
	return drop
}

// dropHighCardinalityLabels removes the tags the current cardinality level
// drops for the named metric, returning the original map when nothing is
// dropped.
func dropHighCardinalityLabels(name string, tags map[string]string) map[string]string {
	drop := highCardinalityLabelsToDrop(name)
	if len(drop) == 0 {
		return tags
	}
	kept := make(map[string]string, len(tags))
	for k, v := range tags {
		if !drop[k] {
			kept[k] = v
		}
	}
	return kept
}

// dropHighCardinalityKeys filters a label-key set with the named metric's
// cardinality drop set, so an instrument never declares a label its series are
// aggregated over.
func dropHighCardinalityKeys(name string, keys []string) []string {
	drop := highCardinalityLabelsToDrop(name)
	if len(drop) == 0 {
		return keys
	}
	kept := make([]string, 0, len(keys))
	for _, k := range keys {
		if !drop[k] {
			kept = append(kept, k)
		}
	}
	return kept
}

// attrsToTags converts OTLP attributes into a tag map, mirroring the Python
// {tag.key: tag.value.string_value for tag in attributes} comprehension: the
// key is kept for every attribute and a value that is not a string renders as
// the empty string, because reading string_value off a non-string AnyValue
// yields the protobuf default.
func attrsToTags(attrs []*otlpcommon.KeyValue) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	tags := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		if kv == nil {
			continue
		}
		tags[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	return tags
}

// attributeKeys returns the keys of the given attributes in order, used to
// collect the label set an instrument must declare.
func attributeKeys(attrs []*otlpcommon.KeyValue) []string {
	if len(attrs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		if kv == nil {
			continue
		}
		keys = append(keys, kv.GetKey())
	}
	return keys
}

// collectLabelKeys returns the sorted, de-duplicated union of the attribute
// keys of one metric's data points. client_golang metric vectors require a
// fixed label set at registration, so the union is declared up front and a
// series that omits a declared key renders it as the empty string -- which is
// how the Python exporter renders a data point that does not carry every
// attribute. Sorting keeps the declaration deterministic.
func collectLabelKeys(pointAttrKeys ...[]string) []string {
	seen := make(map[string]bool)
	var keys []string
	for _, attrs := range pointAttrKeys {
		for _, k := range attrs {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// numberDataPointAttrKeys returns the attribute keys of each number data point,
// in order, for label-key collection.
func numberDataPointAttrKeys(dps []*otlpmetricsv1.NumberDataPoint) [][]string {
	keys := make([][]string, 0, len(dps))
	for _, dp := range dps {
		keys = append(keys, attributeKeys(dp.GetAttributes()))
	}
	return keys
}

// histogramDataPointAttrKeys is the histogram counterpart of
// numberDataPointAttrKeys.
func histogramDataPointAttrKeys(dps []*otlpmetricsv1.HistogramDataPoint) [][]string {
	keys := make([][]string, 0, len(dps))
	for _, dp := range dps {
		keys = append(keys, attributeKeys(dp.GetAttributes()))
	}
	return keys
}

// tagsKey returns a canonical key for a tag set so equal sets compare equal.
func tagsKey(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(0x1f)
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(tags[k])
	}
	return b.String()
}

// projectLabels renders a tag map against an instrument's declared label keys:
// a declared key the series does not carry becomes the empty string, and a tag
// the instrument does not declare is ignored. This is the same projection the
// OpenCensus path applies through MetricsService.RecordGauge.
func projectLabels(labelKeys []string, tags map[string]string) prometheus.Labels {
	labels := make(prometheus.Labels, len(labelKeys))
	for _, k := range labelKeys {
		labels[k] = tags[k]
	}
	return labels
}

// labelsInOrder returns the values of the declared label keys in declaration
// order, as MustNewConstMetric expects them.
func labelsInOrder(labelKeys []string, labels prometheus.Labels) []string {
	values := make([]string, len(labelKeys))
	for i, k := range labelKeys {
		values[i] = labels[k]
	}
	return values
}

// otelObservation is one reported value for one series of an observable gauge,
// together with the monotonic instant it was reported at.
type otelObservation struct {
	tags  map[string]string
	value float64
	at    time.Time
}

// otelGaugeStore holds the observations reported for one observable gauge. The
// Python recorder keeps them in a dict keyed by the full tag set and only drops
// high-cardinality labels at collection time, so the values of the series that
// collapse onto one label set can be aggregated. The store mirrors that:
// observations are keyed by the full tag set and the aggregation happens in
// Collect.
type otelGaugeStore struct {
	mu           sync.Mutex
	observations map[string]otelObservation
	// ttl is how long an observation is served without being refreshed. The
	// Python recorder retains a value for two export intervals so a series that
	// is being reported survives a scrape that lands between reports, and a
	// series whose emitter stopped ages out.
	ttl time.Duration
	// now is the clock, so a test can advance it past the ttl.
	now func() time.Time
}

// otelObservableGauge is the Go counterpart of an OpenTelemetry observable
// gauge. Python registers a callback that the SDK invokes once per collection;
// client_golang invokes Collect once per scrape, so Collect performs the same
// work: drop the observations that have aged out, aggregate the series that
// collapse onto one label set, and emit them. It deliberately does not clear
// what it consumed: with a scrape interval shorter than the report interval,
// clearing would blank the series between two reports, which is the behaviour
// the Python recorder calls out and avoids.
type otelObservableGauge struct {
	desc      *prometheus.Desc
	labelKeys []string
	metric    string
	store     *otelGaugeStore
}

// Describe implements prometheus.Collector.
func (g *otelObservableGauge) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect implements prometheus.Collector by emitting one gauge sample per
// aggregated label set, evicting the observations that outlived the ttl.
func (g *otelObservableGauge) Collect(ch chan<- prometheus.Metric) {
	now := g.store.now()
	g.store.mu.Lock()
	pending := make(map[string]otelObservation, len(g.store.observations))
	for tags, obs := range g.store.observations {
		if now.Sub(obs.at) <= g.store.ttl {
			pending[tags] = obs
			continue
		}
		delete(g.store.observations, tags)
	}
	g.store.mu.Unlock()

	type series struct {
		labels prometheus.Labels
		values []float64
	}
	byLabels := make(map[string]*series, len(pending))
	for _, obs := range pending {
		labels := projectLabels(g.labelKeys, dropHighCardinalityLabels(g.metric, obs.tags))
		key := tagsKey(labels)
		s, ok := byLabels[key]
		if !ok {
			s = &series{labels: labels}
			byLabels[key] = s
		}
		s.values = append(s.values, obs.value)
	}

	// Emit in a deterministic order so repeated scrapes of the same pending set
	// are stable.
	keys := make([]string, 0, len(byLabels))
	for key := range byLabels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		s := byLabels[key]
		ch <- prometheus.MustNewConstMetric(
			g.desc, prometheus.GaugeValue, aggregateValues(g.metric, s.values),
			labelsInOrder(g.labelKeys, s.labels)...)
	}
}

// aggregateValues reduces the values that collapsed onto one label set,
// mirroring MetricCardinality.get_aggregation_function: the high-cardinality
// metrics are summed, and every other metric has a single observation per
// label set so its first value is the value.
func aggregateValues(name string, values []float64) float64 {
	if !aggregatedMetrics[name] {
		return values[0]
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum
}

// otelInstrument is one registered metric family: the Prometheus collector, the
// label keys it declares and, for observable gauges, the observation store its
// collector drains.
type otelInstrument struct {
	collector prometheus.Collector
	labelKeys []string
	store     *otelGaugeStore
}

// OpenTelemetryMetricsService implements the OTLP MetricsService gRPC API and
// converts the reported metrics into Prometheus metrics on the reporter's
// registry. It is the Go counterpart of the Python ReporterAgent's
// MetricsServiceServicer implementation.
type OpenTelemetryMetricsService struct {
	collectormetricsv1.UnimplementedMetricsServiceServer

	// registry is the Prometheus registry the reported metrics are published
	// on. It is shared with the reporter's own gauges so a single /metrics
	// scrape returns both, mirroring the Python agent where the OpenTelemetry
	// PrometheusMetricReader and the reporter's stats exporter write to the
	// same global REGISTRY.
	registry *prometheus.Registry

	// disabled mirrors the agent's _metrics_collection_disabled. The Python
	// agent still registers the servicer when metrics collection is disabled,
	// which makes Export raise on the missing recorder; here the export is
	// acknowledged without recording instead.
	disabled bool

	mu          sync.Mutex
	instruments map[string]*otelInstrument
	midpoints   map[string][]float64
}

// compile-time assertion that OpenTelemetryMetricsService satisfies the
// generated OTLP server interface.
var _ collectormetricsv1.MetricsServiceServer = (*OpenTelemetryMetricsService)(nil)

// NewOpenTelemetryMetricsService returns the OTLP MetricsService servicer
// publishing on registry. When disabled is set every export is acknowledged
// without recording, and when registry is nil nothing is ever registered, so
// the service is safe to construct on an agent that collects no metrics.
func NewOpenTelemetryMetricsService(registry *prometheus.Registry, disabled bool) *OpenTelemetryMetricsService {
	return &OpenTelemetryMetricsService{
		registry:    registry,
		disabled:    disabled,
		instruments: make(map[string]*otelInstrument),
		midpoints:   make(map[string][]float64),
	}
}

// Export receives the metrics Ray components push over OTLP and records them,
// mirroring ReporterAgent.Export. Metrics that cannot be converted are skipped
// individually so one malformed metric never fails the whole request.
func (s *OpenTelemetryMetricsService) Export(_ context.Context, req *collectormetricsv1.ExportMetricsServiceRequest) (*collectormetricsv1.ExportMetricsServiceResponse, error) {
	if s.disabled || s.registry == nil {
		return &collectormetricsv1.ExportMetricsServiceResponse{}, nil
	}
	for _, resourceMetrics := range req.GetResourceMetrics() {
		for _, scopeMetrics := range resourceMetrics.GetScopeMetrics() {
			for _, metric := range scopeMetrics.GetMetrics() {
				s.recordMetric(metric)
			}
		}
	}
	return &collectormetricsv1.ExportMetricsServiceResponse{}, nil
}

// recordMetric dispatches one reported metric, mirroring the
// WhichOneof("data") == "histogram" branch in ReporterAgent.Export.
func (s *OpenTelemetryMetricsService) recordMetric(metric *otlpmetricsv1.Metric) {
	if metric == nil {
		return
	}
	switch data := metric.GetData().(type) {
	case *otlpmetricsv1.Metric_Histogram:
		s.recordHistogramMetric(metric, data.Histogram)
	default:
		s.recordNumberMetric(metric)
	}
}

// recordNumberMetric records a gauge or sum, mirroring _export_number_data: a
// gauge registers an observable gauge, a monotonic sum a counter and a
// non-monotonic sum an up-down counter.
func (s *OpenTelemetryMetricsService) recordNumberMetric(metric *otlpmetricsv1.Metric) {
	var dataPoints []*otlpmetricsv1.NumberDataPoint
	var register func(labelKeys []string)
	switch data := metric.GetData().(type) {
	case *otlpmetricsv1.Metric_Gauge:
		dataPoints = data.Gauge.GetDataPoints()
		register = func(labelKeys []string) {
			s.registerGaugeMetric(metric.GetName(), metric.GetDescription(), labelKeys)
		}
	case *otlpmetricsv1.Metric_Sum:
		dataPoints = data.Sum.GetDataPoints()
		if data.Sum.GetIsMonotonic() {
			register = func(labelKeys []string) {
				s.registerCounterMetric(metric.GetName(), metric.GetDescription(), labelKeys)
			}
		} else {
			register = func(labelKeys []string) {
				s.registerSumMetric(metric.GetName(), metric.GetDescription(), labelKeys)
			}
		}
	default:
		// Exponential histograms, summaries and anything else the Ray
		// components do not report have no counterpart here; they are dropped.
		log.Log.V(1).Info("unsupported otel metric type, skipping", "metric", metric.GetName())
		return
	}

	register(dropHighCardinalityKeys(metric.GetName(), collectLabelKeys(numberDataPointAttrKeys(dataPoints)...)))
	for _, dataPoint := range dataPoints {
		// Every value reported by a Ray component is a double: the C++ API only
		// creates metrics with double values, so reading as_double mirrors
		// Python's data_point.as_double.
		s.setMetricValue(metric.GetName(), attrsToTags(dataPoint.GetAttributes()), dataPoint.GetAsDouble())
	}
}

// recordHistogramMetric records a histogram, mirroring _export_histogram_data:
// the C++ side reports a bucket count vector rather than the observations, so
// the reported data points are reconstructed by replaying each bucket count at
// the bucket's reconstructed midpoint.
func (s *OpenTelemetryMetricsService) recordHistogramMetric(metric *otlpmetricsv1.Metric, histogram *otlpmetricsv1.Histogram) {
	dataPoints := histogram.GetDataPoints()
	if len(dataPoints) == 0 {
		return
	}
	s.registerHistogramMetric(metric.GetName(), metric.GetDescription(),
		dataPoints[0].GetExplicitBounds(),
		dropHighCardinalityKeys(metric.GetName(), collectLabelKeys(histogramDataPointAttrKeys(dataPoints)...)))

	midpoints := s.histogramBucketMidpoints(metric.GetName())
	for _, dataPoint := range dataPoints {
		if dataPoint.GetCount() == 0 {
			continue
		}
		bucketCounts := dataPoint.GetBucketCounts()
		if len(midpoints) != len(bucketCounts) {
			log.Log.V(1).Info("histogram bucket count does not match the registered buckets, skipping data point",
				"metric", metric.GetName(), "buckets", len(midpoints), "bucket_counts", len(bucketCounts))
			continue
		}
		tags := attrsToTags(dataPoint.GetAttributes())
		for i, count := range bucketCounts {
			if count == 0 {
				continue
			}
			midpoint := midpoints[i]
			for observed := uint64(0); observed < count; observed++ {
				s.setMetricValue(metric.GetName(), tags, midpoint)
			}
		}
	}
}

// setMetricValue records a value for the named metric under the given tags,
// mirroring set_metric_value. An observable gauge stores the observation for
// the next collection; the synchronous instruments record immediately.
func (s *OpenTelemetryMetricsService) setMetricValue(name string, tags map[string]string, value float64) {
	s.mu.Lock()
	instrument := s.instruments[name]
	s.mu.Unlock()
	if instrument == nil {
		return
	}

	if instrument.store != nil {
		instrument.store.mu.Lock()
		instrument.store.observations[tagsKey(tags)] = otelObservation{tags: tags, value: value, at: instrument.store.now()}
		instrument.store.mu.Unlock()
		return
	}

	labels := projectLabels(instrument.labelKeys, dropHighCardinalityLabels(name, tags))
	switch collector := instrument.collector.(type) {
	case *prometheus.CounterVec:
		collector.With(labels).Add(value)
	case *prometheus.GaugeVec:
		// The only gauge that reaches here is a non-monotonic sum, which is
		// modelled as an up-down counter: its points carry the increment since
		// the previous report, so they accumulate rather than replace. The
		// observable gauges went through the store above.
		collector.With(labels).Add(value)
	case *prometheus.HistogramVec:
		collector.With(labels).Observe(value)
	default:
		log.Log.V(1).Info("unsupported otel instrument, skipping", "metric", name)
	}
}

// registerGaugeMetric registers the observable gauge for the named metric,
// mirroring register_gauge_metric. Registration happens once per name, because
// the same metric is reported by every Ray component running on the node.
func (s *OpenTelemetryMetricsService) registerGaugeMetric(name, description string, labelKeys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.instruments[name]; ok {
		return
	}
	store := &otelGaugeStore{
		observations: make(map[string]otelObservation),
		ttl:          gaugeMetricTTL(),
		now:          time.Now,
	}
	gauge := &otelObservableGauge{
		desc:      prometheus.NewDesc(metricNamespace+"_"+name, description, labelKeys, nil),
		labelKeys: labelKeys,
		metric:    name,
		store:     store,
	}
	s.instruments[name] = &otelInstrument{collector: gauge, labelKeys: labelKeys, store: store}
	s.register(gauge, name)
}

// registerCounterMetric registers a counter for a monotonic sum, mirroring
// register_counter_metric.
func (s *OpenTelemetryMetricsService) registerCounterMetric(name, description string, labelKeys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.instruments[name]; ok {
		return
	}
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: metricNamespace + "_" + name, Help: description},
		labelKeys,
	)
	s.instruments[name] = &otelInstrument{collector: counter, labelKeys: labelKeys}
	s.register(counter, name)
}

// registerSumMetric registers an up-down counter for a non-monotonic sum,
// mirroring register_sum_metric. Prometheus has no up-down counter, so the
// instrument is modelled as a gauge whose values are added to.
func (s *OpenTelemetryMetricsService) registerSumMetric(name, description string, labelKeys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.instruments[name]; ok {
		return
	}
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: metricNamespace + "_" + name, Help: description},
		labelKeys,
	)
	s.instruments[name] = &otelInstrument{collector: gauge, labelKeys: labelKeys}
	s.register(gauge, name)
}

// registerHistogramMetric registers a histogram with the reported explicit
// bucket bounds and caches the reconstructed bucket midpoints, mirroring
// register_histogram_metric.
func (s *OpenTelemetryMetricsService) registerHistogramMetric(name, description string, buckets []float64, labelKeys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.instruments[name]; ok {
		return
	}
	histogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: metricNamespace + "_" + name, Help: description, Buckets: buckets},
		labelKeys,
	)
	s.instruments[name] = &otelInstrument{collector: histogram, labelKeys: labelKeys}
	s.register(histogram, name)
	s.midpoints[name] = reconstructBucketMidpoints(buckets)
}

// register adds a collector to the registry. A failed registration (a name
// already registered with a different label set or help text) is logged and
// dropped: like the Python agent, a metric the agent cannot publish must not
// take the whole agent down.
func (s *OpenTelemetryMetricsService) register(collector prometheus.Collector, name string) {
	if err := s.registry.Register(collector); err != nil {
		log.Log.V(1).Info("failed to register otel metric, it will not be exported",
			"metric", name, "error", err.Error())
	}
}

// reconstructBucketMidpoints reconstructs the midpoint of every bucket from the
// explicit bucket bounds, mirroring the Python calculation, and appends the
// approximated midpoint of the implicit +Inf bucket.
func reconstructBucketMidpoints(buckets []float64) []float64 {
	midpoints := make([]float64, 0, len(buckets)+1)
	for i, upper := range buckets {
		if i == 0 {
			lower := 0.0
			if upper <= 0 {
				lower = upper * 2.0
			}
			midpoints = append(midpoints, (lower+upper)/2.0)
			continue
		}
		midpoints = append(midpoints, (upper+buckets[i-1])/2.0)
	}
	if len(buckets) == 0 || buckets[len(buckets)-1] <= 0 {
		return append(midpoints, 1.0)
	}
	return append(midpoints, buckets[len(buckets)-1]*2.0)
}

// histogramBucketMidpoints returns the cached midpoints for the named
// histogram.
func (s *OpenTelemetryMetricsService) histogramBucketMidpoints(name string) []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.midpoints[name]
}

// Metrics report interval knobs, mirroring the Python recorder's derivation of
// the gauge retention window: the emitter floors the report interval at one
// second (src/ray/stats/stats.h SetReportInterval), and a reported value is
// served for two intervals so a scrape landing between two reports still sees
// it.
const (
	metricsReportIntervalEnvVar = "RAY_metrics_report_interval_ms"
	defaultReportIntervalMs     = 10_000
	minReportIntervalMs         = 1_000
)

// gaugeMetricTTL returns how long a reported gauge observation is served
// without being refreshed, matching OpenTelemetryMetricRecorder's
// _resolve_gauge_ttl_seconds: twice the effective export interval, with the
// interval floored at one second and defaulting to the RayConfig value.
func gaugeMetricTTL() time.Duration {
	intervalMs := defaultReportIntervalMs
	if raw := os.Getenv(metricsReportIntervalEnvVar); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			intervalMs = v
		}
	}
	if intervalMs < minReportIntervalMs {
		intervalMs = minReportIntervalMs
	}
	return time.Duration(2*intervalMs) * time.Millisecond
}
