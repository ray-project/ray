package reporter

// This file ports the OpenCensus proxy collector of the Python reporter agent
// (python/ray/_private/metrics_agent.py: Component, OpencensusProxyMetric and
// OpenCensusProxyCollector).
//
// The Ray components running on the node (raylet, GCS, core workers) push their
// metrics over the ReportOCMetrics gRPC call. Python keeps those metrics per
// reporting component and converts them at scrape time, which is what enables
// two behaviours:
//
//   - the metrics of a worker that died or hangs disappear once the worker is
//     considered stale (clean_stale_components), and
//   - the per-worker series collapse onto node level series when the
//     cardinality level asks for it
//     (_aggregate_with_recommended_cardinality).
//
// Converting at record time -- the previous Go implementation -- can do
// neither: the series of a dead worker stay frozen forever and the values of
// several workers cannot be summed into one series. The conversion therefore
// happens in Collect, driven by the components recorded since the last report.
//
// The aggregation data of the Python implementation (Count / Sum / LastValue /
// Distribution) is flattened to a single value here, because every reported
// metric is exposed as a Prometheus gauge; a reported distribution has no
// gauge representation and is therefore not recorded at all, which is why the
// Python "skip distributions when aggregating" rule needs no counterpart.

import (
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// globalComponentKey is the component key of the metrics that are not reported
// by a worker (the raylet and GCS ones), mirroring GLOBAL_COMPONENT_KEY in
// python/ray/_private/metrics_agent.py.
const globalComponentKey = "CORE"

// workerTimeoutEnv mirrors RAY_WORKER_TIMEOUT_S, the environment variable the
// Python agent reads the stale component timeout from.
const workerTimeoutEnv = "RAY_WORKER_TIMEOUT_S"

// defaultWorkerTimeoutSeconds mirrors the fallback of the component_timeout_s
// construction in MetricsAgent.__init__.
const defaultWorkerTimeoutSeconds = 120

// labelsSeparator joins label values into a map key. It is the ASCII unit
// separator, which no reported label value carries.
const labelsSeparator = "\x1f"

// componentTimeout returns how long a component may stay silent before its
// metrics are dropped, mirroring component_timeout_s=int(os.getenv(
// RAY_WORKER_TIMEOUT_S, 120)) in MetricsAgent.__init__.
func componentTimeout() time.Duration {
	return time.Duration(common.EnvInteger(workerTimeoutEnv, defaultWorkerTimeoutSeconds)) * time.Second
}

// componentKeyFromWorkerID resolves the component key of a ReportOCMetrics
// request, mirroring reporter_agent.py:
//
//	worker_id = WorkerID(request.worker_id)
//	worker_id = None if worker_id.is_nil() else worker_id.hex()
//
// A nil or empty worker id means the metrics come from the raylet or GCS, and
// the empty string selects the global component key. A malformed id is an
// error: like the Python agent, which lets the WorkerID constructor raise and
// drops the whole batch, an unidentifiable reporter is not attributed to the
// global component.
func componentKeyFromWorkerID(raw []byte) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	workerID, err := ids.WorkerIDFromBinary(raw)
	if err != nil {
		return "", err
	}
	if workerID.IsNil() {
		return "", nil
	}
	return workerID.Hex(), nil
}

// proxySeries is one time series of a reported metric: the label values in the
// order of the descriptor's label keys, and the value recorded for them.
type proxySeries struct {
	labels []string
	value  float64
}

// proxyMetric is one metric reported by one component, mirroring
// OpencensusProxyMetric.
type proxyMetric struct {
	name      string
	help      string
	labelKeys []string
	// labelIndexes records where each retained label key sat in the reported
	// descriptor, so the series label values can be read at exactly those
	// indexes. Dropping an empty key must drop the matching value too, or the
	// values no longer line up with the label names and the emitted series is
	// mislabelled.
	labelIndexes []int
	// series maps a label value tuple (see labelsKey) to the series itself.
	series map[string]*proxySeries
}

// newProxyMetric returns an empty metric with the given descriptor metadata.
func newProxyMetric(name, help string, labelKeys []string, labelIndexes []int) *proxyMetric {
	return &proxyMetric{
		name:         name,
		help:         help,
		labelKeys:    labelKeys,
		labelIndexes: labelIndexes,
		series:       make(map[string]*proxySeries),
	}
}

// record merges one reported time series into the metric, mirroring
// OpencensusProxyMetric.record: a label tuple reported again replaces its
// previous value, and a tuple a later report no longer carries keeps its last
// value. A time series that does not match the declared label keys, or a point
// that has no gauge representation, is skipped so one malformed series cannot
// spoil the rest of the component's report.
func (m *proxyMetric) record(ts *proto.TimeSeries) {
	labels, ok := labelValuesAt(ts, m.labelIndexes)
	if !ok {
		log.Log.V(1).Info("reported series does not match its label keys, skipping",
			"metric", m.name, "label_keys", len(m.labelKeys), "label_values", len(ts.GetLabelValues()))
		return
	}
	var value float64
	recorded := false
	for _, point := range ts.GetPoints() {
		if point == nil {
			continue
		}
		v, ok := pointValue(point)
		if !ok {
			continue
		}
		// The last point of a series is the one Python keeps.
		value, recorded = v, true
	}
	if !recorded {
		return
	}
	m.series[labelsKey(labels)] = &proxySeries{labels: labels, value: value}
}

// family renders the metric as a Prometheus family without touching its
// labels, mirroring the pass-through branch of OpenCensusProxyCollector.collect.
func (m *proxyMetric) family() proxyFamily {
	keys := sortedSeriesKeys(m.series)
	series := make([]proxySeries, 0, len(keys))
	for _, key := range keys {
		series = append(series, *m.series[key])
	}
	return proxyFamily{name: m.name, help: m.help, labelKeys: m.labelKeys, series: series}
}

// proxyComponent holds the metrics reported by one component, mirroring
// Component: the id of the reporting raylet / GCS / worker, the time it last
// reported and the metrics it reported.
type proxyComponent struct {
	id               string
	lastReportedTime time.Time
	metrics          map[string]*proxyMetric
	// order records the metric names in report order so a collection renders
	// them deterministically, as the insertion ordered dict of the Python
	// implementation does.
	order []string
}

// record merges the reported metrics into the component and refreshes its
// last reported time, mirroring Component.record.
func (c *proxyComponent) record(metrics []*proto.Metric, now time.Time) {
	c.lastReportedTime = now
	for _, metric := range metrics {
		if metric == nil || metric.GetMetricDescriptor() == nil {
			continue
		}
		descriptor := metric.GetMetricDescriptor()
		if descriptor.GetName() == "" {
			continue
		}
		proxyMetric, ok := c.metrics[descriptor.GetName()]
		if !ok {
			labelKeys, labelIndexes := labelKeysFromDescriptor(descriptor)
			proxyMetric = newProxyMetric(descriptor.GetName(), descriptor.GetDescription(),
				labelKeys, labelIndexes)
			c.metrics[descriptor.GetName()] = proxyMetric
			c.order = append(c.order, descriptor.GetName())
		}
		for _, ts := range metric.GetTimeseries() {
			if ts == nil {
				continue
			}
			proxyMetric.record(ts)
		}
	}
}

// proxyFamily is one metric rendered for Prometheus: the metric name and help
// text, the label keys its series declare, and the series themselves.
type proxyFamily struct {
	name      string
	help      string
	labelKeys []string
	series    []proxySeries
}

// emit writes one gauge sample per series. A series whose label count does not
// match the declared label keys (the components disagree on the label keys of
// a metric, which Python cannot render either), or a metric name Prometheus
// rejects, is skipped: one unrenderable metric reported by a component must
// not take the whole export down.
func (f proxyFamily) emit(ch chan<- prometheus.Metric) {
	desc := prometheus.NewDesc(metricNamespace+"_"+f.name, f.help, f.labelKeys, nil)
	seen := make(map[string]bool, len(f.series))
	for _, s := range f.series {
		if len(s.labels) != len(f.labelKeys) {
			continue
		}
		// A label tuple several components report identical values for would
		// otherwise be emitted twice, which fails the whole Prometheus scrape.
		key := labelsKey(s.labels)
		if seen[key] {
			continue
		}
		seen[key] = true
		metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, s.value, s.labels...)
		if err != nil {
			log.Log.V(1).Info("failed to build prometheus metric, skipping",
				"metric", f.name, "error", err.Error())
			continue
		}
		ch <- metric
	}
}

// openCensusProxyCollector tracks the metrics reported by every component and
// renders them on each scrape, mirroring OpenCensusProxyCollector. It is
// registered on the reporter's registry, so /metrics exposes both the reporter's
// own gauges and the proxied component metrics.
type openCensusProxyCollector struct {
	mu               sync.Mutex
	components       map[string]*proxyComponent
	order            []string
	componentTimeout time.Duration
	// now returns the current time. It is a field so tests can drive the stale
	// timeout without sleeping; time.Now carries a monotonic reading, which is
	// what Python's time.monotonic() provides.
	now func() time.Time
}

// compile-time assertion that openCensusProxyCollector satisfies the Prometheus
// collector interface.
var _ prometheus.Collector = (*openCensusProxyCollector)(nil)

// newOpenCensusProxyCollector returns a collector that forgets a component after
// it has been silent for longer than timeout.
func newOpenCensusProxyCollector(timeout time.Duration) *openCensusProxyCollector {
	return &openCensusProxyCollector{
		components:       make(map[string]*proxyComponent),
		componentTimeout: timeout,
		now:              time.Now,
	}
}

// record stores the metrics reported by one component. workerIDHex is the
// reporting worker id in hex, empty for the metrics pushed by the raylet or
// GCS, which are attributed to the global component, mirroring record().
func (c *openCensusProxyCollector) record(metrics []*proto.Metric, workerIDHex string) {
	key := globalComponentKey
	if workerIDHex != "" {
		key = workerIDHex
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	component, ok := c.components[key]
	if !ok {
		component = &proxyComponent{id: key, metrics: make(map[string]*proxyMetric)}
		c.components[key] = component
		c.order = append(c.order, key)
	}
	component.record(metrics, c.now())
}

// cleanStaleComponents drops the components that stopped reporting, mirroring
// clean_stale_components: the metrics of a dead or unresponsive worker must not
// be exported forever.
func (c *openCensusProxyCollector) cleanStaleComponents() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	kept := c.order[:0]
	for _, id := range c.order {
		component := c.components[id]
		elapsed := now.Sub(component.lastReportedTime)
		if elapsed > c.componentTimeout {
			log.Log.Info("dropping the metrics of a stale component due to timeout",
				"component", id, "time_since_last_report_s", elapsed.Seconds())
			delete(c.components, id)
			continue
		}
		kept = append(kept, id)
	}
	c.order = kept
}

// Describe implements prometheus.Collector. The reported metric set is
// discovered at scrape time, so the collector is unchecked, exactly like the
// Python collector whose collect() yields whatever the components reported.
func (c *openCensusProxyCollector) Describe(chan<- *prometheus.Desc) {}

// Collect implements prometheus.Collector, mirroring
// OpenCensusProxyCollector.collect: render the reported metrics and emit one
// gauge sample per series.
func (c *openCensusProxyCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	families := c.convert()
	c.mu.Unlock()

	for _, family := range families {
		family.emit(ch)
	}
}

// convert renders the recorded metrics as Prometheus families, mirroring the
// first half of OpenCensusProxyCollector.collect: at the recommended cardinality
// level the instances a metric has per component are aggregated into one node
// level family, and at every other level they are rendered as reported. It must
// be called with the component lock held.
func (c *openCensusProxyCollector) convert() []proxyFamily {
	aggregate := cardinalityLevel() == metricCardinalityRecommended
	var families []proxyFamily
	perName := make(map[string][]*proxyMetric)
	var names []string

	for _, id := range c.order {
		component := c.components[id]
		for _, name := range component.order {
			metric := component.metrics[name]
			if !aggregate {
				families = append(families, metric.family())
				continue
			}
			if _, seen := perName[name]; !seen {
				names = append(names, name)
			}
			perName[name] = append(perName[name], metric)
		}
	}
	for _, name := range names {
		families = append(families, aggregateMetrics(perName[name])...)
	}
	return mergeFamilies(families)
}

// mergeFamilies joins the families that share a metric name into one, keeping
// the name, help text and label keys of the first one, mirroring the
// metrics_map of the Python implementation: a reported metric is rendered once
// per scrape, no matter how many components reported it.
//
// Merging is required rather than cosmetic: the Prometheus registry rejects a
// metric name collected twice with different help text or label keys, and a
// rejected collection fails the whole /metrics scrape -- including the
// reporter's own gauges. The series of a component that declares a different
// label set are dropped in emit instead.
func mergeFamilies(families []proxyFamily) []proxyFamily {
	if len(families) < 2 {
		return families
	}
	index := make(map[string]int, len(families))
	merged := make([]proxyFamily, 0, len(families))
	for _, family := range families {
		i, ok := index[family.name]
		if !ok {
			index[family.name] = len(merged)
			merged = append(merged, family)
			continue
		}
		merged[i].series = append(merged[i].series, family.series...)
	}
	return merged
}

// aggregateMetrics collapses the per-component instances of one metric name onto
// node level families, mirroring _aggregate_with_recommended_cardinality: the
// WorkerId label key and value are dropped and the values that collapse onto one
// label set are summed.
//
// Python only aggregates when the first instance of the metric carries the
// WorkerId label; a metric no component reports with a WorkerId (a raylet or GCS
// metric) is passed through unchanged. The instances arrive in report order, so
// "first" is deterministic.
func aggregateMetrics(perComponent []*proxyMetric) []proxyFamily {
	first := perComponent[0]
	workerIDIndex := slices.Index(first.labelKeys, workerIDTagKey)
	if workerIDIndex < 0 {
		families := make([]proxyFamily, 0, len(perComponent))
		for _, metric := range perComponent {
			families = append(families, metric.family())
		}
		return families
	}

	collapsed := make(map[string]*proxySeries)
	var order []string
	for _, metric := range perComponent {
		for _, key := range sortedSeriesKeys(metric.series) {
			series := metric.series[key]
			if workerIDIndex >= len(series.labels) {
				continue
			}
			labels := slices.Delete(slices.Clone(series.labels), workerIDIndex, workerIDIndex+1)
			collapsedKey := labelsKey(labels)
			if existing, ok := collapsed[collapsedKey]; ok {
				existing.value += series.value
				continue
			}
			collapsed[collapsedKey] = &proxySeries{labels: labels, value: series.value}
			order = append(order, collapsedKey)
		}
	}

	series := make([]proxySeries, 0, len(order))
	for _, key := range order {
		series = append(series, *collapsed[key])
	}
	return []proxyFamily{{
		name:      first.name,
		help:      first.help,
		labelKeys: slices.Delete(slices.Clone(first.labelKeys), workerIDIndex, workerIDIndex+1),
		series:    series,
	}}
}

// labelKeysFromDescriptor returns the label keys a reported metric declares,
// along with the index each retained key occupied in the descriptor. A key
// without a name carries no label, so it and its value are both dropped and the
// retained indexes keep the two sides aligned.
func labelKeysFromDescriptor(descriptor *proto.MetricDescriptor) ([]string, []int) {
	raw := descriptor.GetLabelKeys()
	labelKeys := make([]string, 0, len(raw))
	labelIndexes := make([]int, 0, len(raw))
	for i, key := range raw {
		if key != nil && key.GetKey() != "" {
			labelKeys = append(labelKeys, key.GetKey())
			labelIndexes = append(labelIndexes, i)
		}
	}
	return labelKeys, labelIndexes
}

// labelValuesAt extracts the series label values at the retained descriptor
// indexes, so the result is aligned one-to-one with the label names even when
// some descriptor keys were dropped. It returns false when the series is
// missing a value for a retained index.
func labelValuesAt(ts *proto.TimeSeries, indexes []int) ([]string, bool) {
	raw := ts.GetLabelValues()
	values := make([]string, 0, len(indexes))
	for _, i := range indexes {
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

// labelsKey returns a canonical key for a label value tuple so equal tuples
// compare equal.
func labelsKey(labels []string) string {
	return strings.Join(labels, labelsSeparator)
}

// sortedSeriesKeys returns the series keys in a deterministic order so repeated
// scrapes of the same reported set are stable.
func sortedSeriesKeys(series map[string]*proxySeries) []string {
	keys := make([]string, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
