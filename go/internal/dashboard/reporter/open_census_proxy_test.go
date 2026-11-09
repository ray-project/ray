package reporter

import (
	"context"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// workerIDBytes builds a non-nil binary worker id where every byte is seed.
func workerIDBytes(seed byte) []byte {
	data := make([]byte, ids.UniqueIDSize)
	for i := range data {
		data[i] = seed
	}
	return data
}

// workerIDHexOf returns the hex form of workerIDBytes(seed), i.e. the component
// key the reporter derives for that worker.
func workerIDHexOf(seed byte) string {
	id, err := ids.WorkerIDFromBinary(workerIDBytes(seed))
	if err != nil {
		panic(err)
	}
	return id.Hex()
}

// withCardinalityLevel sets RAY_metric_cardinality_level for the duration of a
// test. cardinalityLevel reads the variable on every call, so no cache has to be
// invalidated.
func withCardinalityLevel(t *testing.T, level string) {
	t.Helper()
	t.Setenv(metricCardinalityLevelEnv, level)
}

// seriesKey returns the map key identifying a label set, independent of the
// order the labels are written in.
func seriesKey(labels ...string) string {
	sorted := slices.Clone(labels)
	sort.Strings(sorted)
	return labelsKey(sorted)
}

// gatherGaugeSamples gathers the registry and returns, per metric family name,
// the value of every sample keyed by its label set (see seriesKey).
func gatherGaugeSamples(t *testing.T, reg *prometheus.Registry) map[string]map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather error = %v", err)
	}
	samples := make(map[string]map[string]float64, len(families))
	for _, family := range families {
		if family.GetType().String() != "GAUGE" {
			continue
		}
		byLabels := make(map[string]float64, len(family.GetMetric()))
		for _, metric := range family.GetMetric() {
			labels := make([]string, 0, len(metric.GetLabel()))
			for _, lp := range metric.GetLabel() {
				labels = append(labels, lp.GetName()+"="+lp.GetValue())
			}
			// Prometheus renders the labels of a family in name order, which is
			// not the declaration order the expectations use.
			byLabels[seriesKey(labels...)] = metric.GetGauge().GetValue()
		}
		samples[family.GetName()] = byLabels
	}
	return samples
}

// TestComponentKeyFromWorkerID verifies the reported worker id is resolved into
// a component key the way reporter_agent.py resolves it: an absent or nil id
// selects the global component key, a valid id its hex form, and a malformed id
// is an error rather than being silently attributed to the global component.
func TestComponentKeyFromWorkerID(t *testing.T) {
	// The nil worker id is the all-0xff binary form the raylet's exporter sends
	// (BaseID's default constructor fills 0xff), not the all-zero one.
	nilIDBytes := ids.NilWorkerID().Binary()
	if !isNilWorkerIDBytes(t, nilIDBytes) {
		t.Fatal("test precondition: NilWorkerID().Binary() must be the nil worker id")
	}

	tests := []struct {
		name    string
		raw     []byte
		want    string
		wantErr bool
	}{
		{name: "absent id selects the global component", raw: nil, want: ""},
		{name: "empty id selects the global component", raw: []byte{}, want: ""},
		{name: "nil worker id selects the global component", raw: nilIDBytes, want: ""},
		{name: "valid id resolves to its hex form", raw: workerIDBytes(7), want: workerIDHexOf(7)},
		{name: "malformed length is an error", raw: []byte{1, 2, 3}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := componentKeyFromWorkerID(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("componentKeyFromWorkerID(%v) error = nil, want an error", tt.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("componentKeyFromWorkerID(%v) error = %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("componentKeyFromWorkerID(%v) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

// isNilWorkerIDBytes reports whether the given binary id parses as the nil
// worker id.
func isNilWorkerIDBytes(t *testing.T, raw []byte) bool {
	t.Helper()
	id, err := ids.WorkerIDFromBinary(raw)
	if err != nil {
		t.Fatalf("WorkerIDFromBinary(%v): %v", raw, err)
	}
	return id.IsNil()
}

// TestComponentTimeout verifies the stale component timeout mirrors
// RAY_WORKER_TIMEOUT_S with the 120s default of MetricsAgent.__init__.
func TestComponentTimeout(t *testing.T) {
	t.Run("defaults to 120 seconds", func(t *testing.T) {
		t.Setenv(workerTimeoutEnv, "")
		if got := componentTimeout(); got != 120*time.Second {
			t.Fatalf("componentTimeout() = %v, want 120s", got)
		}
	})
	t.Run("reads RAY_WORKER_TIMEOUT_S", func(t *testing.T) {
		t.Setenv(workerTimeoutEnv, "7")
		if got := componentTimeout(); got != 7*time.Second {
			t.Fatalf("componentTimeout() = %v, want 7s", got)
		}
	})
	t.Run("falls back to the default on a malformed value", func(t *testing.T) {
		t.Setenv(workerTimeoutEnv, "not-a-number")
		if got := componentTimeout(); got != 120*time.Second {
			t.Fatalf("componentTimeout() = %v, want 120s", got)
		}
	})
}

// TestMetricsServiceExportAttributesToWorkerComponent verifies a report carrying
// a worker id is exported, which is only possible because the id is now read:
// before the fix the id was ignored and every report shared one flat gauge map.
func TestMetricsServiceExportAttributesToWorkerComponent(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	err := svc.Export(context.Background(), workerIDBytes(1), []*proto.Metric{
		newDoubleMetric("tasks", "Current number of tasks",
			[]string{"WorkerId", "State"}, []string{workerIDHexOf(1), "RUNNING"}, 4),
	})
	if err != nil {
		t.Fatalf("Export error = %v", err)
	}

	samples := gatherGaugeSamples(t, reg)
	tasks, ok := samples["ray_tasks"]
	if !ok {
		t.Fatalf("ray_tasks not exported, got families %v", samples)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 tasks series, got %v", tasks)
	}
	want := seriesKey("WorkerId="+workerIDHexOf(1), "State=RUNNING")
	if got, ok := tasks[want]; !ok || got != 4 {
		t.Fatalf("ray_tasks[%q] = %v (present %v), want 4", want, got, ok)
	}
}

// TestMetricsServiceExportAttributesToGlobalComponent verifies a report without
// a worker id is attributed to the global component (raylet / GCS) and is
// therefore exported alongside the worker reported series. The raylet and the
// GCS register WorkerId as a global tag with an empty value, so their series
// carry the WorkerId label too, with an empty value.
func TestMetricsServiceExportAttributesToGlobalComponent(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	ctx := context.Background()
	if err := svc.Export(ctx, nil, []*proto.Metric{
		newDoubleMetric("actors", "An actor can be in one of states",
			[]string{"WorkerId", "State"}, []string{"", "ALIVE"}, 2),
	}); err != nil {
		t.Fatalf("Export (global) error = %v", err)
	}
	if err := svc.Export(ctx, workerIDBytes(2), []*proto.Metric{
		newDoubleMetric("actors", "An actor can be in one of states",
			[]string{"WorkerId", "State"}, []string{workerIDHexOf(2), "ALIVE"}, 3),
	}); err != nil {
		t.Fatalf("Export (worker) error = %v", err)
	}

	samples := gatherGaugeSamples(t, reg)
	actors := samples["ray_actors"]
	if len(actors) != 2 {
		t.Fatalf("expected 2 actor series (global + worker), got %v", actors)
	}
	if got := actors[seriesKey("WorkerId=", "State=ALIVE")]; got != 2 {
		t.Errorf("global actor series = %v, want 2", got)
	}
	wantWorker := seriesKey("WorkerId="+workerIDHexOf(2), "State=ALIVE")
	if got := actors[wantWorker]; got != 3 {
		t.Errorf("worker actor series = %v, want 3", got)
	}
}

// TestMetricsServiceAggregatesWorkerSeriesAtRecommendedLevel verifies the
// per-worker series of a high-cardinality metric collapse onto one node level
// series whose value is the sum, mirroring
// _aggregate_with_recommended_cardinality with the "tasks" aggregation function.
func TestMetricsServiceAggregatesWorkerSeriesAtRecommendedLevel(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityRecommended)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	ctx := context.Background()
	for _, seed := range []byte{1, 2, 3} {
		if err := svc.Export(ctx, workerIDBytes(seed), []*proto.Metric{
			newDoubleMetric("tasks", "Current number of tasks",
				[]string{"WorkerId", "State"}, []string{workerIDHexOf(seed), "RUNNING"}, 4),
		}); err != nil {
			t.Fatalf("Export error = %v", err)
		}
	}

	samples := gatherGaugeSamples(t, reg)
	tasks := samples["ray_tasks"]
	if len(tasks) != 1 {
		t.Fatalf("expected the 3 worker series to collapse into 1, got %v", tasks)
	}
	key := seriesKey("State=RUNNING")
	got, ok := tasks[key]
	if !ok {
		t.Fatalf("collapsed series %q not found, got %v", key, tasks)
	}
	if got != 12 {
		t.Fatalf("collapsed series value = %v, want 12 (4+4+4)", got)
	}
}

// TestMetricsServiceKeepsWorkerSeriesAtLegacyLevel verifies the legacy level
// keeps every per-worker series separate, mirroring the pass-through branch of
// OpenCensusProxyCollector.collect.
func TestMetricsServiceKeepsWorkerSeriesAtLegacyLevel(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	ctx := context.Background()
	for _, seed := range []byte{1, 2} {
		if err := svc.Export(ctx, workerIDBytes(seed), []*proto.Metric{
			newDoubleMetric("tasks", "Current number of tasks",
				[]string{"WorkerId", "State"}, []string{workerIDHexOf(seed), "RUNNING"}, 4),
		}); err != nil {
			t.Fatalf("Export error = %v", err)
		}
	}

	samples := gatherGaugeSamples(t, reg)
	tasks := samples["ray_tasks"]
	if len(tasks) != 2 {
		t.Fatalf("expected 2 per-worker series at the legacy level, got %v", tasks)
	}
	for _, seed := range []byte{1, 2} {
		key := seriesKey("WorkerId="+workerIDHexOf(seed), "State=RUNNING")
		if got, ok := tasks[key]; !ok || got != 4 {
			t.Errorf("series %q = %v (present %v), want 4", key, got, ok)
		}
	}
}

// TestMetricsServicePassesThroughMetricWithoutWorkerIDLabel verifies a metric
// whose instances carry no WorkerId label is not aggregated, mirroring the early
// return of _aggregate_with_recommended_cardinality.
func TestMetricsServicePassesThroughMetricWithoutWorkerIDLabel(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityRecommended)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	metric := newDoubleMetric("internal_num_spilled_tasks", "Spilled leases",
		[]string{"Reason"}, []string{"RateLimited"}, 5)
	ctx := context.Background()
	if err := svc.Export(ctx, nil, []*proto.Metric{metric}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	samples := gatherGaugeSamples(t, reg)
	series := samples["ray_internal_num_spilled_tasks"]
	key := seriesKey("Reason=RateLimited")
	if got, ok := series[key]; !ok || got != 5 {
		t.Fatalf("series %q = %v (present %v), want 5", key, got, ok)
	}
}

// TestMetricsServiceCleanStaleComponentsDropsSilentComponents verifies a
// component whose metrics stopped arriving is dropped after the timeout, so the
// series of a dead worker stop being exported.
func TestMetricsServiceCleanStaleComponentsDropsSilentComponents(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)
	svc.proxy.componentTimeout = 30 * time.Second

	now := time.Now()
	svc.proxy.now = func() time.Time { return now }

	ctx := context.Background()
	if err := svc.Export(ctx, workerIDBytes(1), []*proto.Metric{
		newDoubleMetric("tasks", "Current number of tasks",
			[]string{"WorkerId", "State"}, []string{workerIDHexOf(1), "RUNNING"}, 4),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}
	if got := gatherGaugeSamples(t, reg)["ray_tasks"]; len(got) != 1 {
		t.Fatalf("expected the worker series before the timeout, got %v", got)
	}

	// Still inside the timeout: the series must survive.
	now = now.Add(29 * time.Second)
	svc.CleanStaleComponents()
	if got := gatherGaugeSamples(t, reg)["ray_tasks"]; len(got) != 1 {
		t.Fatalf("worker series dropped before the timeout, got %v", got)
	}

	// Past the timeout: the component is stale and its series disappear.
	now = now.Add(2 * time.Second)
	svc.CleanStaleComponents()
	if got := gatherGaugeSamples(t, reg)["ray_tasks"]; len(got) != 0 {
		t.Fatalf("stale worker series still exported, got %v", got)
	}

	// A later report from the same worker is exported again, i.e. the component
	// is re-created rather than the id being poisoned.
	now = now.Add(time.Second)
	if err := svc.Export(ctx, workerIDBytes(1), []*proto.Metric{
		newDoubleMetric("tasks", "Current number of tasks",
			[]string{"WorkerId", "State"}, []string{workerIDHexOf(1), "RUNNING"}, 9),
	}); err != nil {
		t.Fatalf("Export after cleanup error = %v", err)
	}
	series := gatherGaugeSamples(t, reg)["ray_tasks"]
	key := seriesKey("WorkerId="+workerIDHexOf(1), "State=RUNNING")
	if got, ok := series[key]; !ok || got != 9 {
		t.Fatalf("resent series %q = %v (present %v), want 9", key, got, ok)
	}
}

// TestMetricsServiceCleanStaleComponentsKeepsGlobalComponent verifies the global
// component is subject to the same timeout, mirroring clean_stale_components
// which does not special case GLOBAL_COMPONENT_KEY.
func TestMetricsServiceCleanStaleComponentsKeepsGlobalComponent(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)
	svc.proxy.componentTimeout = 10 * time.Second

	now := time.Now()
	svc.proxy.now = func() time.Time { return now }

	if err := svc.Export(context.Background(), nil, []*proto.Metric{
		newDoubleMetric("actors", "An actor can be in one of states",
			[]string{"State"}, []string{"ALIVE"}, 2),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	now = now.Add(11 * time.Second)
	svc.CleanStaleComponents()
	if got := gatherGaugeSamples(t, reg)["ray_actors"]; len(got) != 0 {
		t.Fatalf("stale global component series still exported, got %v", got)
	}
}

// TestMetricsServiceRecordRefreshesComponentTime verifies every report renews
// the component's last reported time, so a worker that keeps reporting is never
// considered stale.
func TestMetricsServiceRecordRefreshesComponentTime(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)
	svc.proxy.componentTimeout = 10 * time.Second

	now := time.Now()
	svc.proxy.now = func() time.Time { return now }

	ctx := context.Background()
	report := []*proto.Metric{newDoubleMetric("tasks", "Current number of tasks",
		[]string{"WorkerId", "State"}, []string{workerIDHexOf(1), "RUNNING"}, 1)}
	if err := svc.Export(ctx, workerIDBytes(1), report); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	// Two reports closer than the timeout, with the clock advanced in between,
	// must keep the component alive.
	now = now.Add(8 * time.Second)
	if err := svc.Export(ctx, workerIDBytes(1), report); err != nil {
		t.Fatalf("second Export error = %v", err)
	}
	now = now.Add(8 * time.Second)
	svc.CleanStaleComponents()
	if got := gatherGaugeSamples(t, reg)["ray_tasks"]; len(got) != 1 {
		t.Fatalf("a still reporting component was dropped, got %v", got)
	}
}

// TestMetricsServiceKeepsLastValuePerLabelSet verifies a label set reported again
// within one component replaces its value instead of duplicating the series,
// mirroring OpencensusProxyMetric.data being a dict keyed by label values.
func TestMetricsServiceKeepsLastValuePerLabelSet(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	ctx := context.Background()
	for _, value := range []float64{1, 2} {
		if err := svc.Export(ctx, workerIDBytes(1), []*proto.Metric{
			newDoubleMetric("tasks", "Current number of tasks",
				[]string{"WorkerId", "State"}, []string{workerIDHexOf(1), "RUNNING"}, value),
		}); err != nil {
			t.Fatalf("Export error = %v", err)
		}
	}

	series := gatherGaugeSamples(t, reg)["ray_tasks"]
	if len(series) != 1 {
		t.Fatalf("expected 1 series after re-reporting the same labels, got %v", series)
	}
	key := seriesKey("WorkerId="+workerIDHexOf(1), "State=RUNNING")
	if got := series[key]; got != 2 {
		t.Fatalf("series value = %v, want 2 (the last reported value)", got)
	}
}

// TestMetricsServiceMergesSameNameAcrossComponents verifies the same metric name
// reported by several components is rendered as one family. Components disagree
// about the label keys and help text of a metric (a raylet reports "tasks"
// without the per-worker tags, a worker adds WorkerId), and the Prometheus
// registry rejects a name collected twice with inconsistent metadata, which
// would fail the whole /metrics scrape.
func TestMetricsServiceMergesSameNameAcrossComponents(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	ctx := context.Background()
	if err := svc.Export(ctx, workerIDBytes(1), []*proto.Metric{
		newDoubleMetric("tasks", "Number of tasks from a worker",
			[]string{"WorkerId", "State"}, []string{workerIDHexOf(1), "RUNNING"}, 4),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}
	if err := svc.Export(ctx, nil, []*proto.Metric{
		newDoubleMetric("tasks", "Number of tasks from the raylet",
			[]string{"State"}, []string{"RUNNING"}, 5),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather error = %v (the same name was rendered twice)", err)
	}
	count := 0
	for _, family := range families {
		if family.GetName() != "ray_tasks" {
			continue
		}
		count++
		if got := len(family.GetMetric()); got != 1 {
			t.Fatalf("ray_tasks has %d series, want 1 (the mismatched one is dropped)", got)
		}
		if got := family.GetHelp(); got != "Number of tasks from a worker" {
			t.Fatalf("ray_tasks help = %q, want the first component's help", got)
		}
	}
	if count != 1 {
		t.Fatalf("ray_tasks rendered as %d families, want 1", count)
	}
}

// TestMetricsServiceSkipsMalformedWorkerID verifies a report carrying a
// malformed worker id is not attributed to any component: Export returns the
// error and nothing is recorded.
func TestMetricsServiceSkipsMalformedWorkerID(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	err := svc.Export(context.Background(), []byte{1, 2, 3}, []*proto.Metric{
		newDoubleMetric("tasks", "Current number of tasks",
			[]string{"State"}, []string{"RUNNING"}, 4),
	})
	if err == nil {
		t.Fatal("Export with a malformed worker id returned nil error")
	}
	if samples := gatherGaugeSamples(t, reg); len(samples) != 0 {
		t.Fatalf("a malformed worker id still exported metrics: %v", samples)
	}
}

// TestMetricsServiceExportSkipsDistributionPoints verifies a reported
// distribution (no gauge representation) is dropped instead of being exported as
// a bogus gauge value.
func TestMetricsServiceExportSkipsDistributionPoints(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	metric := &proto.Metric{
		MetricDescriptor: &proto.MetricDescriptor{
			Name:      "grpc_io_client_server_latency",
			Type:      proto.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
			LabelKeys: []*proto.LabelKey{{Key: "grpc_client_method"}},
		},
		Timeseries: []*proto.TimeSeries{{
			LabelValues: []*proto.LabelValue{{Value: "/ray.rpc.X/Y", HasValue: true}},
			Points: []*proto.Point{{
				Value: &proto.Point_DistributionValue{
					DistributionValue: &proto.DistributionValue{Count: 3, Sum: 42},
				},
			}},
		}},
	}
	if err := svc.Export(context.Background(), nil, []*proto.Metric{metric}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	if samples := gatherGaugeSamples(t, reg); len(samples) != 0 {
		t.Fatalf("a distribution metric was exported as a gauge: %v", samples)
	}
}

// TestMetricsServiceProxyRegistrationFailureIsNotFatal verifies a nil registry
// degrades gracefully: Export still records without panicking.
func TestMetricsServiceProxyRegistrationFailureIsNotFatal(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	svc := NewMetricsService(nil)
	if svc.proxy == nil {
		t.Fatal("NewMetricsService(nil) left the proxy collector nil")
	}
	if err := svc.Export(context.Background(), nil, []*proto.Metric{
		newDoubleMetric("tasks", "Current number of tasks", []string{"State"}, []string{"RUNNING"}, 1),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}
	// A nil registry has no collector to render the recorded metrics, so
	// collecting must simply emit nothing.
	ch := make(chan prometheus.Metric, 8)
	go func() {
		svc.proxy.Collect(ch)
		close(ch)
	}()
	count := 0
	for range ch {
		count++
	}
	if count != 1 {
		t.Fatalf("proxy Collect emitted %d metrics, want 1", count)
	}
}

// componentKeys returns the component keys the proxy collector currently tracks.
func componentKeys(svc *MetricsService) []string {
	svc.proxy.mu.Lock()
	defer svc.proxy.mu.Unlock()
	return slices.Clone(svc.proxy.order)
}

// TestReporterServiceReportOCMetricsAttributesWorkerID verifies the RPC reads the
// worker id off the request and attributes the reported metrics to it, so a
// worker's series belong to that worker's component and a raylet report (no
// worker id) belongs to the global component. Without reading the field every
// report would land in one anonymous component.
func TestReporterServiceReportOCMetricsAttributesWorkerID(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	reg := prometheus.NewRegistry()

	svc, err := NewReporterService(agent.Config{LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewReporterService: %v", err)
	}
	metricsService := NewMetricsService(reg)
	svc.SetMetricsService(metricsService)

	_, err = svc.ReportOCMetrics(context.Background(), &proto.ReportOCMetricsRequest{
		WorkerId: workerIDBytes(5),
		Metrics: []*proto.Metric{newDoubleMetric("tasks", "Current number of tasks",
			[]string{"WorkerId", "State"}, []string{workerIDHexOf(5), "RUNNING"}, 6)},
	})
	if err != nil {
		t.Fatalf("ReportOCMetrics (worker): %v", err)
	}

	series := gatherGaugeSamples(t, reg)["ray_tasks"]
	key := seriesKey("WorkerId="+workerIDHexOf(5), "State=RUNNING")
	if got, ok := series[key]; !ok || got != 6 {
		t.Fatalf("series %q = %v (present %v), want 6", key, got, ok)
	}
	if got := componentKeys(metricsService); !slices.Equal(got, []string{workerIDHexOf(5)}) {
		t.Fatalf("reported worker attributed to components %v, want [%s]", got, workerIDHexOf(5))
	}

	_, err = svc.ReportOCMetrics(context.Background(), &proto.ReportOCMetricsRequest{
		Metrics: []*proto.Metric{newDoubleMetric("internal_num_spilled_tasks", "Spilled leases",
			[]string{"Reason"}, []string{"RateLimited"}, 3)},
	})
	if err != nil {
		t.Fatalf("ReportOCMetrics (raylet): %v", err)
	}

	want := []string{workerIDHexOf(5), globalComponentKey}
	if got := componentKeys(metricsService); !slices.Equal(got, want) {
		t.Fatalf("components after a raylet report = %v, want %v", got, want)
	}
}

// TestReporterModulePublishOnceCleansStaleComponents verifies the collection
// cycle drops the metrics of a component that stopped reporting, mirroring the
// clean_all_dead_worker_metrics call of the Python _async_compose_stats_payload.
func TestReporterModulePublishOnceCleansStaleComponents(t *testing.T) {
	withCardinalityLevel(t, metricCardinalityLegacy)
	m, _ := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})
	m.metricsService.proxy.componentTimeout = 30 * time.Second

	now := time.Now()
	m.metricsService.proxy.now = func() time.Time { return now }

	if err := m.metricsService.Export(context.Background(), workerIDBytes(1), []*proto.Metric{
		newDoubleMetric("tasks", "Current number of tasks",
			[]string{"WorkerId", "State"}, []string{workerIDHexOf(1), "RUNNING"}, 4),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}
	if got := gatherGaugeSamples(t, m.metricsService.Registry())["ray_tasks"]; len(got) != 1 {
		t.Fatalf("expected the worker series before the cycle, got %v", got)
	}

	// A cycle inside the timeout keeps the series.
	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}
	if got := gatherGaugeSamples(t, m.metricsService.Registry())["ray_tasks"]; len(got) != 1 {
		t.Fatalf("worker series dropped inside the timeout, got %v", got)
	}

	// A cycle past the timeout drops the series, because the worker stopped
	// reporting in between.
	now = now.Add(31 * time.Second)
	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce after the timeout: %v", err)
	}
	if got := gatherGaugeSamples(t, m.metricsService.Registry())["ray_tasks"]; len(got) != 0 {
		t.Fatalf("stale worker series still exported after a publish cycle, got %v", got)
	}
}
