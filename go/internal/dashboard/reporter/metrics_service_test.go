package reporter

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/proto"
)

// newDoubleMetric builds an opencensus proto.Metric with a double gauge value
// and the given label keys/values, mirroring the shape produced by
// ReportOCMetricsRequest.GetMetrics().
func newDoubleMetric(name, desc string, labelKeys []string, labelVals []string, value float64) *proto.Metric {
	keys := make([]*proto.LabelKey, 0, len(labelKeys))
	for _, k := range labelKeys {
		keys = append(keys, &proto.LabelKey{Key: k})
	}
	vals := make([]*proto.LabelValue, 0, len(labelVals))
	for _, v := range labelVals {
		vals = append(vals, &proto.LabelValue{Value: v, HasValue: true})
	}
	return &proto.Metric{
		MetricDescriptor: &proto.MetricDescriptor{
			Name:      name,
			Description: desc,
			Type:      proto.MetricDescriptor_GAUGE_DOUBLE,
			LabelKeys: keys,
		},
		Timeseries: []*proto.TimeSeries{
			{
				LabelValues: vals,
				Points: []*proto.Point{
					{Value: &proto.Point_DoubleValue{DoubleValue: value}},
				},
			},
		},
	}
}

// gatherNames returns the set of metric family names present in the registry.
func gatherNames(reg *prometheus.Registry) (map[string]bool, error) {
	families, err := reg.Gather()
	if err != nil {
		return nil, err
	}
	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
	}
	return names, nil
}

// TestMetricsServiceNew validates the constructor wires up a usable registry.
func TestMetricsServiceNew(t *testing.T) {
	svc := NewMetricsService(prometheus.NewRegistry())
	if svc == nil {
		t.Fatal("NewMetricsService returned nil")
	}
	if svc.Registry() == nil {
		t.Error("Registry() returned nil")
	}
	if svc.registry == nil {
		t.Error("constructor did not store the registry")
	}
	if svc.gauges == nil {
		t.Error("constructor did not initialize the gauge map")
	}
}

// TestMetricsServiceExportDoubleGauge verifies a double gauge is exported to
// the registry with the ray_ prefix, correct value and labels.
func TestMetricsServiceExportDoubleGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	err := svc.Export(context.Background(), []*proto.Metric{
		newDoubleMetric("node_cpu_utilization", "Total CPU usage on a ray node",
			[]string{"ip", "Version", "SessionName", "IsHeadNode", "RayNodeType"},
			[]string{"1.2.3.4", "2.53.0", "sess", "false", "worker"},
			37.5),
	})
	if err != nil {
		t.Fatalf("Export error = %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather error = %v", err)
	}
	if len(families) != 1 {
		t.Fatalf("expected 1 metric family, got %d", len(families))
	}
	fam := families[0]
	if got := fam.GetName(); got != "ray_node_cpu_utilization" {
		t.Fatalf("expected name ray_node_cpu_utilization, got %q", got)
	}
	metric := fam.GetMetric()[0]
	if got := metric.GetGauge().GetValue(); got != 37.5 {
		t.Fatalf("expected gauge value 37.5, got %v", got)
	}
	wantLabels := map[string]string{
		"ip": "1.2.3.4", "Version": "2.53.0", "SessionName": "sess",
		"IsHeadNode": "false", "RayNodeType": "worker",
	}
	gotLabels := make(map[string]string, len(metric.GetLabel()))
	for _, lp := range metric.GetLabel() {
		gotLabels[lp.GetName()] = lp.GetValue()
	}
	if len(gotLabels) != len(wantLabels) {
		t.Fatalf("label count mismatch: got %v want %v", gotLabels, wantLabels)
	}
	for k, v := range wantLabels {
		if gotLabels[k] != v {
			t.Errorf("label %q = %q, want %q", k, gotLabels[k], v)
		}
	}
}

// TestMetricsServiceExportInt64Gauge verifies an int64 gauge value is exported.
func TestMetricsServiceExportInt64Gauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	m := &proto.Metric{
		MetricDescriptor: &proto.MetricDescriptor{
			Name: "node_cpu_count",
			Description: "Total CPUs available on a ray node",
			Type: proto.MetricDescriptor_GAUGE_INT64,
			LabelKeys: []*proto.LabelKey{{Key: "ip"}},
		},
		Timeseries: []*proto.TimeSeries{
			{
				LabelValues: []*proto.LabelValue{{Value: "9.9.9.9", HasValue: true}},
				Points:      []*proto.Point{{Value: &proto.Point_Int64Value{Int64Value: 8}}},
			},
		},
	}

	if err := svc.Export(context.Background(), []*proto.Metric{m}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	families, _ := reg.Gather()
	if len(families) != 1 {
		t.Fatalf("expected 1 family, got %d", len(families))
	}
	if got := families[0].GetMetric()[0].GetGauge().GetValue(); got != 8 {
		t.Fatalf("expected int64 value 8 as gauge, got %v", got)
	}
}

// TestMetricsServiceExportReusesGaugeVec verifies exporting the same metric
// name twice reuses the registered GaugeVec instead of panicking on duplicate
// registration.
func TestMetricsServiceExportReusesGaugeVec(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	for i := 0; i < 2; i++ {
		if err := svc.Export(context.Background(), []*proto.Metric{
			newDoubleMetric("node_mem_used", "Memory usage on a ray node",
				[]string{"ip"}, []string{"1.1.1.1"}, float64(i+1)),
		}); err != nil {
			t.Fatalf("Export iteration %d error = %v", i, err)
		}
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather error = %v", err)
	}
	if len(families) != 1 {
		t.Fatalf("expected 1 family after reuse, got %d", len(families))
	}
	if got := families[0].GetMetric()[0].GetGauge().GetValue(); got != 2 {
		t.Fatalf("expected last value 2, got %v", got)
	}
}

// TestMetricsServiceExportSkipsInvalidMetric verifies malformed metrics
// (nil descriptor, nil timeseries, unsupported point type) degrade gracefully
// without erroring the whole batch or panicking.
func TestMetricsServiceExportSkipsInvalidMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	valid := newDoubleMetric("node_disk_free", "Disk free on a ray node",
		[]string{"ip"}, []string{"5.5.5.5"}, 100)
	unsupported := &proto.Metric{
		MetricDescriptor: &proto.MetricDescriptor{
			Name: "node_disk_io_read",
			Type: proto.MetricDescriptor_GAUGE_DISTRIBUTION,
		},
		Timeseries: []*proto.TimeSeries{{Points: []*proto.Point{}}},
	}

	err := svc.Export(context.Background(), []*proto.Metric{
		nil,
		{},
		unsupported,
		valid,
	})
	if err != nil {
		t.Fatalf("Export with invalid metrics returned error = %v", err)
	}

	names, err := gatherNames(reg)
	if err != nil {
		t.Fatalf("Gather error = %v", err)
	}
	if !names["ray_node_disk_free"] {
		t.Errorf("expected ray_node_disk_free to be exported, got %v", names)
	}
}

// TestMetricsServiceCollect delegates to the underlying registry so a
// Prometheus exporter can scrape exported metrics.
func TestMetricsServiceCollect(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	if err := svc.Export(context.Background(), []*proto.Metric{
		newDoubleMetric("node_network_sent", "Total bytes sent", []string{"ip"}, []string{"7.7.7.7"}, 55),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	ch := make(chan prometheus.Metric, 16)
	go func() {
		svc.Collect(ch)
		close(ch)
	}()
	count := 0
	for range ch {
		count++
	}
	if count == 0 {
		t.Error("Collect emitted no metrics")
	}
}

// TestMetricsServiceExportMetadata verifies the aligned gauge metadata
// (ray_ prefix, help text) for a known Python METRICS_GAUGES gauge.
func TestMetricsServiceExportMetadata(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	if err := svc.Export(context.Background(), []*proto.Metric{
		newDoubleMetric("component_cpu_percentage", "CPU percentage of the component",
			[]string{"ip", "pid", "Version", "Component", "SessionName"},
			[]string{"1.1.1.1", "123", "2.53.0", "agent", "sess"}, 12.3),
	}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	families, _ := reg.Gather()
	if len(families) != 1 {
		t.Fatalf("expected 1 family, got %d", len(families))
	}
	fam := families[0]
	if got := fam.GetName(); got != "ray_component_cpu_percentage" {
		t.Fatalf("expected ray_component_cpu_percentage, got %q", got)
	}
	if got := fam.GetHelp(); got == "" {
		t.Error("expected non-empty help text on exported metric")
	}
}

// TestMetricsServiceExportAlignsFilteredLabelKeys verifies that dropping an
// empty descriptor key also drops the matching series value, so the values stay
// aligned with the retained keys instead of panicking on WithLabelValues with
// the wrong cardinality.
func TestMetricsServiceExportAlignsFilteredLabelKeys(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	m := &proto.Metric{
		MetricDescriptor: &proto.MetricDescriptor{
			Name:      "component_cpu_percentage",
			Type:      proto.MetricDescriptor_GAUGE_DOUBLE,
			LabelKeys: []*proto.LabelKey{{Key: "ip"}, {Key: ""}, {Key: "Component"}},
		},
		Timeseries: []*proto.TimeSeries{{
			LabelValues: []*proto.LabelValue{
				{Value: "1.2.3.4", HasValue: true},
				{Value: "dropped", HasValue: true},
				{Value: "agent", HasValue: true},
			},
			Points: []*proto.Point{{Value: &proto.Point_DoubleValue{DoubleValue: 7.5}}},
		}},
	}
	if err := svc.Export(context.Background(), []*proto.Metric{m}); err != nil {
		t.Fatalf("Export error = %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather error = %v", err)
	}
	if len(families) != 1 {
		t.Fatalf("expected 1 metric family, got %d", len(families))
	}
	metric := families[0].GetMetric()[0]
	gotLabels := make(map[string]string, len(metric.GetLabel()))
	for _, lp := range metric.GetLabel() {
		gotLabels[lp.GetName()] = lp.GetValue()
	}
	wantLabels := map[string]string{"ip": "1.2.3.4", "Component": "agent"}
	if len(gotLabels) != len(wantLabels) {
		t.Fatalf("label count mismatch: got %v want %v", gotLabels, wantLabels)
	}
	for k, v := range wantLabels {
		if gotLabels[k] != v {
			t.Errorf("label %q = %q, want %q", k, gotLabels[k], v)
		}
	}
}

// TestMetricsServiceExportSkipsUnalignedSeries verifies that a series missing a
// value for a retained key is skipped instead of panicking; the rest of the
// batch still exports.
func TestMetricsServiceExportSkipsUnalignedSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	bad := &proto.Metric{
		MetricDescriptor: &proto.MetricDescriptor{
			Name:      "node_mem_used",
			Type:      proto.MetricDescriptor_GAUGE_DOUBLE,
			LabelKeys: []*proto.LabelKey{{Key: "ip"}},
		},
		Timeseries: []*proto.TimeSeries{{
			Points: []*proto.Point{{Value: &proto.Point_DoubleValue{DoubleValue: 1}}},
		}},
	}
	good := newDoubleMetric("node_disk_free", "Disk free on a ray node",
		[]string{"ip"}, []string{"5.5.5.5"}, 100)

	if err := svc.Export(context.Background(), []*proto.Metric{bad, good}); err != nil {
		t.Fatalf("Export error = %v", err)
	}
	names, err := gatherNames(reg)
	if err != nil {
		t.Fatalf("Gather error = %v", err)
	}
	if !names["ray_node_disk_free"] {
		t.Errorf("expected the valid metric to still export, got %v", names)
	}
	if names["ray_node_mem_used"] {
		t.Errorf("expected the unaligned series to be skipped, got %v", names)
	}
}
