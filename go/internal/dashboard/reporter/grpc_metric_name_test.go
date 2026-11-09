package reporter

import (
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ray-project/ray/go/proto"
)

// TestGRPCMetricNameDoesNotBreakScrape pins the behaviour of a component metric
// whose name is not a valid Prometheus name. gRPC's OpenCensus instrument names
// look like `grpc.io/client/server_latency`; the Python agent rewrites them with
// fix_grpc_metric before exporting. This client_golang version sanitizes such a
// name in the text exposition instead of failing the write, so a scrape of the
// reporter's registry stays 200 and the series appears under the same name the
// Python agent would produce. The test exists so a dependency bump that turns
// the invalid name into a gather error cannot silently take the whole endpoint
// down with it.
func TestGRPCMetricNameDoesNotBreakScrape(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)
	m := &proto.Metric{
		MetricDescriptor: &proto.MetricDescriptor{
			Name:      "grpc.io/client/server_latency",
			Type:      proto.MetricDescriptor_GAUGE_DOUBLE,
			LabelKeys: []*proto.LabelKey{{Key: "grpc_client_method"}},
		},
		Timeseries: []*proto.TimeSeries{{
			LabelValues: []*proto.LabelValue{{Value: "/ray.rpc.X/Y", HasValue: true}},
			Points:      []*proto.Point{{Value: &proto.Point_DoubleValue{DoubleValue: 1.5}}},
		}},
	}
	if err := svc.Export(nil, nil, []*proto.Metric{m}); err != nil {
		t.Fatalf("export: %v", err)
	}

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("scrape status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	want := "ray_grpc_io_client_server_latency"
	if body := rec.Body.String(); !contains(body, want) {
		t.Fatalf("scrape body does not carry %q; body: %s", want, body)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
