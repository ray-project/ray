package reporter

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
)

// freePort reserves and immediately releases a TCP port so the test can hand a
// concrete port number to the exporter without racing another listener.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	return port
}

// scrapeOnce fetches /metrics from the given port, retrying briefly so the test
// does not depend on the server goroutine being scheduled first.
func scrapeOnce(t *testing.T, port int) string {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", port)
	client := &http.Client{Timeout: 2 * time.Second}
	var lastErr error
	for i := 0; i < 50; i++ {
		resp, err := client.Get(url)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s status = %d, want 200", url, resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read /metrics body: %v", err)
			}
			return string(body)
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET %s never succeeded: %v", url, lastErr)
	return ""
}

// TestStatsExporterServesRegistry verifies the exporter binds /metrics and
// exposes the gauges written through MetricsService, so a Prometheus scraper
// sees ray_node_cpu_utilization with its recorded value and labels.
func TestStatsExporterServesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsService(reg)

	tags := map[string]string{
		"ip": "10.0.0.7", "Version": "2.53.0", "SessionName": "sess",
		"RayNodeType": "head", "IsHeadNode": "true",
	}
	svc.RecordGauge(gaugeOf(GaugeNodeCPUUtilization), tags, 42.5)
	// A declared label the record does not carry must export as an empty string
	// rather than dropping the series.
	svc.RecordGauge(gaugeOf(GaugeNodeGPUsUtilization), map[string]string{"ip": "10.0.0.7", "GpuIndex": "0"}, 7)

	port := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exp := NewStatsExporter(reg)
	if err := exp.Start(ctx, "127.0.0.1", port); err != nil {
		t.Fatalf("StatsExporter.Start: %v", err)
	}
	defer exp.Close()

	body := scrapeOnce(t, port)
	for _, want := range []string{
		"ray_node_cpu_utilization",
		"42.5",
		`ip="10.0.0.7"`,
		`RayNodeType="head"`,
		"ray_node_gpus_utilization",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics output missing %q\n%s", want, body)
		}
	}
	// The HELP line must carry the Python gauge description.
	if !strings.Contains(body, "Total CPU usage on a ray node") {
		t.Errorf("/metrics output missing the node_cpu_utilization HELP text\n%s", body)
	}
}

// TestStatsExporterDisabledWithoutPort verifies a non-positive port degrades to
// a no-op instead of binding a random port, matching the Python agent which
// keeps running with stats disabled when the exporter cannot start.
func TestStatsExporterBindsDynamicPortForZero(t *testing.T) {
	exp := NewStatsExporter(prometheus.NewRegistry())
	if err := exp.Start(context.Background(), "127.0.0.1", 0); err != nil {
		t.Fatalf("Start with port 0 returned error: %v", err)
	}
	defer exp.Close()
	if exp.server == nil {
		t.Fatal("Start with port 0 did not bind a server")
	}
	// A launcher that configures no export port passes 0, and the Python agent
	// allocates a free port for it; the agent then publishes the bound port, so
	// a sentinel here would leave the node without a scrape target.
	if got := exp.BoundPort(); got <= 0 {
		t.Errorf("BoundPort = %d, want the dynamically allocated port", got)
	}
}

// TestStatsExporterDisabledForNegativePort verifies a negative port disables the
// server: the negative value is the not-in-use sentinel, not a request for a
// dynamic allocation.
func TestStatsExporterDisabledForNegativePort(t *testing.T) {
	exp := NewStatsExporter(prometheus.NewRegistry())
	if err := exp.Start(context.Background(), "127.0.0.1", -1); err != nil {
		t.Fatalf("Start with port -1 returned error: %v", err)
	}
	if exp.server != nil {
		t.Error("Start with port -1 bound a server")
	}
	if got := exp.BoundPort(); got != -1 {
		t.Errorf("BoundPort = %d, want -1", got)
	}
	exp.Close() // must be safe when never started
}

// TestStatsExporterDisabledWithoutRegistry verifies a nil registry degrades to a
// no-op rather than panicking on a nil dereference.
func TestStatsExporterDisabledWithoutRegistry(t *testing.T) {
	exp := NewStatsExporter(nil)
	if err := exp.Start(context.Background(), "127.0.0.1", freePort(t)); err != nil {
		t.Fatalf("Start with nil registry returned error: %v", err)
	}
	if exp.server != nil {
		t.Error("Start with nil registry bound a server")
	}
	exp.Close()
}

// TestStatsExporterPortConflictReturnsError verifies a bind failure is reported
// as an error instead of panicking, so the module can log it and stay alive.
func TestStatsExporterPortConflictReturnsError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	exp := NewStatsExporter(prometheus.NewRegistry())
	err = exp.Start(context.Background(), "127.0.0.1", port)
	if err == nil {
		t.Fatal("Start on an occupied port returned no error")
	}
	if !strings.Contains(err.Error(), "listen on metrics addr") {
		t.Errorf("error = %v, want it to mention the metrics addr", err)
	}
	if exp.server != nil {
		t.Error("failed Start left a server assigned")
	}
	exp.Close()
}

// TestStatsExporterCloseIsIdempotent verifies Close can be called repeatedly and
// stops serving, matching the module Close contract.
func TestStatsExporterCloseIsIdempotent(t *testing.T) {
	port := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exp := NewStatsExporter(prometheus.NewRegistry())
	if err := exp.Start(ctx, "127.0.0.1", port); err != nil {
		t.Fatalf("StatsExporter.Start: %v", err)
	}
	exp.Close()
	exp.Close()

	// The port must be released so a new listener can take it.
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("port not released after Close, rebind failed: %v", err)
	}
	_ = ln.Close()
}

// TestStatsExporterStopsOnContextCancel verifies cancelling the context shuts the
// server down and releases the port, matching the head dashboard metrics server.
func TestStatsExporterStopsOnContextCancel(t *testing.T) {
	port := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())

	exp := NewStatsExporter(prometheus.NewRegistry())
	if err := exp.Start(ctx, "127.0.0.1", port); err != nil {
		t.Fatalf("StatsExporter.Start: %v", err)
	}
	defer exp.Close()

	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = ln.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("port %d still bound after context cancellation", port)
}

// TestMetricsExportHost verifies the exporter binds the loopback address on a
// localhost node and every interface otherwise, matching the Python
// get_localhost_ip()/"" address selection.
func TestMetricsExportHost(t *testing.T) {
	for _, nodeIP := range []string{"127.0.0.1", "localhost", "::1"} {
		got := metricsExportHost(agent.Config{NodeIP: nodeIP})
		addr := net.ParseIP(got)
		if addr == nil || !addr.IsLoopback() {
			t.Errorf("metricsExportHost(%q) = %q, want a loopback address", nodeIP, got)
		}
	}
	if got := metricsExportHost(agent.Config{NodeIP: "10.0.0.7"}); got != "" {
		t.Errorf("metricsExportHost(10.0.0.7) = %q, want the empty all-interface host", got)
	}
}
