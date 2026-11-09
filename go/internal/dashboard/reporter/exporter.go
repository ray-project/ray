package reporter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ray-project/ray/go/pkg/log"
)

// StatsExporter serves the reporter's Prometheus registry over HTTP, mirroring
// the Python PrometheusStatsExporter the reporter agent starts with
// prometheus_exporter.new_stats_exporter when metrics collection is enabled. The
// Python exporter serves the opencensus view data at /metrics; here the
// equivalent registry is the MetricsService registry, which holds both the
// exported opencensus metrics and the reporter's own gauges.
type StatsExporter struct {
	registry *prometheus.Registry
	server   *http.Server
	// boundPort is the port the server actually listens on once Start has
	// succeeded, or -1 when the exporter is disabled or its bind failed. The
	// agent publishes this value instead of the configured one, so a node never
	// advertises a metrics endpoint nothing answers on.
	boundPort int
	// listener is retained so Close can release the port synchronously. Closing
	// the server alone relies on the Serve goroutine having registered the
	// listener first, which leaves the port briefly bound when Close races the
	// startup of that goroutine -- a restart on the same port would then fail.
	listener net.Listener
}

// NewStatsExporter returns an exporter for the given registry. A nil registry
// is tolerated: Start then degrades to a no-op so a misconfigured module keeps
// the agent alive with metrics disabled, matching the Python agent which logs
// and continues when the exporter cannot start.
func NewStatsExporter(registry *prometheus.Registry) *StatsExporter {
	return &StatsExporter{registry: registry, boundPort: -1}
}

// Start binds the /metrics endpoint on host:port and serves it in the
// background until ctx is cancelled. An empty host binds every interface,
// mirroring the Python exporter which passes no address for non-localhost
// nodes. Port 0 asks the kernel for a free port, which is what a launcher that
// does not configure one passes and what the Python agent does with the same
// value; the bound port is published via BoundPort. A negative port or a nil
// registry disables the server, and a bind failure returns an error so the
// caller can decide whether to continue.
func (e *StatsExporter) Start(ctx context.Context, host string, port int) error {
	if e.registry == nil {
		log.Log.Info("reporter metrics registry is nil, metrics server not started")
		return nil
	}
	if port < 0 {
		log.Log.Info("reporter metrics export disabled, metrics server not started")
		return nil
	}

	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{}))
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
		// Timeouts prevent slow/idle Prometheus scrapers from leaking
		// goroutines, mirroring the head dashboard metrics server.
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on metrics addr %s: %w", addr, err)
	}
	e.server = server
	e.listener = ln
	e.boundPort = ln.Addr().(*net.TCPAddr).Port

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Log.Error(err, "reporter metrics server error")
		}
	}()
	log.Log.Info("reporter metrics server started", "addr", addr)
	return nil
}

// BoundPort reports the port the server listens on, or -1 when it is not
// serving. It is only meaningful after Start.
func (e *StatsExporter) BoundPort() int {
	if e == nil || e.listener == nil {
		return -1
	}
	return e.boundPort
}

// Close shuts down the metrics server. It is idempotent and safe to call when
// the exporter was never started or has already been closed.
func (e *StatsExporter) Close() {
	server, listener := e.server, e.listener
	if server == nil && listener == nil {
		return
	}
	e.server = nil
	e.listener = nil
	if server != nil {
		if err := server.Shutdown(context.Background()); err != nil {
			log.Log.Error(err, "failed to shut down reporter metrics server")
		}
	}
	// Explicitly close the listener so the kernel releases the port before
	// Close returns, even when the Serve goroutine has not started yet.
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Log.Error(err, "failed to close reporter metrics listener")
		}
	}
}
