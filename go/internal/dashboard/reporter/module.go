package reporter

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// publishInterval is the interval between node resource usage publications.
// It is a package-level variable so tests can collapse it to make the periodic
// loop fast; tests save and restore it around each run and never run in
// parallel, so the mutable global is safe here. Its default is derived from
// UpdateIntervalMs, which reads the REPORTER_UPDATE_INTERVAL_MS environment
// variable and falls back to DefaultReporterUpdateIntervalMs, mirroring the
// Python REPORTER_UPDATE_INTERVAL_MS.
var publishInterval = time.Duration(UpdateIntervalMs()) * time.Millisecond

// ReporterModule implements agent.Module, mirroring the Python
// ReporterAgent. It assembles the resource collectors, the MetricsService and
// the ReporterService, and runs a periodic loop that publishes node resource
// usage to GCS under the RAY_REPORTER:<node_id> key.
type ReporterModule struct {
	cfg             agent.Config
	statsCollector  *StatsCollector
	gpuCollector    *GPUCollector
	tpuCollector    *TPUCollector
	publisher       gcs.NodeResourceUsagePublisher
	metricsService  *MetricsService
	reporterService *ReporterService
	cancel          context.CancelFunc
}

// noopPublisher is a fallback publisher used when no GCS client is configured.
// It drops publications silently so the module's publisher stays non-nil while
// remaining a harmless no-op at runtime.
type noopPublisher struct{}

func (noopPublisher) PublishNodeResourceUsage(context.Context, string, string) error {
	return nil
}

// compile-time assertion that ReporterModule satisfies the agent.Module
// contract.
var _ agent.Module = (*ReporterModule)(nil)

// resolvePublisher picks the publisher to publish node resource usage with. It
// prefers the explicitly-configured GCS client on the config, falling back to the
// package-level singleton, and finally to a no-op publisher so the module always
// has a non-nil publisher. A client is only used when it also implements
// gcs.NodeResourceUsagePublisher (an independent interface, not part of
// gcs.Client), so clients without node-resource publishing still degrade to the
// no-op instead of failing.
func resolvePublisher(cfg agent.Config) gcs.NodeResourceUsagePublisher {
	if p, ok := asNodeResourceUsagePublisher(cfg.GCS); ok {
		return p
	}
	if c, err := gcs.GetClient(); err == nil && c != nil {
		if p, ok := asNodeResourceUsagePublisher(c); ok {
			return p
		}
	}
	return noopPublisher{}
}

// asNodeResourceUsagePublisher reports whether client implements the
// gcs.NodeResourceUsagePublisher interface and returns it when it does.
func asNodeResourceUsagePublisher(client gcs.Client) (gcs.NodeResourceUsagePublisher, bool) {
	if client == nil {
		return nil, false
	}
	p, ok := client.(gcs.NodeResourceUsagePublisher)
	return p, ok
}

// New assembles the reporter module from the agent config. It builds the
// resource collectors, a Prometheus-backed MetricsService, and the
// ReporterService (with the MetricsService injected so ReportOCMetrics can
// forward metrics), and wires the GCS client as the node resource usage
// publisher. It returns an error when the config cannot build a ReporterService
// (e.g. an empty log directory).
func New(cfg agent.Config) (*ReporterModule, error) {
	metricsService := NewMetricsService(prometheus.NewRegistry())

	reporterService, err := NewReporterService(cfg)
	if err != nil {
		return nil, err
	}
	reporterService.SetMetricsService(metricsService)

	return &ReporterModule{
		cfg:             cfg,
		statsCollector:  NewStatsCollector(cfg),
		gpuCollector:    NewGPUCollector(),
		tpuCollector:    NewTPUCollector(),
		publisher:       resolvePublisher(cfg),
		metricsService:  metricsService,
		reporterService: reporterService,
	}, nil
}

// publishOnce collects a full resource snapshot, injects the GPU and TPU
// utilization collected by the dedicated collectors, composes the JSON payload
// and publishes it to GCS under the RAY_REPORTER:<node_id> key. Publication is
// decoupled from metrics collection, so it runs regardless of the
// MetricsCollectionDisabled flag, matching the Python agent. A nil publisher
// (e.g. no GCS client configured) degrades gracefully by skipping the publish.
func (m *ReporterModule) publishOnce(ctx context.Context) error {
	stats := m.statsCollector.Collect()
	stats["gpus"] = m.gpuCollector.Collect()
	stats["tpus"] = m.tpuCollector.Collect()

	now := float64(time.Now().Unix())
	payload, err := NewStatsPayloadComposer(stats, now).Compose()
	if err != nil {
		return err
	}

	if m.publisher == nil {
		return nil
	}
	keyID := ReporterPrefix + m.cfg.NodeID.Hex()
	return m.publisher.PublishNodeResourceUsage(ctx, keyID, payload)
}

// collectLoop runs the periodic publication loop until ctx is cancelled. A
// failed publication is logged but does not interrupt the loop, mirroring the
// Python _run_loop which catches exceptions per iteration.
func (m *ReporterModule) collectLoop(ctx context.Context) {
	ticker := time.NewTicker(publishInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Re-check cancellation before publishing. When the select races
			// between a ready ticker and a cancelled context, Go picks a case
			// randomly and could otherwise schedule a publish that must no
			// longer run after Close. Re-checking here guarantees Close stops
			// the loop without scheduling new publications (only an in-flight
			// publish that started before Close may still complete).
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := m.publishOnce(ctx); err != nil {
				log.Log.Error(err, "failed to publish node resource usage")
			}
		}
	}
}

// Start launches the periodic collection loop. The loop runs in a background
// goroutine and stops when ctx is cancelled; a derived cancel is stored on the
// module so callers can stop the loop explicitly.
func (m *ReporterModule) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	go m.collectLoop(loopCtx)
	return nil
}

// Close stops the periodic collection loop by cancelling the context derived in
// Start. It is idempotent and safe to call when the module was never started
// (cancel is nil) or has already been closed.
func (m *ReporterModule) Close() {
	if m.cancel != nil {
		m.cancel()
	}
}

// RegisterGRPC registers the ReporterService servicer on the agent's gRPC
// server, mirroring the Python ReporterAgent.run which registers the
// ReporterService gRPC servicer.
func (m *ReporterModule) RegisterGRPC(s *grpc.Server) error {
	proto.RegisterReporterServiceServer(s, m.reporterService)
	return nil
}

// RegisterHTTP is a no-op returning nil: the reporter module exposes no HTTP
// endpoints.
func (m *ReporterModule) RegisterHTTP(_ *http.ServeMux) error {
	return nil
}
