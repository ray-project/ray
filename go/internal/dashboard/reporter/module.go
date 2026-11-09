package reporter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/version"
	"github.com/ray-project/ray/go/proto"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
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
	exporter        *StatsExporter
	otelService     *OpenTelemetryMetricsService
	records         recordsState
	cancel          context.CancelFunc
}

// noopPublisher is a fallback publisher used when no GCS client is configured.
// It drops publications so the module's publisher stays non-nil while
// remaining a harmless no-op at runtime; the fallback itself is announced by
// resolvePublisher so the missing configuration is discoverable.
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
	// Surface the degraded resolution: without a publisher every node resource
	// usage publication is dropped, and RAY_REPORTER:<node_id> keys never
	// appear in GCS, so the misconfiguration must not stay silent.
	log.Log.Error(nil, "no GCS publisher configured; node resource usage publications are dropped")
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

	var exporter *StatsExporter
	if !cfg.MetricsCollectionDisabled {
		exporter = NewStatsExporter(metricsService.Registry())
	}

	return &ReporterModule{
		cfg:             cfg,
		statsCollector:  NewStatsCollector(cfg),
		gpuCollector:    NewGPUCollector(),
		tpuCollector:    NewTPUCollector(),
		publisher:       resolvePublisher(cfg),
		metricsService:  metricsService,
		reporterService: reporterService,
		exporter:        exporter,
		otelService:     NewOpenTelemetryMetricsService(metricsService.Registry(), cfg.MetricsCollectionDisabled),
	}, nil
}

// metricsExportHost returns the address the Prometheus exporter binds to,
// mirroring the Python reporter agent: a localhost node binds the loopback
// address resolved from the hostname, while every other node binds all
// interfaces (the empty host of prometheus_client.start_http_server).
func metricsExportHost(cfg agent.Config) string {
	if common.IsLocalhost(cfg.NodeIP) {
		return common.GetLocalhostIP()
	}
	return ""
}

// recordMetrics converts the collected stats into gauge records and writes them
// to the Prometheus registry, mirroring the Python
// _async_compose_stats_payload record_and_export step. Cluster stats are read
// from the GCS DEBUG_AUTOSCALING_STATUS KV on the head node; a failed or absent
// read simply omits the cluster gauges, matching Python which passes an empty
// cluster_stats dict when the value is missing.
func (m *ReporterModule) recordMetrics(ctx context.Context, stats map[string]interface{}) {
	var clusterStats map[string]interface{}
	if m.cfg.IsHead {
		clusterStats = m.fetchClusterStats(ctx)
	}

	globalTags := map[string]string{
		"Version":     version.RayVersion,
		"SessionName": m.cfg.SessionName,
	}
	for _, record := range m.records.toRecords(stats, clusterStats, m.cfg.IsHead, globalTags) {
		m.metricsService.RecordGauge(record.Gauge, record.Tags, record.Value)
	}
}

// fetchClusterStats reads and decodes the autoscaler debug status from the GCS
// internal KV store, mirroring the Python _run_loop fetch of
// DEBUG_AUTOSCALING_STATUS. A missing client, a failed read or a malformed value
// yields nil so the caller skips the cluster gauges instead of failing the
// collection cycle.
func (m *ReporterModule) fetchClusterStats(ctx context.Context) map[string]interface{} {
	if m.cfg.GCS == nil {
		return nil
	}
	raw, err := m.cfg.GCS.Get(ctx, "", debugAutoscalingStatus)
	if errors.Is(err, gcs.ErrKeyNotFound) {
		// Only autoscaler v1 publishes DEBUG_AUTOSCALING_STATUS, and a cluster
		// without an autoscaler publishes nothing at all, so a missing key is
		// the ordinary case rather than a failure: the cluster gauges are
		// skipped and the cycle continues. The Python agent reads the same key
		// with a None default, for the same reason.
		log.Log.V(1).Info("no autoscaler status published; cluster metrics are skipped")
		return nil
	}
	if err != nil {
		log.Log.Error(err, "failed to get autoscaling status for cluster metrics")
		return nil
	}
	if len(raw) == 0 {
		return nil
	}
	var clusterStats map[string]interface{}
	if err := json.Unmarshal(raw, &clusterStats); err != nil {
		log.Log.Error(err, "failed to decode autoscaling status for cluster metrics")
		return nil
	}
	return clusterStats
}

// publishOnce collects a full resource snapshot, injects the GPU and TPU
// utilization collected by the dedicated collectors, records the Prometheus
// gauges when metrics collection is enabled, composes the JSON payload and
// publishes it to GCS under the RAY_REPORTER:<node_id> key. Publication is
// decoupled from metrics collection, so it runs regardless of the
// MetricsCollectionDisabled flag, matching the Python agent. A nil publisher
// (e.g. no GCS client configured) degrades gracefully by skipping the publish.
func (m *ReporterModule) publishOnce(ctx context.Context) error {
	stats := m.statsCollector.Collect()
	stats["gpus"] = m.gpuCollector.Collect()
	stats["tpus"] = m.tpuCollector.Collect()
	attachWorkerGPUInfo(stats)

	if !m.cfg.MetricsCollectionDisabled {
		m.recordMetrics(ctx, stats)
		// Drop the metrics of the components that stopped reporting, mirroring
		// the clean_all_dead_worker_metrics call of the Python
		// _async_compose_stats_payload.
		m.metricsService.CleanStaleComponents()
	}

	// The published timestamp is the collection cycle's own instant, not a
	// fresh clock read: Python computes to_posix_time(datetime.utcnow()) once
	// in _async_collect_stats and reuses it for the payload and the speed
	// history, so the reported "now" and the sample timestamps agree.
	payload, err := NewStatsPayloadComposer(stats, asFloat(stats["now"])).Compose()
	if err != nil {
		return err
	}

	if m.publisher == nil {
		return nil
	}
	keyID := ReporterPrefix + m.cfg.NodeID.Hex()
	return m.publisher.PublishNodeResourceUsage(ctx, keyID, payload)
}

// attachWorkerGPUInfo attaches the GPU memory (MB) and utilization (%) of each
// worker process, mirroring the gpu_pid_mapping block of Python
// _async_get_workers: a process that owns memory on several devices has its
// usage summed across them. Both fields are always set, defaulting to zero,
// because the Python ProcessInfo payload always carries them and the node
// detail UI reads them. The gpus slice must already be present in stats.
func attachWorkerGPUInfo(stats map[string]interface{}) {
	gpus, _ := stats["gpus"].([]GpuUtilizationInfo)
	byPID := map[int][]ProcessGPUInfo{}
	for _, gpu := range gpus {
		for pid, info := range gpu.ProcessesPIDs {
			byPID[pid] = append(byPID[pid], info)
		}
	}

	for _, worker := range asMapSlice(stats["workers"]) {
		var memory, utilization float64
		for _, info := range byPID[asInt(worker["pid"])] {
			memory += float64(info.GPUMemoryUsage)
			if info.GPUUtilization != nil {
				utilization += float64(*info.GPUUtilization)
			}
		}
		worker["gpu_memory_usage"] = memory
		worker["gpu_utilization"] = utilization
	}
}

// publishCycle runs one collection-and-publication cycle and reports whether
// the loop should continue. A failed publication is logged but does not
// interrupt the loop, mirroring the Python _run_loop which catches exceptions
// per iteration. Cancellation is re-checked before publishing: when a select
// races between a ready ticker and a cancelled context, Go picks a case
// randomly and could otherwise schedule a publish that must no longer run after
// Close. Re-checking guarantees Close stops the loop without scheduling new
// publications (only an in-flight publish that started before Close may still
// complete).
func (m *ReporterModule) publishCycle(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}
	if err := m.publishOnce(ctx); err != nil {
		log.Log.Error(err, "failed to publish node resource usage")
	}
	return true
}

// collectLoop runs the periodic publication loop until ctx is cancelled.
//
// The loop publishes once before waiting for the first tick, mirroring the
// Python _run_loop whose loop body collects, composes and publishes the payload
// before its trailing asyncio.sleep(REPORTER_UPDATE_INTERVAL_MS / 1000). The
// first publication therefore happens as soon as the agent starts; waiting for
// the first tick instead would leave the head node without node resource usage
// (and without node Prometheus metrics) for a full update interval.
func (m *ReporterModule) collectLoop(ctx context.Context) {
	if !m.publishCycle(ctx) {
		return
	}
	ticker := time.NewTicker(publishInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.publishCycle(ctx) {
				return
			}
		}
	}
}

// Start launches the Prometheus exporter and the periodic collection loop. The
// loop runs in a background goroutine and stops when ctx is cancelled; a derived
// cancel is stored on the module so callers can stop the loop explicitly. An
// exporter that cannot bind its port is logged and skipped: like the Python
// agent, a port conflict must not keep the whole agent from starting.
func (m *ReporterModule) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	if m.exporter != nil {
		if err := m.exporter.Start(ctx, metricsExportHost(m.cfg), m.cfg.MetricsExportPort); err != nil {
			log.Log.Error(err, "failed to start prometheus stats exporter, stats disabled")
		}
	}

	go m.collectLoop(loopCtx)
	return nil
}

// BoundMetricsExportPort reports the port the Prometheus exporter actually
// listens on, or -1 when it is disabled or its bind failed. The agent publishes
// this value in the port-discovery file instead of the configured port, so a
// node never advertises a metrics endpoint nothing answers on.
func (m *ReporterModule) BoundMetricsExportPort() int {
	if m.exporter == nil {
		return -1
	}
	return m.exporter.BoundPort()
}

// Close stops the periodic collection loop and the Prometheus exporter. It is
// idempotent and safe to call when the module was never started (cancel is nil)
// or has already been closed.
func (m *ReporterModule) Close() {
	if m.cancel != nil {
		m.cancel()
	}
	if m.exporter != nil {
		m.exporter.Close()
	}
}

// RegisterGRPC registers the ReporterService servicer on the agent's gRPC
// server and, when the OpenTelemetry metrics backend is enabled, the OTLP
// MetricsService servicer alongside it. This mirrors the Python
// ReporterAgent.run, which registers the ReporterService servicer and then the
// MetricsService servicer under RAY_ENABLE_OPEN_TELEMETRY (the default). The
// MetricsService servicer is what makes the same-node OTLP push path exist:
// without it the raylet, the workers and GCS have nowhere to report.
func (m *ReporterModule) RegisterGRPC(s *grpc.Server) error {
	proto.RegisterReporterServiceServer(s, m.reporterService)
	if openTelemetryEnabled() {
		collectormetricsv1.RegisterMetricsServiceServer(s, m.otelService)
	}
	return nil
}

// RegisterHTTP is a no-op returning nil: the reporter module exposes no HTTP
// endpoints.
func (m *ReporterModule) RegisterHTTP(_ *http.ServeMux) error {
	return nil
}
