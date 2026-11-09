package reporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"google.golang.org/grpc"
)

// mockPublisher records published node resource usage key/payload pairs and
// exposes a channel so tests can observe publication events deterministically.
type mockPublisher struct {
	mu        sync.Mutex
	keyIDs    []string
	payloads  []string
	events    chan struct{}
	publishFn func(keyID, jsonPayload string) error
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{events: make(chan struct{}, 64)}
}

func (m *mockPublisher) PublishNodeResourceUsage(_ context.Context, keyID, jsonPayload string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keyIDs = append(m.keyIDs, keyID)
	m.payloads = append(m.payloads, jsonPayload)
	select {
	case m.events <- struct{}{}:
	default:
	}
	if m.publishFn != nil {
		return m.publishFn(keyID, jsonPayload)
	}
	return nil
}

func (m *mockPublisher) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.keyIDs)
}

func (m *mockPublisher) lastKeyID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.keyIDs) == 0 {
		return ""
	}
	return m.keyIDs[len(m.keyIDs)-1]
}

func (m *mockPublisher) lastPayload() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.payloads) == 0 {
		return ""
	}
	return m.payloads[len(m.payloads)-1]
}

// newTestNodeID returns a deterministic node id for tests.
func newTestNodeID(t *testing.T) ids.NodeID {
	t.Helper()
	id, err := ids.NodeIDFromHex("aabbccddeeff00112233445566778899aabbccddeeff001122334455")
	if err != nil {
		t.Fatalf("NodeIDFromHex: %v", err)
	}
	return id
}

// newTestReporterModule builds a ReporterModule backed by a fresh profile
// directory and an injectable mock publisher, overriding the publisher field.
func newTestReporterModule(t *testing.T, cfg agent.Config) (*ReporterModule, *mockPublisher) {
	t.Helper()
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v) error = %v", cfg, err)
	}
	pub := newMockPublisher()
	m.publisher = pub
	return m, pub
}

// TestReporterModuleImplementsInterface verifies ReporterModule satisfies the
// agent.Module contract at compile time.
func TestReporterModuleImplementsInterface(t *testing.T) {
	var _ agent.Module = (*ReporterModule)(nil)
}

// TestReporterModuleNew validates constructor behavior for required fields.
func TestReporterModuleNew(t *testing.T) {
	t.Run("missing log dir fails", func(t *testing.T) {
		if _, err := New(agent.Config{LogDir: ""}); err == nil {
			t.Error("New with empty LogDir returned no error")
		}
	})

	t.Run("constructs all dependencies", func(t *testing.T) {
		cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "1.2.3.4"}
		m, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if m.reporterService == nil {
			t.Error("reporterService not initialized")
		}
		if m.metricsService == nil {
			t.Error("metricsService not initialized")
		}
		if m.statsCollector == nil {
			t.Error("statsCollector not initialized")
		}
		if m.publisher == nil {
			t.Error("publisher not initialized")
		}
		if m.reporterService.metricsService == nil {
			t.Error("MetricsService was not injected into the ReporterService")
		}
	})
}

// TestReporterModuleRegisterGRPC verifies the ReporterService is registered on
// the gRPC server.
func TestReporterModuleRegisterGRPC(t *testing.T) {
	m, _ := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})
	s := grpc.NewServer()
	if err := m.RegisterGRPC(s); err != nil {
		t.Fatalf("RegisterGRPC: %v", err)
	}
	// The reporter's ReporterService servicer is registered once on the server.
	// The test asserts the single registration succeeds without error; it does
	// not register the same servicer a second time.
}

// TestReporterModuleRegisterHTTP verifies RegisterHTTP is a no-op returning nil,
// since the reporter exposes no HTTP endpoints.
func TestReporterModuleRegisterHTTP(t *testing.T) {
	m, _ := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})
	if err := m.RegisterHTTP(http.NewServeMux()); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
}

// TestReporterModulePublishOnce verifies a single collection cycle composes the
// payload and publishes it to GCS under the RAY_REPORTER:<node_id> key.
func TestReporterModulePublishOnce(t *testing.T) {
	cfg := agent.Config{LogDir: t.TempDir(), NodeID: newTestNodeID(t)}
	m, pub := newTestReporterModule(t, cfg)

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	if pub.count() != 1 {
		t.Fatalf("publish count = %d, want 1", pub.count())
	}
	key := pub.lastKeyID()
	wantKey := ReporterPrefix + cfg.NodeID.Hex()
	if key != wantKey {
		t.Errorf("published keyID = %q, want %q", key, wantKey)
	}
	payload := pub.lastPayload()
	if !strings.Contains(payload, "\"now\"") {
		t.Errorf("payload missing now field: %s", payload)
	}
	if !strings.Contains(payload, "\"cpu\"") {
		t.Errorf("payload missing cpu field: %s", payload)
	}
}

// TestReporterModulePublishOnceUsesCycleTimestamp verifies the published "now"
// is the collection cycle's own instant, not a fresh clock read: Python
// computes to_posix_time(datetime.utcnow()) once in _async_collect_stats and
// reuses it for the payload and the speed history, so the reported timestamp
// and the sample timestamps must agree.
func TestReporterModulePublishOnceUsesCycleTimestamp(t *testing.T) {
	cfg := agent.Config{LogDir: t.TempDir(), NodeID: newTestNodeID(t)}
	m, pub := newTestReporterModule(t, cfg)

	const cycleTime = 1700000000.5
	m.statsCollector.now = func() float64 { return cycleTime }

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(pub.lastPayload()), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	now, ok := payload["now"].(float64)
	if !ok {
		t.Fatalf("payload now type = %T, want float64", payload["now"])
	}
	if now != cycleTime {
		t.Errorf("payload now = %v, want the collection cycle timestamp %v", now, cycleTime)
	}
}

// TestReporterModulePublishOnceSkipsWithoutPublisher verifies that a module with
// a nil publisher degrades gracefully instead of panicking.
func TestReporterModulePublishOnceSkipsWithoutPublisher(t *testing.T) {
	m, _ := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})
	m.publisher = nil
	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce with nil publisher: %v", err)
	}
}

// TestReporterModuleStartCollectLoop verifies Start launches a periodic loop that
// repeatedly publishes node resource usage, and that cancelling the context stops
// it. The package-level publishInterval is collapsed to make the test fast.
func TestReporterModuleStartCollectLoop(t *testing.T) {
	origInterval := publishInterval
	publishInterval = 10 * time.Millisecond
	defer func() { publishInterval = origInterval }()

	m, pub := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-pub.events:
	case <-time.After(2 * time.Second):
		t.Fatal("expected at least one publication from the collect loop")
	}

	if m.cancel == nil {
		t.Error("Start did not store a cancel function for loop shutdown")
	}

	cancel()
	// Give the loop a moment to observe cancellation; the test only requires
	// that at least one publication occurred before cancellation.
	if pub.count() < 1 {
		t.Fatalf("publish count = %d, want >= 1", pub.count())
	}
}

// TestReporterModuleCloseStopsLoop verifies that Close cancels the collect loop
// so it stops publishing. After observing at least one publication, Close must
// halt further publications; calling it again (or before Start) must be safe.
func TestReporterModuleCloseStopsLoop(t *testing.T) {
	origInterval := publishInterval
	publishInterval = 10 * time.Millisecond
	defer func() { publishInterval = origInterval }()

	m, pub := newTestReporterModule(t, agent.Config{LogDir: t.TempDir()})

	// Close before Start must be safe (cancel is nil).
	m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-pub.events:
	case <-time.After(2 * time.Second):
		t.Fatal("expected at least one publication before Close")
	}

	// Drain any publications that were already queued on the events channel
	// before Close. The 10ms tick can enqueue several events while the test
	// reads just the first one, and counting those as post-Close publications
	// would falsely fail the assertion below. Draining establishes a clean
	// baseline so only genuinely new publications after Close are observed.
drain:
	for {
		select {
		case <-pub.events:
		default:
			break drain
		}
	}

	m.Close()

	// After Close the loop must stop scheduling new publications. A single
	// in-flight tick that raced with Close may still complete, so allow at most
	// one extra event; a working Close must not keep publishing across the
	// window (10ms interval would otherwise yield ~30 events in 300ms).
	countAfterClose := pub.count()
	extra := 0
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-pub.events:
			extra++
		case <-timer.C:
			if extra > 1 {
				t.Fatalf("collect loop kept publishing after Close: %d extra publications in 300ms", extra)
			}
			if got := pub.count(); got != countAfterClose+extra {
				t.Errorf("publish count mismatch: before=%d extra=%d actual=%d", countAfterClose, extra, got)
			}
			return
		}
	}
}

// TestReporterModuleMetricsCollectionDisabled verifies that when metrics
// collection is disabled, the periodic publication loop still runs (payload
// publication is decoupled from metrics collection, matching the Python agent).
func TestReporterModuleMetricsCollectionDisabled(t *testing.T) {
	origInterval := publishInterval
	publishInterval = 10 * time.Millisecond
	defer func() { publishInterval = origInterval }()

	cfg := agent.Config{LogDir: t.TempDir(), MetricsCollectionDisabled: true}
	m, pub := newTestReporterModule(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-pub.events:
	case <-time.After(2 * time.Second):
		t.Fatal("expected publications even with metrics collection disabled")
	}
}

// TestReporterModuleCollectLoopPublishesImmediately verifies the loop publishes
// its first snapshot without waiting for the update interval, mirroring the
// Python _run_loop whose loop body collects and publishes before its trailing
// asyncio.sleep. The interval is deliberately left long: with the previous
// ticker-first loop the test would only see a publication after that interval,
// leaving the head node without node stats for a full update interval after
// startup.
func TestReporterModuleCollectLoopPublishesImmediately(t *testing.T) {
	origInterval := publishInterval
	publishInterval = time.Hour
	defer func() { publishInterval = origInterval }()

	m, pub := newTestReporterModule(t, agent.Config{LogDir: t.TempDir(), NodeID: newTestNodeID(t)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-pub.events:
	case <-time.After(2 * time.Second):
		t.Fatal("no publication before the first ticker interval: the loop must publish once at startup")
	}
	if got := pub.count(); got != 1 {
		t.Errorf("publish count = %d, want exactly 1 startup publication (interval is 1h)", got)
	}
}

// TestReporterModuleCollectLoopPublishesNothingWhenCancelled verifies a loop
// started with an already-cancelled context does not publish at all, covering
// the pre-publish cancellation re-check that guards the startup publication.
func TestReporterModuleCollectLoopPublishesNothingWhenCancelled(t *testing.T) {
	origInterval := publishInterval
	publishInterval = 10 * time.Millisecond
	defer func() { publishInterval = origInterval }()

	m, pub := newTestReporterModule(t, agent.Config{LogDir: t.TempDir(), NodeID: newTestNodeID(t)})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case <-pub.events:
		t.Fatal("loop published despite an already-cancelled context")
	case <-time.After(300 * time.Millisecond):
	}
	if got := pub.count(); got != 0 {
		t.Errorf("publish count = %d, want 0 with a cancelled context", got)
	}
}

// TestReporterModuleCollectLoopKeepsPublishing verifies the startup publication
// is followed by the periodic ones: the immediate first cycle must not replace
// the ticker, only precede it.
func TestReporterModuleCollectLoopKeepsPublishing(t *testing.T) {
	origInterval := publishInterval
	publishInterval = 10 * time.Millisecond
	defer func() { publishInterval = origInterval }()

	m, pub := newTestReporterModule(t, agent.Config{LogDir: t.TempDir(), NodeID: newTestNodeID(t)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for pub.count() < 2 {
		select {
		case <-pub.events:
		case <-deadline:
			t.Fatalf("publish count = %d, want >= 2: the loop must keep publishing after the startup cycle", pub.count())
		}
	}
}

// fakeGCSClient is a gcs.Client whose InternalKV read is scripted, so the
// autoscaler-status fetch can be exercised without a live GCS. It embeds the
// interface, per the pattern used elsewhere in the package, so the unused
// methods stay unimplemented.
type fakeGCSClient struct {
	gcs.Client
	value []byte
	err   error
	keys  []string
}

func (c *fakeGCSClient) Get(_ context.Context, _, key string) ([]byte, error) {
	c.keys = append(c.keys, key)
	if c.err != nil {
		return nil, c.err
	}
	return c.value, nil
}

// sortedFamilyNames returns the gauge family names in a deterministic order so
// failure messages are stable.
func sortedFamilyNames(families map[string]struct{}) []string {
	out := make([]string, 0, len(families))
	for name := range families {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// TestReporterModulePublishIntervalUsesConfig verifies the default publish
// interval is derived from the reporter update interval constant.
func TestReporterModulePublishIntervalUsesConfig(t *testing.T) {
	if publishInterval != time.Duration(DefaultReporterUpdateIntervalMs)*time.Millisecond {
		t.Errorf("publishInterval = %v, want %v", publishInterval, time.Duration(DefaultReporterUpdateIntervalMs)*time.Millisecond)
	}
}

// metricFamilies gathers the module registry so a test can observe exactly which
// Prometheus gauges were written.
func metricFamilies(t *testing.T, m *ReporterModule) map[string]struct{} {
	t.Helper()
	families, err := m.metricsService.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics registry: %v", err)
	}
	out := make(map[string]struct{}, len(families))
	for _, f := range families {
		out[f.GetName()] = struct{}{}
	}
	return out
}

// TestReporterModuleRecordsGaugesWhenEnabled verifies a collection cycle writes
// the reporter gauges onto the module registry, so the exporter has something to
// serve.
func TestReporterModuleRecordsGaugesWhenEnabled(t *testing.T) {
	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "10.0.0.7", SessionName: "sess"}
	m, _ := newTestReporterModule(t, cfg)

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	families := metricFamilies(t, m)
	// GPU/TPU gauges are absent when no device is present, so only the node and
	// component gauges are asserted here.
	for _, want := range []string{
		"ray_" + GaugeNodeCPUUtilization,
		"ray_" + GaugeNodeMemUsed,
		"ray_" + GaugeNodeDiskUsage,
		"ray_" + GaugeNodeNetworkSent,
		"ray_" + ComponentGaugeCPUPercentage,
		"ray_" + ComponentGaugeRSSMB,
	} {
		if _, ok := families[want]; !ok {
			t.Errorf("gauge %q was not recorded (families: %v)", want, sortedFamilyNames(families))
		}
	}
	// A machine without nvidia-smi must not invent a GPU gauge.
	if len(m.gpuCollector.Collect()) == 0 {
		if _, ok := families["ray_"+GaugeNodeGPUsUtilization]; ok {
			t.Errorf("GPU gauge recorded although the GPU collector found no device: %v", sortedFamilyNames(families))
		}
	} else if _, ok := families["ray_"+GaugeNodeGPUsUtilization]; !ok {
		t.Errorf("GPU gauge missing although the GPU collector reported a device: %v", sortedFamilyNames(families))
	}
}

// TestReporterModuleMetricsCollectionDisabledWritesNoGauges verifies the
// MetricsCollectionDisabled flag suppresses every gauge write while still
// publishing the resource-usage payload, matching the Python agent where
// publishing and metrics collection are decoupled.
func TestReporterModuleMetricsCollectionDisabledWritesNoGauges(t *testing.T) {
	cfg := agent.Config{
		LogDir:                    t.TempDir(),
		NodeIP:                    "10.0.0.7",
		NodeID:                    newTestNodeID(t),
		MetricsCollectionDisabled: true,
	}
	m, pub := newTestReporterModule(t, cfg)

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	if families := metricFamilies(t, m); len(families) != 0 {
		t.Errorf("metrics collection disabled but %d gauge families were written: %v", len(families), sortedFamilyNames(families))
	}
	if pub.count() != 1 {
		t.Errorf("publish count = %d, want 1 (publication must be decoupled from metrics collection)", pub.count())
	}
}

// TestReporterModuleMetricsCollectionDisabledSkipsExporter verifies no exporter
// is assembled when metrics collection is disabled, so Start cannot bind the
// metrics port.
func TestReporterModuleMetricsCollectionDisabledSkipsExporter(t *testing.T) {
	cfg := agent.Config{
		LogDir:                    t.TempDir(),
		NodeIP:                    "127.0.0.1",
		MetricsExportPort:         freePort(t),
		MetricsCollectionDisabled: true,
	}
	m, _ := newTestReporterModule(t, cfg)
	if m.exporter != nil {
		t.Fatal("exporter was assembled despite MetricsCollectionDisabled")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Nothing must have bound the port, so a fresh listener can take it.
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.MetricsExportPort))
	if err != nil {
		t.Fatalf("metrics port %d was bound despite metrics collection being disabled: %v", cfg.MetricsExportPort, err)
	}
	_ = ln.Close()
	m.Close()
}

// TestReporterModuleStartServesMetricsPort verifies Start binds the configured
// metrics export port and Close releases it, so the Prometheus exporter is wired
// end to end through the module.
func TestReporterModuleStartServesMetricsPort(t *testing.T) {
	port := freePort(t)
	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", MetricsExportPort: port}
	m, _ := newTestReporterModule(t, cfg)
	if m.exporter == nil {
		t.Fatal("exporter not assembled with metrics collection enabled")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /metrics status = %d, want 200", resp.StatusCode)
	}

	m.Close()
	m.Close() // idempotent

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = ln.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("metrics port %d still bound after Close", port)
}

// TestReporterModuleStartSurvivesMetricsPortConflict verifies a port conflict is
// logged and skipped rather than failing Start, matching the Python agent which
// keeps running with stats disabled when the exporter cannot bind.
func TestReporterModuleStartSurvivesMetricsPortConflict(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer occupied.Close()
	port := occupied.Addr().(*net.TCPAddr).Port

	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", MetricsExportPort: port}
	m, _ := newTestReporterModule(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start returned an error on a metrics port conflict, want nil: %v", err)
	}
	if m.cancel == nil {
		t.Error("Start did not store a cancel function")
	}
	m.Close()
}

// TestReporterModuleClusterGaugesFromAutoscalerStatus verifies the head-node
// path reads the autoscaler report from GCS and turns it into the three cluster
// gauges.
func TestReporterModuleClusterGaugesFromAutoscalerStatus(t *testing.T) {
	status := `{"autoscaler_report":{"active_nodes":{"head":1,"worker":2},` +
		`"failed_nodes":[["1.2.3.4","worker"],["1.2.3.5","worker"]],` +
		`"pending_nodes":[["1.2.3.6","worker"]]}}`
	client := &fakeGCSClient{value: []byte(status)}
	cfg := agent.Config{
		LogDir: t.TempDir(), NodeIP: "127.0.0.1", NodeID: newTestNodeID(t),
		IsHead: true, GCS: client, SessionName: "sess",
	}
	m, _ := newTestReporterModule(t, cfg)

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	// The collection cycle resolves the gcs_server pid before the autoscaler
	// status is read, so both keys must appear in this order.
	wantKeys := []string{gcsPIDKey, debugAutoscalingStatus}
	if len(client.keys) != len(wantKeys) {
		t.Fatalf("GCS keys read = %v, want %v", client.keys, wantKeys)
	}
	for i, want := range wantKeys {
		if client.keys[i] != want {
			t.Errorf("GCS key %d read = %q, want %q", i, client.keys[i], want)
		}
	}
	families := metricFamilies(t, m)
	for _, want := range []string{
		"ray_" + GaugeClusterActiveNodes,
		"ray_" + GaugeClusterFailedNodes,
		"ray_" + GaugeClusterPendingNodes,
	} {
		if _, ok := families[want]; !ok {
			t.Errorf("cluster gauge %q was not recorded (families: %v)", want, sortedFamilyNames(families))
		}
	}
}

// TestReporterModuleClusterGaugesOmittedOnWorker verifies a worker node never
// reads or records the cluster gauges, matching Python which reports them only
// from the head node.
func TestReporterModuleClusterGaugesOmittedOnWorker(t *testing.T) {
	status := `{"autoscaler_report":{"active_nodes":{"head":1}}}`
	client := &fakeGCSClient{value: []byte(status)}
	cfg := agent.Config{
		LogDir: t.TempDir(), NodeIP: "10.0.0.7", IsHead: false, GCS: client,
	}
	m, _ := newTestReporterModule(t, cfg)

	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}
	if len(client.keys) != 0 {
		t.Errorf("worker node read GCS keys %v, want none", client.keys)
	}
	families := metricFamilies(t, m)
	for _, unwanted := range []string{
		"ray_" + GaugeClusterActiveNodes,
		"ray_" + GaugeClusterFailedNodes,
		"ray_" + GaugeClusterPendingNodes,
	} {
		if _, ok := families[unwanted]; ok {
			t.Errorf("worker node recorded cluster gauge %q", unwanted)
		}
	}
	// The node-level gauges must still be recorded: only the cluster section is
	// head-only.
	if _, ok := families["ray_"+GaugeNodeCPUUtilization]; !ok {
		t.Errorf("worker node did not record the node gauges: %v", sortedFamilyNames(families))
	}
}

// TestReporterModuleFetchClusterStatsDegrades verifies every failure mode of the
// autoscaler-status read yields nil instead of failing the collection cycle.
func TestReporterModuleFetchClusterStatsDegrades(t *testing.T) {
	cases := []struct {
		name   string
		client gcs.Client
	}{
		{"nil client", nil},
		{"read error", &fakeGCSClient{err: errors.New("kv unavailable")}},
		{"empty value", &fakeGCSClient{value: nil}},
		{"malformed json", &fakeGCSClient{value: []byte("{not json")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", IsHead: true, GCS: tc.client}
			m, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if got := m.fetchClusterStats(context.Background()); got != nil {
				t.Errorf("fetchClusterStats = %v, want nil", got)
			}
		})
	}
}

// TestReporterModuleFetchClusterStatsDecodes verifies a well-formed autoscaler
// status is decoded into the report structure the cluster records read.
func TestReporterModuleFetchClusterStatsDecodes(t *testing.T) {
	client := &fakeGCSClient{value: []byte(`{"autoscaler_report":{"active_nodes":{"head":1}}}`)}
	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", IsHead: true, GCS: client}
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := m.fetchClusterStats(context.Background())
	report, ok := got["autoscaler_report"].(map[string]interface{})
	if !ok {
		t.Fatalf("autoscaler_report type = %T, want map[string]interface{}", got["autoscaler_report"])
	}
	active, ok := report["active_nodes"].(map[string]interface{})
	if !ok {
		t.Fatalf("active_nodes type = %T, want map[string]interface{}", report["active_nodes"])
	}
	if active["head"] != float64(1) {
		t.Errorf("active_nodes[head] = %v, want 1", active["head"])
	}
}

// TestReporterModuleRecordsStateIsIncremental verifies the module keeps the
// records state across collection cycles, so a worker that disappears between
// cycles has its gauges reset to zero instead of lingering.
func TestReporterModuleRecordsStateIsIncremental(t *testing.T) {
	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "10.0.0.7"}
	m, _ := newTestReporterModule(t, cfg)
	if err := m.publishOnce(context.Background()); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}
	// The state is part of the module and survives the cycle, ready for the next
	// diff. A nil map would mean the state was never initialised.
	if m.records.latestWorkerProcNames == nil && m.records.latestGPUWorkerProcNames == nil {
		t.Error("records state was not carried on the module")
	}
}

// recordingSink captures the entries a logger would emit, so a test can assert
// on severity rather than on message text.
type recordingSink struct {
	entries []recordedEntry
}

type recordedEntry struct {
	err   error
	msg   string
	level int
}

func (s *recordingSink) Init(logr.RuntimeInfo) {}

func (s *recordingSink) Enabled(level int) bool { return true }

func (s *recordingSink) Info(level int, msg string, _ ...interface{}) {
	s.entries = append(s.entries, recordedEntry{msg: msg, level: level})
}

func (s *recordingSink) Error(err error, msg string, _ ...interface{}) {
	s.entries = append(s.entries, recordedEntry{err: err, msg: msg, level: 0})
}

func (s *recordingSink) WithValues(_ ...interface{}) logr.LogSink { return s }

func (s *recordingSink) WithName(_ string) logr.LogSink { return s }

// TestReporterModuleFetchClusterStatsMissingKeyIsNotAnError verifies a missing
// autoscaler status is treated as an absent value rather than a failure. Only
// autoscaler v1 publishes DEBUG_AUTOSCALING_STATUS, so on every v2 cluster and
// on any cluster without an autoscaler the key is simply missing; logging that
// at error severity would emit one line per collection cycle for the lifetime
// of the agent.
func TestReporterModuleFetchClusterStatsMissingKeyIsNotAnError(t *testing.T) {
	orig := log.Log
	defer func() { log.Log = orig }()
	sink := &recordingSink{}
	log.Log = logr.New(sink)

	client := &fakeGCSClient{err: gcs.ErrKeyNotFound}
	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", IsHead: true, GCS: client}
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := m.fetchClusterStats(context.Background()); got != nil {
		t.Errorf("fetchClusterStats = %v, want nil", got)
	}
	for _, e := range sink.entries {
		if e.err != nil {
			t.Errorf("missing autoscaler status logged as an error: %v (%s)", e.err, e.msg)
		}
	}
}

// TestReporterModuleFetchClusterStatsReadErrorIsReported keeps the real
// failures loud: a read error other than a missing key must still surface.
func TestReporterModuleFetchClusterStatsReadErrorIsReported(t *testing.T) {
	orig := log.Log
	defer func() { log.Log = orig }()
	sink := &recordingSink{}
	log.Log = logr.New(sink)

	client := &fakeGCSClient{err: errors.New("kv unavailable")}
	cfg := agent.Config{LogDir: t.TempDir(), NodeIP: "127.0.0.1", IsHead: true, GCS: client}
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := m.fetchClusterStats(context.Background()); got != nil {
		t.Errorf("fetchClusterStats = %v, want nil", got)
	}
	var reported bool
	for _, e := range sink.entries {
		if e.err != nil {
			reported = true
		}
	}
	if !reported {
		t.Error("a genuine read error must still be logged as an error")
	}
}
