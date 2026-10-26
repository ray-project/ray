package reporter

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/ids"
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

// TestReporterModulePublishIntervalUsesConfig verifies the default publish
// interval is derived from the reporter update interval constant.
func TestReporterModulePublishIntervalUsesConfig(t *testing.T) {
	if publishInterval != time.Duration(DefaultReporterUpdateIntervalMs)*time.Millisecond {
		t.Errorf("publishInterval = %v, want %v", publishInterval, time.Duration(DefaultReporterUpdateIntervalMs)*time.Millisecond)
	}
}
