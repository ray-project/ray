package reporter

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// publishingClient is a gcs.Client that also implements
// gcs.NodeResourceUsagePublisher, delegating publications to an inner
// publisher so tests can observe them.
type publishingClient struct {
	gcs.Client
	inner gcs.NodeResourceUsagePublisher
}

func (c *publishingClient) PublishNodeResourceUsage(ctx context.Context, keyID, jsonPayload string) error {
	return c.inner.PublishNodeResourceUsage(ctx, keyID, jsonPayload)
}

// nonPublishingClient is a gcs.Client that does NOT implement
// gcs.NodeResourceUsagePublisher. It embeds the interface (per the pattern in
// agent_test.go) so it satisfies gcs.Client while lacking the publishing method,
// which lets resolvePublisher fall through to the singleton/no-op branches.
type nonPublishingClient struct {
	gcs.Client
}

// TestResolvePublisherUsesConfiguredClient verifies that when the config carries
// a GCS client that also implements NodeResourceUsagePublisher, resolvePublisher
// returns that client's publisher without consulting the global singleton.
func TestResolvePublisherUsesConfiguredClient(t *testing.T) {
	inner := newMockPublisher()
	cfg := agent.Config{GCS: &publishingClient{inner: inner}}
	got := resolvePublisher(cfg)
	if got == nil {
		t.Fatal("resolvePublisher returned nil")
	}
	if err := got.PublishNodeResourceUsage(context.Background(), "k", "v"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if inner.count() != 1 {
		t.Errorf("inner publish count = %d, want 1 (publisher not routed to configured client)", inner.count())
	}
}

// TestResolvePublisherFallsBackToSingleton verifies that when the configured
// client is not a NodeResourceUsagePublisher but the global singleton is,
// resolvePublisher returns the singleton's publisher.
func TestResolvePublisherFallsBackToSingleton(t *testing.T) {
	inner := newMockPublisher()
	gcs.ClearClient()
	defer gcs.ClearClient()
	gcs.SetClient(&publishingClient{inner: inner})

	got := resolvePublisher(agent.Config{GCS: &nonPublishingClient{}})
	if got == nil {
		t.Fatal("resolvePublisher returned nil")
	}
	if err := got.PublishNodeResourceUsage(context.Background(), "k", "v"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if inner.count() != 1 {
		t.Errorf("singleton publish count = %d, want 1 (publisher not routed to singleton)", inner.count())
	}
}

// TestResolvePublisherFallsBackToSingletonWhenNoConfiguredClient verifies the
// singleton is used when the config has no GCS client at all.
func TestResolvePublisherFallsBackToSingletonWhenNoConfiguredClient(t *testing.T) {
	inner := newMockPublisher()
	gcs.ClearClient()
	defer gcs.ClearClient()
	gcs.SetClient(&publishingClient{inner: inner})

	got := resolvePublisher(agent.Config{})
	if got == nil {
		t.Fatal("resolvePublisher returned nil")
	}
	if err := got.PublishNodeResourceUsage(context.Background(), "k", "v"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if inner.count() != 1 {
		t.Errorf("singleton publish count = %d, want 1", inner.count())
	}
}

// TestResolvePublisherDefaultsToNoop verifies that when neither the configured
// client nor the global singleton provides a NodeResourceUsagePublisher,
// resolvePublisher returns the noop publisher.
func TestResolvePublisherDefaultsToNoop(t *testing.T) {
	gcs.ClearClient()
	defer gcs.ClearClient()

	got := resolvePublisher(agent.Config{GCS: &nonPublishingClient{}})
	if got == nil {
		t.Fatal("resolvePublisher returned nil")
	}
	if _, ok := got.(noopPublisher); !ok {
		t.Errorf("resolvePublisher returned %T, want noopPublisher", got)
	}
}

// TestResolvePublisherDefaultsToNoopWithoutAnyClient verifies the noop fallback
// when the config has no GCS client and no singleton is registered.
func TestResolvePublisherDefaultsToNoopWithoutAnyClient(t *testing.T) {
	gcs.ClearClient()
	defer gcs.ClearClient()

	got := resolvePublisher(agent.Config{})
	if got == nil {
		t.Fatal("resolvePublisher returned nil")
	}
	if _, ok := got.(noopPublisher); !ok {
		t.Errorf("resolvePublisher returned %T, want noopPublisher", got)
	}
}

// logCaptureSink is a minimal logr.LogSink recording the most recent message so
// tests can assert on emitted log lines (mirrors the capture sink used by the
// head package tests).
type logCaptureSink struct {
	msg string
}

func (c *logCaptureSink) Init(logr.RuntimeInfo)                       {}
func (c *logCaptureSink) Enabled(int) bool                            { return true }
func (c *logCaptureSink) Info(_ int, msg string, _ ...interface{})    { c.msg = msg }
func (c *logCaptureSink) Error(_ error, msg string, _ ...interface{}) { c.msg = msg }
func (c *logCaptureSink) WithValues(...interface{}) logr.LogSink      { return c }
func (c *logCaptureSink) WithName(string) logr.LogSink                { return c }
func (c *logCaptureSink) V(int) logr.LogSink                          { return c }

// TestResolvePublisherNoopFallbackLogsWarning verifies the no-op fallback is
// announced with a log line, so a missing GCS publisher configuration is
// discoverable instead of node resource usage publications being dropped
// silently.
func TestResolvePublisherNoopFallbackLogsWarning(t *testing.T) {
	gcs.ClearClient()
	defer gcs.ClearClient()

	sink := &logCaptureSink{}
	log.SetLogger(logr.New(sink))
	defer log.SetLogger(logr.New(&log.NullLogSink{}))

	got := resolvePublisher(agent.Config{})
	if got == nil {
		t.Fatal("resolvePublisher returned nil")
	}
	if _, ok := got.(noopPublisher); !ok {
		t.Fatalf("resolvePublisher returned %T, want noopPublisher", got)
	}
	if sink.msg != "no GCS publisher configured; node resource usage publications are dropped" {
		t.Errorf("fallback log message = %q, want the publisher-dropped warning", sink.msg)
	}
}
