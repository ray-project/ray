// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agent

import (
	"context"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// configuredGCSClient is a GCSClient whose PublishErrorInfo records
// publications so tests can observe them. It embeds the narrow interface (which
// already requires PublishErrorInfo) and overrides that method.
type configuredGCSClient struct {
	GCSClient
	mu       sync.Mutex
	payloads []gcs.ErrorInfoPayload
}

func (c *configuredGCSClient) PublishErrorInfo(_ context.Context, _ string, payload gcs.ErrorInfoPayload) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.payloads = append(c.payloads, payload)
	return nil
}

func (c *configuredGCSClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.payloads)
}

// TestResolveErrorPublisherUsesConfiguredClient verifies that when the config
// carries a GCS client (which always implements ErrorInfoPublisher through the
// narrow interface), ResolveErrorPublisher returns that exact client without
// consulting the global singleton.
func TestResolveErrorPublisherUsesConfiguredClient(t *testing.T) {
	pub := &configuredGCSClient{}
	got := ResolveErrorPublisher(Config{GCS: pub})
	if got == nil {
		t.Fatal("ResolveErrorPublisher returned nil")
	}
	if got != gcs.ErrorInfoPublisher(pub) {
		t.Fatalf("ResolveErrorPublisher returned %T, want the configured publishing client", got)
	}
	if err := got.PublishErrorInfo(context.Background(), "k", gcs.ErrorInfoPayload{Type: "t"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n := pub.count(); n != 1 {
		t.Errorf("configured client publish count = %d, want 1 (publisher not routed to configured client)", n)
	}
}

// TestResolveErrorPublisherDefaultsToNoopWithoutAnyClient verifies the no-op
// fallback when the config has no GCS client and no singleton is registered.
func TestResolveErrorPublisherDefaultsToNoopWithoutAnyClient(t *testing.T) {
	gcs.ClearClient()
	defer gcs.ClearClient()

	got := ResolveErrorPublisher(Config{})
	if got == nil {
		t.Fatal("ResolveErrorPublisher returned nil")
	}
	if _, ok := got.(noopErrorPublisher); !ok {
		t.Errorf("ResolveErrorPublisher returned %T, want noopErrorPublisher", got)
	}
}
