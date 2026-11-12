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

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// noopErrorPublisher drops error publications so the watcher's publisher stays
// non-nil while remaining a harmless no-op at runtime; the fallback itself is
// announced by ResolveErrorPublisher so the missing configuration is
// discoverable.
type noopErrorPublisher struct{}

func (noopErrorPublisher) PublishErrorInfo(context.Context, string, gcs.ErrorInfoPayload) error {
	return nil
}

// ResolveErrorPublisher picks the publisher the parent watcher publishes
// raylet death errors with. It prefers the explicitly-configured GCS client on
// the config, falls back to the package-level singleton, and finally to a
// no-op publisher so the watcher always has a non-nil publisher. A client is
// only used when it also implements gcs.ErrorInfoPublisher (an independent
// interface, not part of gcs.Client), so clients without error publishing
// still degrade to the no-op instead of failing.
func ResolveErrorPublisher(cfg Config) gcs.ErrorInfoPublisher {
	if p, ok := asErrorInfoPublisher(cfg.GCS); ok {
		return p
	}
	if c, err := gcs.GetClient(); err == nil && c != nil {
		if p, ok := asErrorInfoPublisher(c); ok {
			return p
		}
	}
	// Surface the degraded resolution: without a publisher a raylet death is
	// visible in the agent log but never reaches a driver, so the
	// misconfiguration must not stay silent.
	log.Log.Error(nil, "no GCS error publisher configured; raylet death errors are not published to drivers")
	return noopErrorPublisher{}
}

// asErrorInfoPublisher reports whether client implements the
// gcs.ErrorInfoPublisher interface and returns it when it does.
func asErrorInfoPublisher(client gcs.Client) (gcs.ErrorInfoPublisher, bool) {
	if client == nil {
		return nil, false
	}
	p, ok := client.(gcs.ErrorInfoPublisher)
	return p, ok
}
