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

package head

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/ray-project/ray/go/pkg/version"
)

// TestNewMetricsRegistryRegistersInitAttempts verifies the Go-extension
// ray_dashboard_runtime_init_attempts_total counter is registered and wired
// through DashboardMetrics.recordInitAttempt, so a lazy init outcome can be
// scraped from /metrics even though the Python DashboardPrometheusMetrics has
// no such series.
func TestNewMetricsRegistryRegistersInitAttempts(t *testing.T) {
	reg := NewMetricsRegistry(&HeadConfig{SessionName: "sess"})
	dm := reg.DashboardMetrics()
	dm.recordInitAttempt("success", "sess")
	dm.recordInitAttempt("failure", "sess")
	dm.recordInitAttempt("success", "sess")

	if got := testutil.ToFloat64(dm.initAttempts.WithLabelValues("success", version.RayVersion, "sess", "dashboard")); got != 2.0 {
		t.Fatalf("success attempts = %v, want 2", got)
	}
	if got := testutil.ToFloat64(dm.initAttempts.WithLabelValues("failure", version.RayVersion, "sess", "dashboard")); got != 1.0 {
		t.Fatalf("failure attempts = %v, want 1", got)
	}
	// The registry gathers the counter without error, i.e. it is registered.
	if _, err := reg.Registry.Gather(); err != nil {
		t.Fatalf("gather registry: %v", err)
	}
}
