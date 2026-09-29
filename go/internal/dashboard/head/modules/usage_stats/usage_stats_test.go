// Copyright 2025 The Ray Authors.
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

package usage_stats

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
)

func serve(t *testing.T, u *UsageStatsHead, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := u.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func TestUsageStatsHeadName(t *testing.T) {
	u := New(&head.HeadConfig{}, nil)
	if u.Name() != "UsageStatsHead" {
		t.Fatalf("name = %q", u.Name())
	}
	if !u.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := u.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUsageStatsEnabledDefault(t *testing.T) {
	os.Unsetenv(usageStatsEnabledEnvVar)
	os.Unsetenv(usageStatsPromptEnabledEnvVar)
	u := New(&head.HeadConfig{}, nil)
	rr := serve(t, u, "/usage_stats_enabled")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			UsageStatsEnabled       bool `json:"usageStatsEnabled"`
			UsageStatsPromptEnabled bool `json:"usageStatsPromptEnabled"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Data.UsageStatsEnabled {
		t.Fatal("usage_stats_enabled should default to true")
	}
	if !body.Data.UsageStatsPromptEnabled {
		t.Fatal("usage_stats_prompt_enabled should default to true")
	}
}

func TestUsageStatsEnabledDisabled(t *testing.T) {
	os.Setenv(usageStatsEnabledEnvVar, "0")
	defer os.Unsetenv(usageStatsEnabledEnvVar)
	u := New(&head.HeadConfig{}, nil)
	rr := serve(t, u, "/usage_stats_enabled")
	var body struct {
		Data struct {
			UsageStatsEnabled bool `json:"usageStatsEnabled"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.UsageStatsEnabled {
		t.Fatal("usage_stats_enabled should be false when RAY_USAGE_STATS_ENABLED=0")
	}
}

func TestUsageStatsEnabledViaConfigFile(t *testing.T) {
	os.Unsetenv(usageStatsEnabledEnvVar)
	dir := t.TempDir()
	path := dir + "/config.json"
	t.Setenv(usageStatsConfigPathEnvVar, path)

	// usage_stats=false in the config file disables stats.
	if err := os.WriteFile(path, []byte(`{"usage_stats": false}`), 0644); err != nil {
		t.Fatal(err)
	}
	u := New(&head.HeadConfig{}, nil)
	rr := serve(t, u, "/usage_stats_enabled")
	var body struct {
		Data struct {
			UsageStatsEnabled bool `json:"usageStatsEnabled"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.UsageStatsEnabled {
		t.Fatal("usage_stats_enabled should be false when config usage_stats=false")
	}

	// usage_stats=true enables stats.
	if err := os.WriteFile(path, []byte(`{"usage_stats": true}`), 0644); err != nil {
		t.Fatal(err)
	}
	rr = serve(t, u, "/usage_stats_enabled")
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Data.UsageStatsEnabled {
		t.Fatal("usage_stats_enabled should be true when config usage_stats=true")
	}

	// Env var takes priority over the config file.
	os.Setenv(usageStatsEnabledEnvVar, "0")
	defer os.Unsetenv(usageStatsEnabledEnvVar)
	rr = serve(t, u, "/usage_stats_enabled")
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.UsageStatsEnabled {
		t.Fatal("usage_stats_enabled should be false when env overrides config")
	}
}

func TestClusterID(t *testing.T) {
	u := New(&head.HeadConfig{ClusterIDHex: "0123456789abcdef"}, nil)
	rr := serve(t, u, "/cluster_id")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			ClusterID string `json:"clusterId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.ClusterID != "0123456789abcdef" {
		t.Fatalf("cluster_id = %q", body.Data.ClusterID)
	}
}
