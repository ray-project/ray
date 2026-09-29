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

// Package usage_stats implements the UsageStatsHead dashboard module that
// exposes the usage stats enabled flag and cluster id, aligned with the Python
// UsageStatsHead in
// python/ray/dashboard/modules/usage_stats/usage_stats_head.py.
package usage_stats

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ray-project/ray/go/internal/dashboard/head"
)

// usageStatsEnabledEnvVar is USAGE_STATS_ENABLED_ENV_VAR from
// python/ray/_common/usage/usage_constants.py.
const usageStatsEnabledEnvVar = "RAY_USAGE_STATS_ENABLED"

// usageStatsPromptEnabledEnvVar mirrors RAY_USAGE_STATS_PROMPT_ENABLED from
// usage_lib.usage_stats_prompt_enabled (default 1).
const usageStatsPromptEnabledEnvVar = "RAY_USAGE_STATS_PROMPT_ENABLED"

// usageStatsConfigPathEnvVar is RAY_USAGE_STATS_CONFIG_PATH from usage_lib.py;
// it overrides the default ~/.ray/config.json.
const usageStatsConfigPathEnvVar = "RAY_USAGE_STATS_CONFIG_PATH"

// UsageStatsHead exposes usage stats settings and the cluster id.
type UsageStatsHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
}

// New creates a UsageStatsHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *UsageStatsHead {
	return &UsageStatsHead{cfg: cfg, client: client}
}

// Name returns the module name.
func (u *UsageStatsHead) Name() string { return "UsageStatsHead" }

// Start has no background tasks (the periodic usage report loop is not part of
// the dashboard head migration).
func (u *UsageStatsHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (u *UsageStatsHead) Healthy() bool { return true }

// RegisterHTTP registers the usage stats routes.
func (u *UsageStatsHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /usage_stats_enabled", u.handleUsageStatsEnabled)
	mux.HandleFunc("GET /cluster_id", u.handleClusterID)
	return nil
}

// usageStatsEnabled mirrors ray_usage_lib.usage_stats_enabled: false only when
// disabled explicitly, either via the RAY_USAGE_STATS_ENABLED env var ("0") or
// the "usage_stats" field of ~/.ray/config.json (false). The env var takes
// priority over the config file, and the default is enabled, matching
// _usage_stats_enabledness in usage_lib.py.
func usageStatsEnabled() bool {
	switch os.Getenv(usageStatsEnabledEnvVar) {
	case "0":
		return false
	case "1":
		return true
	}
	// Env var unset: fall back to the config file's "usage_stats" field.
	path := os.Getenv(usageStatsConfigPathEnvVar)
	if path == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			path = filepath.Join(home, ".ray", "config.json")
		}
	}
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var config struct {
				UsageStats *bool `json:"usage_stats"`
			}
			if err := json.Unmarshal(data, &config); err == nil && config.UsageStats != nil {
				return *config.UsageStats
			}
		}
	}
	// No explicit override: usage stats are enabled by default
	// (ENABLED_BY_DEFAULT in usage_lib.py).
	return true
}

// usageStatsPromptEnabled mirrors usage_stats_prompt_enabled: true when the env
// var is "1" (the default), false when "0".
func usageStatsPromptEnabled() bool {
	return os.Getenv(usageStatsPromptEnabledEnvVar) != "0"
}

// handleUsageStatsEnabled serves /usage_stats_enabled.
func (u *UsageStatsHead) handleUsageStatsEnabled(w http.ResponseWriter, r *http.Request) {
	head.RESTResponseCamel(w, http.StatusOK, "Fetched usage stats enabled", map[string]interface{}{
		"usage_stats_enabled":        usageStatsEnabled(),
		"usage_stats_prompt_enabled": usageStatsPromptEnabled(),
	})
}

// handleClusterID serves /cluster_id with the hex cluster id.
func (u *UsageStatsHead) handleClusterID(w http.ResponseWriter, r *http.Request) {
	head.RESTResponseCamel(w, http.StatusOK, "Fetched cluster id", map[string]interface{}{
		"cluster_id": u.cfg.ClusterIDHex,
	})
}
