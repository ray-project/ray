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

package agent

// Dashboard KV namespace in GCS InternalKV. Values here must match
// python/ray/dashboard/consts.py (KV_NAMESPACE_DASHBOARD = b"dashboard").
const (
	KVNamespaceDashboard = "dashboard"

	DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX = "DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:"
	DASHBOARD_AGENT_ADDR_IP_PREFIX      = "DASHBOARD_AGENT_ADDR_IP_PREFIX:"
)
