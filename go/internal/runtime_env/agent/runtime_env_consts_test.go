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

import "testing"

func TestLogFilename(t *testing.T) {
	if RuntimeEnvLogFilename != "runtime_env.log" {
		t.Errorf("Expected RuntimeEnvLogFilename='runtime_env.log', got %s", RuntimeEnvLogFilename)
	}
}

func TestRuntimeEnvAgentConstants(t *testing.T) {
	if RuntimeEnvAgentPortPrefix != "RUNTIME_ENV_AGENT_PORT_PREFIX:" {
		t.Errorf("Expected RuntimeEnvAgentPortPrefix='RUNTIME_ENV_AGENT_PORT_PREFIX:', got %s", RuntimeEnvAgentPortPrefix)
	}
	if RuntimeEnvAgentLogFilename != "runtime_env_agent.log" {
		t.Errorf("Expected RuntimeEnvAgentLogFilename='runtime_env_agent.log', got %s", RuntimeEnvAgentLogFilename)
	}
	if RuntimeEnvAgentCheckParentIntervalSEnvName != "RAY_RUNTIME_ENV_AGENT_CHECK_PARENT_INTERVAL_S" {
		t.Errorf("Expected RuntimeEnvAgentCheckParentIntervalSEnvName='RAY_RUNTIME_ENV_AGENT_CHECK_PARENT_INTERVAL_S', got %s", RuntimeEnvAgentCheckParentIntervalSEnvName)
	}
}

func TestRetryConstants(t *testing.T) {
	// Verify the default values.
	if RuntimeEnvRetryTimes != 3 {
		t.Errorf("Expected RuntimeEnvRetryTimes=3, got %d", RuntimeEnvRetryTimes)
	}
	if RuntimeEnvRetryIntervalMs != 1000 {
		t.Errorf("Expected RuntimeEnvRetryIntervalMs=1000, got %d", RuntimeEnvRetryIntervalMs)
	}
	if BadRuntimeEnvCacheTTLSeconds != 600 {
		t.Errorf("Expected BadRuntimeEnvCacheTTLSeconds=600, got %d", BadRuntimeEnvCacheTTLSeconds)
	}
}
