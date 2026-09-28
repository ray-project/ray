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

import "github.com/ray-project/ray/go/internal/common"

const (
	// RuntimeEnvLogFilename is the runtime env log filename.
	RuntimeEnvLogFilename = "runtime_env.log"

	// RuntimeEnvAgentPortPrefix is the port prefix of the runtime env agent.
	RuntimeEnvAgentPortPrefix = "RUNTIME_ENV_AGENT_PORT_PREFIX:"

	// RuntimeEnvAgentLogFilename is the runtime env agent log filename.
	RuntimeEnvAgentLogFilename = "runtime_env_agent.log"

	// RuntimeEnvAgentCheckParentIntervalSEnvName is the env var name for the
	// agent's parent-process check interval.
	RuntimeEnvAgentCheckParentIntervalSEnvName = "RAY_RUNTIME_ENV_AGENT_CHECK_PARENT_INTERVAL_S"
)

// Constants that can be overridden via environment variables.
var (
	// RuntimeEnvRetryTimes is the number of retries; overridable via the
	// RUNTIME_ENV_RETRY_TIMES environment variable.
	RuntimeEnvRetryTimes = common.EnvInteger("RUNTIME_ENV_RETRY_TIMES", 3)

	// RuntimeEnvRetryIntervalMs is the retry interval in milliseconds;
	// overridable via the RUNTIME_ENV_RETRY_INTERVAL_MS environment variable.
	RuntimeEnvRetryIntervalMs = common.EnvInteger("RUNTIME_ENV_RETRY_INTERVAL_MS", 1000)

	// BadRuntimeEnvCacheTTLSeconds is the TTL (seconds) for caching failed
	// runtime envs; overridable via the BAD_RUNTIME_ENV_CACHE_TTL_SECONDS
	// environment variable.
	BadRuntimeEnvCacheTTLSeconds = common.EnvInteger("BAD_RUNTIME_ENV_CACHE_TTL_SECONDS", 60*10)

	// MaxJobLoggerCount is the maximum number of per-job loggers allowed per
	// runtime env agent.
	// The default of 5000 is based on:
	// - Memory footprint: each PerJobLogger takes a few KB of memory (logger
	//   instance, LRU node, etc.), so 5000 loggers cost roughly 10-20MB.
	// - Typical workloads: the number of concurrent jobs on a single node of
	//   most Ray clusters is far below 5000.
	// - LRU protection: once the limit is exceeded, the least recently used
	//   logger is evicted, preventing unbounded memory growth.
	// - Configurability: tune RAY_JOB_LOGGER_COUNT according to the actual
	//   memory budget and workload.
	MaxJobLoggerCount = common.EnvInteger("RAY_JOB_LOGGER_COUNT", 5000)
)
