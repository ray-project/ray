# Copyright 2026 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Go Log Monitor

This package contains the Go implementation of Ray's log monitor core logic.

## Scope

`go/internal/logmonitor` is responsible for:

- discovering Ray log files under a session logs directory
- managing open/closed file lifecycles under open-file pressure
- incrementally reading appended log content
- parsing `:actor_name:`, `:task_name:`, and `:job_id:` prefixes
- handling replacement, rotation, truncation, and disappearing files
- publishing normalized `LogBatch` payloads through a narrow publisher interface

This package is not responsible for:

- CLI parsing
- Python/Go implementation selection
- Ray process startup orchestration

Those responsibilities live in:

- `go/cmd/raygo/log_monitor`
- `python/ray/_private/services.py`

## Runtime Integration

The runtime entrypoint is:

- `raygo log-monitor`

The Python control plane enables it through:

- `RAY_ENABLE_GO_LOG_MONITOR=1`

Current behavior:

- default path: Python log monitor
- Go path: explicitly enabled by environment variable
- no silent fallback from Go back to Python

For source-tree validation, `RAYGO_EXECUTABLE` can be used to override the
binary path that Python launches.

## Key Files

- `monitor.go`
  - monitor lifecycle, discovery, open/close logic, publish loop
- `file_info.go`
  - per-file reopen and truncation behavior
- `patterns.go`
  - discovery ordering and file classification helpers
- `runtime_publisher.go`
  - runtime publish adapter used by the real GCS path
- `real_session_replay_test.go`
  - replay parity test against captured real Ray session logs
- `python_parity_test.go`
  - synthetic parity scenarios against the Python reference implementation
- `python_parity.md`
  - behavior-to-test mapping against Python

## Tests

Main Bazel target:

```bash
bazel test //go/internal/logmonitor:logmonitor_test \
  --test_arg=-test.v \
  --test_env=PATH \
  --test_output=streamed \
  --nocache_test_results \
  --spawn_strategy=local
```

Coverage includes:

- unit-style lifecycle tests
- integration-style tests using a real temporary filesystem
- synthetic Python parity scenarios
- real session replay parity
- runtime publisher payload tests

## Real Session Replay Parity

`TestRealSessionReplayParity` compares the Go implementation and the Python
reference implementation on the same captured real session logs.

Example:

```bash
bazel test //go/internal/logmonitor:logmonitor_test \
  --remote_executor= \
  --remote_cache= \
  --spawn_strategy=local \
  --test_arg=-test.v \
  --test_arg=-test.run=TestRealSessionReplayParity \
  --test_env=PATH \
  --test_env=LOGMONITOR_REAL_LOGS_DIR=/path/to/session/logs \
  --test_output=streamed \
  --nocache_test_results
```

## Manual Validation

For Python-vs-Go manual validation steps, see:

- `the internal validation record (kept outside this repo)`

That flow covers:

- Python baseline mode
- Go mode with `RAY_ENABLE_GO_LOG_MONITOR=1`
- worker log comparison
- replay parity against real captured logs
