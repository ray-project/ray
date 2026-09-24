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

# Python Log Monitor Parity Mapping

Reference implementation:

- `python/ray/_private/log_monitor.py`
- `python/ray/tests/test_logging.py`

Runtime integration reference:

- `python/ray/_private/services.py`
- `go/cmd/raygo/log_monitor/log_monitor.go`

Current integration model:

- Python remains the default log monitor implementation
- `RAY_ENABLE_GO_LOG_MONITOR=1` switches startup to `raygo log-monitor`
- Go runtime parity must be verified both at the core-logic layer and at the startup/integration layer

Behavior-to-test mapping:

- Python worker filename regex -> `TestParseWorkerFilenameParity`
- Python runtime_env filename regex -> `TestParseRuntimeEnvFilenameParity`
- Python component classification rules -> `TestClassifyComponentPIDParity`
- Python discovery pattern set -> `TestLogPatternsParity`
- Python incremental read and batch publish -> `TestCheckLogFilesAndPublishUpdatesParity`
- Python per-iteration line cap -> `TestMaxLinesPerReadParity`
- Python actor/task/job prefix propagation -> `TestCheckLogFilesAndPublishUpdatesParity`
- Python reopen after file replacement -> `TestReopenIfNecessaryAfterReplacementParity`
- Python keep-position behavior for larger replacement files -> `TestReopenIfNecessaryKeepsPositionForLargerReplacementParity`
- Python reset after truncation -> `TestReopenIfNecessaryAfterTruncationParity`
- Python in-place truncate-and-rewrite detection -> `TestInPlaceRewriteDetected`
- Python open-file backpressure -> `TestOpenClosedFilesParity`
- Python unchanged closed files remain closed -> `TestOpenClosedFilesSkipsUnchangedClosedFilesParity`
- Python dead-worker archive behavior -> `TestCloseAllFilesArchivesDeadWorkersParity`
- Python disappearing-file tolerance -> `TestUpdateLogFilenamesRemovesMissingFilesParity`
- Python publish-failure tolerance -> `TestPublishFailureDoesNotStopLoopParity`
- Python update-throttle behavior -> `TestShouldUpdateFilenamesBackpressureParity`

Runtime/startup behavior mapping:

- Python control-plane chooses Go command when enabled -> `python/ray/tests/test_logging.py::test_start_log_monitor_uses_go_command_when_enabled`
- Go command replaces the Python monitor when enabled -> `python/ray/tests/test_logging.py::test_start_log_monitor_uses_go_command_over_python`
- Python control-plane passes cluster ID to Go -> `python/ray/tests/test_logging.py::test_start_log_monitor_passes_cluster_id_to_go_command`
- Python control-plane passes node IP to Go -> `python/ray/tests/test_logging.py::test_start_log_monitor_passes_node_ip_to_go_command`
- `Node.start_log_monitor()` forwards node IP into services -> `python/ray/tests/test_logging.py::test_node_start_log_monitor_passes_node_ip_to_services`
- Real session replay parity -> `TestRealSessionReplayParity`

Recommended validation layers:

1. `//go/internal/logmonitor:logmonitor_test`
2. Python switching tests in `python/ray/tests/test_logging.py`
3. Manual cluster validation in `the internal validation record (kept outside this repo)`
