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
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
)

// Fate-sharing constants, aligned with python/ray/dashboard/consts.py and
// python/ray/_private/process_watcher.py.
const (
	// rayletPIDEnvName carries the pid of the raylet that spawned this agent,
	// set by the raylet before it launches the dashboard agent process.
	rayletPIDEnvName = "RAY_RAYLET_PID"
	// parentCheckIntervalSEnvName overrides the parent liveness poll interval.
	parentCheckIntervalSEnvName = "RAY_DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S"
	// defaultParentCheckInterval mirrors DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S.
	defaultParentCheckInterval = 400 * time.Millisecond
	// parentDeathThreshold is how many consecutive failed polls count as a
	// confirmed parent death, mirroring _PARENT_DEATH_THREASHOLD.
	parentDeathThreshold = 5
	// rayletGracefulExitMarker is the log line that marks a graceful raylet
	// shutdown; its absence means the raylet was killed or crashed.
	rayletGracefulExitMarker = "Raylet received SIGTERM"
	// rayletLogTailSize is how much of raylet.out is read back, mirroring
	// _RAYLET_LOG_MAX_TAIL_SIZE.
	rayletLogTailSize = 1 << 20
	// rayletLogMaxPublishLines bounds the tail surfaced on an unexpected death,
	// mirroring _RAYLET_LOG_MAX_PUBLISH_LINES.
	rayletLogMaxPublishLines = 20
)

// parentPidFn returns the current parent pid. It is a package-level variable so
// tests can drive the death-detection loop without re-parenting the test
// process.
var parentPidFn = os.Getppid

// StartParentWatcher monitors the raylet that spawned this agent and
// fate-shares with it: after parentDeathThreshold consecutive polls in which
// this process's parent is not the raylet, it records a raylet death
// diagnostic and cancels ctx so the agent shuts down. It mirrors Python's
// create_check_raylet_task on the pid-polling path.
//
// When RAY_RAYLET_PID is unset or invalid the watcher does nothing: an agent
// started outside a raylet must not shut itself down.
//
// Python's alternative PARENT_HEALTH_CHECK_BY_PIPE path is not implemented.
//
// The raylet death is reported to the agent log only. Python additionally
// publishes RAYLET_DIED_ERROR to every driver through the core worker
// (publish_error_to_driver); the Go agent has no equivalent path, because the
// GCS client it holds exposes no error-table write.
func StartParentWatcher(ctx context.Context, cancel context.CancelFunc, logDir string) {
	rayletPid, ok := rayletPid()
	if !ok {
		return
	}
	interval := parentCheckInterval()
	log.Log.Info("starting raylet health check",
		"raylet_pid", rayletPid,
		"interval", interval.String(),
		"threshold", parentDeathThreshold)

	// Capture the parent-pid source once: the goroutine must not read the
	// package-level seam after launch, or a test that restores it in cleanup
	// would race with a poll still in flight.
	pidOf := parentPidFn

	go func() {
		deathCount := 0
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			// The raylet is this process's parent. Once it dies the process is
			// re-parented (to init, or to a subreaper), so Getppid no longer
			// returns the raylet pid.
			actual := pidOf()
			if actual == rayletPid {
				deathCount = 0
				continue
			}
			deathCount++
			log.Log.Info("raylet considered dead",
				"death_count", deathCount,
				"threshold", parentDeathThreshold,
				"expected_pid", rayletPid,
				"actual_ppid", actual)
			if deathCount >= parentDeathThreshold {
				reportRayletDeath(logDir)
				cancel()
				return
			}
		}
	}()
}

// rayletPid reads and validates RAY_RAYLET_PID, returning ok=false when it is
// absent or not a positive integer.
func rayletPid() (int, bool) {
	raw := strings.TrimSpace(os.Getenv(rayletPIDEnvName))
	if raw == "" {
		log.Log.V(1).Info("skipping raylet health check: " + rayletPIDEnvName + " is not set")
		return 0, false
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		log.Log.V(1).Info("skipping raylet health check: invalid "+rayletPIDEnvName+" value",
			"raylet_pid", raw)
		return 0, false
	}
	return pid, true
}

// parentCheckInterval returns the poll interval from
// RAY_DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S seconds, falling back to the
// default when it is unset, not positive, or not representable as a duration.
func parentCheckInterval() time.Duration {
	if secs := common.EnvFloat(parentCheckIntervalSEnvName, 0); secs > 0 && secs <= float64(math.MaxInt64/int64(time.Second)) {
		// A positive float can still convert to a non-positive duration on
		// amd64 (out-of-range conversion yields MinInt64), and on arm64 the
		// same conversion saturates to a positive MaxInt64 that would pass a
		// d > 0 check and yield a ~292-year interval. Bounding the seconds
		// against the largest representable value before converting makes
		// the fallback deterministic across architectures. A sub-nanosecond
		// fraction truncates to zero, so the zero guard is kept too:
		// time.NewTicker panics on a non-positive interval, and the panic
		// would fire inside the watcher goroutine, taking the agent down at
		// startup.
		if d := time.Duration(secs * float64(time.Second)); d > 0 {
			return d
		}
	}
	return defaultParentCheckInterval
}

// reportRayletDeath records whether the raylet exited gracefully, and the tail
// of its log when it did not. It mirrors the local half of Python's
// report_raylet_error_logs.
func reportRayletDeath(logDir string) {
	if logDir == "" {
		msg := "Raylet is terminated. Termination is unexpected: log dir is empty"
		log.Log.Error(fmt.Errorf("%s", msg), "raylet termination is unexpected")
		return
	}
	path := filepath.Join(logDir, "raylet.out")
	lines, err := tailFileLines(path, rayletLogTailSize)
	if err != nil {
		log.Log.Error(err, "Raylet is terminated. Failed to read Raylet logs", "path", path)
		return
	}
	graceful, msg := classifyRayletDeath(lines)
	if graceful {
		log.Log.Info("Raylet is terminated. Termination is graceful.")
		return
	}
	log.Log.Error(fmt.Errorf("%s", msg), "raylet termination is unexpected", "log_dir", logDir)
}

// classifyRayletDeath reports whether the tail of raylet.out shows a graceful
// shutdown and, when it does not, the diagnostic to record. It is the decision
// half of reportRayletDeath, split out so it can be asserted directly instead
// of through the log.
func classifyRayletDeath(lines []string) (graceful bool, msg string) {
	for _, line := range lines {
		if strings.Contains(line, rayletGracefulExitMarker) {
			return true, ""
		}
	}
	msg = "Raylet is terminated. Termination is unexpected. Possible reasons " +
		"include: (1) SIGKILL by the user or system OOM killer, " +
		"(2) Invalid memory access from Raylet causing SIGSEGV or SIGBUS, " +
		"(3) Other termination signals. Last " +
		strconv.Itoa(rayletLogMaxPublishLines) + " lines of the Raylet logs:\n"
	if n := len(lines); n > rayletLogMaxPublishLines {
		lines = lines[n-rayletLogMaxPublishLines:]
	}
	// Python renders `"    " + "    ".join(last_20_lines)`, where every
	// readlines() element still carries its trailing newline, so each tail line
	// ends up prefixed with four spaces. These lines were split on the newline
	// and carry none, so the same rendering is a four-space prefix plus a
	// newline-and-four-space join.
	return false, msg + "    " + strings.Join(lines, "\n    ")
}

// tailFileLines reads at most tailSize bytes from the end of path and returns
// them split into lines.
func tailFileLines(path string, tailSize int64) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	start := info.Size() - tailSize
	if start < 0 {
		start = 0
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(data), "\n"), nil
}
