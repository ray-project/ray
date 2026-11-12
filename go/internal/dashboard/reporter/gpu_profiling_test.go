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

package reporter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMain pins the GPU probe to false for the whole package. Building a
// ReporterService now starts the dynolog monitoring daemon, so without this
// default every test that constructs a service would fork nvidia-smi and, on a
// GPU host with dynolog installed, launch a real daemon from a unit test. Tests
// that need a GPU node override nodeHasGPUs themselves, as newGPUManager does.
func TestMain(m *testing.M) {
	origNodeHasGPUs := nodeHasGPUs
	nodeHasGPUs = func() bool { return false }
	code := m.Run()
	nodeHasGPUs = origNodeHasGPUs
	os.Exit(code)
}

// gpuTestHooks carries the state the stubbed GPU profiling seams read and
// record, so a test can preset the daemon pid and alive flag and assert on the
// launches and commands it drove.
type gpuTestHooks struct {
	daemonLaunches int
	daemonPid      int32
	daemonAlive    bool
	commands       []string
	commandResult  CommandResult
}

func newGPUManager(t *testing.T, enabled bool, hooks *gpuTestHooks) *ProfileManager {
	t.Helper()

	oldLookPath := lookPath
	oldHasGPUs := nodeHasGPUs
	oldPIDAlive := gpuProfilingPIDAlive
	oldLaunch := launchDynologDaemon
	oldAlive := gpuProfilingDaemonAlive
	oldTimestamp := gpuTraceTimestamp
	oldTimeout := gpuProfileTimeoutSeconds
	oldInterval := gpuProfileTraceWaitInterval
	oldExec := runCommandDetail
	t.Cleanup(func() {
		lookPath = oldLookPath
		nodeHasGPUs = oldHasGPUs
		gpuProfilingPIDAlive = oldPIDAlive
		launchDynologDaemon = oldLaunch
		gpuProfilingDaemonAlive = oldAlive
		gpuTraceTimestamp = oldTimestamp
		gpuProfileTimeoutSeconds = oldTimeout
		gpuProfileTraceWaitInterval = oldInterval
		runCommandDetail = oldExec
	})

	lookPath = func(name string) (string, error) {
		if !enabled {
			return "", os.ErrNotExist
		}
		return "/usr/bin/" + name, nil
	}
	nodeHasGPUs = func() bool { return enabled }
	gpuProfilingPIDAlive = func(int) bool { return true }
	gpuProfilingDaemonAlive = func(int) bool { return hooks.daemonAlive }
	gpuTraceTimestamp = func() string { return "20260101120000" }
	gpuProfileTimeoutSeconds = 5
	gpuProfileTraceWaitInterval = time.Millisecond
	launchDynologDaemon = func(string, int, string) (int, error) {
		hooks.daemonLaunches++
		return int(hooks.daemonPid), nil
	}
	runCommandDetail = func(name string, args ...string) CommandResult {
		hooks.commands = append(hooks.commands, strings.Join(append([]string{name}, args...), " "))
		return hooks.commandResult
	}

	m, err := NewProfileManager(t.TempDir(), "192.0.2.10")
	if err != nil {
		t.Fatalf("NewProfileManager: %v", err)
	}
	if enabled && hooks.daemonPid != 0 {
		m.daemonPID.Store(hooks.daemonPid)
	}
	return m
}

func TestGpuProfileDisabledNodeReturnsTheDisabledMessage(t *testing.T) {
	hooks := &gpuTestHooks{}
	m := newGPUManager(t, false, hooks)

	success, output, err := m.GpuProfile(context.Background(), 1234, 10)
	if err != nil {
		t.Fatalf("GpuProfile: %v", err)
	}
	if success {
		t.Fatal("GpuProfile succeeded on a node without GPUs")
	}
	if !strings.Contains(output, "GPU profiling is not enabled on node 192.0.2.10") {
		t.Fatalf("output = %q, want the disabled message", output)
	}
	if hooks.daemonLaunches != 0 {
		t.Fatalf("daemon launched %d times on a disabled node", hooks.daemonLaunches)
	}
}

func TestGpuProfileWithoutDaemonReturnsAnError(t *testing.T) {
	m := newGPUManager(t, true, &gpuTestHooks{})

	_, _, err := m.GpuProfile(context.Background(), 1234, 10)
	if err == nil {
		t.Fatal("GpuProfile returned no error although the daemon was never started")
	}
	if !strings.Contains(err.Error(), "Must call `start_monitoring_daemon` before profiling.") {
		t.Fatalf("err = %v, want the Python RuntimeError message", err)
	}
}

func TestGpuProfileWithDeadDaemonReturnsTheDaemonMessage(t *testing.T) {
	hooks := &gpuTestHooks{daemonPid: 777, daemonAlive: false}
	m := newGPUManager(t, true, hooks)

	success, output, err := m.GpuProfile(context.Background(), 1234, 10)
	if err != nil {
		t.Fatalf("GpuProfile: %v", err)
	}
	if success {
		t.Fatal("GpuProfile succeeded with a dead daemon")
	}
	if !strings.Contains(output, "GPU monitoring daemon (pid=777) is not running on node 192.0.2.10") {
		t.Fatalf("output = %q, want the daemon-not-running message", output)
	}
}

func TestGpuProfileWithDeadTargetReturnsTheDeadProcessMessage(t *testing.T) {
	hooks := &gpuTestHooks{daemonPid: 777, daemonAlive: true}
	m := newGPUManager(t, true, hooks)
	gpuProfilingPIDAlive = func(int) bool { return false }

	success, output, err := m.GpuProfile(context.Background(), 1234, 10)
	if err != nil {
		t.Fatalf("GpuProfile: %v", err)
	}
	if success {
		t.Fatal("GpuProfile succeeded for a dead target process")
	}
	if !strings.Contains(output, "The requested process to profile with pid=1234 on node 192.0.2.10 is no longer running") {
		t.Fatalf("output = %q, want the dead-process message", output)
	}
}

func TestGpuProfileNoProcessesMatched(t *testing.T) {
	hooks := &gpuTestHooks{
		daemonPid:     777,
		daemonAlive:   true,
		commandResult: CommandResult{Stdout: "No processes were matched for the request", ExitCode: 0},
	}
	m := newGPUManager(t, true, hooks)

	success, output, err := m.GpuProfile(context.Background(), 1234, 10)
	if err != nil {
		t.Fatalf("GpuProfile: %v", err)
	}
	if success {
		t.Fatal("GpuProfile succeeded although no process matched")
	}
	if !strings.Contains(output, "There are a few potential reasons for this") {
		t.Fatalf("output = %q, want the no-processes-matched guidance", output)
	}
}

func TestGpuProfileTimesOutWithoutATraceFile(t *testing.T) {
	hooks := &gpuTestHooks{daemonPid: 777, daemonAlive: true}
	m := newGPUManager(t, true, hooks)

	success, output, err := m.GpuProfile(context.Background(), 1234, 10)
	if err != nil {
		t.Fatalf("GpuProfile: %v", err)
	}
	if success {
		t.Fatal("GpuProfile succeeded without a trace file")
	}
	if !strings.Contains(output, "GPU profiling timed out after 5 seconds, please try again.") {
		t.Fatalf("output = %q, want the timeout message", output)
	}
}

// TestGpuProfileStopsWaitingWhenTheContextIsCancelled pins that the trace-file
// poll loop observes cancellation. Without it a disconnected caller leaves the
// goroutine polling until the timeout, which the Python coroutine (cancelled
// with the RPC) never does.
func TestGpuProfileStopsWaitingWhenTheContextIsCancelled(t *testing.T) {
	hooks := &gpuTestHooks{daemonPid: 777, daemonAlive: true}
	m := newGPUManager(t, true, hooks)
	// Only cancellation can end the wait: the timeout is far away and the
	// stubbed command never creates a trace file.
	gpuProfileTimeoutSeconds = 60

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()

	start := time.Now()
	success, output, err := m.GpuProfile(ctx, 1234, 10)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("GpuProfile returned no error; success = %v, output = %q", success, output)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a context.Canceled error", err)
	}
	if success || output != "" {
		t.Fatalf("GpuProfile = (%v, %q, %v), want the (false, \"\", err) shape", success, output, err)
	}
	if elapsed >= 30*time.Second {
		t.Fatalf("GpuProfile returned after %v, want well before the 60s timeout", elapsed)
	}
}

// TestGpuProfileStopsWaitingWhenTheDeadlineExpires pins that a caller-imposed
// RPC deadline shorter than the internal wait surfaces as a deadline error, not
// the internal timeout reply. Only an expired context can end this wait: the
// internal timeout is 60s and the stubbed command never creates a trace file.
func TestGpuProfileStopsWaitingWhenTheDeadlineExpires(t *testing.T) {
	hooks := &gpuTestHooks{daemonPid: 777, daemonAlive: true}
	m := newGPUManager(t, true, hooks)
	gpuProfileTimeoutSeconds = 60

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Millisecond))
	defer cancel()

	success, output, err := m.GpuProfile(ctx, 1234, 10)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a context.DeadlineExceeded error", err)
	}
	if success || output != "" {
		t.Fatalf("GpuProfile = (%v, %q, %v), want the (false, \"\", err) shape", success, output, err)
	}
}

func TestGpuProfileSucceedsWhenTheTraceFileAppears(t *testing.T) {
	hooks := &gpuTestHooks{daemonPid: 777, daemonAlive: true}
	m := newGPUManager(t, true, hooks)
	// The real dyno invocation is replaced by this hook; drop a trace file with
	// the timestamp the stub pins so the poll loop finds it.
	runCommandDetail = func(name string, args ...string) CommandResult {
		hooks.commands = append(hooks.commands, strings.Join(append([]string{name}, args...), " "))
		dir := filepath.Join(m.profileDir, "profiles")
		_ = os.WriteFile(filepath.Join(dir, "gputrace_192.0.2.10_20260101120000_42.json"), []byte("{}"), 0o644)
		return CommandResult{ExitCode: 0}
	}

	success, output, err := m.GpuProfile(context.Background(), 1234, 10)
	if err != nil {
		t.Fatalf("GpuProfile: %v", err)
	}
	if !success {
		t.Fatalf("GpuProfile failed: %s", output)
	}
	if !strings.HasPrefix(output, "profiles/gputrace_192.0.2.10_20260101120000") {
		t.Fatalf("output = %q, want a path relative to the log directory", output)
	}
	if len(hooks.commands) != 1 || !strings.Contains(hooks.commands[0], "gputrace") {
		t.Fatalf("commands = %v, want a single dyno gputrace invocation", hooks.commands)
	}
	if !strings.Contains(hooks.commands[0], "--port 65406") || !strings.Contains(hooks.commands[0], "--iterations 10") {
		t.Fatalf("command = %q, want the dynolog port and iteration count", hooks.commands[0])
	}
	if !strings.Contains(hooks.commands[0], "--process-limit 1") {
		t.Fatalf("command = %q, want the single-process limit", hooks.commands[0])
	}
}

func TestStartMonitoringDaemonSkipsDisabledNodes(t *testing.T) {
	hooks := &gpuTestHooks{}
	m := newGPUManager(t, false, hooks)
	m.StartMonitoringDaemon()
	if hooks.daemonLaunches != 0 {
		t.Fatalf("daemon launched %d times on a disabled node", hooks.daemonLaunches)
	}
}

func TestStartMonitoringDaemonLaunchesOnce(t *testing.T) {
	hooks := &gpuTestHooks{daemonPid: 777, daemonAlive: true}
	m := newGPUManager(t, true, hooks)
	// A freshly built manager has not launched a daemon yet; newGPUManager
	// pre-records the fake pid only for the tests that profile against an
	// already-running daemon.
	m.daemonPID.Store(0)
	m.StartMonitoringDaemon()
	m.StartMonitoringDaemon()
	if hooks.daemonLaunches != 1 {
		t.Fatalf("daemon launched %d times, want once", hooks.daemonLaunches)
	}
	if got := m.daemonPID.Load(); got != 777 {
		t.Fatalf("daemonPID = %d, want 777", got)
	}
}
