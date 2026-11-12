package reporter

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testNodeIP is the node address handed to the ProfileManager under test so
// the GPU profiling messages can be asserted on a fixed value.
const testNodeIP = "10.0.0.7"

// stubGpuProfiling installs deterministic GPU profiling seams for the duration
// of a test: nodeHasGPUs and lookPath are replaced so no real nvidia-smi or
// dynolog installation is required, gpuProfilingPIDAlive is replaced so the
// dead-process branch does not depend on the machine's process table, and the
// monitoring daemon is pinned as alive while its launcher always fails so no
// dynolog process is ever started.
func stubGpuProfiling(t *testing.T, hasGPUs bool, dynologPath, dynoPath string, pidAlive func(int) bool) {
	t.Helper()
	origNodeHasGPUs, origLookPath, origPIDAlive := nodeHasGPUs, lookPath, gpuProfilingPIDAlive
	origDaemonAlive, origLaunch := gpuProfilingDaemonAlive, launchDynologDaemon
	nodeHasGPUs = func() bool { return hasGPUs }
	gpuProfilingDaemonAlive = func(int) bool { return true }
	launchDynologDaemon = func(string, int, string) (int, error) { return 0, exec.ErrNotFound }
	lookPath = func(name string) (string, error) {
		switch name {
		case "dynolog":
			if dynologPath == "" {
				return "", exec.ErrNotFound
			}
			return dynologPath, nil
		case "dyno":
			if dynoPath == "" {
				return "", exec.ErrNotFound
			}
			return dynoPath, nil
		}
		return "", exec.ErrNotFound
	}
	if pidAlive != nil {
		gpuProfilingPIDAlive = pidAlive
	}
	t.Cleanup(func() {
		nodeHasGPUs, lookPath, gpuProfilingPIDAlive = origNodeHasGPUs, origLookPath, origPIDAlive
		gpuProfilingDaemonAlive, launchDynologDaemon = origDaemonAlive, origLaunch
	})
}

// newTestManager builds a ProfileManager for the given directory, failing the
// test if directory creation fails.
func newTestManager(t *testing.T, dir string) *ProfileManager {
	t.Helper()
	m, err := NewProfileManager(dir, testNodeIP)
	if err != nil {
		t.Fatalf("NewProfileManager(%q) error = %v", dir, err)
	}
	return m
}

// gpuTestDaemonPID is the monitoring daemon pid recorded by
// newGPUManagerWithDaemon.
const gpuTestDaemonPID int32 = 777

// newGPUManagerWithDaemon builds a manager that already records a running
// monitoring daemon, so gpu_profile proceeds past the start_monitoring_daemon
// precondition.
func newGPUManagerWithDaemon(t *testing.T) *ProfileManager {
	t.Helper()
	m := newTestManager(t, t.TempDir())
	m.daemonPID.Store(gpuTestDaemonPID)
	return m
}

// cmdCapture records the commands handed to a fake commandExecutor so tests can
// assert on command construction without invoking real profiler binaries.
type cmdCapture struct {
	names []string
	args  [][]string
}

func (c *cmdCapture) record(name string, args []string) {
	c.names = append(c.names, name)
	c.args = append(c.args, args)
}

func (c *cmdCapture) lastArgs() []string {
	if len(c.args) == 0 {
		return nil
	}
	return c.args[len(c.args)-1]
}

func (c *cmdCapture) callCount() int {
	return len(c.names)
}

// capturingExecutor returns a commandExecutor that records invocations and
// always returns the supplied result.
func capturingExecutor(cap *cmdCapture, result CommandResult) commandExecutor {
	return func(name string, args ...string) CommandResult {
		cap.record(name, args)
		return result
	}
}

// stubPasswordlessSudo pins the result of the `sudo -n true` probe for the
// duration of a test, so command construction does not depend on the sudo
// configuration of the build machine.
func stubPasswordlessSudo(t *testing.T, available bool) {
	t.Helper()
	orig := canPasswordlessSudo
	canPasswordlessSudo = func() bool { return available }
	t.Cleanup(func() { canPasswordlessSudo = orig })
}

// testProfilerPath is the absolute path stubLookPath reports for a profiler, so
// assertions can name the exact binary the agent executes.
func testProfilerPath(name string) string {
	return "/opt/profilers/bin/" + name
}

// stubLookPath pins profiler resolution so a test does not depend on which
// py-spy or memray happens to be installed on the build machine.
func stubLookPath(t *testing.T, missing ...string) {
	t.Helper()
	orig := lookPath
	lookPath = func(name string) (string, error) {
		for _, skip := range missing {
			if name == skip {
				return "", exec.ErrNotFound
			}
		}
		return testProfilerPath(name), nil
	}
	t.Cleanup(func() { lookPath = orig })
}

func okResult() CommandResult {
	return CommandResult{Stdout: "ok", Stderr: "", ExitCode: 0}
}

func TestProfileValidatePID(t *testing.T) {
	for _, pid := range []int{1, 42, 9999} {
		if err := validatePID(pid); err != nil {
			t.Errorf("validatePID(%d) = %v, want nil", pid, err)
		}
	}
	for _, pid := range []int{0, -1, -42} {
		if err := validatePID(pid); err == nil {
			t.Errorf("validatePID(%d) = nil, want error (non-positive)", pid)
		}
	}
}

func TestProfileValidateDuration(t *testing.T) {
	for _, d := range []int{1, 5, 300} {
		if err := validateDuration(d); err != nil {
			t.Errorf("validateDuration(%d) = %v, want nil", d, err)
		}
	}
	for _, d := range []int{0, -1, 301, 9999} {
		if err := validateDuration(d); err == nil {
			t.Errorf("validateDuration(%d) = nil, want error (out of range)", d)
		}
	}
}

func TestProfileValidateToken(t *testing.T) {
	valid := []string{"flamegraph", "raw", "speedscope", "profile_1.txt", "a.b"}
	for _, s := range valid {
		if err := validateToken(s); err != nil {
			t.Errorf("validateToken(%q) = %v, want nil", s, err)
		}
	}

	invalid := []string{
		"a;b",    // command separator
		"a&b",    // background / and
		"a|b",    // pipe
		"a$b",    // variable expansion
		"a`b",    // backtick command substitution
		"a b",    // whitespace
		"../etc", // path traversal
		"a..b",   // dot-dot sequence
		"a\nb",   // newline
		"a>b",    // redirect
		"a<b",    // redirect
	}
	for _, s := range invalid {
		if err := validateToken(s); err == nil {
			t.Errorf("validateToken(%q) = nil, want error (injection)", s)
		}
	}
}

func TestProfileTraceDumpRejectsInvalidPID(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, output := m.TraceDump(0, false, false)

	if success {
		t.Errorf("TraceDump(0) success = true, want false")
	}
	if output == "" {
		t.Errorf("TraceDump(0) output is empty, want error message")
	}
	if cap.callCount() != 0 {
		t.Errorf("TraceDump(0) executed %d commands, want 0 (invalid pid blocked)", cap.callCount())
	}
}

// TestProbePasswordlessSudo covers the `sudo -n true` probe: only a zero exit
// code counts as available, and a missing sudo binary does not.
func TestProbePasswordlessSudo(t *testing.T) {
	orig := runCommandDetail
	defer func() { runCommandDetail = orig }()

	cap := &cmdCapture{}
	runCommandDetail = capturingExecutor(cap, CommandResult{ExitCode: 0})
	if !probePasswordlessSudo() {
		t.Error("probePasswordlessSudo() = false when `sudo -n true` exits 0, want true")
	}
	if cap.callCount() != 1 {
		t.Fatalf("probe executed %d commands, want 1", cap.callCount())
	}
	if cap.names[0] != "sudo" {
		t.Errorf("probe command = %q, want sudo", cap.names[0])
	}
	want := []string{"-n", "true"}
	got := cap.lastArgs()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("probe args = %v, want %v", got, want)
	}

	// A non-zero exit means sudo exists but cannot escalate without a password.
	runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{ExitCode: 1})
	if probePasswordlessSudo() {
		t.Error("probePasswordlessSudo() = true when `sudo -n true` exits 1, want false")
	}

	// A negative exit code means the sudo binary could not be launched at all.
	runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{ExitCode: -1})
	if probePasswordlessSudo() {
		t.Error("probePasswordlessSudo() = true when sudo is missing, want false")
	}
}

// TestResolveProfilerReturnsAbsolutePath asserts the profiler pre-check hands
// the resolved absolute path to the command line, mirroring the Python
// `shutil.which(self.profiler_name)` call sites. A bare "py-spy" would be
// resolved again by sudo against its hardened secure_path and fail.
func TestResolveProfilerReturnsAbsolutePath(t *testing.T) {
	stubLookPath(t)

	path, err := resolveProfiler("py-spy")
	if err != nil {
		t.Fatalf("resolveProfiler(py-spy) error = %v, want nil", err)
	}
	if want := testProfilerPath("py-spy"); path != want {
		t.Errorf("resolveProfiler(py-spy) = %q, want %q", path, want)
	}
}

// TestResolveProfilerNotInstalled asserts the not-installed message is
// reproduced verbatim from the Python profile managers, which return
// "Failed to execute: <profiler> is not installed" before ever exec'ing.
func TestResolveProfilerNotInstalled(t *testing.T) {
	for _, name := range []string{"py-spy", "memray"} {
		t.Run(name, func(t *testing.T) {
			stubLookPath(t, name)

			if _, err := resolveProfiler(name); err == nil {
				t.Fatalf("resolveProfiler(%s) error = nil, want a not-installed error", name)
			} else if want := "Failed to execute: " + name + " is not installed"; err.Error() != want {
				t.Errorf("resolveProfiler(%s) error = %q, want %q", name, err.Error(), want)
			}
		})
	}
}

// TestProfileNotInstalledShortCircuits asserts every profiler entry point
// returns the not-installed message without executing anything, so the agent
// never shells out to a missing binary (and never leaks the confusing
// "sudo: <profiler>: command not found" from the escalated path).
func TestProfileNotInstalledShortCircuits(t *testing.T) {
	for _, tc := range []struct {
		name     string
		profiler string
		call     func(m *ProfileManager) string
	}{
		{
			name:     "TraceDump",
			profiler: "py-spy",
			call: func(m *ProfileManager) string {
				_, out := m.TraceDump(123, false, false)
				return out
			},
		},
		{
			name:     "CPUProfile",
			profiler: "py-spy",
			call: func(m *ProfileManager) string {
				_, out := m.CPUProfile(123, "flamegraph", 5, false, false)
				return out
			},
		},
		{
			name:     "MemoryProfileAttach",
			profiler: "memray",
			call: func(m *ProfileManager) string {
				_, _, out := m.MemoryProfileAttach(123, false, false, false)
				return out
			},
		},
		{
			name:     "MemoryProfileDetach",
			profiler: "memray",
			call: func(m *ProfileManager) string {
				_, out := m.MemoryProfileDetach(123, false)
				return out
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubPasswordlessSudo(t, true)
			stubLookPath(t, tc.profiler)

			cap := &cmdCapture{}
			orig := runCommandDetail
			runCommandDetail = capturingExecutor(cap, okResult())
			defer func() { runCommandDetail = orig }()

			got := tc.call(newTestManager(t, t.TempDir()))
			if want := "Failed to execute: " + tc.profiler + " is not installed"; got != want {
				t.Errorf("%s output = %q, want %q", tc.name, got, want)
			}
			if n := cap.callCount(); n != 0 {
				t.Errorf("%s executed %d commands, want 0 (missing profiler must short-circuit)", tc.name, n)
			}
		})
	}
}

// TestMemoryProfileResultNotInstalled asserts the result-conversion path runs
// its profiler pre-check before touching the filesystem. Python checks
// shutil.which first (profile_manager.py:219-221) and only then stats the
// profile file, so a missing memray reports "not installed" rather than
// "has not been profiled" even when no profile exists.
func TestMemoryProfileResultNotInstalled(t *testing.T) {
	stubPasswordlessSudo(t, true)
	stubLookPath(t, "memray")

	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	success, out := newTestManager(t, t.TempDir()).MemoryProfileResult(123, "123_memory_profiling.bin", "flamegraph", false)
	if success {
		t.Errorf("MemoryProfileResult success = true, want false")
	}
	if want := "Failed to execute: memray is not installed"; out != want {
		t.Errorf("MemoryProfileResult output = %q, want %q", out, want)
	}
	if n := cap.callCount(); n != 0 {
		t.Errorf("MemoryProfileResult executed %d commands, want 0", n)
	}
}

// TestProfilePrecheckOrderMatchesPython pins the relative order of the not-installed
// pre-check against the other input validations, which differs per call site in
// profile_manager.py. Getting it wrong swaps which error an invalid request
// reports, so each order is asserted explicitly.
func TestProfilePrecheckOrderMatchesPython(t *testing.T) {
	stubPasswordlessSudo(t, false)
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	// cpu_profile resolves the profiler before validating format
	// (profile_manager.py:146-153), so an invalid format still reports the
	// missing profiler when py-spy is absent.
	t.Run("CPUProfile pre-check precedes format validation", func(t *testing.T) {
		stubLookPath(t, "py-spy")
		_, out := newTestManager(t, t.TempDir()).CPUProfile(9, "bogus", 5, false, false)
		if want := "Failed to execute: py-spy is not installed"; out != want {
			t.Errorf("output = %q, want %q", out, want)
		}
	})

	// cpu_profile reports the invalid format when py-spy IS available
	// (profile_manager.py:148-153).
	t.Run("CPUProfile reports invalid format when installed", func(t *testing.T) {
		stubLookPath(t)
		_, out := newTestManager(t, t.TempDir()).CPUProfile(9, "bogus", 5, false, false)
		if want := "Failed to execute: Invalid format bogus, must be [flamegraph, raw, speedscope]"; out != want {
			t.Errorf("output = %q, want %q", out, want)
		}
	})

	// get_profile_result checks the profile FILE before the format
	// (profile_manager.py:222-249), so a missing file wins over an unsupported
	// format.
	t.Run("MemoryProfileResult file check precedes format validation", func(t *testing.T) {
		stubLookPath(t)
		_, out := newTestManager(t, t.TempDir()).MemoryProfileResult(9, "9_memory_profiling_1.bin", "bogus", false)
		if want := "Failed to execute: process 9 has not been profiled"; out != want {
			t.Errorf("output = %q, want %q", out, want)
		}
	})
}

// TestMemoryProfileResultUsesAbsolutePath asserts the conversion command uses
// the resolved absolute memray path and is NOT escalated through sudo, which
// mirrors profile_manager.py:255 (the only one of the five call sites that runs
// the visualize command directly).
func TestMemoryProfileResultUsesAbsolutePath(t *testing.T) {
	stubPasswordlessSudo(t, true)
	stubLookPath(t)

	dir := t.TempDir()
	memrayDir := dir + "/memray"
	if err := os.MkdirAll(memrayDir, 0755); err != nil {
		t.Fatalf("MkdirAll error: %v", err)
	}
	const filename = "123_memory_profiling.bin"
	if err := os.WriteFile(memrayDir+"/"+filename, []byte("profile"), 0644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}
	if err := os.WriteFile(memrayDir+"/123_memory_profiling.html", []byte("<html/>"), 0644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	if success, out := newTestManager(t, dir).MemoryProfileResult(123, filename, "flamegraph", false); !success {
		t.Fatalf("MemoryProfileResult success = false (%s), want true", out)
	}
	if cap.callCount() != 1 {
		t.Fatalf("executed %d commands, want 1", cap.callCount())
	}
	if want := testProfilerPath("memray"); cap.names[0] != want {
		t.Errorf("command = %q, want the absolute path %q", cap.names[0], want)
	}
}

// TestProfilerArgvPrefixesSudo asserts the exact argv the escalation helper
// builds, for both available and unavailable passwordless sudo.
func TestProfilerArgvPrefixesSudo(t *testing.T) {
	t.Run("sudo available", func(t *testing.T) {
		stubPasswordlessSudo(t, true)
		argv, command := profilerArgv("/usr/local/bin/py-spy", "dump", "-p", "7")
		want := []string{"sudo", "-n", "/usr/local/bin/py-spy", "dump", "-p", "7"}
		if len(argv) != len(want) {
			t.Fatalf("argv = %v, want %v", argv, want)
		}
		for i := range want {
			if argv[i] != want[i] {
				t.Errorf("argv[%d] = %q, want %q", i, argv[i], want[i])
			}
		}
		if command != "sudo -n /usr/local/bin/py-spy dump -p 7" {
			t.Errorf("command = %q, want the full escalated command line", command)
		}
	})

	t.Run("sudo unavailable", func(t *testing.T) {
		stubPasswordlessSudo(t, false)
		argv, command := profilerArgv("/usr/local/bin/memray", "attach", "7")
		want := []string{"/usr/local/bin/memray", "attach", "7"}
		if len(argv) != len(want) {
			t.Fatalf("argv = %v, want %v", argv, want)
		}
		for i := range want {
			if argv[i] != want[i] {
				t.Errorf("argv[%d] = %q, want %q", i, argv[i], want[i])
			}
		}
		if command != "/usr/local/bin/memray attach 7" {
			t.Errorf("command = %q, want the unescalated command line", command)
		}
	})
}

// TestProfileEscalatesThroughSudo asserts that a non-root agent prefixes every
// profiler command with `sudo -n`, mirroring the four Python call sites that
// inspect _can_passwordless_sudo (trace_dump, cpu_profile, attach_profiler and
// detach_profiler).
func TestProfileEscalatesThroughSudo(t *testing.T) {
	for _, tc := range []struct {
		name string
		// call runs the profiler operation against a manager rooted at dir.
		call func(t *testing.T, m *ProfileManager, dir string)
		// wantPrefix is the expected beginning of the escalated argv, starting
		// with the sudo prefix and then the profiler binary.
		wantPrefix []string
	}{
		{
			name: "TraceDump",
			call: func(t *testing.T, m *ProfileManager, _ string) {
				t.Helper()
				if success, output := m.TraceDump(123, false, false); !success {
					t.Fatalf("TraceDump success = false (%s), want true", output)
				}
			},
			wantPrefix: []string{"-n", testProfilerPath("py-spy"), "dump", "-p", "123"},
		},
		{
			name: "CPUProfile",
			call: func(t *testing.T, m *ProfileManager, dir string) {
				t.Helper()
				if err := os.WriteFile(dir+"/flamegraph_9_cpu_profiling.svg", []byte("<svg/>"), 0644); err != nil {
					t.Fatalf("WriteFile error: %v", err)
				}
				if success, output := m.CPUProfile(9, "flamegraph", 5, false, false); !success {
					t.Fatalf("CPUProfile success = false (%s), want true", output)
				}
			},
			wantPrefix: []string{"-n", testProfilerPath("py-spy"), "record"},
		},
		{
			name: "MemoryProfileAttach",
			call: func(t *testing.T, m *ProfileManager, _ string) {
				t.Helper()
				if success, _, output := m.MemoryProfileAttach(5, false, false, false); !success {
					t.Fatalf("MemoryProfileAttach success = false (%s), want true", output)
				}
			},
			wantPrefix: []string{"-n", testProfilerPath("memray"), "attach", "5", "-o"},
		},
		{
			name: "MemoryProfileDetach",
			call: func(t *testing.T, m *ProfileManager, _ string) {
				t.Helper()
				if success, output := m.MemoryProfileDetach(5, false); !success {
					t.Fatalf("MemoryProfileDetach success = false (%s), want true", output)
				}
			},
			wantPrefix: []string{"-n", testProfilerPath("memray"), "detach", "5"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubPasswordlessSudo(t, true)
			stubLookPath(t)

			cap := &cmdCapture{}
			orig := runCommandDetail
			runCommandDetail = capturingExecutor(cap, okResult())
			defer func() { runCommandDetail = orig }()

			dir := t.TempDir()
			tc.call(t, newTestManager(t, dir), dir)

			if cap.callCount() != 1 {
				t.Fatalf("%s executed %d commands, want 1 (the escalated profiler)", tc.name, cap.callCount())
			}
			if cap.names[0] != "sudo" {
				t.Errorf("command = %q, want sudo", cap.names[0])
			}
			args := cap.lastArgs()
			if len(args) < len(tc.wantPrefix) {
				t.Fatalf("%s escalated args = %v, want at least %v", tc.name, args, tc.wantPrefix)
			}
			for i, want := range tc.wantPrefix {
				if args[i] != want {
					t.Errorf("%s args[%d] = %q, want %q (full argv %v)", tc.name, i, args[i], want, args)
				}
			}
			for _, a := range args {
				if a == "sudo" {
					t.Errorf("%s args = %v, want a single sudo prefix", tc.name, args)
					break
				}
			}
		})
	}
}

// TestProfileFailureMessageNamesTheEscalatedCommand asserts that the error
// surfaced to the head node shows the escalated command line, so an operator
// can tell whether sudo was actually attempted.
func TestProfileFailureMessageNamesTheEscalatedCommand(t *testing.T) {
	stubPasswordlessSudo(t, true)
	stubLookPath(t)

	orig := runCommandDetail
	runCommandDetail = func(name string, args ...string) CommandResult {
		if name == "sudo" && len(args) > 1 && args[0] == "-n" && args[1] == "true" {
			return CommandResult{ExitCode: 0}
		}
		return CommandResult{Stderr: "operation not permitted", ExitCode: 1}
	}
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, t.TempDir())
	success, output := m.TraceDump(123, false, false)
	if success {
		t.Fatalf("TraceDump success = true, want false on a failed command")
	}
	if !strings.Contains(output, "sudo -n "+testProfilerPath("py-spy")+" dump -p 123") {
		t.Errorf("output = %q, want the escalated command line quoted", output)
	}
}

// TestProfileMemoryResultIsNotEscalated pins the Python parity detail that
// get_profile_result converts an existing local file and therefore never
// prefixes sudo.
func TestProfileMemoryResultIsNotEscalated(t *testing.T) {
	stubPasswordlessSudo(t, true)
	stubLookPath(t)

	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	dir := t.TempDir()
	m := newTestManager(t, dir)
	if err := os.WriteFile(dir+"/memray/5_memory_profiling_1.bin", []byte("bin"), 0644); err != nil {
		t.Fatalf("WriteFile bin error: %v", err)
	}
	if err := os.WriteFile(dir+"/memray/5_memory_profiling_1.html", []byte("<html/>"), 0644); err != nil {
		t.Fatalf("WriteFile html error: %v", err)
	}

	if success, output := m.MemoryProfileResult(5, "5_memory_profiling_1.bin", "flamegraph", false); !success {
		t.Fatalf("MemoryProfileResult success = false (%s), want true", output)
	}
	if cap.callCount() != 1 {
		t.Fatalf("MemoryProfileResult executed %d commands, want 1 (no sudo probe)", cap.callCount())
	}
	if cap.names[0] != testProfilerPath("memray") {
		t.Errorf("command = %q, want memray without escalation", cap.names[0])
	}
}

func TestProfileTraceDumpCommandConstruction(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, _ := m.TraceDump(123, false, false)

	if !success {
		t.Fatalf("TraceDump(123) success = false, want true")
	}
	if cap.callCount() != 1 {
		t.Fatalf("TraceDump executed %d commands, want 1", cap.callCount())
	}
	if cap.names[0] != testProfilerPath("py-spy") {
		t.Errorf("command name = %q, want py-spy", cap.names[0])
	}
	args := cap.lastArgs()
	want := []string{"dump", "-p", "123"}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("args[%d] = %q, want %q", i, args[i], want[i])
		}
	}
}

func TestProfileTraceDumpNativeAddsFlag(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	cap := &cmdCapture{}
	orig := runCommandDetail
	origOS := osName
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() {
		runCommandDetail = orig
		osName = origOS
	}()

	osName = "linux"
	m := newTestManager(t, "/tmp/profiles")
	m.TraceDump(7, true, false)

	args := cap.lastArgs()
	found := false
	for _, a := range args {
		if a == "--native" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("TraceDump(native=true) args = %v, want to include --native", args)
	}
}

func TestProfileTraceDumpNativeOmittedOnNonLinux(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	cap := &cmdCapture{}
	orig := runCommandDetail
	origOS := osName
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() {
		runCommandDetail = orig
		osName = origOS
	}()

	osName = "darwin"
	m := newTestManager(t, "/tmp/profiles")
	m.TraceDump(7, true, false)

	args := cap.lastArgs()
	for _, a := range args {
		if a == "--native" {
			t.Errorf("TraceDump(native=true) on darwin args = %v, must NOT include --native", args)
		}
	}
}

func TestProfileTraceDumpResultMapping(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	orig := runCommandDetail
	defer func() { runCommandDetail = orig }()

	t.Run("success", func(t *testing.T) {
		runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stdout: "trace-data", ExitCode: 0})
		m := newTestManager(t, "/tmp/profiles")
		success, output := m.TraceDump(1, false, false)
		if !success {
			t.Errorf("success = false, want true")
		}
		if output != "trace-data" {
			t.Errorf("output = %q, want trace-data", output)
		}
	})

	t.Run("failure", func(t *testing.T) {
		runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stdout: "out", Stderr: "permission denied", ExitCode: 1})
		m := newTestManager(t, "/tmp/profiles")
		success, output := m.TraceDump(1, false, false)
		if success {
			t.Errorf("success = true, want false on non-zero exit")
		}
		if !strings.Contains(output, "permission denied") {
			t.Errorf("output = %q, want to include stderr", output)
		}
	})
}

func TestProfileCPUProfileCommandConstruction(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	dir := t.TempDir()
	m := newTestManager(t, dir)
	if err := os.WriteFile(dir+"/flamegraph_9_cpu_profiling.svg", []byte("<svg/>"), 0644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}
	success, _ := m.CPUProfile(9, "flamegraph", 5, false, false)

	if !success {
		t.Fatalf("CPUProfile success = false, want true")
	}
	if cap.callCount() != 1 {
		t.Fatalf("CPUProfile executed %d commands, want 1", cap.callCount())
	}
	args := cap.lastArgs()
	if cap.names[0] != testProfilerPath("py-spy") {
		t.Errorf("command name = %q, want py-spy", cap.names[0])
	}
	if len(args) < 7 {
		t.Fatalf("args = %v, want full record command", args)
	}
	if args[0] != "record" {
		t.Errorf("args[0] = %q, want record", args[0])
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-o") || !strings.Contains(joined, "-p") ||
		!strings.Contains(joined, "-d 5") || !strings.Contains(joined, "-f") {
		t.Errorf("args = %v, want -o -p -d -f present", args)
	}
	if !strings.Contains(joined, strconv.Itoa(9)) {
		t.Errorf("args = %v, want pid 9 present", args)
	}
}

func TestProfileCPUProfileInvalidFormat(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, output := m.CPUProfile(9, "bogus", 5, false, false)

	if success {
		t.Errorf("CPUProfile(bogus) success = true, want false")
	}
	if output == "" {
		t.Errorf("CPUProfile(bogus) output is empty, want error message")
	}
	if cap.callCount() != 0 {
		t.Errorf("CPUProfile(bogus) executed %d commands, want 0", cap.callCount())
	}
}

func TestProfileCPUProfileInvalidPID(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, _ := m.CPUProfile(-3, "flamegraph", 5, false, false)

	if success {
		t.Errorf("CPUProfile(-3) success = true, want false")
	}
	if cap.callCount() != 0 {
		t.Errorf("CPUProfile(-3) executed %d commands, want 0", cap.callCount())
	}
}

func TestProfileMemoryAttach(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, filename, output := m.MemoryProfileAttach(5, false, false, false)

	if !success {
		t.Fatalf("MemoryProfileAttach success = false, want true")
	}
	if !strings.HasSuffix(filename, ".bin") {
		t.Errorf("filename = %q, want .bin suffix", filename)
	}
	if !strings.Contains(output, "Success attaching memray") {
		t.Errorf("output = %q, want attach success message", output)
	}
	if cap.callCount() != 1 {
		t.Fatalf("MemoryProfileAttach executed %d commands, want 1", cap.callCount())
	}
	if cap.names[0] != testProfilerPath("memray") {
		t.Errorf("command name = %q, want memray", cap.names[0])
	}
	args := cap.lastArgs()
	if len(args) < 3 {
		t.Fatalf("args = %v, want attach command", args)
	}
	if args[0] != "attach" || args[1] != "5" || args[2] != "-o" {
		t.Errorf("args = %v, want [attach 5 -o ...]", args)
	}
}

func TestProfileMemoryAttachDegradesOnFailure(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stderr: "memray not found", ExitCode: 1})
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, filename, output := m.MemoryProfileAttach(5, false, false, false)

	if success {
		t.Errorf("MemoryProfileAttach success = true, want false")
	}
	if filename != "" {
		t.Errorf("filename = %q, want empty on failure", filename)
	}
	if !strings.Contains(output, "memray not found") {
		t.Errorf("output = %q, want stderr in error message", output)
	}
}

func TestProfileMemoryAttachRejectsInvalidPID(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, _, _ := m.MemoryProfileAttach(0, false, false, false)

	if success {
		t.Errorf("MemoryProfileAttach(0) success = true, want false")
	}
	if cap.callCount() != 0 {
		t.Errorf("MemoryProfileAttach(0) executed %d commands, want 0", cap.callCount())
	}
}

func TestProfileMemoryDetach(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, _ := m.MemoryProfileDetach(5, false)

	if !success {
		t.Fatalf("MemoryProfileDetach success = false, want true")
	}
	if cap.callCount() != 1 {
		t.Fatalf("MemoryProfileDetach executed %d commands, want 1", cap.callCount())
	}
	args := cap.lastArgs()
	if cap.names[0] != testProfilerPath("memray") || args[0] != "detach" || args[len(args)-1] != "5" {
		t.Errorf("command = %s %v, want memray detach ... 5", cap.names[0], args)
	}
}

func TestProfileMemoryResult(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	orig := runCommandDetail
	defer func() { runCommandDetail = orig }()

	t.Run("flamegraph", func(t *testing.T) {
		dir := t.TempDir()
		m := newTestManager(t, dir)
		if err := os.WriteFile(dir+"/memray/5_memory_profiling_1.bin", []byte("bin"), 0644); err != nil {
			t.Fatalf("WriteFile bin error: %v", err)
		}
		html := []byte("<html>flamegraph</html>")
		if err := os.WriteFile(dir+"/memray/5_memory_profiling_1.html", html, 0644); err != nil {
			t.Fatalf("WriteFile html error: %v", err)
		}

		cap := &cmdCapture{}
		runCommandDetail = capturingExecutor(cap, okResult())
		success, output := m.MemoryProfileResult(5, "5_memory_profiling_1.bin", "flamegraph", false)
		if !success {
			t.Fatalf("MemoryProfileResult success = false, want true")
		}
		if output != string(html) {
			t.Errorf("output = %q, want html content %q", output, string(html))
		}
		args := cap.lastArgs()
		if cap.names[0] != testProfilerPath("memray") || args[0] != "flamegraph" {
			t.Errorf("command = %s %v, want memray flamegraph ...", cap.names[0], args)
		}
	})

	t.Run("unsupported format", func(t *testing.T) {
		dir := t.TempDir()
		m := newTestManager(t, dir)
		if err := os.WriteFile(dir+"/memray/5_memory_profiling_1.bin", []byte("bin"), 0644); err != nil {
			t.Fatalf("WriteFile bin error: %v", err)
		}

		cap := &cmdCapture{}
		runCommandDetail = capturingExecutor(cap, okResult())
		success, output := m.MemoryProfileResult(5, "5_memory_profiling_1.bin", "bogus", false)
		if success {
			t.Errorf("MemoryProfileResult(bogus) success = true, want false")
		}
		if output == "" {
			t.Errorf("output is empty, want error message")
		}
		if cap.callCount() != 0 {
			t.Errorf("MemoryProfileResult(bogus) executed %d commands, want 0", cap.callCount())
		}
	})

	t.Run("injection filename", func(t *testing.T) {
		dir := t.TempDir()
		m := newTestManager(t, dir)
		if err := os.WriteFile(dir+"/memray/a;rm -rf.bin", []byte("bin"), 0644); err != nil {
			t.Fatalf("WriteFile bin error: %v", err)
		}

		cap := &cmdCapture{}
		runCommandDetail = capturingExecutor(cap, okResult())
		success, _ := m.MemoryProfileResult(5, "a;rm -rf.bin", "flamegraph", false)
		if success {
			t.Errorf("MemoryProfileResult(injection) success = true, want false")
		}
		if cap.callCount() != 0 {
			t.Errorf("MemoryProfileResult(injection) executed %d commands, want 0", cap.callCount())
		}
	})
}

func TestProfileTraceDumpForwardsSubprocesses(t *testing.T) {
	stubLookPath(t)
	var cap cmdCapture
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(&cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := &ProfileManager{}
	m.TraceDump(1234, false, true)

	joined := strings.Join(cap.lastArgs(), " ")
	if !strings.Contains(joined, "--subprocesses") {
		t.Fatalf("py-spy dump args %v missing --subprocesses", cap.lastArgs())
	}
}

func TestProfileCPUProfileForwardsSubprocesses(t *testing.T) {
	stubLookPath(t)
	var cap cmdCapture
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(&cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := &ProfileManager{}
	m.CPUProfile(1234, "raw", 2, false, true)

	joined := strings.Join(cap.lastArgs(), " ")
	if !strings.Contains(joined, "--subprocesses") {
		t.Fatalf("py-spy record args %v missing --subprocesses", cap.lastArgs())
	}
}

func TestProfileCPUProfileOmitsSubprocessesWhenFalse(t *testing.T) {
	stubLookPath(t)
	var cap cmdCapture
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(&cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := &ProfileManager{}
	m.CPUProfile(1234, "raw", 2, false, false)

	for _, arg := range cap.lastArgs() {
		if arg == "--subprocesses" {
			t.Fatalf("py-spy record args %v unexpectedly contain --subprocesses", cap.lastArgs())
		}
	}
}

func TestProfileNewManagerCreatesDirectories(t *testing.T) {
	dir := t.TempDir()
	profileDir := dir + "/profiles"
	if _, err := NewProfileManager(profileDir, testNodeIP); err != nil {
		t.Fatalf("NewProfileManager error = %v", err)
	}
	for _, p := range []string{profileDir, profileDir + "/memray", profileDir + "/profiles"} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected directory %s to exist, got %v", p, err)
		}
	}
}

func TestProfileNewManagerErrorOnCreateFailure(t *testing.T) {
	file := t.TempDir() + "/afile"
	if err := os.WriteFile(file, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}
	if _, err := NewProfileManager(file+"/sub", testNodeIP); err == nil {
		t.Errorf("NewProfileManager under a file: err = nil, want error")
	}
}

func TestProfileCPUProfileReturnsFileContent(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	dir := t.TempDir()
	m := newTestManager(t, dir)
	profileFile := dir + "/flamegraph_9_cpu_profiling.svg"
	content := []byte("<svg>flamegraph</svg>")
	if err := os.WriteFile(profileFile, content, 0644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	orig := runCommandDetail
	runCommandDetail = capturingExecutor(&cmdCapture{}, okResult())
	defer func() { runCommandDetail = orig }()

	success, output := m.CPUProfile(9, "flamegraph", 5, false, false)
	if !success {
		t.Fatalf("CPUProfile success = false, want true")
	}
	if output != string(content) {
		t.Errorf("output = %q, want file content %q", output, string(content))
	}
}

func TestProfileGpuProfileRejectsInvalidPID(t *testing.T) {
	stubGpuProfiling(t, true, "/usr/bin/dynolog", "/usr/bin/dyno", func(int) bool { return true })
	m := newTestManager(t, t.TempDir())

	for _, pid := range []int{0, -1, -1234} {
		success, output, err := m.GpuProfile(context.Background(), pid, 10)
		if err != nil {
			t.Errorf("GpuProfile(pid=%d) error = %v, want nil", pid, err)
		}
		if success {
			t.Errorf("GpuProfile(pid=%d) success = true, want false", pid)
		}
		if !strings.Contains(output, "invalid pid") {
			t.Errorf("GpuProfile(pid=%d) output = %q, want invalid-pid message", pid, output)
		}
	}
}

func TestProfileGpuProfileDisabledWithoutGPUs(t *testing.T) {
	stubGpuProfiling(t, false, "/usr/bin/dynolog", "/usr/bin/dyno", func(int) bool { return true })
	m := newTestManager(t, t.TempDir())

	success, output, err := m.GpuProfile(context.Background(), 123, 10)
	if err != nil {
		t.Errorf("GpuProfile error = %v, want nil (a disabled node is a reply, not an error)", err)
	}
	if success {
		t.Errorf("GpuProfile success = true, want false (disabled)")
	}
	want := "GPU profiling is not enabled on node " + testNodeIP + ". " +
		"This is the case if no GPUs are detected on the node or if " +
		"the profiling dependency `dynolog` is not installed on the node.\n" +
		"Please ensure that GPUs are available on the node and that " +
		"`dynolog` is installed."
	if output != want {
		t.Errorf("GpuProfile output =\n%q\nwant\n%q", output, want)
	}
}

func TestProfileGpuProfileDisabledWithoutDynolog(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dynologPath string
		dynoPath    string
	}{
		{name: "dynolog missing", dynologPath: "", dynoPath: "/usr/bin/dyno"},
		{name: "dyno missing", dynologPath: "/usr/bin/dynolog", dynoPath: ""},
		{name: "both missing", dynologPath: "", dynoPath: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubGpuProfiling(t, true, tc.dynologPath, tc.dynoPath, func(int) bool { return true })
			m := newTestManager(t, t.TempDir())

			success, output, err := m.GpuProfile(context.Background(), 123, 10)
			if err != nil {
				t.Errorf("GpuProfile error = %v, want nil (a missing tool is a reply, not an error)", err)
			}
			if success {
				t.Errorf("GpuProfile success = true, want false (disabled)")
			}
			if !strings.Contains(output, "GPU profiling is not enabled on node "+testNodeIP) {
				t.Errorf("GpuProfile output = %q, want disabled message naming the node", output)
			}
		})
	}
}

func TestProfileGpuProfileDeadProcess(t *testing.T) {
	stubGpuProfiling(t, true, "/usr/bin/dynolog", "/usr/bin/dyno", func(int) bool { return false })
	m := newGPUManagerWithDaemon(t)

	success, output, err := m.GpuProfile(context.Background(), 4242, 10)
	if err != nil {
		t.Errorf("GpuProfile error = %v, want nil (a dead target is a reply, not an error)", err)
	}
	if success {
		t.Errorf("GpuProfile success = true, want false (dead process)")
	}
	want := "The requested process to profile with pid=4242 on node " +
		testNodeIP + " is no longer running. " +
		"GPU profiling is not available for this process."
	if output != want {
		t.Errorf("GpuProfile output =\n%q\nwant\n%q", output, want)
	}
	if strings.Contains(output, "not enabled on node") {
		t.Errorf("GpuProfile output = %q, want the dead-process message, not the disabled one", output)
	}
}

// TestProfileGpuProfileDisabledTakesPrecedence pins the check order of the
// Python gpu_profile: the node capability is evaluated before the process
// liveness, so a disabled node reports the disabled message even when the pid
// is also gone.
func TestProfileGpuProfileDisabledTakesPrecedence(t *testing.T) {
	stubGpuProfiling(t, false, "", "", func(int) bool { return false })
	m := newTestManager(t, t.TempDir())

	success, output, err := m.GpuProfile(context.Background(), 4242, 10)
	if err != nil {
		t.Errorf("GpuProfile error = %v, want nil (a disabled node wins over the dead process)", err)
	}
	if success {
		t.Errorf("GpuProfile success = true, want false (disabled)")
	}
	if !strings.Contains(output, "GPU profiling is not enabled on node "+testNodeIP) {
		t.Errorf("GpuProfile output = %q, want the disabled message", output)
	}
	if strings.Contains(output, "is no longer running") {
		t.Errorf("GpuProfile output = %q, want the disabled message to win over the dead-process one", output)
	}
}

// TestProfileGpuProfileWithoutDaemonReturnsError pins the one state Python
// models as an exception: gpu_profile ran on an enabled node before
// start_monitoring_daemon, so the caller must surface an error instead of a
// failed-but-successful reply.
func TestProfileGpuProfileWithoutDaemonReturnsError(t *testing.T) {
	stubGpuProfiling(t, true, "/usr/bin/dynolog", "/usr/bin/dyno", func(int) bool { return true })

	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, t.TempDir())
	success, output, err := m.GpuProfile(context.Background(), 123, 10)

	if err == nil {
		t.Fatal("GpuProfile returned no error although no daemon was started")
	}
	if !strings.Contains(err.Error(), gpuProfilingDaemonNotStartedMessage) {
		t.Errorf("GpuProfile error = %v, want it to contain %q", err, gpuProfilingDaemonNotStartedMessage)
	}
	if success || output != "" {
		t.Errorf("GpuProfile success = %v, output = %q, want (false, \"\") alongside the error", success, output)
	}
	if cap.callCount() != 0 {
		t.Errorf("GpuProfile executed %d commands, want 0 (no dyno run without a daemon)", cap.callCount())
	}
}

func TestNewProfileManagerResolvesDynologTools(t *testing.T) {
	stubGpuProfiling(t, true, "/opt/dynolog/bin/dynolog", "/opt/dynolog/bin/dyno", nil)

	m := newTestManager(t, t.TempDir())
	if m.dynologBin != "/opt/dynolog/bin/dynolog" {
		t.Errorf("dynologBin = %q, want the resolved path", m.dynologBin)
	}
	if m.dynoBin != "/opt/dynolog/bin/dyno" {
		t.Errorf("dynoBin = %q, want the resolved path", m.dynoBin)
	}
	if !m.gpuProfilingEnabled() {
		t.Error("gpuProfilingEnabled() = false, want true with GPUs and both tools present")
	}
}

func TestProfileManagerGpuProfilingEnabledRequiresAll(t *testing.T) {
	for _, tc := range []struct {
		name        string
		hasGPUs     bool
		dynologPath string
		dynoPath    string
		want        bool
	}{
		{name: "all present", hasGPUs: true, dynologPath: "/d", dynoPath: "/o", want: true},
		{name: "no GPUs", hasGPUs: false, dynologPath: "/d", dynoPath: "/o", want: false},
		{name: "no dynolog", hasGPUs: true, dynologPath: "", dynoPath: "/o", want: false},
		{name: "no dyno", hasGPUs: true, dynologPath: "/d", dynoPath: "", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubGpuProfiling(t, tc.hasGPUs, tc.dynologPath, tc.dynoPath, nil)
			m := newTestManager(t, t.TempDir())
			if got := m.gpuProfilingEnabled(); got != tc.want {
				t.Errorf("gpuProfilingEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// stubGPUProbeArgv points the nvidia-smi probe at a fake binary for the
// duration of a test, so no test depends on the build machine's nvidia-smi.
func stubGPUProbeArgv(t *testing.T, binary string, args ...string) {
	t.Helper()
	origBinary, origArgs := gpuProbeBinary, gpuProbeArgs
	gpuProbeBinary, gpuProbeArgs = binary, args
	t.Cleanup(func() { gpuProbeBinary, gpuProbeArgs = origBinary, origArgs })
}

// TestGPUProbeMatchesPythonArguments pins the probe argv to the command
// GpuProfilingManager.node_has_gpus runs.
func TestGPUProbeMatchesPythonArguments(t *testing.T) {
	if gpuProbeBinary != "nvidia-smi" {
		t.Errorf("gpuProbeBinary = %q, want %q", gpuProbeBinary, "nvidia-smi")
	}
	want := []string{"--query-gpu=name", "--format=csv,noheader"}
	if strings.Join(gpuProbeArgs, " ") != strings.Join(want, " ") {
		t.Errorf("gpuProbeArgs = %v, want %v", gpuProbeArgs, want)
	}
}

// TestProbeNodeHasGPUs covers the production nvidia-smi probe: only a
// successful exit counts as "GPUs present".
func TestProbeNodeHasGPUs(t *testing.T) {
	cases := []struct {
		name string
		argv []string
		want bool
	}{
		{"success", []string{"/bin/true"}, true},
		{"missing binary", []string{"/nonexistent/nvidia-smi"}, false},
		{"non-zero exit", []string{"/bin/false"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stubGPUProbeArgv(t, c.argv[0], c.argv[1:]...)
			if got := probeNodeHasGPUs(); got != c.want {
				t.Errorf("probeNodeHasGPUs() = %v, want %v", got, c.want)
			}
		})
	}
}

// TestProbeNodeHasGPUsHonoursTheTimeout pins the timeout contract: a probe that
// never returns must be killed at gpuProbeTimeout and report "no GPUs" instead
// of blocking the reporter constructor, which runs on the agent startup path.
func TestProbeNodeHasGPUsHonoursTheTimeout(t *testing.T) {
	const collapsed = 50 * time.Millisecond
	origTimeout := gpuProbeTimeout
	gpuProbeTimeout = collapsed
	t.Cleanup(func() { gpuProbeTimeout = origTimeout })

	// A child that outlives the collapsed timeout by a wide margin.
	stubGPUProbeArgv(t, "/bin/sleep", "30")

	start := time.Now()
	if probeNodeHasGPUs() {
		t.Fatal("probeNodeHasGPUs() = true for a probe that never exits, want false")
	}
	elapsed := time.Since(start)
	if elapsed >= 2*time.Second {
		t.Fatalf("probe took %v, want it bounded by gpuProbeTimeout (%v): the probe is unbounded", elapsed, collapsed)
	}
	if elapsed < collapsed {
		t.Fatalf("probe returned after %v, want at least gpuProbeTimeout (%v) so the timeout was the reason", elapsed, collapsed)
	}
}

func TestGpuProfilingPIDAlive(t *testing.T) {
	if !gpuProfilingPIDAlive(os.Getpid()) {
		t.Error("gpuProfilingPIDAlive(current process) = false, want true")
	}
	// A pid that cannot be read from /proc is not alive.
	if gpuProfilingPIDAlive(-1) {
		t.Error("gpuProfilingPIDAlive(-1) = true, want false")
	}

	// A zombie child is readable through /proc but must not count as alive,
	// mirroring psutil.ZombieProcess handling in the Python implementation.
	child := exec.Command("/bin/true")
	if err := child.Start(); err != nil {
		t.Fatalf("starting a child process to become a zombie failed: %v", err)
	}
	pid := child.Process.Pid
	defer func() { _ = child.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	var state byte
	for {
		s, err := processState(pid)
		if err == nil && s == procStateZombie {
			state = s
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child pid %d never reached the zombie state (last state %q, err %v)", pid, s, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if state != procStateZombie {
		t.Fatalf("child pid %d state = %q, want zombie", pid, state)
	}
	if gpuProfilingPIDAlive(pid) {
		t.Errorf("gpuProfilingPIDAlive(zombie pid %d) = true, want false", pid)
	}
}

func TestProfileCPUProfileRejectsInvalidDuration(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	for _, duration := range []int{0, -1, 301, 9999} {
		success, output := m.CPUProfile(9, "flamegraph", duration, false, false)
		if success {
			t.Errorf("CPUProfile(duration=%d) success = true, want false", duration)
		}
		if output == "" {
			t.Errorf("CPUProfile(duration=%d) output is empty, want error message", duration)
		}
		if cap.callCount() != 0 {
			t.Fatalf("CPUProfile(duration=%d) executed %d commands, want 0", duration, cap.callCount())
		}
	}
}

func TestProfileMemoryResultProfileFileNotExist(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	dir := t.TempDir()
	m := newTestManager(t, dir)
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	success, output := m.MemoryProfileResult(5, "5_memory_profiling_1.bin", "flamegraph", false)
	if success {
		t.Errorf("MemoryProfileResult success = true, want false")
	}
	want := "Failed to execute: process 5 has not been profiled"
	if output != want {
		t.Errorf("output = %q, want %q", output, want)
	}
	if cap.callCount() != 0 {
		t.Errorf("executed %d commands, want 0 (profile file missing)", cap.callCount())
	}
}
