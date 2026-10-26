package reporter

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// newTestManager builds a ProfileManager for the given directory, failing the
// test if directory creation fails.
func newTestManager(t *testing.T, dir string) *ProfileManager {
	t.Helper()
	m, err := NewProfileManager(dir)
	if err != nil {
		t.Fatalf("NewProfileManager(%q) error = %v", dir, err)
	}
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
		"a;b",     // command separator
		"a&b",     // background / and
		"a|b",     // pipe
		"a$b",     // variable expansion
		"a`b",     // backtick command substitution
		"a b",     // whitespace
		"../etc",  // path traversal
		"a..b",    // dot-dot sequence
		"a\nb",    // newline
		"a>b",     // redirect
		"a<b",     // redirect
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
	success, output := m.TraceDump(0, false)

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

func TestProfileTraceDumpCommandConstruction(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, _ := m.TraceDump(123, false)

	if !success {
		t.Fatalf("TraceDump(123) success = false, want true")
	}
	if cap.callCount() != 1 {
		t.Fatalf("TraceDump executed %d commands, want 1", cap.callCount())
	}
	if cap.names[0] != "py-spy" {
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
	m.TraceDump(7, true)

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
	m.TraceDump(7, true)

	args := cap.lastArgs()
	for _, a := range args {
		if a == "--native" {
			t.Errorf("TraceDump(native=true) on darwin args = %v, must NOT include --native", args)
		}
	}
}

func TestProfileTraceDumpResultMapping(t *testing.T) {
	orig := runCommandDetail
	defer func() { runCommandDetail = orig }()

	t.Run("success", func(t *testing.T) {
		runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stdout: "trace-data", ExitCode: 0})
		m := newTestManager(t, "/tmp/profiles")
		success, output := m.TraceDump(1, false)
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
		success, output := m.TraceDump(1, false)
		if success {
			t.Errorf("success = true, want false on non-zero exit")
		}
		if !strings.Contains(output, "permission denied") {
			t.Errorf("output = %q, want to include stderr", output)
		}
	})
}

func TestProfileCPUProfileCommandConstruction(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	dir := t.TempDir()
	m := newTestManager(t, dir)
	if err := os.WriteFile(dir+"/flamegraph_9_cpu_profiling.svg", []byte("<svg/>"), 0644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}
	success, _ := m.CPUProfile(9, "flamegraph", 5, false)

	if !success {
		t.Fatalf("CPUProfile success = false, want true")
	}
	if cap.callCount() != 1 {
		t.Fatalf("CPUProfile executed %d commands, want 1", cap.callCount())
	}
	args := cap.lastArgs()
	if cap.names[0] != "py-spy" {
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
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, output := m.CPUProfile(9, "bogus", 5, false)

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
	success, _ := m.CPUProfile(-3, "flamegraph", 5, false)

	if success {
		t.Errorf("CPUProfile(-3) success = true, want false")
	}
	if cap.callCount() != 0 {
		t.Errorf("CPUProfile(-3) executed %d commands, want 0", cap.callCount())
	}
}

func TestProfileMemoryAttach(t *testing.T) {
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
	if cap.names[0] != "memray" {
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
	if cap.names[0] != "memray" || args[0] != "detach" || args[len(args)-1] != "5" {
		t.Errorf("command = %s %v, want memray detach ... 5", cap.names[0], args)
	}
}

func TestProfileMemoryResult(t *testing.T) {
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
		if cap.names[0] != "memray" || args[0] != "flamegraph" {
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

func TestProfileNewManagerCreatesDirectories(t *testing.T) {
	dir := t.TempDir()
	profileDir := dir + "/profiles"
	if _, err := NewProfileManager(profileDir); err != nil {
		t.Fatalf("NewProfileManager error = %v", err)
	}
	for _, p := range []string{profileDir, profileDir + "/memray"} {
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
	if _, err := NewProfileManager(file + "/sub"); err == nil {
		t.Errorf("NewProfileManager under a file: err = nil, want error")
	}
}

func TestProfileCPUProfileReturnsFileContent(t *testing.T) {
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

	success, output := m.CPUProfile(9, "flamegraph", 5, false)
	if !success {
		t.Fatalf("CPUProfile success = false, want true")
	}
	if output != string(content) {
		t.Errorf("output = %q, want file content %q", output, string(content))
	}
}

func TestProfileGpuProfileDegrades(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	success, output := m.GpuProfile(123, 10)

	if success {
		t.Errorf("GpuProfile success = true, want false (degraded)")
	}
	if !strings.Contains(output, "GPU profiling is not supported") {
		t.Errorf("GpuProfile output = %q, want degradation message", output)
	}
	if cap.callCount() != 0 {
		t.Errorf("GpuProfile executed %d commands, want 0 (no external command)", cap.callCount())
	}
}

func TestProfileCPUProfileRejectsInvalidDuration(t *testing.T) {
	cap := &cmdCapture{}
	orig := runCommandDetail
	runCommandDetail = capturingExecutor(cap, okResult())
	defer func() { runCommandDetail = orig }()

	m := newTestManager(t, "/tmp/profiles")
	for _, duration := range []int{0, -1, 301, 9999} {
		success, output := m.CPUProfile(9, "flamegraph", duration, false)
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
