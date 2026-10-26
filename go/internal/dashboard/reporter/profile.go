package reporter

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// CommandResult captures the outcome of an external command execution,
// exposing stdout, stderr and the exit code separately so callers can map
// them onto profiler success/failure semantics.
type CommandResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// commandExecutor runs an external command with separated stdout/stderr and
// captures the exit code. It is a package-level variable so tests can inject
// a fake and avoid invoking the real py-spy/memray binaries.
type commandExecutor func(name string, args ...string) CommandResult

// profilerCommandTimeout bounds how long a single external profiler command may
// run before it is killed. It is derived from maxCPUDurationSeconds (the longest
// accepted sampling window) plus a small margin, so a hung py-spy/memray process
// can never block the reporter worker indefinitely.
var profilerCommandTimeout = maxCPUDurationSeconds*time.Second + 10*time.Second

// runCommandDetail is the production commandExecutor wrapping os/exec. A
// negative exit code signals a launch failure (e.g. binary not found) rather
// than a non-zero process exit. The command runs under profilerCommandTimeout;
// if it exceeds that deadline the process is killed and a non-zero exit code is
// reported so callers degrade instead of blocking forever.
var runCommandDetail commandExecutor = func(name string, args ...string) CommandResult {
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), profilerCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			code = -1
		}
	}
	return CommandResult{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: code}
}

// osName reports the current operating system and is overridable in tests so
// the linux-only --native flag can be exercised deterministically.
var osName = runtime.GOOS

// shellMeta is the set of shell metacharacters and whitespace that must never
// appear in externally supplied tokens that are appended to a command line.
const shellMeta = ";&|$`<>\"'\\()*?[]{}! \t\r\n"

// validatePID rejects non-positive pids, which are the only invalid integer
// inputs accepted by the profiler commands. A pid must be strictly positive.
func validatePID(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d: must be a positive integer", pid)
	}
	return nil
}

// maxCPUDurationSeconds bounds the py-spy sampling duration to prevent
// unbounded sampling runs.
const maxCPUDurationSeconds = 300

// validateDuration rejects sampling durations outside the supported range.
// A duration must be strictly positive and no greater than
// maxCPUDurationSeconds.
func validateDuration(duration int) error {
	if duration <= 0 {
		return fmt.Errorf("invalid duration %d: must be a positive integer", duration)
	}
	if duration > maxCPUDurationSeconds {
		return fmt.Errorf("invalid duration %d: must not exceed %d seconds", duration, maxCPUDurationSeconds)
	}
	return nil
}

// validateToken rejects tokens that contain shell metacharacters, whitespace
// or path-traversal sequences, preventing command injection via externally
// supplied file names, formats or paths. Only safe, printable tokens pass.
func validateToken(s string) error {
	if strings.Contains(s, "..") {
		return fmt.Errorf("invalid token %q: path traversal not allowed", s)
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return strings.ContainsRune(shellMeta, r) || r < 0x20 || r == 0x7f
	}) >= 0 {
		return fmt.Errorf("invalid token %q: contains shell metacharacters or whitespace", s)
	}
	return nil
}

// ProfileManager bridges the Python CpuProfilingManager and
// MemoryProfilingManager, executing py-spy and memray through an injectable
// command runner while validating all externally supplied arguments to prevent
// command injection.
type ProfileManager struct {
	profileDir string
}

// NewProfileManager builds a ProfileManager writing profile artifacts under
// the given directory, creating the directory and its memray subdirectory if
// they do not yet exist (mirroring the Python constructors). It returns an
// error if either directory cannot be created.
func NewProfileManager(profileDir string) (*ProfileManager, error) {
	if err := os.MkdirAll(profileDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create profile dir %q: %w", profileDir, err)
	}
	if err := os.MkdirAll(profileDir+"/memray", 0755); err != nil {
		return nil, fmt.Errorf("failed to create memray dir %q: %w", profileDir+"/memray", err)
	}
	return &ProfileManager{profileDir: profileDir}, nil
}

// formatFailedCommand builds a human-readable failure message embedding the
// failed command together with its captured stderr and stdout, mirroring the
// Python _format_failed_profiler_command helper.
func formatFailedCommand(cmd string, res CommandResult) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Failed to execute `%s`.\n", cmd))
	if res.Stderr != "" {
		b.WriteString("=== stderr ===\n")
		b.WriteString(res.Stderr)
		b.WriteString("\n")
	}
	if res.Stdout != "" {
		b.WriteString("=== stdout ===\n")
		b.WriteString(res.Stdout)
		b.WriteString("\n")
	}
	return b.String()
}

// TraceDump captures a stack trace for pid via `py-spy dump -p <pid>`,
// appending --native on linux when native is requested. It returns
// (success, output) where output is the trace on success or an error message
// on failure.
func (m *ProfileManager) TraceDump(pid int, native bool) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}

	args := []string{"dump", "-p", strconv.Itoa(pid)}
	if osName == "linux" && native {
		args = append(args, "--native")
	}
	res := runCommandDetail("py-spy", args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand("py-spy "+strings.Join(args, " "), res)
	}
	return true, res.Stdout
}

// CPUProfile samples the process for duration seconds via `py-spy record`
// writing the result in the requested format. Valid formats are flamegraph,
// raw and speedscope, matching the Python CpuProfilingManager.
func (m *ProfileManager) CPUProfile(pid int, format string, duration int, native bool) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}
	if err := validateDuration(duration); err != nil {
		return false, err.Error()
	}
	switch format {
	case "flamegraph", "raw", "speedscope":
	default:
		return false, fmt.Sprintf(
			"Failed to execute: Invalid format %s, must be [flamegraph, raw, speedscope]", format)
	}

	extension := "txt"
	if format == "flamegraph" {
		extension = "svg"
	}
	profileFile := fmt.Sprintf("%s_%d_cpu_profiling.%s", format, pid, extension)
	if err := validateToken(profileFile); err != nil {
		return false, err.Error()
	}
	profilePath := m.profileDir + "/" + profileFile

	args := []string{
		"record",
		"-o", profilePath,
		"-p", strconv.Itoa(pid),
		"-d", strconv.Itoa(duration),
		"-f", format,
	}
	if osName == "linux" && native {
		args = append(args, "--native")
	}
	res := runCommandDetail("py-spy", args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand("py-spy "+strings.Join(args, " "), res)
	}
	data, err := os.ReadFile(profilePath)
	if err != nil {
		return false, fmt.Sprintf("Failed to read profile file %q: %v", profilePath, err)
	}
	return true, string(data)
}

// GpuProfile is a degraded implementation of GPU profiling. GPU profiling is
// not supported on the Go side and always returns a clear error without
// crashing, mirroring the Python GpuProfilingManager downgrade contract.
func (m *ProfileManager) GpuProfile(pid int, numIterations int) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}
	return false, "GPU profiling is not supported"
}

// MemoryProfileAttach attaches a memray profiler to pid, generating a
// profiler output file. It returns (success, filename, output); on failure the
// filename is empty and output carries the error message.
func (m *ProfileManager) MemoryProfileAttach(pid int, native, tracePythonAllocators, verbose bool) (bool, string, string) {
	if err := validatePID(pid); err != nil {
		return false, "", err.Error()
	}

	timestamp := time.Now().Format("20060102150405")
	filename := fmt.Sprintf("%d_memory_profiling_%s.bin", pid, timestamp)
	profilePath := m.profileDir + "/memray/" + filename

	args := []string{"attach", strconv.Itoa(pid), "-o", profilePath}
	if native {
		args = append(args, "--native")
	}
	if tracePythonAllocators {
		args = append(args, "--trace-python-allocators")
	}
	if verbose {
		args = append(args, "--verbose")
	}

	res := runCommandDetail("memray", args...)
	if res.ExitCode != 0 {
		return false, "", formatFailedCommand("memray "+strings.Join(args, " "), res)
	}
	return true, filename, fmt.Sprintf("Success attaching memray to process %d", pid)
}

// MemoryProfileDetach detaches the memray profiler from pid. It returns
// (success, output).
func (m *ProfileManager) MemoryProfileDetach(pid int, verbose bool) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}

	args := []string{"detach"}
	if verbose {
		args = append(args, "--verbose")
	}
	args = append(args, strconv.Itoa(pid))

	res := runCommandDetail("memray", args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand("memray "+strings.Join(args, " "), res)
	}
	return true, fmt.Sprintf("Success detaching memray from process %d", pid)
}

// MemoryProfileResult converts a recorded memray profile to the requested
// format (flamegraph or table). It degrades to an error message rather than
// panicking when memray is unavailable or the conversion fails.
func (m *ProfileManager) MemoryProfileResult(pid int, profilerFilename, format string, leaks bool) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}
	if err := validateToken(profilerFilename); err != nil {
		return false, err.Error()
	}

	var command string
	switch format {
	case "flamegraph":
		command = "flamegraph"
	case "table":
		command = "table"
	default:
		return false, fmt.Sprintf("Failed to execute: Report with format: %s is not supported", format)
	}

	profilePath := m.profileDir + "/memray/" + profilerFilename
	if _, err := os.Stat(profilePath); err != nil {
		return false, fmt.Sprintf("Failed to execute: process %d has not been profiled", pid)
	}
	base := strings.TrimSuffix(profilerFilename, ".bin")
	visualizePath := m.profileDir + "/memray/" + base + ".html"

	args := []string{command, "-o", visualizePath, "-f"}
	if leaks {
		args = append(args, "--leaks")
	}
	args = append(args, profilePath)

	res := runCommandDetail("memray", args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand("memray "+strings.Join(args, " "), res)
	}
	data, err := os.ReadFile(visualizePath)
	if err != nil {
		return false, fmt.Sprintf("Failed to read profile result %q: %v", visualizePath, err)
	}
	return true, string(data)
}
