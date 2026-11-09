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

	"github.com/ray-project/ray/go/pkg/log"
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

// probePasswordlessSudo runs `sudo -n true` and reports whether it succeeded,
// mirroring the Python _can_passwordless_sudo helper. The `-n` flag makes sudo
// fail immediately instead of prompting, so a zero exit code means the agent
// may escalate to root without an interactive terminal. A missing sudo binary
// surfaces as a launch failure (negative exit code) and counts as unavailable.
func probePasswordlessSudo() bool {
	return runCommandDetail("sudo", "-n", "true").ExitCode == 0
}

// canPasswordlessSudo reports whether profiler commands may be prefixed with
// `sudo -n`. It is a package-level variable so tests can pin the answer instead
// of depending on the sudo configuration of the build machine.
var canPasswordlessSudo = probePasswordlessSudo

// profilerArgv builds the argv for a profiler binary plus its arguments,
// prefixed with `sudo -n` when passwordless sudo is available, and returns it
// together with the space-joined command line used in failure messages.
// Escalation is what lets py-spy and memray attach to a worker process owned by
// another user in a non-root deployment. The path must already be absolute: it
// mirrors the resolved binary the Python call sites put at cmd[0].
func profilerArgv(path string, args ...string) ([]string, string) {
	argv := make([]string, 0, len(args)+3)
	if canPasswordlessSudo() {
		argv = append(argv, "sudo", "-n")
	}
	argv = append(argv, path)
	argv = append(argv, args...)
	return argv, strings.Join(argv, " ")
}

// resolveProfiler locates a profiler binary, returning its absolute path. It
// mirrors the `shutil.which(self.profiler_name)` pre-check of the Python
// profile managers: a bare binary name is not enough because every profiler
// command may be escalated through `sudo -n`, and sudo resets PATH to its
// hardened secure_path (typically /sbin:/bin:/usr/sbin:/usr/bin), which does
// not contain a user-level install such as ~/.local/bin/py-spy.
func resolveProfiler(name string) (string, error) {
	path, err := lookPath(name)
	if err != nil || path == "" {
		return "", fmt.Errorf("Failed to execute: %s is not installed", name)
	}
	return path, nil
}

// runProfiler executes a resolved profiler binary with the given arguments,
// escalating through passwordless sudo when it is available. It returns the
// captured result together with the full command line for error reporting.
func runProfiler(path string, args ...string) (CommandResult, string) {
	argv, command := profilerArgv(path, args...)
	if argv[0] == "sudo" {
		log.Log.V(1).Info("escalating the profiler command through passwordless sudo",
			"command", command)
	}
	return runCommandDetail(argv[0], argv[1:]...), command
}

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

// gpuProfilingDisabledMessage is the message returned when GPU profiling
// cannot be enabled on this node. It reproduces the Python
// GpuProfilingManager._DISABLED_ERROR_MESSAGE verbatim, with the node IP
// substituted.
const gpuProfilingDisabledMessage = "GPU profiling is not enabled on node %s. " +
	"This is the case if no GPUs are detected on the node or if " +
	"the profiling dependency `dynolog` is not installed on the node.\n" +
	"Please ensure that GPUs are available on the node and that " +
	"`dynolog` is installed."

// gpuProfilingDeadProcessMessage is the message returned when the requested
// pid is no longer running. It reproduces the Python
// GpuProfilingManager._DEAD_PROCESS_ERROR_MESSAGE verbatim, with the pid and
// the node IP substituted.
const gpuProfilingDeadProcessMessage = "The requested process to profile with pid=%d on node " +
	"%s is no longer running. " +
	"GPU profiling is not available for this process."

// gpuProfilingUnavailableMessage is returned when GPU profiling is enabled on
// the node but the Go reporter does not implement the dynolog gputrace
// execution path. It keeps the degradation explicit instead of reporting a
// success that never produced a trace file.
const gpuProfilingUnavailableMessage = "GPU profiling is not implemented in the Go reporter: " +
	"the dynolog gputrace execution path is unavailable."

// lookPath resolves an executable on PATH. It is a package-level variable so
// tests can exercise the GPU profiling branches without installing dynolog and
// dyno on the build machine.
var lookPath = exec.LookPath

// probeNodeHasGPUs runs nvidia-smi and reports whether it succeeded,
// mirroring the Python GpuProfilingManager.node_has_gpus. Any failure (binary
// missing, non-zero exit, permission denied) counts as "no GPUs".
func probeNodeHasGPUs() bool {
	_, err := runCommand("nvidia-smi")
	return err == nil
}

// nodeHasGPUs reports whether this node has GPUs. It is a package-level
// variable so tests can exercise the GPU profiling branches without a GPU; the
// production value is probeNodeHasGPUs.
var nodeHasGPUs = probeNodeHasGPUs

// gpuProfilingPIDAlive mirrors the Python GpuProfilingManager.is_pid_alive: a
// pid counts as alive only while its /proc entry is readable and it has not
// become a zombie. It is a package-level variable so tests can drive the
// dead-process branch deterministically.
var gpuProfilingPIDAlive = func(pid int) bool {
	state, err := processState(pid)
	return err == nil && state != procStateZombie
}

// ProfileManager bridges the Python CpuProfilingManager,
// MemoryProfilingManager and GpuProfilingManager, executing py-spy and memray
// through an injectable command runner while validating all externally
// supplied arguments to prevent command injection.
type ProfileManager struct {
	profileDir string
	ipAddress  string
	// dynologBin and dynoBin cache the dynolog tool paths resolved at
	// construction time, mirroring the shutil.which calls of the Python
	// GpuProfilingManager constructor. An empty value means the tool is
	// missing and GPU profiling is disabled.
	dynologBin string
	dynoBin    string
}

// NewProfileManager builds a ProfileManager writing profile artifacts under
// the given directory, creating the directory and its memray subdirectory if
// they do not yet exist (mirroring the Python constructors). ipAddress is
// reported in the GPU profiling error messages. It returns an error if either
// directory cannot be created.
func NewProfileManager(profileDir string, ipAddress string) (*ProfileManager, error) {
	if err := os.MkdirAll(profileDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create profile dir %q: %w", profileDir, err)
	}
	if err := os.MkdirAll(profileDir+"/memray", 0755); err != nil {
		return nil, fmt.Errorf("failed to create memray dir %q: %w", profileDir+"/memray", err)
	}
	dynologBin, _ := lookPath("dynolog")
	dynoBin, _ := lookPath("dyno")
	return &ProfileManager{
		profileDir: profileDir,
		ipAddress:  ipAddress,
		dynologBin: dynologBin,
		dynoBin:    dynoBin,
	}, nil
}

// gpuProfilingEnabled reports whether GPU profiling can run on this node,
// mirroring the Python GpuProfilingManager.enabled property: the node must have
// GPUs and both dynolog tools must be installed.
func (m *ProfileManager) gpuProfilingEnabled() bool {
	return nodeHasGPUs() && m.dynologBin != "" && m.dynoBin != ""
}

// profilerPermissionsMessage is the permission guidance Python's
// PROFILER_PERMISSIONS_ERROR_MESSAGE appends when a profiler command fails with
// a permission error. The leading and trailing newlines are significant: they
// are what separates the paragraph from the "Failed to execute" line and from
// the stderr section.
const profilerPermissionsMessage = `
Note that this command requires %[1]s to be installed with root permissions. You
can install %[1]s and give it root permissions as follows:
  $ pip install %[2]s
  $ %[3]s
  $ sudo chmod u+s %[4]s

Alternatively, you can start Ray with passwordless sudo / root permissions.

`

// profilerSetChownCommand renders Python's DARWIN_SET_CHOWN_CMD or
// LINUX_SET_CHOWN_CMD, which differ only in the chown group.
func profilerSetChownCommand(profiler string) string {
	if osName == "darwin" {
		return fmt.Sprintf("sudo chown root: %s", profilerQuotedWhich(profiler))
	}
	return fmt.Sprintf("sudo chown root:root %s", profilerQuotedWhich(profiler))
}

// profilerQuotedWhich renders Python's `which {profiler}` backtick-quoted
// command substitution. It is built rather than written as a literal because the
// backtick cannot appear inside a raw string.
func profilerQuotedWhich(profiler string) string {
	return "`which " + profiler + "`"
}

// profilerPermissionsHint returns the permission guidance for profiler, or an
// empty string when stderr does not look like a permission failure. Python
// tests for the substring "permission" case-insensitively and appends the hint
// on any match, without requiring a specific errno or exit code.
func profilerPermissionsHint(profiler, stderr string) string {
	if !strings.Contains(strings.ToLower(stderr), "permission") {
		return ""
	}
	return fmt.Sprintf(profilerPermissionsMessage, "`"+profiler+"`",
		profiler, profilerSetChownCommand(profiler), profilerQuotedWhich(profiler))
}

// formatFailedCommand builds the failure message Python's
// _format_failed_profiler_command returns. The layout is reproduced verbatim,
// including the blank line that always follows the "Failed to execute" line, the
// two blank lines before the stderr section, and the unconditional stderr and
// stdout sections — Python emits both headers even when the streams are empty,
// whereas an earlier Go version omitted empty sections and the permission hint
// entirely.
func formatFailedCommand(cmd, profiler string, res CommandResult) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Failed to execute `%s`.\n", cmd))
	b.WriteString(profilerPermissionsHint(profiler, res.Stderr))
	b.WriteString("\n=== stderr ===\n")
	b.WriteString(res.Stderr)
	b.WriteString("\n\n=== stdout ===\n")
	b.WriteString(res.Stdout)
	b.WriteString("\n")
	return b.String()
}

// TraceDump captures a stack trace for pid via `py-spy dump -p <pid>`,
// appending --native on linux when native is requested and
// --subprocesses when requested, matching the Python profile manager. It
// returns (success, output) where output is the trace on success or an error
// message on failure.
func (m *ProfileManager) TraceDump(pid int, native, subprocesses bool) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}

	pyspy, err := resolveProfiler("py-spy")
	if err != nil {
		return false, err.Error()
	}

	args := []string{"dump", "-p", strconv.Itoa(pid)}
	if osName == "linux" && native {
		args = append(args, "--native")
	}
	if subprocesses {
		args = append(args, "--subprocesses")
	}
	res, command := runProfiler(pyspy, args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand(command, "py-spy", res)
	}
	return true, res.Stdout
}

// CPUProfile samples the process for duration seconds via `py-spy record`
// writing the result in the requested format. Valid formats are flamegraph,
// raw and speedscope, matching the Python CpuProfilingManager. When
// subprocesses is requested, --subprocesses is appended so child processes are
// sampled too, matching the Python profile manager.
func (m *ProfileManager) CPUProfile(pid int, format string, duration int, native, subprocesses bool) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}
	if err := validateDuration(duration); err != nil {
		return false, err.Error()
	}
	pyspy, err := resolveProfiler("py-spy")
	if err != nil {
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
	if subprocesses {
		args = append(args, "--subprocesses")
	}
	res, command := runProfiler(pyspy, args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand(command, "py-spy", res)
	}
	data, err := os.ReadFile(profilePath)
	if err != nil {
		return false, fmt.Sprintf("Failed to read profile file %q: %v", profilePath, err)
	}
	return true, string(data)
}

// GpuProfile reports why GPU profiling cannot serve this request. The Go
// reporter does not implement the dynolog gputrace execution path, but the
// node capability and process liveness checks of the Python
// GpuProfilingManager.gpu_profile are reproduced so the head node receives the
// same precise diagnostics: a disabled node, a dead target process and an
// unimplemented execution path are distinguishable instead of collapsing into
// one generic message.
func (m *ProfileManager) GpuProfile(pid int, numIterations int) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}
	if !m.gpuProfilingEnabled() {
		log.Log.V(1).Info("GPU profiling is not enabled on this node",
			"node_ip", m.ipAddress, "pid", pid, "num_iterations", numIterations)
		return false, fmt.Sprintf(gpuProfilingDisabledMessage, m.ipAddress)
	}
	if !gpuProfilingPIDAlive(pid) {
		log.Log.Info("the requested process for GPU profiling is no longer running",
			"node_ip", m.ipAddress, "pid", pid)
		return false, fmt.Sprintf(gpuProfilingDeadProcessMessage, pid, m.ipAddress)
	}
	log.Log.Info("GPU profiling is enabled on this node but is not implemented in the Go reporter",
		"node_ip", m.ipAddress, "pid", pid, "num_iterations", numIterations)
	return false, gpuProfilingUnavailableMessage
}

// MemoryProfileAttach attaches a memray profiler to pid, generating a
// profiler output file. It returns (success, filename, output); on failure the
// filename is empty and output carries the error message.
func (m *ProfileManager) MemoryProfileAttach(pid int, native, tracePythonAllocators, verbose bool) (bool, string, string) {
	if err := validatePID(pid); err != nil {
		return false, "", err.Error()
	}

	memray, err := resolveProfiler("memray")
	if err != nil {
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

	res, command := runProfiler(memray, args...)
	if res.ExitCode != 0 {
		return false, "", formatFailedCommand(command, "memray", res)
	}
	return true, filename, fmt.Sprintf("Success attaching memray to process %d", pid)
}

// MemoryProfileDetach detaches the memray profiler from pid. It returns
// (success, output).
func (m *ProfileManager) MemoryProfileDetach(pid int, verbose bool) (bool, string) {
	if err := validatePID(pid); err != nil {
		return false, err.Error()
	}

	memray, err := resolveProfiler("memray")
	if err != nil {
		return false, err.Error()
	}

	args := []string{"detach"}
	if verbose {
		args = append(args, "--verbose")
	}
	args = append(args, strconv.Itoa(pid))

	res, command := runProfiler(memray, args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand(command, "memray", res)
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

	memray, err := resolveProfiler("memray")
	if err != nil {
		return false, err.Error()
	}

	profilePath := m.profileDir + "/memray/" + profilerFilename
	if _, err := os.Stat(profilePath); err != nil {
		return false, fmt.Sprintf("Failed to execute: process %d has not been profiled", pid)
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
	base := strings.TrimSuffix(profilerFilename, ".bin")
	visualizePath := m.profileDir + "/memray/" + base + ".html"

	args := []string{command, "-o", visualizePath, "-f"}
	if leaks {
		args = append(args, "--leaks")
	}
	args = append(args, profilePath)

	res := runCommandDetail(memray, args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand(memray+" "+strings.Join(args, " "), "memray", res)
	}
	data, err := os.ReadFile(visualizePath)
	if err != nil {
		return false, fmt.Sprintf("Failed to read profile result %q: %v", visualizePath, err)
	}
	return true, string(data)
}
