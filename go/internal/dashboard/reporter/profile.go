package reporter

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
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

// gpuProfilingNoProcessesMatchedMessage reproduces the Python
// GpuProfilingManager._NO_PROCESSES_MATCHED_ERROR_MESSAGE verbatim.
const gpuProfilingNoProcessesMatchedMessage = "The profiling command failed for pid=%d on node %s. " +
	"There are a few potential reasons for this:\n" +
	"1. The `KINETO_USE_DAEMON=1 KINETO_DAEMON_INIT_DELAY_S=5` environment variables " +
	"are not set for the training worker processes.\n" +
	"2. The process requested for profiling is not running a " +
	"PyTorch training script. GPU profiling is only supported for " +
	"PyTorch training scripts, typically launched via " +
	"`ray.train.torch.TorchTrainer`."

// gpuProfilingDaemonNotRunningMessage is returned when the dynolog daemon has
// exited since it was launched.
const gpuProfilingDaemonNotRunningMessage = "GPU monitoring daemon (pid=%d) is not running on node %s. " +
	"See log for more details: %s"

// gpuProfilingDaemonNotStartedMessage reproduces the Python RuntimeError raised
// when gpu_profile runs before start_monitoring_daemon.
const gpuProfilingDaemonNotStartedMessage = "Must call `start_monitoring_daemon` before profiling."

// gpuProfilingTimedOutMessage mirrors the Python _wait_for_trace_file timeout.
const gpuProfilingTimedOutMessage = "GPU profiling timed out after %d seconds, please try again."

// dynologPort is the port the dynolog monitoring daemon listens on, mirroring
// GpuProfilingManager._DYNOLOG_PORT.
const dynologPort = 65406

// noProcessesMatchedPrefix marks a dyno gputrace stdout that matched no
// process, mirroring _NO_PROCESSES_MATCHED_ERROR_MESSAGE_PREFIX.
const noProcessesMatchedPrefix = "No processes were matched"

// gpuProfileTimeoutSeconds bounds the wait for the trace file, mirroring
// _DEFAULT_TIMEOUT_S. A variable so tests can shorten it.
var gpuProfileTimeoutSeconds = 5 * 60

// gpuProfileTraceWaitInterval is the trace file poll interval, mirroring the
// Python _wait_for_trace_file sleep. A variable so tests can collapse it.
var gpuProfileTraceWaitInterval = 250 * time.Millisecond

// gpuTraceTimestamp returns the timestamp embedded in a trace file name,
// mirroring Python's datetime.now().strftime("%Y%m%d%H%M%S"). A variable so
// tests can pin it.
var gpuTraceTimestamp = func() string { return time.Now().Format("20060102150405") }

// gpuProfilingDaemonAlive reports whether the dynolog daemon is still running,
// mirroring the Python poll()-based is_monitoring_daemon_running. A variable so
// tests can drive the daemon-died branch.
var gpuProfilingDaemonAlive = func(pid int) bool {
	state, err := processState(pid)
	return err == nil && state != procStateZombie
}

// launchDynologDaemon starts the dynolog monitoring daemon, redirecting its
// output to logPath, and returns its pid. Setpgid detaches it into its own
// process group, mirroring Python's start_new_session=True so it survives
// signal delivery to the agent's group. A variable so tests need no dynolog
// binary.
var launchDynologDaemon = func(logPath string, port int, bin string) (int, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	cmd := exec.Command(bin, "--enable_ipc_monitor", "--port", strconv.Itoa(port))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return 0, err
	}
	// The child holds its own duplicate of the descriptor, so close the parent's.
	logFile.Close()
	// Reap the daemon when it exits so it does not linger as a zombie in the
	// agent's process table until the agent itself terminates.
	go func() { _ = cmd.Wait() }()
	return cmd.Process.Pid, nil
}

// lookPath resolves an executable on PATH. It is a package-level variable so
// tests can exercise the GPU profiling branches without installing dynolog and
// dyno on the build machine.
var lookPath = exec.LookPath

// gpuProbeTimeout bounds the nvidia-smi probe, mirroring the Python
// GpuProfilingManager.node_has_gpus timeout=10. The probe runs in the
// reporter constructor on the agent startup path, so an unbounded probe on a
// wedged GPU driver would hang the agent before it binds any port. A variable
// so tests can collapse it.
var gpuProbeTimeout = 10 * time.Second

// gpuProbeBinary and gpuProbeArgs are the probe argv, reproducing the Python
// GpuProfilingManager.node_has_gpus command verbatim. They are variables so
// tests can point the probe at a fake process instead of relying on the build
// machine's nvidia-smi; the command is still built by probeNodeHasGPUs, so the
// timeout under test stays in production code.
var (
	gpuProbeBinary = "nvidia-smi"
	gpuProbeArgs   = []string{"--query-gpu=name", "--format=csv,noheader"}
)

// probeNodeHasGPUs runs nvidia-smi and reports whether it succeeded, mirroring
// the Python GpuProfilingManager.node_has_gpus: the same arguments Python
// passes, a 10-second timeout, a warning on timeout, and any other failure
// (binary missing, non-zero exit, permission denied) counts as "no GPUs".
func probeNodeHasGPUs() bool {
	ctx, cancel := context.WithTimeout(context.Background(), gpuProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, gpuProbeBinary, gpuProbeArgs...)
	// stderr=subprocess.DEVNULL in Python: the failure detail is not used, and
	// a nil writer makes os/exec discard the child's output rather than
	// inherit the agent's stdio.
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			// go/pkg/log has no Warning level, so Info (visible by default) is
			// used, matching the Python logger.warning call site.
			log.Log.Info("nvidia-smi command timed out after 10s. GPU profiling may not function correctly.")
		}
		return false
	}
	return true
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
	// profilesDir holds GPU trace files and the dynolog daemon log.
	profilesDir string
	// dynologBin and dynoBin cache the dynolog tool paths resolved at
	// construction time, mirroring the shutil.which calls of the Python
	// GpuProfilingManager constructor. An empty value means the tool is
	// missing and GPU profiling is disabled.
	dynologBin string
	dynoBin    string
	// nodeHasGPUs caches the GPU probe, mirroring the functools.cache of the
	// Python GpuProfilingManager, so gpuProfilingEnabled does not fork
	// nvidia-smi on every call.
	nodeHasGPUs bool
	// daemonPID is the pid of the launched dynolog daemon, 0 when none was
	// launched. Recording the pid (rather than a started flag) lets GpuProfile
	// verify the daemon is still alive the way Python's
	// is_monitoring_daemon_running does.
	daemonPID atomic.Int32
}

// NewProfileManager builds a ProfileManager writing profile artifacts under
// the given directory, creating the directory and its memray and profiles
// subdirectories if they do not yet exist (mirroring the Python constructors).
// ipAddress is reported in the GPU profiling error messages. It returns an
// error if any directory cannot be created.
func NewProfileManager(profileDir string, ipAddress string) (*ProfileManager, error) {
	if err := os.MkdirAll(profileDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create profile dir %q: %w", profileDir, err)
	}
	if err := os.MkdirAll(profileDir+"/memray", 0755); err != nil {
		return nil, fmt.Errorf("failed to create memray dir %q: %w", profileDir+"/memray", err)
	}
	profilesDir := filepath.Join(profileDir, "profiles")
	if err := os.MkdirAll(profilesDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create profiles dir %q: %w", profilesDir, err)
	}
	dynologBin, _ := lookPath("dynolog")
	dynoBin, _ := lookPath("dyno")
	return &ProfileManager{
		profileDir:  profileDir,
		ipAddress:   ipAddress,
		profilesDir: profilesDir,
		dynologBin:  dynologBin,
		dynoBin:     dynoBin,
		nodeHasGPUs: nodeHasGPUs(),
	}, nil
}

// gpuProfilingEnabled reports whether GPU profiling can run on this node,
// mirroring the Python GpuProfilingManager.enabled property: the node must have
// GPUs and both dynolog tools must be installed.
func (m *ProfileManager) gpuProfilingEnabled() bool {
	return m.nodeHasGPUs && m.dynologBin != "" && m.dynoBin != ""
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

// daemonLogPath is where the dynolog daemon writes its output, mirroring the
// Python _daemon_log_file_path naming.
func (m *ProfileManager) daemonLogPath() string {
	return filepath.Join(m.profilesDir, fmt.Sprintf("dynolog_daemon_%d.log", os.Getpid()))
}

// StartMonitoringDaemon launches the dynolog monitoring daemon that gputrace
// talks to. It is a no-op on a disabled node and when a daemon is already
// running. Failures are logged and never returned, mirroring the Python
// start_monitoring_daemon, so a node without a working dynolog still serves its
// reporter endpoints.
func (m *ProfileManager) StartMonitoringDaemon() {
	if !m.gpuProfilingEnabled() {
		log.Log.V(1).Info("GPU profiling is disabled, skipping daemon setup", "node_ip", m.ipAddress)
		return
	}
	if pid := m.daemonPID.Load(); pid != 0 && gpuProfilingDaemonAlive(int(pid)) {
		log.Log.V(1).Info("GPU profiling monitoring daemon is already running", "pid", pid)
		return
	}
	logPath := m.daemonLogPath()
	pid, err := launchDynologDaemon(logPath, dynologPort, m.dynologBin)
	if err != nil {
		log.Log.Error(err, "failed to launch the GPU profiling monitoring daemon", "log_path", logPath)
		return
	}
	m.daemonPID.Store(int32(pid))
	log.Log.Info("launched the GPU profiling monitoring daemon",
		"pid", pid, "port", dynologPort, "log_path", logPath)
}

// GpuProfile runs `dyno gputrace` against pid and waits for the trace file it
// produces, mirroring GpuProfilingManager.gpu_profile. The returned string is
// the trace file path relative to the log directory, or an error message. A
// non-nil error is reserved for the states Python models as an exception (the
// daemon was never started, or the request context was cancelled), which the
// caller surfaces as a gRPC error rather than as a failed-but-successful reply.
// ctx bounds the wait for the trace file so a disconnected caller does not
// leave a poll loop running to the deadline.
func (m *ProfileManager) GpuProfile(ctx context.Context, pid int, numIterations int) (bool, string, error) {
	if err := validatePID(pid); err != nil {
		return false, err.Error(), nil
	}
	if !m.gpuProfilingEnabled() {
		log.Log.V(1).Info("GPU profiling is not enabled on this node",
			"node_ip", m.ipAddress, "pid", pid, "num_iterations", numIterations)
		return false, fmt.Sprintf(gpuProfilingDisabledMessage, m.ipAddress), nil
	}
	daemonPID := m.daemonPID.Load()
	if daemonPID == 0 {
		return false, "", fmt.Errorf("%s", gpuProfilingDaemonNotStartedMessage)
	}
	if !gpuProfilingDaemonAlive(int(daemonPID)) {
		msg := fmt.Sprintf(gpuProfilingDaemonNotRunningMessage, daemonPID, m.ipAddress, m.daemonLogPath())
		log.Log.Error(fmt.Errorf("%s", msg), "GPU profiling daemon is gone", "pid", daemonPID)
		return false, msg, nil
	}
	if !gpuProfilingPIDAlive(pid) {
		log.Log.Info("the requested process for GPU profiling is no longer running",
			"node_ip", m.ipAddress, "pid", pid)
		return false, fmt.Sprintf(gpuProfilingDeadProcessMessage, pid, m.ipAddress), nil
	}

	traceName := fmt.Sprintf("gputrace_%s_%s.json", m.ipAddress, gpuTraceTimestamp())
	tracePath := filepath.Join(m.profilesDir, traceName)
	args := []string{
		"--port", strconv.Itoa(dynologPort),
		"gputrace",
		"--pids", strconv.Itoa(pid),
		"--log-file", tracePath,
		"--process-limit", "1",
		"--iterations", strconv.Itoa(numIterations),
	}
	command := strings.Join(append([]string{m.dynoBin}, args...), " ")
	res := runCommandDetail(m.dynoBin, args...)
	if res.ExitCode != 0 {
		return false, formatFailedCommand(command, "dyno", res), nil
	}
	if strings.Contains(res.Stdout, noProcessesMatchedPrefix) {
		msg := fmt.Sprintf(gpuProfilingNoProcessesMatchedMessage, pid, m.ipAddress)
		log.Log.Error(fmt.Errorf("%s", msg), "GPU profiling matched no processes", "pid", pid)
		return false, msg, nil
	}

	// dyno returns as soon as the trace is queued: it writes <prefix>.tmp.json
	// and renames it to <prefix>.json when the trace is complete, so the
	// completion signal is the file appearing under the wildcard name.
	pattern := strings.Replace(traceName, ".json", "*.json", 1)
	return m.waitForTraceFile(ctx, pid, pattern)
}

// waitForTraceFile polls for the trace file until it appears, the timeout
// expires, the target process dies, or ctx is cancelled, mirroring
// _wait_for_trace_file.
func (m *ProfileManager) waitForTraceFile(ctx context.Context, pid int, pattern string) (bool, string, error) {
	deadline := time.Now().Add(time.Duration(gpuProfileTimeoutSeconds) * time.Second)
	for {
		matches, err := filepath.Glob(filepath.Join(m.profilesDir, pattern))
		if err == nil && len(matches) > 0 {
			rel, relErr := filepath.Rel(m.profileDir, matches[0])
			if relErr != nil {
				rel = matches[0]
			}
			log.Log.Info("GPU profiling finished", "trace_file", rel)
			return true, rel, nil
		}
		if !time.Now().Before(deadline) {
			return false, fmt.Sprintf(gpuProfilingTimedOutMessage, gpuProfileTimeoutSeconds), nil
		}
		timer := time.NewTimer(gpuProfileTraceWaitInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, "", ctx.Err()
		case <-timer.C:
		}
		if !gpuProfilingPIDAlive(pid) {
			return false, fmt.Sprintf(gpuProfilingDeadProcessMessage, pid, m.ipAddress), nil
		}
	}
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
