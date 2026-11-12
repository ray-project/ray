package reporter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func u32p(v uint32) *uint32 { return &v }
func strp(v string) *string { return &v }
func boolp(v bool) *bool    { return &v }

// newTestReporterService builds a ReporterService backed by a fresh profile
// directory under dir, failing the test if construction fails.
func newTestReporterService(t *testing.T, dir string) *ReporterService {
	t.Helper()
	s, err := NewReporterService(agent.Config{LogDir: dir, NodeIP: testNodeIP})
	if err != nil {
		t.Fatalf("NewReporterService(%q) error = %v", dir, err)
	}
	return s
}

// TestReporterServiceNew validates constructor argument handling.
func TestReporterServiceNew(t *testing.T) {
	if _, err := NewReporterService(agent.Config{LogDir: ""}); err == nil {
		t.Error("NewReporterService with empty LogDir returned no error")
	}
	s, err := NewReporterService(agent.Config{LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewReporterService: %v", err)
	}
	if s == nil {
		t.Fatal("NewReporterService returned nil service")
	}
	if s.profileManager == nil {
		t.Error("NewReporterService did not initialize the profile manager")
	}
}

// TestReporterServiceImplementsInterface verifies ReporterService satisfies the
// generated gRPC server interface at compile time.
func TestReporterServiceImplementsInterface(t *testing.T) {
	var _ proto.ReporterServiceServer = (*ReporterService)(nil)
}

func TestReporterServiceGetTraceback(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	orig := runCommandDetail
	defer func() { runCommandDetail = orig }()

	dir := t.TempDir()

	t.Run("success", func(t *testing.T) {
		runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stdout: "trace-data", ExitCode: 0})
		s := newTestReporterService(t, dir)
		rep, err := s.GetTraceback(context.Background(), &proto.GetTracebackRequest{Pid: 5})
		if err != nil {
			t.Fatalf("GetTraceback: %v", err)
		}
		if !rep.GetSuccess() {
			t.Error("GetTraceback success = false, want true")
		}
		if rep.GetOutput() != "trace-data" {
			t.Errorf("GetTraceback output = %q, want trace-data", rep.GetOutput())
		}
	})

	t.Run("failure", func(t *testing.T) {
		runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stderr: "permission denied", ExitCode: 1})
		s := newTestReporterService(t, dir)
		rep, err := s.GetTraceback(context.Background(), &proto.GetTracebackRequest{Pid: 5})
		if err != nil {
			t.Fatalf("GetTraceback: %v", err)
		}
		if rep.GetSuccess() {
			t.Error("GetTraceback success = true, want false on failure")
		}
		if !strings.Contains(rep.GetOutput(), "permission denied") {
			t.Errorf("GetTraceback output = %q, want stderr embedded", rep.GetOutput())
		}
	})

	t.Run("nil request is invalid", func(t *testing.T) {
		s := newTestReporterService(t, dir)
		if _, err := s.GetTraceback(context.Background(), nil); err == nil {
			t.Error("GetTraceback(nil) returned no error")
		}
	})
}

func TestReporterServiceCpuProfiling(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	orig := runCommandDetail
	defer func() { runCommandDetail = orig }()

	dir := t.TempDir()

	t.Run("success", func(t *testing.T) {
		runCommandDetail = cpuProfilingExecutor(&cmdCapture{}, "cpu-data")
		s := newTestReporterService(t, dir)
		rep, err := s.CpuProfiling(context.Background(), &proto.CpuProfilingRequest{Pid: 5, Format: strp("flamegraph"), Duration: u32p(5)})
		if err != nil {
			t.Fatalf("CpuProfiling: %v", err)
		}
		if !rep.GetSuccess() {
			t.Error("CpuProfiling success = false, want true")
		}
		if rep.GetOutput() != "cpu-data" {
			t.Errorf("CpuProfiling output = %q, want cpu-data", rep.GetOutput())
		}
	})

	t.Run("invalid format fails", func(t *testing.T) {
		runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stdout: "ok", ExitCode: 0})
		s := newTestReporterService(t, dir)
		rep, err := s.CpuProfiling(context.Background(), &proto.CpuProfilingRequest{Pid: 5, Format: strp("bogus"), Duration: u32p(5)})
		if err != nil {
			t.Fatalf("CpuProfiling: %v", err)
		}
		if rep.GetSuccess() {
			t.Error("CpuProfiling(bogus) success = true, want false")
		}
	})

	t.Run("nil request is invalid", func(t *testing.T) {
		s := newTestReporterService(t, dir)
		if _, err := s.CpuProfiling(context.Background(), nil); err == nil {
			t.Error("CpuProfiling(nil) returned no error")
		}
	})
}

func TestReporterServiceGpuProfiling(t *testing.T) {
	// This node has no GPUs, so the RPC must report the disabled diagnostic as a
	// reply rather than as a gRPC error, and name the configured node address.
	stubGpuProfiling(t, false, "/usr/bin/dynolog", "/usr/bin/dyno", nil)
	s := newTestReporterService(t, t.TempDir())
	rep, err := s.GpuProfiling(context.Background(), &proto.GpuProfilingRequest{Pid: 5, NumIterations: u32p(10)})
	if err != nil {
		t.Fatalf("GpuProfiling: %v", err)
	}
	if rep.GetSuccess() {
		t.Error("GpuProfiling success = true, want the disabled diagnostic")
	}
	if !strings.Contains(rep.GetOutput(), "GPU profiling is not enabled on node "+testNodeIP) {
		t.Errorf("GpuProfiling output = %q, want the disabled message naming %s", rep.GetOutput(), testNodeIP)
	}
}

// TestReporterServiceGpuProfilingWithoutDaemonReturnsUnknownStatus pins the one
// GPU profiling failure Python raises as an exception: profiling on an enabled
// node before the monitoring daemon was started must surface as a gRPC error
// instead of a reply with success=false.
func TestReporterServiceGpuProfilingWithoutDaemonReturnsUnknownStatus(t *testing.T) {
	stubGpuProfiling(t, true, "/usr/bin/dynolog", "/usr/bin/dyno", nil)
	s := newTestReporterService(t, t.TempDir())

	rep, err := s.GpuProfiling(context.Background(), &proto.GpuProfilingRequest{Pid: 5, NumIterations: u32p(10)})
	if err == nil {
		t.Fatalf("GpuProfiling returned no error although no daemon was started (reply %v)", rep)
	}
	if code := status.Code(err); code != codes.Unknown {
		t.Errorf("GpuProfiling status code = %v, want %v", code, codes.Unknown)
	}
	if !strings.Contains(err.Error(), "Must call `start_monitoring_daemon` before profiling.") {
		t.Errorf("GpuProfiling error = %v, want the Python RuntimeError message", err)
	}
	if rep != nil {
		t.Errorf("GpuProfiling reply = %v, want nil alongside the error", rep)
	}
}

// TestReporterServiceGpuProfilingMapsExpiredContextToDeadlineExceeded pins the
// handler-level mapping: a caller whose RPC deadline expires during the trace
// wait must see codes.DeadlineExceeded, not codes.Unknown, so client retry and
// timeout logic keyed off that code behaves as it does against the Python agent.
func TestReporterServiceGpuProfilingMapsExpiredContextToDeadlineExceeded(t *testing.T) {
	s := &ReporterService{profileManager: newGPUManager(t, true, &gpuTestHooks{daemonPid: 777, daemonAlive: true})}
	// Only the caller's deadline can end the wait: the internal timeout is far
	// away and the stubbed command never creates a trace file.
	gpuProfileTimeoutSeconds = 60

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Millisecond))
	defer cancel()

	rep, err := s.GpuProfiling(ctx, &proto.GpuProfilingRequest{Pid: 1234, NumIterations: u32p(10)})
	if code := status.Code(err); code != codes.DeadlineExceeded {
		t.Fatalf("GpuProfiling status code = %v, want %v", code, codes.DeadlineExceeded)
	}
	if rep != nil {
		t.Fatalf("GpuProfiling reply = %v, want nil alongside the error", rep)
	}
}

func TestReporterServiceGpuProfilingPropagatesNodeIP(t *testing.T) {
	s, err := NewReporterService(agent.Config{LogDir: t.TempDir(), NodeIP: "192.168.9.9"})
	if err != nil {
		t.Fatalf("NewReporterService error = %v", err)
	}
	if s.profileManager.ipAddress != "192.168.9.9" {
		t.Errorf("profileManager.ipAddress = %q, want %q", s.profileManager.ipAddress, "192.168.9.9")
	}
}

func TestReporterServiceGpuProfilingNilRequest(t *testing.T) {
	s := newTestReporterService(t, t.TempDir())
	if _, err := s.GpuProfiling(context.Background(), nil); err == nil {
		t.Error("GpuProfiling(nil) returned no error")
	}
}

func TestReporterServiceMemoryProfiling(t *testing.T) {
	stubPasswordlessSudo(t, false)
	stubLookPath(t)
	orig := runCommandDetail
	origSleep := memoryProfileSleep
	origSudo := canPasswordlessSudo
	defer func() {
		runCommandDetail = orig
		memoryProfileSleep = origSleep
		canPasswordlessSudo = origSudo
	}()
	// Pin the sudo answer so the counted commands are exactly the attach and
	// the detach (the real probe would add its own command to the capture).
	canPasswordlessSudo = func() bool { return false }
	memoryProfileSleep = 0

	dir := t.TempDir()

	t.Run("attach failure returns early", func(t *testing.T) {
		runCommandDetail = capturingExecutor(&cmdCapture{}, CommandResult{Stderr: "memray not found", ExitCode: 1})
		s := newTestReporterService(t, dir)
		rep, err := s.MemoryProfiling(context.Background(), &proto.MemoryProfilingRequest{Pid: 5, Format: strp("flamegraph"), Duration: u32p(1)})
		if err != nil {
			t.Fatalf("MemoryProfiling: %v", err)
		}
		if rep.GetSuccess() {
			t.Error("MemoryProfiling attach-fail success = true, want false")
		}
		if !strings.Contains(rep.GetOutput(), "memray not found") {
			t.Errorf("MemoryProfiling output = %q, want stderr embedded", rep.GetOutput())
		}
	})

	t.Run("success path attaches, sleeps, detaches and reports", func(t *testing.T) {
		runCommandDetail = memoryProfilingExecutor(&cmdCapture{})
		s := newTestReporterService(t, dir)
		rep, err := s.MemoryProfiling(context.Background(), &proto.MemoryProfilingRequest{Pid: 5, Format: strp("flamegraph"), Duration: u32p(1), Leaks: boolp(true)})
		if err != nil {
			t.Fatalf("MemoryProfiling: %v", err)
		}
		if !rep.GetSuccess() {
			t.Fatalf("MemoryProfiling success = false, output = %q", rep.GetOutput())
		}
		if rep.GetOutput() == "" {
			t.Error("MemoryProfiling output is empty, want profile result")
		}
		if rep.GetWarning() != "" {
			t.Errorf("MemoryProfiling warning = %q, want empty on success", rep.GetWarning())
		}
	})

	t.Run("nil request is invalid", func(t *testing.T) {
		s := newTestReporterService(t, dir)
		if _, err := s.MemoryProfiling(context.Background(), nil); err == nil {
			t.Error("MemoryProfiling(nil) returned no error")
		}
	})
}

// TestReporterServiceMemoryProfilingCancelDetaches verifies that a cancelled
// MemoryProfiling request still detaches the memray profiler from the target
// pid before returning. The profiler is already attached when cancellation is
// observed; skipping the detach would leave sampling overhead on the target
// process until it exits.
func TestReporterServiceMemoryProfilingCancelDetaches(t *testing.T) {
	stubLookPath(t)
	orig := runCommandDetail
	origSleep := memoryProfileSleep
	origSudo := canPasswordlessSudo
	defer func() {
		runCommandDetail = orig
		memoryProfileSleep = origSleep
		canPasswordlessSudo = origSudo
	}()
	// Pin the sudo answer so the counted commands are exactly the attach and
	// the detach (the real probe would add its own command to the capture).
	canPasswordlessSudo = func() bool { return false }
	// A non-zero sleep keeps the sampling timer pending so the pre-cancelled
	// request context deterministically wins the select inside MemoryProfiling
	// (with the collapsed zero sleep used above both select cases would be
	// ready and Go could pick the timer branch instead).
	memoryProfileSleep = 50 * time.Millisecond

	cap := &cmdCapture{}
	runCommandDetail = memoryProfilingExecutor(cap)
	s := newTestReporterService(t, t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rep, err := s.MemoryProfiling(ctx, &proto.MemoryProfilingRequest{Pid: 5, Format: strp("flamegraph"), Duration: u32p(1)})
	if err != nil {
		t.Fatalf("MemoryProfiling: %v", err)
	}
	if rep.GetSuccess() {
		t.Error("MemoryProfiling(cancelled) success = true, want false")
	}
	if !strings.Contains(rep.GetOutput(), "interrupted") {
		t.Errorf("MemoryProfiling output = %q, want cancellation message", rep.GetOutput())
	}
	// The attach must be followed by a detach on the cancellation path too.
	if got := cap.callCount(); got != 2 {
		t.Fatalf("command count = %d, want 2 (attach + detach), commands: %v", got, cap.names)
	}
	if args := cap.lastArgs(); len(args) == 0 || args[0] != "detach" {
		t.Errorf("last command args = %v, want detach args", args)
	}
}

func TestReporterServiceHealthCheck(t *testing.T) {
	s := newTestReporterService(t, t.TempDir())
	rep, err := s.HealthCheck(context.Background(), &proto.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if rep == nil {
		t.Fatal("HealthCheck returned nil reply")
	}
}

func TestReporterServiceReportOCMetrics(t *testing.T) {
	s := newTestReporterService(t, t.TempDir())
	rep, err := s.ReportOCMetrics(context.Background(), &proto.ReportOCMetricsRequest{})
	if err != nil {
		t.Fatalf("ReportOCMetrics: %v", err)
	}
	if rep == nil {
		t.Fatal("ReportOCMetrics returned nil reply")
	}
}

// TestReporterServiceReportOCMetricsRespectsDisabled verifies that when
// MetricsCollectionDisabled is set, ReportOCMetrics returns an acknowledgement
// without forwarding the metrics to the MetricsService (no gauge is exported),
// mirroring the Python reporter_agent which returns early on
// _metrics_collection_disabled.
func TestReporterServiceReportOCMetricsRespectsDisabled(t *testing.T) {
	metric := newDoubleMetric("node_cpu_utilization", "Total CPU usage on a ray node",
		[]string{"ip"}, []string{"1.2.3.4"}, 37.5)

	t.Run("disabled flag returns without exporting", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		svc, err := NewReporterService(agent.Config{LogDir: t.TempDir(), MetricsCollectionDisabled: true})
		if err != nil {
			t.Fatalf("NewReporterService: %v", err)
		}
		svc.SetMetricsService(NewMetricsService(reg))

		rep, err := svc.ReportOCMetrics(context.Background(), &proto.ReportOCMetricsRequest{Metrics: []*proto.Metric{metric}})
		if err != nil {
			t.Fatalf("ReportOCMetrics: %v", err)
		}
		if rep == nil {
			t.Fatal("ReportOCMetrics returned nil reply")
		}

		names, err := gatherNames(reg)
		if err != nil {
			t.Fatalf("Gather error = %v", err)
		}
		if names["ray_node_cpu_utilization"] {
			t.Errorf("metric exported despite MetricsCollectionDisabled: %v", names)
		}
	})

	t.Run("enabled flag forwards to export", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		svc, err := NewReporterService(agent.Config{LogDir: t.TempDir(), MetricsCollectionDisabled: false})
		if err != nil {
			t.Fatalf("NewReporterService: %v", err)
		}
		svc.SetMetricsService(NewMetricsService(reg))

		rep, err := svc.ReportOCMetrics(context.Background(), &proto.ReportOCMetricsRequest{Metrics: []*proto.Metric{metric}})
		if err != nil {
			t.Fatalf("ReportOCMetrics: %v", err)
		}
		if rep == nil {
			t.Fatal("ReportOCMetrics returned nil reply")
		}

		names, err := gatherNames(reg)
		if err != nil {
			t.Fatalf("Gather error = %v", err)
		}
		if !names["ray_node_cpu_utilization"] {
			t.Errorf("metric not exported when collection enabled: %v", names)
		}
	})
}

// cpuProfilingExecutor simulates the py-spy record flow, materializing the
// output profile file the ProfileManager reads back after the command runs.
// The record command carries "-o <dir>/<name>_<pid>_cpu_profiling.<ext>", so
// the fake writes the supplied content to that path.
func cpuProfilingExecutor(cap *cmdCapture, content string) commandExecutor {
	return func(name string, args ...string) CommandResult {
		cap.record(name, args)
		for i, a := range args {
			if a == "-o" && i+1 < len(args) {
				if err := os.WriteFile(args[i+1], []byte(content), 0644); err != nil {
					return CommandResult{Stderr: err.Error(), ExitCode: 1}
				}
			}
		}
		return CommandResult{Stdout: content, ExitCode: 0}
	}
}

func TestEffectiveMemoryProfileDurationClampsNonPositive(t *testing.T) {
	cases := []struct {
		name string
		in   int64
		want int64
	}{
		{"zero falls back to the default", 0, memoryProfileDefaultDurationSeconds},
		{"negative falls back to the default", -5, memoryProfileDefaultDurationSeconds},
		{"positive passes through", 7, 7},
		{"above the maximum is capped", memoryProfileMaxDurationSeconds + 1000, memoryProfileMaxDurationSeconds},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveMemoryProfileDuration(tc.in); got != tc.want {
				t.Fatalf("effectiveMemoryProfileDuration(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// memoryProfilingExecutor simulates the memray attach/detach/report flow,
// creating the artifact files the ProfileManager reads back after each command.
// The attach command carries "-o <dir>/memray/<name>.bin" and the report command
// carries "-o <dir>/memray/<name>.html", so the fake materializes both.
func memoryProfilingExecutor(cap *cmdCapture) commandExecutor {
	return func(name string, args ...string) CommandResult {
		cap.record(name, args)
		for i, a := range args {
			if a == "-o" && i+1 < len(args) {
				path := args[i+1]
				if strings.HasSuffix(path, ".bin") || strings.HasSuffix(path, ".html") {
					if err := os.WriteFile(path, []byte("profile-data"), 0644); err != nil {
						return CommandResult{Stderr: err.Error(), ExitCode: 1}
					}
				}
			}
		}
		return CommandResult{Stdout: "ok", ExitCode: 0}
	}
}
