package reporter

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/proto"
)

func u32p(v uint32) *uint32 { return &v }
func strp(v string) *string { return &v }
func boolp(v bool) *bool    { return &v }

// newTestReporterService builds a ReporterService backed by a fresh profile
// directory under dir, failing the test if construction fails.
func newTestReporterService(t *testing.T, dir string) *ReporterService {
	t.Helper()
	s, err := NewReporterService(agent.Config{LogDir: dir})
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
	s := newTestReporterService(t, t.TempDir())
	rep, err := s.GpuProfiling(context.Background(), &proto.GpuProfilingRequest{Pid: 5, NumIterations: u32p(10)})
	if err != nil {
		t.Fatalf("GpuProfiling: %v", err)
	}
	if rep.GetSuccess() {
		t.Error("GpuProfiling success = true, want degraded (not supported)")
	}
	if !strings.Contains(rep.GetOutput(), "not supported") {
		t.Errorf("GpuProfiling output = %q, want not-supported message", rep.GetOutput())
	}
}

func TestReporterServiceMemoryProfiling(t *testing.T) {
	orig := runCommandDetail
	origSleep := memoryProfileSleep
	defer func() {
		runCommandDetail = orig
		memoryProfileSleep = origSleep
	}()
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
