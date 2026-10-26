package reporter

import (
	"context"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// memoryProfileSleep is the base sleep applied after attaching the memray
// profiler so sampling can run before detaching. It is a package-level variable
// so tests can collapse it to zero.
var memoryProfileSleep = time.Second

// memoryProfileDefaultDurationSeconds is the sampling duration used when a
// MemoryProfiling request omits a duration, matching the Python agent default.
const memoryProfileDefaultDurationSeconds = 10

// memoryProfileMaxDurationSeconds caps the MemoryProfiling sampling duration to
// a sane bound, preventing an unbounded (potentially years-long) sleep when a
// request carries an arbitrary large duration value.
const memoryProfileMaxDurationSeconds = 3600

// ReporterService implements proto.ReporterServiceServer, bridging the gRPC
// profiler endpoints to the ProfileManager and forwarding OpenCensus metrics to
// the MetricsService. It is constructed from an agent.Config and can be
// registered on a gRPC server.
type ReporterService struct {
	proto.UnimplementedReporterServiceServer
	profileManager            *ProfileManager
	metricsService            *MetricsService
	metricsCollectionDisabled bool
}

// compile-time assertion that ReporterService satisfies the generated server
// interface.
var _ proto.ReporterServiceServer = (*ReporterService)(nil)

// NewReporterService builds a ReporterService using cfg.LogDir as the profile
// artifact directory. It returns an error when no log directory is configured
// or the profile directories cannot be created.
func NewReporterService(cfg agent.Config) (*ReporterService, error) {
	if cfg.LogDir == "" {
		return nil, status.Error(codes.InvalidArgument, "log directory must be configured")
	}
	manager, err := NewProfileManager(cfg.LogDir)
	if err != nil {
		return nil, err
	}
	return &ReporterService{
		profileManager:            manager,
		metricsCollectionDisabled: cfg.MetricsCollectionDisabled,
	}, nil
}

// SetMetricsService injects the MetricsService used to forward OpenCensus
// metrics reported through ReportOCMetrics. It is called by the module assembly
// after construction so NewReporterService's signature stays independent of the
// metrics pipeline.
func (s *ReporterService) SetMetricsService(metrics *MetricsService) {
	s.metricsService = metrics
}

// GetTraceback captures a stack trace for the requested pid.
func (s *ReporterService) GetTraceback(_ context.Context, req *proto.GetTracebackRequest) (*proto.GetTracebackReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	success, output := s.profileManager.TraceDump(int(req.GetPid()), req.GetNative())
	return &proto.GetTracebackReply{Success: success, Output: output}, nil
}

// CpuProfiling samples the requested pid for the given duration and format.
func (s *ReporterService) CpuProfiling(_ context.Context, req *proto.CpuProfilingRequest) (*proto.CpuProfilingReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	success, output := s.profileManager.CPUProfile(int(req.GetPid()), req.GetFormat(), int(req.GetDuration()), req.GetNative())
	return &proto.CpuProfilingReply{Success: success, Output: output}, nil
}

// GpuProfiling is a degraded implementation: GPU profiling is not supported on
// the Go side and always reports the not-supported message.
func (s *ReporterService) GpuProfiling(_ context.Context, req *proto.GpuProfilingRequest) (*proto.GpuProfilingReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	success, output := s.profileManager.GpuProfile(int(req.GetPid()), int(req.GetNumIterations()))
	return &proto.GpuProfilingReply{Success: success, Output: output}, nil
}

// MemoryProfiling attaches a memray profiler to the requested pid, waits for
// sampling to run, detaches, and converts the recorded profile to the
// requested format. A successful detach yields no warning; a failed detach is
// surfaced through the warning field, mirroring the Python agent.
func (s *ReporterService) MemoryProfiling(ctx context.Context, req *proto.MemoryProfilingRequest) (*proto.MemoryProfilingReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	pid := int(req.GetPid())
	duration := req.GetDuration()
	if duration == 0 {
		duration = memoryProfileDefaultDurationSeconds
	}
	if duration > memoryProfileMaxDurationSeconds {
		duration = memoryProfileMaxDurationSeconds
	}

	attachSuccess, profilerFilename, attachOutput := s.profileManager.MemoryProfileAttach(
		pid, req.GetNative(), req.GetTracePythonAllocators(), false)
	if !attachSuccess {
		return &proto.MemoryProfilingReply{Success: false, Output: attachOutput}, nil
	}

	// Wait for the profiler to sample, bounded by the request duration. The
	// wait is interruptible by context cancellation so a long profile does not
	// block the gRPC worker forever.
	timer := time.NewTimer(time.Duration(duration+1) * memoryProfileSleep)
	select {
	case <-ctx.Done():
		timer.Stop()
		return &proto.MemoryProfilingReply{Success: false, Output: "memory profiling interrupted by request cancellation"}, nil
	case <-timer.C:
	}

	var warning *string
	if detachSuccess, detachOutput := s.profileManager.MemoryProfileDetach(pid, false); !detachSuccess {
		w := detachOutput
		warning = &w
	}

	resultSuccess, resultOutput := s.profileManager.MemoryProfileResult(
		pid, profilerFilename, req.GetFormat(), req.GetLeaks())
	return &proto.MemoryProfilingReply{Success: resultSuccess, Output: resultOutput, Warning: warning}, nil
}

// HealthCheck reports the service is alive with an empty reply.
func (s *ReporterService) HealthCheck(_ context.Context, _ *proto.HealthCheckRequest) (*proto.HealthCheckReply, error) {
	return &proto.HealthCheckReply{}, nil
}

// ReportOCMetrics forwards the reported OpenCensus metrics to the MetricsService
// for export as Prometheus gauges. When MetricsCollectionDisabled is set the
// metrics are dropped and an acknowledgement is returned without forwarding,
// mirroring the Python reporter_agent which returns early on
// _metrics_collection_disabled. A nil request is rejected; a missing
// MetricsService degrades to an acknowledgement without forwarding.
func (s *ReporterService) ReportOCMetrics(ctx context.Context, req *proto.ReportOCMetricsRequest) (*proto.ReportOCMetricsReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}
	if s.metricsCollectionDisabled {
		return &proto.ReportOCMetricsReply{}, nil
	}
	if s.metricsService != nil {
		if err := s.metricsService.Export(ctx, req.GetMetrics()); err != nil {
			return nil, err
		}
	}
	return &proto.ReportOCMetricsReply{}, nil
}
