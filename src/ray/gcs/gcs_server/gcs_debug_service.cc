#include "ray/gcs/gcs_server/gcs_debug_service.h"

#include <sstream>


#if defined(__linux__)
#include <stdlib.h>
#include "gperftools/heap-profiler.h"
#endif

namespace ray {
namespace gcs {

GcsDebugService::GcsDebugService(instrumented_io_context &io_service)
    : io_service_(io_service) {}

grpc::ServerUnaryReactor *GcsDebugService::CollectMemoryStats(
    grpc::CallbackServerContext *context,
    const ray::rpc::CollectMemoryStatsRequest *request,
    ray::rpc::CollectMemoryStatsReply *reply) {
  reply->set_stats("Not supported on this platform.");

  auto *reactor = context->DefaultReactor();
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor *GcsDebugService::StopMemoryProfile(
      grpc::CallbackServerContext *context,
      const ray::rpc::StopMemoryProfileRequest *request,
      ray::rpc::StopMemoryProfileReply *reply) {
  auto reactor = context->DefaultReactor();
#if defined(__linux__)
  if (!IsHeapProfilerRunning()) {
    reactor->Finish(grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                 "There is no request running profiling."));
    return reactor;
  }
  HeapProfilerDump("Last dump");
  HeapProfilerStop();
  reactor->Finish(grpc::Status::OK);
#else
  reactor->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                               "Memory profiling is not supported on this platform."));
#endif
  return reactor;
}

grpc::ServerUnaryReactor *GcsDebugService::StartMemoryProfile(
    grpc::CallbackServerContext *context,
    const ray::rpc::StartMemoryProfileRequest *request,
    ray::rpc::StartMemoryProfileReply *reply) {
  auto reactor = context->DefaultReactor();
#if defined(__linux__)
  if (IsHeapProfilerRunning()) {
    reactor->Finish(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                 "There is a request running profiling."));
    return reactor;
  }

  if (request->duration() <= 0) {
    reactor->Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                 " duration must be greater or equal than 0."));
    return reactor;
  }
  HeapProfilerStart("gcs_server");
  if(request->duration() != 0) {
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
    timer->expires_from_now(boost::posix_time::seconds(request->duration()));
    timer->async_wait([this, timer, reactor](const boost::system::error_code &) {
      HeapProfilerDump("Last dump");
      HeapProfilerStop();
      reactor->Finish(grpc::Status::OK);
    });
  }
#else
  reactor->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                               "Memory profiling is not supported on this platform."));
#endif
  return reactor;
}

}  // namespace gcs
}  // namespace ray
