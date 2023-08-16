#include "ray/gcs/gcs_server/gcs_debug_service.h"

#include <sstream>

#include "gperftools/heap-profiler.h"

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

grpc::ServerUnaryReactor *GcsDebugService::StartMemoryProfile(
    grpc::CallbackServerContext *context,
    const ray::rpc::StartMemoryProfileRequest *request,
    ray::rpc::StartMemoryProfileReply *reply) {
  auto reactor = context->DefaultReactor();
#if defined(__linux__)
  if (is_memory_profiling_.exchange(true)) {
    reactor->Finish(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                 "There is a request running profiling."));
    return reactor;
  }
  if (request->duration() <= 0 || request->dump_prefix().size() == 0) {
    reactor->Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                 "duration and dump_prefix must be set."));
    return reactor;
  }

  HeapProfilerStart(request->dump_prefix().c_str());

  auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_);

  timer->expires_from_now(boost::posix_time::seconds(request->duration()));
  timer->async_wait([this, timer, reactor](const boost::system::error_code &) {
    HeapProfilerDump("Request End");
    HeapProfilerStop();
    is_memory_profiling_ = false;
    reactor->Finish(grpc::Status::OK);
  });
#else
  reactor->Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                               "Memory profiling is not supported on this platform."));
#endif
  return reactor;
}

}  // namespace gcs
}  // namespace ray
