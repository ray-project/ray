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
  // std::stringstream ss;
  // const char* opts = request->opts().c_str();
  // if(request->opts().size() == 0) {
  //   opts = nullptr;
  // }

  // auto write_cb = +[](void *cbopaque, const char *s) {
  //   auto *ss = reinterpret_cast<std::stringstream *>(cbopaque);
  //   *ss << s;
  // };
  // malloc_stats_print(write_cb, &ss, opts);
  // reply->set_stats(ss.str());

  reply->set_stats("Not supported on this platform.");

  auto *reactor = context->DefaultReactor();
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

// #ifdef __MALLOC__
// template<typename T>
// T GetJemallocOption(const char* name) {
//   T r;
//   auto len = sizeof(r);
//   mallctl(name, &r, &len, nullptr, 0);
//   return r;
// }

// template<typename T>
// T SetJemallocOption(const char* name, T value) {
//   T r;
//   auto len = sizeof(r);
//   mallctl(name, &r, &len, &value, len);
//   return r;
// }

// template<>
// std::string SetJemallocOption<std::string>(
//     const char* name,
//     std::string value) {
//   std::string r;
//   r.reserve(256);
//   auto r_ptr = r.c_str();
//   size_t len = r.capacity();
//   auto v_ptr = value.c_str();
//   mallctl(name, &r_ptr, &len, &v_ptr, value.size());
//   r.resize(len);
//   return r;
// }

// #endif

grpc::ServerUnaryReactor *GcsDebugService::StartMemoryProfile(
    grpc::CallbackServerContext *context,
    const ray::rpc::StartMemoryProfileRequest *request,
    ray::rpc::StartMemoryProfileReply *reply) {
  auto reactor = context->DefaultReactor();

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

  // #ifdef __MALLOC__
  //   std::unordered_map<std::string, std::string> old_opts;
  //   auto prof_build = GetJemallocOption<bool>("config.prof");
  //   if(prof_build == false) {
  //     reactor->Finish(grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Profiling is
  //     not enabled.")); return reactor;
  //   }

  //   mallctl("prof.dump", nullptr, 0, nullptr, 0);

  //   std::string prefix = request->dump_prefix();
  //   auto old_prefix = SetJemallocOption<std::string>("prof.dump_prefix", prefix);

  //   SetJemallocOption<bool>("prof.active", true);

  //   auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_);

  //   timer->expires_from_now(boost::posix_time::seconds(request->duration()));
  //   timer->async_wait([this, timer, reactor, old_prefix](const
  //   boost::system::error_code&) {
  //     SetJemallocOption<bool>("prof.active", false);
  //     SetJemallocOption<std::string>("prof.dump_prefix", old_prefix);
  //     mallctl("prof.dump", NULL, NULL, NULL, 0);
  //     is_memory_profiling_ = false;
  //     reactor->Finish(grpc::Status::OK);
  //   });

  // #endif

  auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_);

  timer->expires_from_now(boost::posix_time::seconds(request->duration()));
  timer->async_wait([this, timer, reactor](const boost::system::error_code &) {
    HeapProfilerDump("Request End");
    HeapProfilerStop();
    is_memory_profiling_ = false;
    reactor->Finish(grpc::Status::OK);
  });

  return reactor;
}

}  // namespace gcs
}  // namespace ray
