#ifndef RAY_CORE_WORKER_PROFILING_H
#define RAY_CORE_WORKER_PROFILING_H

#include "ray/core_worker/context.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/util.h"

namespace ray {

namespace worker {

class Profiler {
 public:
  Profiler(WorkerContext &worker_context, const std::string &node_ip_address,
           boost::asio::io_service &io_service,
           std::unique_ptr<gcs::RedisGcsClient> &gcs_client);

  // Add an event to the queue to be flushed periodically.
  void AddEvent(const rpc::ProfileTableData::ProfileEvent &event);

 private:
  // Flush all of the events that have been added since last flush to the GCS.
  void FlushEvents();

  // ASIO IO service event loop. Must be started by the caller.
  boost::asio::io_service &io_service_;

  // Timer used to periodically flush events to the GCS.
  boost::asio::steady_timer timer_;

  // RPC message containing profiling data. Holds the queue of profile events
  // until they are flushed.
  rpc::ProfileTableData rpc_profile_data_;

  std::unique_ptr<gcs::RedisGcsClient> &gcs_client_;
};

class ProfileEvent {
 public:
  ProfileEvent(const std::shared_ptr<Profiler> profiler, const std::string &event_type);

  ~ProfileEvent() {
    rpc_event_.set_end_time(current_sys_time_seconds());
    profiler_->AddEvent(rpc_event_);
  }

  void SetExtraData(const std::string &extra_data) {
    rpc_event_.set_extra_data(extra_data);
  }

 private:
  const std::shared_ptr<Profiler> profiler_;
  rpc::ProfileTableData::ProfileEvent rpc_event_;
};

}  // namespace worker

}  // namespace ray

#endif  // RAY_CORE_WORKER_PROFILING_H
