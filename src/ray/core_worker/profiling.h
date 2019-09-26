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
           std::unique_ptr<gcs::RedisGcsClient> &gcs_client);

  ~Profiler() {
    {
      // Gracefully kill the background thread.
      std::lock_guard<std::mutex> lock(mutex_);
      killed_ = true;
      kill_cond_.notify_one();
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  // Spawn a thread to start flushing events periodically.
  void Start();

  // Add an event to the queue to be flushed periodically.
  void AddEvent(const rpc::ProfileTableData::ProfileEvent &event);

 private:
  // Periodically flush all of the events that have been added to the GCS.
  void PeriodicallyFlushEvents();

  rpc::ProfileTableData profile_info_;
  WorkerContext &worker_context_;
  std::unique_ptr<gcs::RedisGcsClient> &gcs_client_;

  // Background thread that runs PeriodicalllyFlushEvents().
  std::thread thread_;
  // Flag checked by the background thread so it knows when to exit.
  bool killed_;
  // Mutex guarding profile_info_ and the killed_ flag.
  std::mutex mutex_;
  // Condition variable used to signal to the thread that killed_ has been set.
  std::condition_variable kill_cond_;
};

class ProfileEvent {
 public:
  ProfileEvent(const std::shared_ptr<Profiler> profiler, const std::string &event_type);

  void SetExtraData(const std::string &extra_data) {
    rpc_event_.set_extra_data(extra_data);
  }

  ~ProfileEvent() {
    rpc_event_.set_end_time(current_sys_time_seconds());
    profiler_->AddEvent(rpc_event_);
  }

 private:
  const std::shared_ptr<Profiler> profiler_;
  rpc::ProfileTableData::ProfileEvent rpc_event_;
};

}  // namespace worker

}  // namespace ray

#endif  // RAY_CORE_WORKER_PROFILING_H
