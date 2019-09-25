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
      std::lock_guard<std::mutex> cond_lock(kill_mutex_);
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
  std::mutex profile_info_mutex_;
  WorkerContext &worker_context_;
  std::unique_ptr<gcs::RedisGcsClient> &gcs_client_;

  // Background thread that runs PeriodicalllyFlushEvents().
  std::thread thread_;
  // Flag checked by the background thread so it knows when to exit.
  bool killed_ = false;
  // Mutex guarding the killed_ flag.
  std::mutex kill_mutex_;
  // Condition variable used to signal to the thread that killed_ has been set.
  std::condition_variable kill_cond_;
};

class ProfilingEvent {
 public:
  ProfilingEvent(Profiler &profiler, const std::string &event_type);

  void SetExtraData(const std::string &extra_data) { inner_.set_extra_data(extra_data); }

  ~ProfilingEvent() {
    inner_.set_end_time(current_sys_time_seconds());
    profiler_.AddEvent(inner_);
  }

 private:
  Profiler &profiler_;
  rpc::ProfileTableData::ProfileEvent inner_;
};

}  // namespace worker

}  // namespace ray

#endif  // RAY_CORE_WORKER_PROFILING_H
