#ifndef RAY_CORE_WORKER_PROFILING_H
#define RAY_CORE_WORKER_PROFILING_H

#include "ray/core_worker/context.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/util.h"

namespace ray {

namespace worker {

class Profiler {
 public:
  Profiler(WorkerContext &worker_context,
           std::unique_ptr<gcs::RedisGcsClient> &gcs_client);

  ~Profiler() {
    {
      // Gracefully kill the background thread.
      std::lock_guard<std::mutex> cond_lock(kill_mutex_);
      killed_ = true;
      kill_cond_.notify_one();
    }
    thread_.join();
  }

  void AddEvent(const rpc::ProfileTableData::ProfileEvent &event);

 private:
  void PushEvents();

  rpc::ProfileTableData profile_info_;

  // Used to gracefully kill the background thread.
  bool killed_ = false;
  std::mutex kill_mutex_;
  std::condition_variable kill_cond_;

  std::mutex mutex_;
  std::thread thread_;
  WorkerContext &worker_context_;
  std::unique_ptr<gcs::RedisGcsClient> &gcs_client_;
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
