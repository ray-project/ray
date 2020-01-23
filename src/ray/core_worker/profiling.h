#ifndef RAY_CORE_WORKER_PROFILING_H
#define RAY_CORE_WORKER_PROFILING_H

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "ray/core_worker/context.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace worker {

class Profiler {
 public:
  Profiler(WorkerContext &worker_context, const std::string &node_ip_address,
           boost::asio::io_service &io_service,
           const std::shared_ptr<gcs::GcsClient> &gcs_client);

  // Add an event to the queue to be flushed periodically.
  void AddEvent(const rpc::ProfileTableData::ProfileEvent &event) LOCKS_EXCLUDED(mutex_);

 private:
  // Flush all of the events that have been added since last flush to the GCS.
  void FlushEvents() LOCKS_EXCLUDED(mutex_);

  // Mutex guarding rpc_profile_data_.
  absl::Mutex mutex_;

  // ASIO IO service event loop. Must be started by the caller.
  boost::asio::io_service &io_service_;

  // Timer used to periodically flush events to the GCS.
  boost::asio::steady_timer timer_;

  // RPC message containing profiling data. Holds the queue of profile events
  // until they are flushed.
  std::shared_ptr<rpc::ProfileTableData> rpc_profile_data_ GUARDED_BY(mutex_);

  // Client to the GCS used to push profile events to it.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
};

class ProfileEvent {
 public:
  ProfileEvent(const std::shared_ptr<Profiler> &profiler, const std::string &event_type);

  // Set the end time for the event and add it to the profiler.
  ~ProfileEvent() {
    rpc_event_.set_end_time(absl::GetCurrentTimeNanos() / 1e9);
    profiler_->AddEvent(rpc_event_);
  }

  // Set extra metadata for the event, which could change during the event.
  void SetExtraData(const std::string &extra_data) {
    rpc_event_.set_extra_data(extra_data);
  }

 private:
  // shared_ptr to the profiler that this event will be added to when it is destructed.
  std::shared_ptr<Profiler> profiler_;

  // Underlying proto data structure that holds the event data.
  rpc::ProfileTableData::ProfileEvent rpc_event_;
};

}  // namespace worker

}  // namespace ray

#endif  // RAY_CORE_WORKER_PROFILING_H
