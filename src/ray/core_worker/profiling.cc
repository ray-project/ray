#include <chrono>

#include "ray/core_worker/profiling.h"

namespace ray {

namespace worker {

ProfilingEvent::ProfilingEvent(Profiler &profiler, const std::string &event_type)
    : profiler_(profiler) {
  inner_.set_event_type(event_type);
  inner_.set_start_time(current_sys_time_seconds());
}

Profiler::Profiler(WorkerContext &worker_context,
                   std::unique_ptr<gcs::RedisGcsClient> &gcs_client)
    : worker_context_(worker_context), gcs_client_(gcs_client) {
  profile_info_.set_component_type(WorkerTypeString(worker_context_.GetWorkerType()));
  profile_info_.set_component_id(worker_context_.GetWorkerID().Binary());
  profile_info_.set_node_ip_address(worker_context_.GetNodeIPAddress());
  thread_ = std::thread(&Profiler::PushEvents, this);
}

void Profiler::AddEvent(const rpc::ProfileTableData::ProfileEvent &event) {
  std::lock_guard<std::mutex> lock(mutex_);
  profile_info_.add_profile_events()->CopyFrom(event);
}

void Profiler::PushEvents() {
  while (true) {
    // Push events every 1 second until killed_ is set.
    std::unique_lock<std::mutex> cond_lock(kill_mutex_);
    kill_cond_.wait_for(cond_lock, std::chrono::seconds(1));
    if (killed_) {
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (profile_info_.profile_events_size() == 0) {
        continue;
      }
      if (!gcs_client_->profile_table().AddProfileEventBatch(profile_info_).ok()) {
        RAY_LOG(WARNING) << "Failed to push profile events to GCS.";
      } else {
        RAY_LOG(DEBUG) << "Pushed " << profile_info_.profile_events_size()
                       << "events to GCS.";
      }
      profile_info_.clear_profile_events();
    }
  }
}

}  // namespace worker

}  // namespace ray
