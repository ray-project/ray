#include <chrono>

#include "ray/core_worker/profiling.h"

namespace ray {

namespace worker {

ProfileEvent::ProfileEvent(const std::shared_ptr<Profiler> profiler,
                           const std::string &event_type)
    : profiler_(profiler) {
  rpc_event_.set_event_type(event_type);
  rpc_event_.set_start_time(current_sys_time_seconds());
}

Profiler::Profiler(WorkerContext &worker_context, const std::string &node_ip_address,
                   boost::asio::io_service &io_service,
                   std::unique_ptr<gcs::RedisGcsClient> &gcs_client)
    : io_service_(io_service),
      timer_(io_service_, boost::asio::chrono::seconds(1)),
      gcs_client_(gcs_client) {
  rpc_profile_data_.set_component_type(WorkerTypeString(worker_context.GetWorkerType()));
  rpc_profile_data_.set_component_id(worker_context.GetWorkerID().Binary());
  rpc_profile_data_.set_node_ip_address(node_ip_address);
  timer_.async_wait(boost::bind(&Profiler::FlushEvents, this));
}

void Profiler::AddEvent(const rpc::ProfileTableData::ProfileEvent &event) {
  io_service_.post([this, event]() -> void {
    rpc_profile_data_.add_profile_events()->CopyFrom(event);
  });
}

void Profiler::FlushEvents() {
  if (rpc_profile_data_.profile_events_size() != 0) {
    // TODO(edoakes): this should be migrated to use the new GCS client interface
    // instead of the raw table interface once it's ready.
    if (!gcs_client_->profile_table().AddProfileEventBatch(rpc_profile_data_).ok()) {
      RAY_LOG(WARNING) << "Failed to push profile events to GCS.";
    } else {
      RAY_LOG(DEBUG) << "Pushed " << rpc_profile_data_.profile_events_size()
                     << "events to GCS.";
    }
    rpc_profile_data_.clear_profile_events();
  }
  // Reset the timer to 1 second from the previous expiration time to avoid drift.
  timer_.expires_at(timer_.expiry() + boost::asio::chrono::seconds(1));
  timer_.async_wait(boost::bind(&Profiler::FlushEvents, this));
}

}  // namespace worker

}  // namespace ray
