// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/profiling.h"

#include <chrono>

namespace ray {
namespace core {

namespace worker {

ProfileEvent::ProfileEvent(const std::shared_ptr<Profiler> &profiler,
                           const std::string &event_type)
    : profiler_(profiler) {
  rpc_event_.set_event_type(event_type);
  rpc_event_.set_start_time(absl::GetCurrentTimeNanos() / 1e9);
}

Profiler::Profiler(WorkerContext &worker_context,
                   const std::string &node_ip_address,
                   instrumented_io_context &io_service,
                   const std::shared_ptr<gcs::GcsClient> &gcs_client)
    : io_service_(io_service),
      periodical_runner_(io_service_),
      rpc_profile_data_(new rpc::ProfileTableData()),
      gcs_client_(gcs_client) {
  rpc_profile_data_->set_component_type(WorkerTypeString(worker_context.GetWorkerType()));
  rpc_profile_data_->set_component_id(worker_context.GetWorkerID().Binary());
  rpc_profile_data_->set_node_ip_address(node_ip_address);
  periodical_runner_.RunFnPeriodically(
      [this] { FlushEvents(); },
      1000,
      "CoreWorker.deadline_timer.flush_profiling_events");
}

void Profiler::AddEvent(const rpc::ProfileTableData::ProfileEvent &event) {
  absl::MutexLock lock(&mutex_);
  rpc_profile_data_->add_profile_events()->CopyFrom(event);
}

void Profiler::FlushEvents() {
  auto cur_profile_data = std::make_shared<rpc::ProfileTableData>();
  {
    absl::MutexLock lock(&mutex_);
    if (rpc_profile_data_->profile_events_size() != 0) {
      cur_profile_data->set_component_type(rpc_profile_data_->component_type());
      cur_profile_data->set_component_id(rpc_profile_data_->component_id());
      cur_profile_data->set_node_ip_address(rpc_profile_data_->node_ip_address());
      rpc_profile_data_.swap(cur_profile_data);
    }
  }

  auto on_complete = [this](const Status &status) {
    absl::MutexLock lock(&mutex_);
    profile_flush_active_ = false;
  };

  if (cur_profile_data->profile_events_size() != 0) {
    // Check if we're backlogged first.
    {
      absl::MutexLock lock(&mutex_);
      if (profile_flush_active_) {
        RAY_LOG(WARNING) << "The GCS is backlogged processing profiling data. "
                            "Some events may be dropped.";
        return;  // Drop the events; we're behind.
      } else {
        profile_flush_active_ = true;
      }
    }
    if (!gcs_client_->Stats().AsyncAddProfileData(cur_profile_data, on_complete).ok()) {
      RAY_LOG(WARNING)
          << "Failed to push profile events to GCS. This won't affect core Ray, but you "
             "might lose profile data, and ray timeline might not work as expected.";
    } else {
      RAY_LOG(DEBUG) << "Pushed " << cur_profile_data->profile_events_size()
                     << " events to GCS.";
    }
  }
}

}  // namespace worker

}  // namespace core
}  // namespace ray
