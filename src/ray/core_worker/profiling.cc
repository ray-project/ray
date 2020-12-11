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

namespace worker {

ProfileEvent::ProfileEvent(const std::shared_ptr<Profiler> &profiler,
                           const std::string &event_type)
    : profiler_(profiler) {
  rpc_event_.set_event_type(event_type);
  rpc_event_.set_start_time(absl::GetCurrentTimeNanos() / 1e9);
}

Profiler::Profiler(WorkerContext &worker_context, const std::string &node_ip_address,
                   boost::asio::io_service &io_service,
                   const std::shared_ptr<gcs::GcsClient> &gcs_client)
    : io_service_(io_service),
      timer_(io_service_, boost::asio::chrono::seconds(1)),
      rpc_profile_data_(new rpc::ProfileTableData()),
      gcs_client_(gcs_client) {
  rpc_profile_data_->set_component_type(WorkerTypeString(worker_context.GetWorkerType()));
  rpc_profile_data_->set_component_id(worker_context.GetWorkerID().Binary());
  rpc_profile_data_->set_node_ip_address(node_ip_address);
  timer_.async_wait(boost::bind(&Profiler::FlushEvents, this));
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

  if (cur_profile_data->profile_events_size() != 0) {
    if (!gcs_client_->Stats().AsyncAddProfileData(cur_profile_data, nullptr).ok()) {
      RAY_LOG(WARNING)
          << "Failed to push profile events to GCS. This won't affect core Ray, but you "
             "might lose profile data, and ray timeline might not work as expected.";
    } else {
      RAY_LOG(DEBUG) << "Pushed " << cur_profile_data->profile_events_size()
                     << " events to GCS.";
    }
  }

  // Reset the timer to 1 second from the previous expiration time to avoid drift.
  timer_.expires_at(timer_.expiry() + boost::asio::chrono::seconds(1));
  timer_.async_wait(boost::bind(&Profiler::FlushEvents, this));
}

}  // namespace worker

}  // namespace ray
