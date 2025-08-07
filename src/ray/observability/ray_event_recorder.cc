// Copyright 2025 The Ray Authors.
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

#include "ray/observability/ray_event_recorder.h"

#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace observability {

RayEventRecorder::RayEventRecorder(instrumented_io_context &io_service,
                                   int dashboard_agent_port)
    : periodical_runner_(PeriodicalRunner::Create(io_service)) {
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(
      io_service,
      /*record_stats=*/true,
      ClusterID::Nil(),
      RayConfig::instance().gcs_server_rpc_client_thread_num());
  event_aggregator_client_ = std::make_unique<rpc::EventAggregatorClientImpl>(
      dashboard_agent_port, *client_call_manager_);
}

void RayEventRecorder::StartExportingEvents() {
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(!exporting_started_)
      << "RayEventRecorder::StartExportingEvents() should be called only once.";
  exporting_started_ = true;
  periodical_runner_->RunFnPeriodically(
      [this]() { ExportEvents(); },
      RayConfig::instance().ray_events_report_interval_ms(),
      "RayEventRecorder.ExportEvents");
}

void RayEventRecorder::ExportEvents() {
  absl::MutexLock lock(&mutex_);
  if (buffer_.empty()) {
    return;
  }
  rpc::events::AddEventsRequest request;
  rpc::events::RayEventsData ray_event_data;
  // TODO(can-anyscale): To further optimize the performance, we can merge multiple
  // events with the same resource ID into a single event.
  for (auto const &event : buffer_) {
    rpc::events::RayEvent ray_event;
    event->Serialize(&ray_event);
    *ray_event_data.mutable_events()->Add() = std::move(ray_event);
  }
  *request.mutable_events_data() = std::move(ray_event_data);
  buffer_.clear();

  event_aggregator_client_->AddEvents(
      request, [](Status status, rpc::events::AddEventsReply reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to record ray event: " << status.ToString();
        }
      });
}

void RayEventRecorder::AddEvents(
    std::vector<std::unique_ptr<RayEventInterface>> &&data_list) {
  absl::MutexLock lock(&mutex_);
  for (auto &data : data_list) {
    buffer_.emplace_back(std::move(data));
  }
}

}  // namespace observability
}  // namespace ray
