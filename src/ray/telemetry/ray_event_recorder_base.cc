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

#include "ray/telemetry/ray_event_recorder_base.h"

#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace telemetry {

template <typename TEventData>
RayEventRecorderBase<TEventData>::RayEventRecorderBase(
    rpc::EventAggregatorClient &event_aggregator_client,
    instrumented_io_context &io_service)
    : event_aggregator_client_(event_aggregator_client),
      periodical_runner_(PeriodicalRunner::Create(io_service)) {}

template <typename TEventData>
void RayEventRecorderBase<TEventData>::StartExportingEvents() {
  absl::MutexLock lock(&mutex_);
  if (exporting_started_) {
    return;  // Already started, don't start again
  }
  exporting_started_ = true;
  periodical_runner_->RunFnPeriodically(
      [this]() { ExportEvents(); },
      RayConfig::instance().ray_events_report_interval_ms(),
      "RayEventRecorderBase<" + std::string(typeid(TEventData).name()) +
          ">.ExportEvents");
}

template <typename TEventData>
void RayEventRecorderBase<TEventData>::ExportEvents() {
  absl::MutexLock lock(&mutex_);
  if (buffer_.empty()) {
    return;
  }
  rpc::events::AddEventsRequest request;
  rpc::events::RayEventsData ray_event_data;
  for (auto const &event : buffer_) {
    *ray_event_data.mutable_events()->Add() = std::move(event);
  }
  *request.mutable_events_data() = std::move(ray_event_data);
  buffer_.clear();

  event_aggregator_client_.AddEvents(
      request, [](Status status, rpc::events::AddEventsReply reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to record ray event: " << status.ToString();
        }
      });
}

template <typename TEventData>
void RayEventRecorderBase<TEventData>::AddEvents(
    const std::vector<TEventData> &data_list) {
  absl::MutexLock lock(&mutex_);
  for (auto const &data : data_list) {
    ConvertToRayEvent(data, buffer_.emplace_back());
  }
}

template class RayEventRecorderBase<rpc::JobTableData>;
}  // namespace telemetry
}  // namespace ray
