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
    std::unique_ptr<rpc::EventAggregatorClient> event_aggregator_client)
    : event_aggregator_client_(std::move(event_aggregator_client)) {}

template <typename TEventData>
void RayEventRecorderBase<TEventData>::RecordEvents(
    const std::vector<TEventData> &data_list) {
  rpc::events::AddEventRequest request;
  rpc::events::RayEventsData ray_event_data;
  for (auto const &data : data_list) {
    rpc::events::RayEvent ray_event;
    ConvertToRayEvent(data, ray_event);
    ray_event_data.mutable_events()->Add(std::move(ray_event));
  }
  *request.mutable_events_data() = std::move(ray_event_data);

  event_aggregator_client_->AddEvents(
      request, [](Status status, rpc::events::AddEventReply reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to record ray event: " << status.ToString();
        }
      });
}

template <typename TEventData>
void RayEventRecorderBase<TEventData>::SetCurrentTimestamp(
    google::protobuf::Timestamp *timestamp) {
  auto now = absl::Now();
  auto duration = now - absl::UnixEpoch();
  timestamp->set_seconds(absl::ToInt64Seconds(duration));
  timestamp->set_nanos(absl::ToInt64Nanoseconds(duration) % 1000000000);
}

template class RayEventRecorderBase<rpc::JobTableData>;
}  // namespace telemetry
}  // namespace ray
