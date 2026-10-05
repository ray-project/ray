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

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace observability {

RayEventRecorder::RayEventRecorder(
    rpc::EventAggregatorClient &event_aggregator_client,
    std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
    size_t max_buffer_size,
    std::string_view metric_source,
    ray::observability::MetricInterface &dropped_events_counter,
    const NodeID &node_id)
    : RayEventRecorderBase(event_aggregator_client,
                           std::move(periodical_runner),
                           max_buffer_size,
                           metric_source,
                           dropped_events_counter,
                           node_id),
      buffer_(max_buffer_size) {}

void RayEventRecorder::ExportEvents() {
  absl::MutexLock lock(&mutex_);
  if (buffer_.empty()) {
    return;
  }
  // Skip if there's already an in-flight gRPC call to avoid overlapping requests.
  // This prevents the race where StopExportingEvents() could return while a
  // periodic export is still in flight.
  if (grpc_in_progress_) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "Previous RayEventRecorder export in progress: new events will be exported "
           "once previous export completes.";
    return;
  }
  // Drain the buffer and hand the events to the shared group/serialize/send path.
  std::list<std::unique_ptr<RayEventInterface>> events;
  for (auto &event : buffer_) {
    events.push_back(std::move(event));
  }
  buffer_.clear();

  rpc::events::AddEventsRequest request;
  GroupAndSerializeEvents(std::move(events), request.mutable_events_data());
  SendRequest(std::move(request));
}

void RayEventRecorder::AddEvents(
    std::vector<std::unique_ptr<RayEventInterface>> &&data_list) {
  absl::MutexLock lock(&mutex_);
  if (!enabled_) {
    return;
  }
  if (!RayConfig::instance().enable_ray_event()) {
    return;
  }
  if (data_list.size() + buffer_.size() > max_buffer_size_) {
    size_t events_to_remove = data_list.size() + buffer_.size() - max_buffer_size_;
    // Record dropped events from the buffer
    RAY_LOG(ERROR) << "Dropping " << events_to_remove << " events from the buffer.";
    dropped_events_counter_.Record(events_to_remove,
                                   {{"Source", std::string(metric_source_)}});
  }
  for (auto &event : data_list) {
    buffer_.push_back(std::move(event));
  }
}

}  // namespace observability
}  // namespace ray
