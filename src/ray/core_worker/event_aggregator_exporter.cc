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

#include "ray/core_worker/event_aggregator_exporter.h"

#include <memory>
#include <utility>

#include "ray/common/status.h"
#include "ray/rpc/event_aggregator_client.h"

namespace ray {

Status EventAggregatorExporter::AsyncAddRayEventData(
    std::unique_ptr<rpc::events::RayEventData> data_ptr,
    std::function<void(Status status)> callback) {
  rpc::events::AddEventRequest request;
  *request.mutable_events_data() = std::move(*data_ptr);
  client_impl_->AddEvents(
      request, [callback](const Status &status, const rpc::events::AddEventReply &reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Add Ray events. Status: " << status;
      });

  return Status::OK();
}

}  // namespace ray
