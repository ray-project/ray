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

#pragma once

#include "ray/telemetry/ray_event_recorder_base.h"
#include "src/ray/protobuf/events_driver_job_event.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace telemetry {

class RayJobEventRecorder : public RayEventRecorderBase<rpc::JobTableData> {
 public:
  RayJobEventRecorder(
      std::unique_ptr<rpc::EventAggregatorClient> event_aggregator_client);
  ~RayJobEventRecorder() override = default;

 private:
  void ConvertToRayEvent(const rpc::JobTableData &data,
                         rpc::events::RayEvent &ray_event) override;
};

}  // namespace telemetry
}  // namespace ray
