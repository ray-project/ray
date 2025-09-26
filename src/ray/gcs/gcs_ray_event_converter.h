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

#include <vector>

#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// Convert an AddEventsRequest to a list of AddTaskEventDataRequest objects,
/// grouping entries by job id.
///
/// \param request The AddEventsRequest to convert.
/// \return A list of AddTaskEventDataRequest grouped by job id.
std::vector<rpc::AddTaskEventDataRequest> ConvertToTaskEventDataRequests(
    rpc::events::AddEventsRequest &&request);

}  // namespace gcs
}  // namespace ray
