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

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN  // The macro ensures that windows.h will include winsock2.h
                             // and not winsock.h. boost.asio (another dependency in the
                             // codebase) is not compatible with winsock.h.
                             // (https://stackoverflow.com/a/8294669).
#include <winsock2.h>
#endif  // #ifdef _WIN32

#include "ray/stats/metric.h"

namespace ray {

inline ray::stats::Gauge GetActorByStateGaugeMetric() {
  /// Tracks actors by state, including pending, running, and idle actors.
  ///
  /// To avoid metric collection conflicts between components reporting on the same actor,
  /// we use the "Source" required label.
  return ray::stats::Gauge{
      /*name=*/"actors",
      /*description=*/
      "An actor can be in one of DEPENDENCIES_UNREADY, PENDING_CREATION, ALIVE, "
      "ALIVE_IDLE, ALIVE_RUNNING_TASKS, RESTARTING, or DEAD states. "
      "An actor is considered ALIVE_IDLE if it is not executing any tasks.",
      /*unit=*/"",
      // State: the actor state, which is from rpc::ActorTableData::ActorState,
      // For ALIVE actor the sub-state can be IDLE, RUNNING_TASK,
      // RUNNING_IN_RAY_GET, and RUNNING_IN_RAY_WAIT.
      // Name: the name of actor class (Keep in sync with the TASK_OR_ACTOR_NAME_TAG_KEY
      // in python/ray/_private/telemetry/metric_cardinality.py) Source: component
      // reporting, e.g., "gcs" or "executor".
      /*tag_keys=*/{"State", "Name", "Source", "JobId"},
  };
}

}  // namespace ray
