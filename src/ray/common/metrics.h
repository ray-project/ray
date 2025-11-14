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

inline ray::stats::Gauge GetObjectStoreMemoryGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"object_store_memory",
      /*description=*/"Object store memory by various sub-kinds on this node",
      /*unit=*/"",
      /// Location:
      ///    - MMAP_SHM: currently in shared memory(e.g. /dev/shm).
      ///    - MMAP_DISK: memory that's fallback allocated on mmapped disk,
      ///      e.g. /tmp.
      ///    - WORKER_HEAP: ray objects smaller than ('max_direct_call_object_size',
      ///      default 100KiB) stored in process memory, i.e. inlined return
      ///      values, placeholders for objects stored in plasma store.
      ///    - SPILLED: current number of bytes from objects spilled
      ///      to external storage. Note this might be smaller than
      ///      the physical storage incurred on the external storage because
      ///      Ray might fuse spilled objects into a single file, so a deleted
      ///      spill object might still exist in the spilled file. Check
      ///      spilled object fusing for more details.
      /// ObjectState:
      ///    - SEALED: sealed objects bytes (could be MMAP_SHM or MMAP_DISK)
      ///    - UNSEALED: unsealed objects bytes (could be MMAP_SHM or MMAP_DISK)
      /*tag_keys=*/{"Location", "ObjectState"},
  };
}

inline ray::stats::Histogram GetSchedulerPlacementTimeMsHistogramMetric() {
  return ray::stats::Histogram{
      /*name=*/"scheduler_placement_time_ms",
      /*description=*/
      "The time it takes for a workload (task, actor, placement group) to "
      "be placed. This is the time from when the tasks dependencies are "
      "resolved to when it actually reserves resources on a node to run.",
      /*unit=*/"ms",
      /*boundaries=*/{1, 10, 100, 1000, 10000},
      /*tag_keys=*/{"WorkloadType"},
  };
}

}  // namespace ray
