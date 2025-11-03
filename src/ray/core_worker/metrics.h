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

#include "ray/stats/metric.h"

namespace ray {
namespace core {

inline ray::stats::Gauge GetTaskByStateGaugeMetric() {
  /// Tracks tasks by state, including pending, running, and finished tasks.
  /// This metric may be recorded from multiple components processing the task in Ray,
  /// including the submitting core worker, executor core worker, and pull manager.
  ///
  /// To avoid metric collection conflicts between components reporting on the same task,
  /// we use the "Source" required label.
  return ray::stats::Gauge{
      /*name=*/"tasks",
      /*description=*/"Current number of tasks currently in a particular state.",
      /*unit=*/"",
      // Expected tags:
      // - State: the task state, as described by rpc::TaskState proto in common.proto
      // - Name: the name of the function called (Keep this tag name in sync with the
      // TASK_OR_ACTOR_NAME_TAG_KEY in
      // python/ray/_private/telemetry/metric_cardinality.py)
      // - IsRetry: whether the task is a retry
      // - Source: component reporting, e.g., "core_worker", "executor", or "pull_manager"
      /*tag_keys=*/{"State", "Name", "Source", "IsRetry", "JobId"},
  };
}

inline ray::stats::Gauge GetTotalLineageBytesGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"total_lineage_bytes",
      /*description=*/
      "Total amount of memory used to store task specs for lineage reconstruction.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

}  // namespace core
}  // namespace ray
