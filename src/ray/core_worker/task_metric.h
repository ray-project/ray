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

namespace ray::core {

/// Returns the metric definition for the ray_tasks metrics. We define a helper function
/// since the metric is re-defined in multiple processes (raylet, core_worker, etc.).
///
/// Do not reuse this file to define other metrics. Doing so will potentially bloat the
/// build dependency graph, causing the build to take longer. For most use cases, metrics
/// should be directly defined inside the component that uses it.
inline ray::stats::Gauge GetTaskMetric() {
  /// Tracks tasks by state, including pending, running, and finished tasks.
  /// This metric may be recorded from multiple components processing the task in Ray,
  /// including the submitting core worker, executor core worker, and pull manager.
  ///
  /// To avoid metric collection conflicts between components reporting on the same task,
  /// we use the "Source" required label.
  return ray::stats::Gauge{
      /*name=*/"tasks",
      // State: the task state, as described by rpc::TaskState proto in common.proto.
      // Name: the name of the function called (Keep in sync with the
      // TASK_OR_ACTOR_NAME_TAG_KEY in
      // python/ray/_private/telemetry/metric_cardinality.py) Source: component reporting,
      // e.g., "core_worker", "executor", or "pull_manager". IsRetry: whether this task is
      // a retry.
      /*description=*/"Current number of tasks currently in a particular state.",
      /*unit=*/"",
      /*tag_keys=*/{"State", "Name", "Source", "IsRetry", "JobId"},
  };
}

}  // namespace ray::core
