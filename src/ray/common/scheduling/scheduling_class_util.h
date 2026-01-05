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

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/function_descriptor.h"
#include "ray/common/scheduling/fallback_strategy.h"
#include "ray/common/scheduling/label_selector.h"
#include "ray/common/scheduling/resource_set.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

bool operator==(const ray::rpc::SchedulingStrategy &lhs,
                const ray::rpc::SchedulingStrategy &rhs);

struct SchedulingClassDescriptor {
 public:
  explicit SchedulingClassDescriptor(ResourceSet rs,
                                     LabelSelector ls,
                                     FunctionDescriptor fd,
                                     int64_t d,
                                     rpc::SchedulingStrategy sched_strategy,
                                     std::vector<FallbackOption> fallback_strategy_p);
  ResourceSet resource_set;
  LabelSelector label_selector;
  FunctionDescriptor function_descriptor;
  int64_t depth;
  rpc::SchedulingStrategy scheduling_strategy;
  std::vector<FallbackOption> fallback_strategy;

  bool operator==(const SchedulingClassDescriptor &other) const;
  std::string DebugString() const;
  std::string ResourceSetStr() const;
};

template <typename H>
H AbslHashValue(H h, const SchedulingClassDescriptor &sched_cls) {
  return H::combine(std::move(h),
                    sched_cls.resource_set,
                    sched_cls.function_descriptor->Hash(),
                    sched_cls.depth,
                    sched_cls.scheduling_strategy,
                    sched_cls.label_selector,
                    sched_cls.fallback_strategy);
}

using SchedulingClass = int;

struct SchedulingClassToIds {
  /// Below static fields could be mutated in `ComputeResources` concurrently due to
  /// multi-threading, we need a mutex to protect it.
  static absl::Mutex mutex_;
  /// Keep global static id mappings for SchedulingClass for performance.
  static absl::flat_hash_map<SchedulingClassDescriptor, SchedulingClass> sched_cls_to_id_
      ABSL_GUARDED_BY(mutex_);
  static absl::flat_hash_map<SchedulingClass, SchedulingClassDescriptor> sched_id_to_cls_
      ABSL_GUARDED_BY(mutex_);
  static int next_sched_id_ ABSL_GUARDED_BY(mutex_);

  /// Gets the scheduling class descriptor for the given id.
  static SchedulingClassDescriptor &GetSchedulingClassDescriptor(SchedulingClass id);

  /// Gets or creates a scheduling class id for the given descriptor.
  static SchedulingClass GetSchedulingClass(const SchedulingClassDescriptor &sched_cls);
};

// Get a Hash for the runtime environment string.
// "" and "{}" have the same hash.
// Other than that, only compare literal strings. i.e. '{"a": 1, "b": 2}' and '{"b": 2,
// "a": 1}' have different hashes.
int CalculateRuntimeEnvHash(const std::string &serialized_runtime_env);
}  // namespace ray

// Template specializations for std::hash
namespace std {

template <>
struct hash<ray::rpc::LabelOperator> {
  size_t operator()(const ray::rpc::LabelOperator &label_operator) const {
    size_t hash_value = std::hash<size_t>()(label_operator.label_operator_case());
    if (label_operator.has_label_in()) {
      for (const auto &value : label_operator.label_in().values()) {
        hash_value ^= std::hash<std::string>()(value);
      }
    } else if (label_operator.has_label_not_in()) {
      for (const auto &value : label_operator.label_not_in().values()) {
        hash_value ^= std::hash<std::string>()(value);
      }
    }
    return hash_value;
  }
};

template <>
struct hash<ray::rpc::LabelMatchExpression> {
  size_t operator()(const ray::rpc::LabelMatchExpression &expression) const {
    size_t hash_val = std::hash<std::string>()(expression.key());
    hash_val ^= std::hash<ray::rpc::LabelOperator>()(expression.operator_());
    return hash_val;
  }
};

template <>
struct hash<ray::rpc::LabelMatchExpressions> {
  size_t operator()(const ray::rpc::LabelMatchExpressions &expressions) const {
    size_t hash_val = 0;
    for (const auto &expression : expressions.expressions()) {
      hash_val ^= std::hash<ray::rpc::LabelMatchExpression>()(expression);
    }
    return hash_val;
  }
};

template <>
struct hash<ray::rpc::SchedulingStrategy> {
  size_t operator()(const ray::rpc::SchedulingStrategy &scheduling_strategy) const {
    size_t hash_val = std::hash<size_t>()(scheduling_strategy.scheduling_strategy_case());
    if (scheduling_strategy.scheduling_strategy_case() ==
        ray::rpc::SchedulingStrategy::kNodeAffinitySchedulingStrategy) {
      hash_val ^= std::hash<std::string>()(
          scheduling_strategy.node_affinity_scheduling_strategy().node_id());
      // soft returns a bool
      hash_val ^= static_cast<size_t>(
          scheduling_strategy.node_affinity_scheduling_strategy().soft());
      hash_val ^= static_cast<size_t>(
          scheduling_strategy.node_affinity_scheduling_strategy().spill_on_unavailable());
      hash_val ^= static_cast<size_t>(
          scheduling_strategy.node_affinity_scheduling_strategy().fail_on_unavailable());
    } else if (scheduling_strategy.scheduling_strategy_case() ==
               ray::rpc::SchedulingStrategy::kPlacementGroupSchedulingStrategy) {
      hash_val ^= std::hash<std::string>()(
          scheduling_strategy.placement_group_scheduling_strategy().placement_group_id());
      hash_val ^= scheduling_strategy.placement_group_scheduling_strategy()
                      .placement_group_bundle_index();
      // placement_group_capture_child_tasks returns a bool
      hash_val ^=
          static_cast<size_t>(scheduling_strategy.placement_group_scheduling_strategy()
                                  .placement_group_capture_child_tasks());
    } else if (scheduling_strategy.has_node_label_scheduling_strategy()) {
      if (scheduling_strategy.node_label_scheduling_strategy().hard().expressions_size() >
          0) {
        hash_val ^= std::hash<std::string>()("hard");
        hash_val ^= std::hash<ray::rpc::LabelMatchExpressions>()(
            scheduling_strategy.node_label_scheduling_strategy().hard());
      }
      if (scheduling_strategy.node_label_scheduling_strategy().soft().expressions_size() >
          0) {
        hash_val ^= std::hash<std::string>()("soft");
        hash_val ^= std::hash<ray::rpc::LabelMatchExpressions>()(
            scheduling_strategy.node_label_scheduling_strategy().soft());
      }
    }
    return hash_val;
  }
};

}  // namespace std
