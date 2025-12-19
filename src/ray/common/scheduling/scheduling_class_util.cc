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

#include "ray/common/scheduling/scheduling_class_util.h"

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/util/message_differencer.h"
#include "ray/common/runtime_env_common.h"
#include "ray/util/logging.h"

namespace ray {

SchedulingClassDescriptor::SchedulingClassDescriptor(
    ResourceSet rs,
    LabelSelector ls,
    FunctionDescriptor fd,
    int64_t d,
    rpc::SchedulingStrategy sched_strategy,
    std::vector<FallbackOption> fallback_strat)
    : resource_set(std::move(rs)),
      label_selector(std::move(ls)),
      function_descriptor(std::move(fd)),
      depth(d),
      scheduling_strategy(std::move(sched_strategy)),
      fallback_strategy(std::move(fallback_strat)) {}

bool operator==(const ray::rpc::SchedulingStrategy &lhs,
                const ray::rpc::SchedulingStrategy &rhs) {
  if (lhs.scheduling_strategy_case() != rhs.scheduling_strategy_case()) {
    return false;
  }

  switch (lhs.scheduling_strategy_case()) {
  case ray::rpc::SchedulingStrategy::kNodeAffinitySchedulingStrategy: {
    return (lhs.node_affinity_scheduling_strategy().node_id() ==
            rhs.node_affinity_scheduling_strategy().node_id()) &&
           (lhs.node_affinity_scheduling_strategy().soft() ==
            rhs.node_affinity_scheduling_strategy().soft()) &&
           (lhs.node_affinity_scheduling_strategy().spill_on_unavailable() ==
            rhs.node_affinity_scheduling_strategy().spill_on_unavailable()) &&
           (lhs.node_affinity_scheduling_strategy().fail_on_unavailable() ==
            rhs.node_affinity_scheduling_strategy().fail_on_unavailable());
  }
  case ray::rpc::SchedulingStrategy::kPlacementGroupSchedulingStrategy: {
    return (lhs.placement_group_scheduling_strategy().placement_group_id() ==
            rhs.placement_group_scheduling_strategy().placement_group_id()) &&
           (lhs.placement_group_scheduling_strategy().placement_group_bundle_index() ==
            rhs.placement_group_scheduling_strategy().placement_group_bundle_index()) &&
           (lhs.placement_group_scheduling_strategy()
                .placement_group_capture_child_tasks() ==
            rhs.placement_group_scheduling_strategy()
                .placement_group_capture_child_tasks());
  }
  case ray::rpc::SchedulingStrategy::kNodeLabelSchedulingStrategy: {
    return google::protobuf::util::MessageDifferencer::Equivalent(
        lhs.node_label_scheduling_strategy(), rhs.node_label_scheduling_strategy());
  }
  default:
    return true;
  }
}

// SchedulingClassDescriptor methods
bool SchedulingClassDescriptor::operator==(const SchedulingClassDescriptor &other) const {
  return depth == other.depth && resource_set == other.resource_set &&
         label_selector == other.label_selector &&
         function_descriptor == other.function_descriptor &&
         scheduling_strategy == other.scheduling_strategy &&
         fallback_strategy == other.fallback_strategy;
}

std::string SchedulingClassDescriptor::DebugString() const {
  std::stringstream buffer;
  buffer << "{"
         << "depth=" << depth << " "
         << "function_descriptor=" << function_descriptor->ToString() << " "
         << "scheduling_strategy=" << scheduling_strategy.DebugString() << " "
         << "resource_set="
         << "{";
  for (const auto &pair : resource_set.GetResourceMap()) {
    buffer << pair.first << " : " << pair.second << ", ";
  }
  buffer << "}";

  buffer << "label_selector={";
  for (const auto &constraint : label_selector.GetConstraints()) {
    buffer << constraint.GetLabelKey() << " "
           << (constraint.GetOperator() == ray::LabelSelectorOperator::LABEL_IN ? "in"
                                                                                : "!in")
           << " (";
    for (const auto &val : constraint.GetLabelValues()) {
      buffer << val << ", ";
    }
    buffer << "), ";
  }
  buffer << "}}";

  // Add fallback strategy LabelSelectors.
  buffer << "fallback_strategy=[";
  bool is_first_option = true;
  for (const auto &fallback_option : fallback_strategy) {
    if (!is_first_option) {
      buffer << ", ";
    }
    buffer << "{";
    bool is_first_constraint = true;
    for (const auto &constraint : fallback_option.label_selector.GetConstraints()) {
      if (!is_first_constraint) {
        buffer << ", ";
      }
      buffer << constraint.GetLabelKey() << " "
             << (constraint.GetOperator() == ray::LabelSelectorOperator::LABEL_IN ? "in"
                                                                                  : "!in")
             << " (";
      bool is_first_value = true;
      for (const auto &val : constraint.GetLabelValues()) {
        if (!is_first_value) {
          buffer << ", ";
        }
        buffer << val;
        is_first_value = false;
      }
      buffer << ")";
      is_first_constraint = false;
    }
    buffer << "}";
    is_first_option = false;
  }
  buffer << "]";

  return buffer.str();
}

std::string SchedulingClassDescriptor::ResourceSetStr() const {
  std::stringstream buffer;
  buffer << "{";
  for (const auto &pair : resource_set.GetResourceMap()) {
    buffer << pair.first << " : " << pair.second << ", ";
  }
  buffer << "}";
  return buffer.str();
}

// Static member definitions
absl::Mutex SchedulingClassToIds::mutex_;
absl::flat_hash_map<SchedulingClassDescriptor, SchedulingClass>
    SchedulingClassToIds::sched_cls_to_id_;
absl::flat_hash_map<SchedulingClass, SchedulingClassDescriptor>
    SchedulingClassToIds::sched_id_to_cls_;
int SchedulingClassToIds::next_sched_id_;

SchedulingClassDescriptor &SchedulingClassToIds::GetSchedulingClassDescriptor(
    SchedulingClass id) {
  absl::MutexLock lock(&mutex_);
  auto it = sched_id_to_cls_.find(id);
  RAY_CHECK(it != sched_id_to_cls_.end()) << "invalid id: " << id;
  return it->second;
}

SchedulingClass SchedulingClassToIds::GetSchedulingClass(
    const SchedulingClassDescriptor &sched_cls) {
  SchedulingClass sched_cls_id = 0;
  absl::MutexLock lock(&mutex_);
  auto it = sched_cls_to_id_.find(sched_cls);
  if (it == sched_cls_to_id_.end()) {
    sched_cls_id = ++next_sched_id_;
    // TODO(ekl) we might want to try cleaning up task types in these cases
    if (sched_cls_id > 100) {
      RAY_LOG_EVERY_MS(WARNING, 1000)
          << "More than " << sched_cls_id
          << " types of tasks seen, this may reduce performance.";
    }
    sched_cls_to_id_[sched_cls] = sched_cls_id;
    sched_id_to_cls_.emplace(sched_cls_id, sched_cls);
  } else {
    sched_cls_id = it->second;
  }
  return sched_cls_id;
}

int CalculateRuntimeEnvHash(const std::string &serialized_runtime_env) {
  if (IsRuntimeEnvEmpty(serialized_runtime_env)) {
    // It's useful to have the same predetermined value for both unspecified and empty
    // runtime envs.
    return 0;
  }
  size_t hash = std::hash<std::string>()(serialized_runtime_env);
  return static_cast<int>(hash);
}

}  // namespace ray
