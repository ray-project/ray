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

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/function_descriptor.h"
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
                                     rpc::SchedulingStrategy sched_strategy);
  ResourceSet resource_set;
  LabelSelector label_selector;
  FunctionDescriptor function_descriptor;
  int64_t depth;
  rpc::SchedulingStrategy scheduling_strategy;

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
                    sched_cls.label_selector);
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
