// Copyright 2017-2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/task/lease_spec.h"

namespace ray {

LeaseSpec::LeaseSpec(const TaskSpecification &task_spec)
    : job_id_(task_spec.JobId()),
      lease_id_(task_spec.LeaseId()),
      caller_address_(task_spec.CallerAddress()),
      type_(task_spec.GetMessage().type()),
      actor_creation_id_(task_spec.ActorCreationId()),
      is_detached_actor_(task_spec.IsDetachedActor()),
      root_detached_actor_id_(task_spec.RootDetachedActorId()),
      sched_cls_id_(task_spec.GetSchedulingClass()),
      required_resources_(task_spec.GetRequiredResources()),
      required_placement_resources_(task_spec.GetRequiredPlacementResources()),
      scheduling_strategy_(task_spec.GetSchedulingStrategy()),
      label_selector_(task_spec.GetLabelSelector()),
      depth_(task_spec.GetDepth()),
      runtime_env_hash_(task_spec.GetRuntimeEnvHash()),
      dependencies_(task_spec.GetDependencies()) {}

bool LeaseSpec::IsNodeAffinitySchedulingStrategy() const {
  return scheduling_strategy_.scheduling_strategy_case() ==
         rpc::SchedulingStrategy::kNodeAffinitySchedulingStrategy;
}

NodeID LeaseSpec::GetNodeAffinitySchedulingStrategyNodeId() const {
  if (!IsNodeAffinitySchedulingStrategy()) {
    return NodeID::Nil();
  }
  return NodeID::FromBinary(
      scheduling_strategy_.node_affinity_scheduling_strategy().node_id());
}

bool LeaseSpec::GetNodeAffinitySchedulingStrategySoft() const {
  if (!IsNodeAffinitySchedulingStrategy()) {
    return false;
  }
  return scheduling_strategy_.node_affinity_scheduling_strategy().soft();
}

std::vector<ObjectID> LeaseSpec::GetDependencyIds() const {
  std::vector<ObjectID> ids;
  ids.reserve(dependencies_.size());
  for (const auto &ref : dependencies_) {
    ids.emplace_back(ObjectRefToId(ref));
  }
  return ids;
}

WorkerID LeaseSpec::CallerWorkerId() const {
  if (caller_address_.worker_id().empty()) {
    return WorkerID::Nil();
  }
  return WorkerID::FromBinary(caller_address_.worker_id());
}

NodeID LeaseSpec::CallerNodeId() const {
  if (caller_address_.raylet_id().empty()) {
    return NodeID::Nil();
  }
  return NodeID::FromBinary(caller_address_.raylet_id());
}

const BundleID LeaseSpec::PlacementGroupBundleId() const {
  if (scheduling_strategy_.scheduling_strategy_case() !=
      rpc::SchedulingStrategy::kPlacementGroupSchedulingStrategy) {
    return std::make_pair(PlacementGroupID::Nil(), -1);
  }
  const auto &pg = scheduling_strategy_.placement_group_scheduling_strategy();
  return std::make_pair(PlacementGroupID::FromBinary(pg.placement_group_id()),
                        pg.placement_group_bundle_index());
}

}  // namespace ray
