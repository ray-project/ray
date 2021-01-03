// Copyright 2017 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_actor_schedule_strategy.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"

namespace ray {
namespace gcs {

std::shared_ptr<rpc::GcsNodeInfo> GcsRandomActorScheduleStrategy::Schedule(
    std::shared_ptr<GcsActor> actor) {
  // Select a node to lease worker for the actor.
  std::shared_ptr<rpc::GcsNodeInfo> node;

  // If an actor has resource requirements, we will try to schedule it on the same node as
  // the owner if possible.
  const auto &task_spec = actor->GetCreationTaskSpecification();
  if (!task_spec.GetRequiredResources().IsEmpty()) {
    auto maybe_node = gcs_node_manager_->GetAliveNode(actor->GetOwnerNodeID());
    node = maybe_node.has_value() ? maybe_node.value() : SelectNodeRandomly();
  } else {
    node = SelectNodeRandomly();
  }

  return node;
}

std::shared_ptr<rpc::GcsNodeInfo> GcsRandomActorScheduleStrategy::SelectNodeRandomly()
    const {
  auto &alive_nodes = gcs_node_manager_->GetAllAliveNodes();
  if (alive_nodes.empty()) {
    return nullptr;
  }

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, alive_nodes.size() - 1);
  int key_index = distribution(gen_);
  int index = 0;
  auto iter = alive_nodes.begin();
  for (; index != key_index && iter != alive_nodes.end(); ++index, ++iter)
    ;
  return iter->second;
}

}  // namespace gcs
}  // namespace ray
