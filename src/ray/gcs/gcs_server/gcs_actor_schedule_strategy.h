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

#pragma once

#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"

namespace ray {
namespace gcs {

class GcsActor;

/// \class GcsActorScheduleStrategyInterface
///
/// Used for different kinds of actor scheduling strategy.
class GcsActorScheduleStrategyInterface {
 public:
  virtual ~GcsActorScheduleStrategyInterface() = default;

  /// Select a node to schedule the actor.
  ///
  /// \param actor The actor to be scheduled.
  /// \return The selected node. If the scheduling fails, nullptr is returned.
  virtual std::shared_ptr<rpc::GcsNodeInfo> Schedule(std::shared_ptr<GcsActor> actor) = 0;
};

/// \class GcsRandomActorScheduleStrategy
///
/// This strategy will select node randomly from the node pool to schedule the actor.
class GcsRandomActorScheduleStrategy : public GcsActorScheduleStrategyInterface {
 public:
  /// Create a GcsRandomActorScheduleStrategy
  ///
  /// \param gcs_node_manager Node management of the cluster, which provides interfaces
  /// to access the node information.
  explicit GcsRandomActorScheduleStrategy(
      std::shared_ptr<GcsNodeManager> gcs_node_manager)
      : gcs_node_manager_(std::move(gcs_node_manager)) {}

  virtual ~GcsRandomActorScheduleStrategy() = default;

  /// Select a node to schedule the actor.
  ///
  /// \param actor The actor to be scheduled.
  /// \return The selected node. If the scheduling fails, nullptr is returned.
  std::shared_ptr<rpc::GcsNodeInfo> Schedule(std::shared_ptr<GcsActor> actor) override;

 private:
  /// Select a node from alive nodes randomly.
  ///
  /// \return The selected node. If the scheduling fails, `nullptr` is returned.
  std::shared_ptr<rpc::GcsNodeInfo> SelectNodeRandomly() const;

  /// The node manager.
  std::shared_ptr<GcsNodeManager> gcs_node_manager_;
};

}  // namespace gcs
}  // namespace ray
