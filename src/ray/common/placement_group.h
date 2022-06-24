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

#include "ray/common/bundle_spec.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};

using BundleLocations =
    absl::flat_hash_map<BundleID,
                        std::pair<NodeID, std::shared_ptr<const BundleSpecification>>,
                        pair_hash>;

class PlacementGroupSpecification : public MessageWrapper<rpc::PlacementGroupSpec> {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit PlacementGroupSpecification(rpc::PlacementGroupSpec message)
      : MessageWrapper(message) {
    ConstructBundles();
  }
  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit PlacementGroupSpecification(std::shared_ptr<rpc::PlacementGroupSpec> message)
      : MessageWrapper(message) {
    ConstructBundles();
  }
  /// Return the placement group id.
  PlacementGroupID PlacementGroupId() const;
  /// Return the bundles in this placement group.
  std::vector<BundleSpecification> GetBundles() const;
  /// Return the strategy of the placement group.
  rpc::PlacementStrategy GetStrategy() const;
  /// Return the bundle by given index.
  BundleSpecification GetBundle(int position) const;
  /// Return the name of this placement group.
  std::string GetName() const;

 private:
  /// Construct bundle vector from protobuf.
  void ConstructBundles();
  /// The bundles in this placement group.
  std::vector<BundleSpecification> bundles_;
};

class PlacementGroupSpecBuilder {
 public:
  PlacementGroupSpecBuilder() : message_(std::make_shared<rpc::PlacementGroupSpec>()) {}

  /// Set the common attributes of the placement group spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  PlacementGroupSpecBuilder &SetPlacementGroupSpec(
      const PlacementGroupID &placement_group_id,
      std::string name,
      const std::vector<std::unordered_map<std::string, double>> &bundles,
      const rpc::PlacementStrategy strategy,
      const bool is_detached,
      const JobID &creator_job_id,
      const ActorID &creator_actor_id,
      bool is_creator_detached_actor) {
    message_->set_placement_group_id(placement_group_id.Binary());
    message_->set_name(name);
    message_->set_strategy(strategy);
    // Configure creator job and actor ID for automatic lifecycle management.
    RAY_CHECK(!creator_job_id.IsNil());
    message_->set_creator_job_id(creator_job_id.Binary());
    // When the creator is detached actor, we should just consider the job is dead.
    // It is because the detached actor can be created AFTER the job is dead.
    // Imagine a case where detached actor is restarted by GCS after the creator job is
    // dead.
    message_->set_creator_job_dead(is_creator_detached_actor);
    message_->set_creator_actor_id(creator_actor_id.Binary());
    message_->set_creator_actor_dead(creator_actor_id.IsNil());
    message_->set_is_detached(is_detached);

    for (size_t i = 0; i < bundles.size(); i++) {
      auto resources = bundles[i];
      auto message_bundle = message_->add_bundles();
      auto mutable_bundle_id = message_bundle->mutable_bundle_id();
      mutable_bundle_id->set_bundle_index(i);
      mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
      auto mutable_unit_resources = message_bundle->mutable_unit_resources();
      for (auto it = resources.begin(); it != resources.end();) {
        auto current = it++;
        // Remove a resource with value 0 because they are not allowed.
        if (current->second == 0) {
          resources.erase(current);
        } else {
          mutable_unit_resources->insert({current->first, current->second});
        }
      }
    }
    return *this;
  }

  PlacementGroupSpecification Build() { return PlacementGroupSpecification(message_); }

 private:
  std::shared_ptr<rpc::PlacementGroupSpec> message_;
};

}  // namespace ray
