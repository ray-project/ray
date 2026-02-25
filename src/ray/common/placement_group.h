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

#include "absl/container/flat_hash_map.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/label_selector.h"
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

// Defines the structure of a scheduling option to try when scheduling the placement
// group.
struct PlacementGroupSchedulingOption {
  std::vector<std::unordered_map<std::string, double>> bundles;
  std::vector<LabelSelector> bundle_label_selector;
};

class PlacementGroupSpecification : public MessageWrapper<rpc::PlacementGroupSpec> {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit PlacementGroupSpecification(rpc::PlacementGroupSpec message)
      : MessageWrapper(std::move(message)) {
    ConstructBundles();
  }
  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit PlacementGroupSpecification(std::shared_ptr<rpc::PlacementGroupSpec> message)
      : MessageWrapper(std::move(message)) {
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
      NodeID soft_target_node_id,
      const JobID &creator_job_id,
      const ActorID &creator_actor_id,
      bool is_creator_detached_actor,
      const std::vector<LabelSelector> &bundle_label_selector = {},
      const std::vector<PlacementGroupSchedulingOption> &fallback_strategy = {}) {
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
    message_->set_soft_target_node_id(soft_target_node_id.Binary());

    // Populate primary strategy bundles.
    PopulateBundles(message_.get(), placement_group_id, bundles, bundle_label_selector);

    // Populate fallback strategy bundles.
    for (const auto &option : fallback_strategy) {
      auto *fallback_message = message_->add_fallback_strategy();
      PopulateBundles(fallback_message,
                      placement_group_id,
                      option.bundles,
                      option.bundle_label_selector);
    }

    return *this;
  }

  PlacementGroupSpecification Build() { return PlacementGroupSpecification(message_); }

 private:
  /// Helper to populate bundles for both primary and fallback scheduling options.
  template <typename ProtoMessageType>
  void PopulateBundles(
      ProtoMessageType *proto_message,
      const PlacementGroupID &placement_group_id,
      const std::vector<std::unordered_map<std::string, double>> &bundles,
      const std::vector<LabelSelector> &label_selectors) {
    for (size_t i = 0; i < bundles.size(); i++) {
      auto *message_bundle = proto_message->add_bundles();
      auto *mutable_bundle_id = message_bundle->mutable_bundle_id();
      mutable_bundle_id->set_bundle_index(i);
      mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());

      auto *mutable_unit_resources = message_bundle->mutable_unit_resources();
      for (const auto &resource : bundles[i]) {
        // Resources with value 0 are not allowed.
        if (resource.second > 0) {
          mutable_unit_resources->insert({resource.first, resource.second});
        }
      }

      // Set the label selector for this bundle if provided.
      if (label_selectors.size() > i) {
        label_selectors[i].ToProto(message_bundle->mutable_label_selector());
      }
    }
  }

  std::shared_ptr<rpc::PlacementGroupSpec> message_;
};

}  // namespace ray
