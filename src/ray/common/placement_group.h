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
      const PlacementGroupID &placement_group_id, std::string name,
      const std::vector<std::unordered_map<std::string, double>> &bundles,
      const rpc::PlacementStrategy strategy) {
    message_->set_placement_group_id(placement_group_id.Binary());
    message_->set_name(name);
    message_->set_strategy(strategy);
    for (size_t i = 0; i < bundles.size(); i++) {
      auto resources = bundles[i];
      auto message_bundle = message_->add_bundles();
      auto mutable_bundle_id = message_bundle->mutable_bundle_id();
      mutable_bundle_id->set_bundle_index(i);
      mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
      message_bundle->mutable_unit_resources()->insert(resources.begin(),
                                                       resources.end());
    }
    return *this;
  }

  PlacementGroupSpecification Build() { return PlacementGroupSpecification(message_); }

 private:
  std::shared_ptr<rpc::PlacementGroupSpec> message_;
};

}  // namespace ray
