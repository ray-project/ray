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

#ifndef RAY_COMMON_PLACEMENT_GROUP_H
#define RAY_COMMON_PLACEMENT_GROUP_H

#include "ray/common/bundle_spec.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/protobuf/common.pb.h"

namespace ray {

enum class Strategy {
  PACK = 0x0,
  SPREAD = 0x1,
};

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

  PlacementGroupID PlacementGroupId() const;

  int64_t MaxActorRestarts() const;

  std::vector<BundleSpecification> GetBundles() const;

  Strategy GetStrategy() const;

  BundleSpecification GetBundle(int position) const;

  std::string GetName() const;

 private:
  void ConstructBundles();

  std::vector<BundleSpecification> bundles;
};

class PlacementGroupSpecBuilder {
 public:
  PlacementGroupSpecBuilder() : message_(std::make_shared<rpc::PlacementGroupSpec>()) {}

  /// Set the common attributes of the placement group spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  PlacementGroupSpecBuilder &SetPlacementGroupSpec(
      const PlacementGroupID &placement_group_id, const int64_t restart, std::string name,
      const std::vector<rpc::Bundle> &bundles, const rpc::PlacementStrategy strategy) {
    message_->set_placement_group_id(placement_group_id.Binary());
    message_->set_max_placement_group_restart(restart);
    message_->set_name(name);
    message_->set_strategy(strategy);
    for (auto bundle : bundles) {
      auto message_bundle = message_->add_bundles();
      message_bundle->set_bundle_id(bundle.bundle_id());
      message_bundle->mutable_unit_resources()->insert(bundle.unit_resources().begin(),
                                                       bundle.unit_resources().end());
      message_bundle->set_unit_count(bundle.unit_count());
    }
    return *this;
  }

  PlacementGroupSpecification Build() { return PlacementGroupSpecification(message_); }

 private:
  std::shared_ptr<rpc::PlacementGroupSpec> message_;
};

}  // namespace ray

#endif