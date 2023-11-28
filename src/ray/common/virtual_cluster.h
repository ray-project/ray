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

class VirtualClusterSpecification : public MessageWrapper<rpc::VirtualClusterSpec> {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit VirtualClusterSpecification(rpc::VirtualClusterSpec message)
      : MessageWrapper(std::move(message)) {}
  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit VirtualClusterSpecification(std::shared_ptr<rpc::VirtualClusterSpec> message)
      : MessageWrapper(message) {}
  /// Return the virtual cluster id.
  VirtualClusterID VirtualClusterId() const;
};

class VirtualClusterSpecBuilder {
 public:
  // Creates a Builder with no bundle sets.
  VirtualClusterSpecBuilder(const VirtualClusterID &virtual_cluster_id,
                            rpc::PlacementStrategy strategy)
      : message_(std::make_shared<rpc::VirtualClusterSpec>()) {
    message_->set_virtual_cluster_id(virtual_cluster_id.Binary());
    message_->set_strategy(strategy);
  }

  VirtualClusterSpecBuilder &AddBundleSet(
      const std::unordered_map<std::string, double> &resources,
      const std::unordered_map<std::string, std::string> labels,
      bool allow_wildcard_resources,
      int64_t min_replicas,
      int64_t max_replicas) {
    rpc::VirtualClusterBundleSet *bundle_set = message_->add_bundles();
    bundle_set->set_min_replicas(min_replicas);
    bundle_set->set_max_replicas(max_replicas);
    rpc::VirtualClusterBundle *bundle = bundle_set->mutable_bundle();
    bundle->set_allow_wildcard_resources(allow_wildcard_resources);

    for (const auto &[name, count] : resources) {
      if (count != 0) {
        bundle->mutable_resources()->insert({name, count});
      }
    }
    for (const auto &[key, val] : labels) {
      bundle->mutable_labels()->insert({key, val});
    }
    return *this;
  }

  VirtualClusterSpecification Build() { return VirtualClusterSpecification(message_); }

 private:
  std::shared_ptr<rpc::VirtualClusterSpec> message_;
};

}  // namespace ray
