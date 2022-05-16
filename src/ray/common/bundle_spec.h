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

#include <cstddef>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/function_descriptor.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task_common.h"

namespace ray {

/// Arguments are the raylet ID to spill back to, the raylet's
/// address and the raylet's port.
typedef std::function<void()> SpillbackBundleCallback;

const std::string kGroupKeyword = "_group_";
const size_t kGroupKeywordSize = kGroupKeyword.size();

class BundleSpecification : public MessageWrapper<rpc::Bundle> {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit BundleSpecification(rpc::Bundle message) : MessageWrapper(message) {
    ComputeResources();
  }
  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit BundleSpecification(std::shared_ptr<rpc::Bundle> message)
      : MessageWrapper(message) {
    ComputeResources();
  }
  // Return the bundle_id
  BundleID BundleId() const;

  // Return the Placement Group id which the Bundle belong to.
  PlacementGroupID PlacementGroupId() const;

  // Get a node ID that this bundle is scheduled on.
  NodeID NodeId() const;

  // Return the index of the bundle.
  int64_t Index() const;

  /// Return the resources that are to be acquired by this bundle.
  ///
  /// \return The resources that will be acquired by this bundle.
  const ResourceRequest &GetRequiredResources() const;

  /// Get all placement group bundle resource labels.
  const absl::flat_hash_map<std::string, double> &GetFormattedResources() const {
    return bundle_resource_labels_;
  }

  std::string DebugString() const;

 private:
  void ComputeResources();
  void ComputeBundleResourceLabels();

  /// Field storing unit resources. Initialized in constructor.
  /// TODO(ekl) consider optimizing the representation of ResourceSet for fast copies
  /// instead of keeping shared pointers here.
  std::shared_ptr<ResourceRequest> unit_resource_;

  /// When a bundle is assigned on a node, we'll add the following special resources on
  /// that node:
  /// 1) `CPU_group_${group_id}`: this is the requested resource when the actor
  /// or task specifies placement group without bundle id.
  /// 2) `CPU_group_${bundle_index}_${group_id}`: this is the requested resource
  /// when the actor or task specifies placement group with bundle id.
  absl::flat_hash_map<std::string, double> bundle_resource_labels_;
};

/// Format a placement group resource, e.g., CPU -> CPU_group_i
std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const PlacementGroupID &group_id,
                                         int64_t bundle_index = -1);

/// Format a placement group resource, e.g., CPU -> CPU_group_YYY_i
std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const BundleSpecification &bundle_spec);

/// Return whether a formatted resource is a bundle of the given index.
bool IsBundleIndex(const std::string &resource,
                   const PlacementGroupID &group_id,
                   const int bundle_index);

/// Return the original resource name of the placement group resource.
std::string GetOriginalResourceName(const std::string &resource);

/// Generate debug information of given bundles.
std::string GetDebugStringForBundles(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundles);

}  // namespace ray
