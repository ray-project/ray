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

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// Arguments are the node ID to spill back to, the raylet's
/// address and the raylet's port.
using SpillbackBundleCallback = std::function<void()>;

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

/// Format a placement group resource with provided parameters.
///
/// \param original_resource_name The original resource name of the pg resource.
/// \param group_id_str The group id in string format.
/// \param bundle_index The bundle index. If -1, generate the wildcard pg resource.
///                     E.g., [original_resource_name]_group_[group_id_str].
///                     If >=0, generate the indexed pg resource. E.g.,
///                     [original_resource_name]_group_[bundle_index]_[group_id_str]
/// \return The corresponding formatted placement group resource string.
std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const std::string &group_id_hex,
                                         int64_t bundle_index = -1);

/// Format a placement group resource, e.g., CPU -> CPU_group_i
std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const PlacementGroupID &group_id,
                                         int64_t bundle_index = -1);

/// Return the original resource name of the placement group resource.
std::string GetOriginalResourceName(const std::string &resource);

// Return the original resource name of the placement group resource
// if the resource is the wildcard resource (resource without a bundle id).
// Returns "" if the resource is not a wildcard resource.
std::string GetOriginalResourceNameFromWildcardResource(const std::string &resource);

/// Generate debug information of given bundles.
std::string GetDebugStringForBundles(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundles);

/// Format the placement group resource set, e.g., CPU -> CPU_group_YYY_i
std::unordered_map<std::string, double> AddPlacementGroupConstraint(
    const std::unordered_map<std::string, double> &resources,
    const PlacementGroupID &placement_group_id,
    int64_t bundle_index);

/// Format the placement group resource set, e.g., CPU -> CPU_group_YYY_i
std::unordered_map<std::string, double> AddPlacementGroupConstraint(
    const std::unordered_map<std::string, double> &resources,
    const rpc::SchedulingStrategy &scheduling_strategy);

/// Get the group id (as in resources like `CPU_group_${group_id}`) of the placement group
/// resource.
std::string GetGroupIDFromResource(const std::string &resource);

}  // namespace ray
