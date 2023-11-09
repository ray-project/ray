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
#include <optional>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/function_descriptor.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/task/task_common.h"

namespace ray {

constexpr static std::string_view kVirtualClusterKeyword = "_vc_";
constexpr static size_t kVirtualClusterKeywordSize = kVirtualClusterKeyword.size();

struct VirtualClusterBundleResourceLabel {
  std::string original_resource;
  VirtualClusterID vc_id;
  std::optional<size_t> bundle_index;

  static std::optional<VirtualClusterBundleResourceLabel> ParseFromIndexed(
      const std::string &resource);
  static std::optional<VirtualClusterBundleResourceLabel> ParseFromWildcard(
      const std::string &resource);
  static std::optional<VirtualClusterBundleResourceLabel> ParseFromEither(
      const std::string &resource);
};

class VirtualClusterBundleSpec : public MessageWrapper<rpc::VirtualClusterBundle> {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit VirtualClusterBundleSpec(rpc::VirtualClusterBundle message,
                                    VirtualClusterBundleID vc_bundle_id)
      : MessageWrapper(std::move(message)),
        vc_bundle_id_(vc_bundle_id),
        unit_resource_(ComputeResources(*message_)),
        bundle_resource_labels_(
            ComputeFormattedBundleResourceLabels(unit_resource_, vc_bundle_id_)) {}

  explicit VirtualClusterBundleSpec(const VirtualClusterBundleSpec &) = default;

  VirtualClusterBundleID GetBundleId() const { return vc_bundle_id_; }

  std::string DebugString() const;

  /// Return the resources that are to be acquired by this bundle.
  ///
  /// \return The resources that will be acquired by this bundle.
  const ResourceRequest &GetRequiredResources() const { return unit_resource_; }

  /// Get all virtual cluster bundle resource labels.
  /// When a bundle is commited on a node, we'll add the following special resources on
  /// that node:
  /// 1) `CPU_vc_${vc_id}`: this is the requested resource when the actor
  /// or task specifies virtual cluster without bundle id.
  /// 2) `CPU_vc_${bundle_index}_${vc_id}`: this is the requested resource
  /// when the actor or task specifies virtual cluster with bundle id.
  ///
  /// Other than the resources asked by the user (e.g. `CPU`) we will also implicitly make
  /// resources named `vcbundle` for 1000. For example:
  /// 1) `vcbundle_vc_1_vchex`: 1000
  /// 1) `vcbundle_vc_vchex`: 1000
  const absl::flat_hash_map<std::string, double> &GetFormattedResources() const {
    return bundle_resource_labels_;
  }

 private:
  // First: VC ID.
  // Second: This bundle's index in the virtual cluster's bundle requirements, starting
  // from 0.
  const VirtualClusterBundleID vc_bundle_id_;

  /// Field storing unit resources. Initialized in constructor.
  /// TODO(ekl) consider optimizing the representation of ResourceSet for fast copies
  /// instead of keeping shared pointers here.
  const ResourceRequest unit_resource_;

  const absl::flat_hash_map<std::string, double> bundle_resource_labels_;

  // static compute methods

  static ResourceRequest ComputeResources(const rpc::VirtualClusterBundle &);

  /// When a bundle is assigned on a node, we'll add the following special resources on
  /// that node:
  /// 1) `CPU_vc_${vc_id_hex}`: this is the requested resource when the actor
  /// or task specifies virtual cluster without bundle id.
  /// 2) `CPU_vc_${bundle_index}_${vc_id_hex}`: this is the requested resource
  /// when the actor or task specifies virtual cluster with bundle id.
  static absl::flat_hash_map<std::string, double> ComputeFormattedBundleResourceLabels(
      const ray::ResourceRequest &, VirtualClusterBundleID);
};

/// Format a virtual cluster resource, e.g., CPU -> CPU_vc_i_vchex
std::string FormatVirtualClusterResource(const std::string &original_resource_name,
                                         const VirtualClusterBundleID &vc_bundle_id);

/// Format a virtual cluster resource, e.g., CPU -> CPU_vc_vchex
std::string FormatVirtualClusterResource(const std::string &original_resource_name,
                                         const VirtualClusterID &vc_id);

}  // namespace ray
