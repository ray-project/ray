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

#include <memory>
#include <string>
#include <vector>

#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "ray/common/placement_group.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// \class PlacementGroupInfoAccessorInterface
/// Interface for PlacementGroupInfo operations.
class PlacementGroupInfoAccessorInterface {
 public:
  virtual ~PlacementGroupInfoAccessorInterface() = default;

  /// Create a placement group synchronously.
  virtual Status SyncCreatePlacementGroup(
      const PlacementGroupSpecification &placement_group_spec) = 0;

  /// Remove a placement group synchronously.
  virtual Status SyncRemovePlacementGroup(const PlacementGroupID &placement_group_id) = 0;

  /// Get a placement group by ID synchronously.
  virtual Status SyncGet(const PlacementGroupID &placement_group_id,
                         PlacementGroupSpecification *placement_group_spec) = 0;

  /// Get a placement group by name synchronously.
  virtual Status SyncGetByName(const std::string &placement_group_name,
                               const std::string &ray_namespace,
                               PlacementGroupSpecification *placement_group_spec) = 0;

  /// Get all placement groups synchronously.
  virtual Status SyncGetAll(
      std::vector<rpc::PlacementGroupTableData> *placement_group_info_list,
      int64_t timeout_ms = -1) = 0;

  /// Wait for placement group until ready asynchronously.
  virtual Status SyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                                    int64_t timeout_seconds) = 0;

  // Additional async methods from original accessor.h
  virtual void AsyncGet(
      const PlacementGroupID &placement_group_id,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) = 0;

  virtual void AsyncGetByName(
      const std::string &placement_group_name,
      const std::string &ray_namespace,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
      int64_t timeout_ms = -1) = 0;

  virtual void AsyncGetAll(
      const MultiItemCallback<rpc::PlacementGroupTableData> &callback) = 0;
};

}  // namespace gcs
}  // namespace ray
