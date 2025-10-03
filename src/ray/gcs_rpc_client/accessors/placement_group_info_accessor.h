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

#include "ray/gcs_rpc_client/accessors/placement_group_info_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"

namespace ray {
namespace gcs {

/// \class PlacementGroupInfoAccessor
/// Implementation of PlacementGroupInfoAccessorInterface.
class PlacementGroupInfoAccessor : public PlacementGroupInfoAccessorInterface {
 public:
  PlacementGroupInfoAccessor() = default;
  explicit PlacementGroupInfoAccessor(GcsClientContext *context);
  virtual ~PlacementGroupInfoAccessor() = default;

  /// Create a placement group synchronously.
  virtual Status SyncCreatePlacementGroup(
      const PlacementGroupSpecification &placement_group_spec) override;

  /// Remove a placement group synchronously.
  virtual Status SyncRemovePlacementGroup(
      const PlacementGroupID &placement_group_id) override;

  /// Get a placement group by ID synchronously.
  virtual Status SyncGet(const PlacementGroupID &placement_group_id,
                         PlacementGroupSpecification *placement_group_spec) override;

  /// Get a placement group by name synchronously.
  virtual Status SyncGetByName(
      const std::string &placement_group_name,
      const std::string &ray_namespace,
      PlacementGroupSpecification *placement_group_spec) override;

  /// Get all placement groups synchronously.
  virtual Status SyncGetAll(
      std::vector<rpc::PlacementGroupTableData> *placement_group_info_list,
      int64_t timeout_ms = -1) override;

  /// Wait for placement group until ready asynchronously.
  virtual Status SyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                                    int64_t timeout_seconds) override;

  // Additional async methods from original accessor.h
  virtual void AsyncGet(
      const PlacementGroupID &placement_group_id,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) override;

  virtual void AsyncGetByName(
      const std::string &placement_group_name,
      const std::string &ray_namespace,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
      int64_t timeout_ms = -1) override;

  virtual void AsyncGetAll(
      const MultiItemCallback<rpc::PlacementGroupTableData> &callback) override;

 private:
  // GCS client implementation.
  GcsClientContext *context_ = nullptr;
};

}  // namespace gcs
}  // namespace ray
