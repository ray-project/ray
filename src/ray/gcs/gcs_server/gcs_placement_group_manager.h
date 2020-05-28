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

#ifndef RAY_GCS_PLACEMENT_GROUP_MANAGER_H
#define RAY_GCS_PLACEMENT_GROUP_MANAGER_H

#include <ray/common/id.h>
#include <ray/gcs/accessor.h>
#include <ray/protobuf/gcs.pb.h>
#include <ray/rpc/client_call.h>
#include <ray/rpc/gcs_server/gcs_rpc_server.h>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"

namespace ray {
namespace gcs {

class GcsPlacementGroupManager  : public rpc::PlacementGroupInfoHandler {
 public:
  /// Create a GcsPlacementGroupManager.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param placement_group_info_accessor The node info accessor.
  /// \param error_info_accessor The error info accessor, which is used to report error
  /// when detecting the death of nodes.
  explicit GcsPlacementGroupManager(boost::asio::io_service &io_service,
                          gcs::PlacementGroupInfoAccessor &placement_group_info_accessor,
                          gcs::ErrorInfoAccessor &error_info_accessor);
  ~GcsPlacementGroupManager() = default;

  void HandleCreatePlacementGroup(const rpc::CreatePlacementGroupRequest &request, rpc::CreatePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback);

  /// Add a placement group node.
  ///
  /// \param placement_group The info of the placement group to be added.
  void AddPlacementGroup(std::shared_ptr<rpc::PlacementGroupTableData> placement_group); 



 private:
  /// Placement Group info accessor.
  gcs::PlacementGroupInfoAccessor &placement_group_info_accessor_;
  /// Error info accessor.
  gcs::ErrorInfoAccessor &error_info_accessor_;

  /// Alive Placement Group
  absl::flat_hash_map<PlacementGroupID, std::shared_ptr<rpc::PlacementGroupTableData>> alive_placement_groups_;

};


} // namespace gcs
} // namespace gcs


#endif