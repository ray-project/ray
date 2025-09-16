/// Copyright  The Ray Authors.
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

#include <gmock/gmock.h>

#include "ray/gcs/gcs_placement_group_manager.h"

namespace ray {
namespace gcs {

class MockGcsPlacementGroupManager : public GcsPlacementGroupManager {
 public:
  explicit MockGcsPlacementGroupManager(GcsResourceManager &gcs_resource_manager)
      : GcsPlacementGroupManager(context_, gcs_resource_manager) {}
  MOCK_METHOD(void,
              HandleCreatePlacementGroup,
              (rpc::CreatePlacementGroupRequest request,
               rpc::CreatePlacementGroupReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleRemovePlacementGroup,
              (rpc::RemovePlacementGroupRequest request,
               rpc::RemovePlacementGroupReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetPlacementGroup,
              (rpc::GetPlacementGroupRequest request,
               rpc::GetPlacementGroupReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetNamedPlacementGroup,
              (rpc::GetNamedPlacementGroupRequest request,
               rpc::GetNamedPlacementGroupReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllPlacementGroup,
              (rpc::GetAllPlacementGroupRequest request,
               rpc::GetAllPlacementGroupReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleWaitPlacementGroupUntilReady,
              (rpc::WaitPlacementGroupUntilReadyRequest request,
               rpc::WaitPlacementGroupUntilReadyReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));

  MOCK_METHOD((absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>),
              GetBundlesOnNode,
              (const NodeID &node_id),
              (const, override));

  MOCK_METHOD((std::shared_ptr<rpc::PlacementGroupLoad>),
              GetPlacementGroupLoad,
              (),
              (const, override));

  instrumented_io_context context_;
};

}  // namespace gcs
}  // namespace ray
