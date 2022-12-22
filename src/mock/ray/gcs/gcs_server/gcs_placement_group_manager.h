// Copyright  The Ray Authors.
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

namespace ray {
namespace gcs {

class MockGcsPlacementGroup : public GcsPlacementGroup {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsPlacementGroupManager : public GcsPlacementGroupManager {
 public:
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
};

}  // namespace gcs
}  // namespace ray
