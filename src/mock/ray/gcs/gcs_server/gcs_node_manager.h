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
#include "gmock/gmock.h"

namespace ray {
namespace gcs {

class MockGcsNodeManager : public GcsNodeManager {
 public:
  MockGcsNodeManager() : GcsNodeManager(nullptr, nullptr, nullptr, ClusterID::Nil()) {}
  MOCK_METHOD(void,
              HandleRegisterNode,
              (rpc::RegisterNodeRequest request,
               rpc::RegisterNodeReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleDrainNode,
              (rpc::DrainNodeRequest request,
               rpc::DrainNodeReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllNodeInfo,
              (rpc::GetAllNodeInfoRequest request,
               rpc::GetAllNodeInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetInternalConfig,
              (rpc::GetInternalConfigRequest request,
               rpc::GetInternalConfigReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, DrainNode, (const NodeID &node_id), (override));
};

}  // namespace gcs
}  // namespace ray
