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

#pragma once

#include <gmock/gmock.h>

#include "ray/gcs/gcs_node_manager.h"
#include "ray/observability/fake_ray_event_recorder.h"

namespace ray {
namespace gcs {

class MockGcsNodeManager : public GcsNodeManager {
 public:
  MockGcsNodeManager()
      : GcsNodeManager(/*gcs_publisher=*/nullptr,
                       /*gcs_table_storage=*/nullptr,
                       /*io_context=*/mocked_io_context_not_used_,
                       /*raylet_client_pool=*/nullptr,
                       /*cluster_id=*/ClusterID::Nil(),
                       /*ray_event_recorder=*/fake_ray_event_recorder_,
                       /*session_name=*/"") {}
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
  MOCK_METHOD(void, DrainNode, (const NodeID &node_id), (override));

  instrumented_io_context mocked_io_context_not_used_;
  observability::FakeRayEventRecorder fake_ray_event_recorder_;
};

}  // namespace gcs
}  // namespace ray
