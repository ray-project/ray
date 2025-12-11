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

#include "mock/ray/pubsub/publisher.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/gcs_virtual_cluster_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {
namespace gcs {

class MockGcsNodeManager : public GcsNodeManager {
 public:
  MockGcsNodeManager()
      : GcsNodeManager(GetMockGcsPublisher(),
                       GetMockGcsTableStorage(),
                       *GetMockIoContext(),
                       nullptr,
                       ClusterID::Nil(),
                       *GetMockVirtualClusterManager(),
                       fake_ray_event_recorder_,
                       "") {}
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

 private:
  static instrumented_io_context *GetMockIoContext() {
    static instrumented_io_context *io_context = new instrumented_io_context();
    return io_context;
  }
  static gcs::GcsTableStorage *GetMockGcsTableStorage() {
    static gcs::GcsTableStorage *gcs_table_storage =
        new gcs::GcsTableStorage(std::make_unique<InMemoryStoreClient>());
    return gcs_table_storage;
  }
  static pubsub::GcsPublisher *GetMockGcsPublisher() {
    static std::unique_ptr<ray::pubsub::MockPublisher> publisher(
        new ray::pubsub::MockPublisher());
    static pubsub::GcsPublisher *gcs_publisher =
        new pubsub::GcsPublisher(std::move(publisher));
    return gcs_publisher;
  }
  static ray::ClusterResourceManager *GetMockClusterResourceManager() {
    static ray::ClusterResourceManager *cluster_resource_manager =
        new ray::ClusterResourceManager(*GetMockIoContext());
    return cluster_resource_manager;
  }
  static gcs::GcsVirtualClusterManager *GetMockVirtualClusterManager() {
    static std::shared_ptr<ray::PeriodicalRunner> periodical_runner = nullptr;
    static gcs::GcsVirtualClusterManager *manager =
        new gcs::GcsVirtualClusterManager(*GetMockIoContext(),
                                          *GetMockGcsTableStorage(),
                                          *GetMockGcsPublisher(),
                                          *GetMockClusterResourceManager(),
                                          periodical_runner);
    return manager;
  }

  instrumented_io_context mocked_io_context_not_used_;
  observability::FakeRayEventRecorder fake_ray_event_recorder_;
};

}  // namespace gcs
}  // namespace ray
