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
#include "gmock/gmock.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "mock/ray/pubsub/publisher.h"

namespace ray {
namespace gcs {

class MockGcsNodeManager : public GcsNodeManager {
 public:
  MockGcsNodeManager()
      : GcsNodeManager(
            GetMockGcsPublisher(),
            GetMockGcsTableStorage(),
            *GetMockIoContext(),
            nullptr,
            ClusterID::Nil(),
            *GetMockVirtualClusterManager()) {}

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
  static instrumented_io_context* GetMockIoContext() {
    static instrumented_io_context* io_context = new instrumented_io_context();
    return io_context;
  }
  static gcs::InMemoryGcsTableStorage* GetMockGcsTableStorage() {
    static gcs::InMemoryGcsTableStorage* gcs_table_storage = new gcs::InMemoryGcsTableStorage();
    return gcs_table_storage;
  }
  static gcs::GcsPublisher* GetMockGcsPublisher() {
    static std::unique_ptr<ray::pubsub::Publisher> publisher(new ray::pubsub::MockPublisher());
    static gcs::GcsPublisher* gcs_publisher = new gcs::GcsPublisher(std::move(publisher));
    return gcs_publisher;
  }
  static ray::ClusterResourceManager* GetMockClusterResourceManager() {
    static ray::ClusterResourceManager* cluster_resource_manager = new ray::ClusterResourceManager(*GetMockIoContext());
    return cluster_resource_manager;
  }
  static gcs::GcsVirtualClusterManager* GetMockVirtualClusterManager() {
    static std::shared_ptr<ray::PeriodicalRunner> periodical_runner = nullptr;
    static gcs::GcsVirtualClusterManager* manager =
        new gcs::GcsVirtualClusterManager(*GetMockIoContext(),
                                          *GetMockGcsTableStorage(),
                                          *GetMockGcsPublisher(),
                                          *GetMockClusterResourceManager(),
                                          periodical_runner);
    return manager;
  }
};

}  // namespace gcs
}  // namespace ray