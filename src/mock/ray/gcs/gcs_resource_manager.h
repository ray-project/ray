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

#include "mock/ray/gcs/gcs_node_manager.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/gcs_virtual_cluster_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {
namespace gcs {

class MockGcsResourceManager : public GcsResourceManager {
 public:
  explicit MockGcsResourceManager(ClusterResourceManager &cluster_resource_manager,
                                  GcsNodeManager &gcs_node_manager)
      : GcsResourceManager(*GetMockIoContext(),
                           cluster_resource_manager,
                           gcs_node_manager,
                           NodeID::FromRandom(),
                           *GetMockVirtualClusterManager(),
                           nullptr) {}
  MockGcsResourceManager(instrumented_io_context &io_context,
                         ClusterResourceManager &cluster_resource_manager,
                         GcsNodeManager &gcs_node_manager,
                         NodeID node_id)
      : GcsResourceManager(io_context,
                           cluster_resource_manager,
                           gcs_node_manager,
                           node_id,
                           *GetMockVirtualClusterManager(),
                           nullptr) {}
  MOCK_METHOD(void,
              HandleGetAllAvailableResources,
              (rpc::GetAllAvailableResourcesRequest request,
               rpc::GetAllAvailableResourcesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllResourceUsage,
              (rpc::GetAllResourceUsageRequest request,
               rpc::GetAllResourceUsageReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));

 private:
  static instrumented_io_context *GetMockIoContext() {
    static instrumented_io_context *io_context = new instrumented_io_context();
    return io_context;
  }
  static ray::ClusterResourceManager *GetMockClusterResourceManager() {
    static ray::ClusterResourceManager *cluster_resource_manager =
        new ray::ClusterResourceManager(*GetMockIoContext());
    return cluster_resource_manager;
  }
  static gcs::GcsNodeManager *GetMockGcsNodeManager() {
    static gcs::GcsNodeManager *node_manager = new gcs::MockGcsNodeManager();
    return node_manager;
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
};

}  // namespace gcs
}  // namespace ray
