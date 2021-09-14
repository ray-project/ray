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

// clang-format off
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "mock/ray/gcs/store_client/store_client.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"
#include "mock/ray/raylet_client/raylet_client.h"
#include "mock/ray/pubsub/subscriber.h"
#include "mock/ray/gcs/pubsub/gcs_pub_sub.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
// clang-format on
namespace ray {
namespace gcs {
struct MockCallback {
  MOCK_METHOD(void, Call, ((std::shared_ptr<GcsActor>)));
  void operator()(std::shared_ptr<GcsActor> a) {
    return Call(a);
  }
};

class GcsActorSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    store_client = std::make_shared<MockStoreClient>();
    actor_table = std::make_unique<GcsActorTable>(store_client);
    gcs_node_manager = std::make_unique<MockGcsNodeManager>();
    pub_sub = std::make_shared<MockGcsPubSub>();
    raylet_client = std::make_shared<MockRayletClientInterface>();
    core_worker_client = std::make_shared<rpc::MockCoreWorkerClientInterface>();
    client_pool = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address&) {
          return raylet_client;
        });
    actor_scheduler = std::make_unique<RayletBasedActorScheduler>(
        io_context, *actor_table, *gcs_node_manager, pub_sub,
        [this](auto a) {
          schedule_failure_handler(a);
        },
        [this](auto a) {
          schedule_success_handler(a);
        },
        client_pool,
        [this](const rpc::Address&) {
          return core_worker_client;
        });
  }
  std::shared_ptr<MockRayletClientInterface> raylet_client;
  instrumented_io_context io_context;
  std::shared_ptr<MockStoreClient> store_client;
  std::unique_ptr<GcsActorTable> actor_table;
  std::unique_ptr<GcsActorScheduler> actor_scheduler;
  std::unique_ptr<MockGcsNodeManager> gcs_node_manager;
  std::shared_ptr<MockGcsPubSub> pub_sub;
  std::shared_ptr<rpc::MockCoreWorkerClientInterface> core_worker_client;
  std::shared_ptr<rpc::NodeManagerClientPool> client_pool;
  MockCallback schedule_failure_handler;
  MockCallback schedule_success_handler;
};

}
}
