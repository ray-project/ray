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

#include "ray/raylet/local_object_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/raylet/test/util.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

namespace raylet {

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void WaitForObjectEviction(
      const rpc::WaitForObjectEvictionRequest &request,
      const rpc::ClientCallback<rpc::WaitForObjectEvictionReply> &callback) override {
    callbacks.push_back(callback);
  }

  bool ReplyObjectEviction(Status status = Status::OK()) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::WaitForObjectEvictionReply();
    callback(status, reply);
    callbacks.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::WaitForObjectEvictionReply>> callbacks;
};

class MockIOWorkerPool : public IOWorkerPoolInterface {
  MOCK_METHOD1(PushIOWorker, void(const std::shared_ptr<WorkerInterface> &worker));

  void PopIOWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    callback(worker);
  }

  std::shared_ptr<WorkerInterface> worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
};

class MockObjectInfoAccessor : public gcs::ObjectInfoAccessor {
  MOCK_METHOD2(AsyncGetLocations, Status(
      const ObjectID &object_id,
      const gcs::OptionalItemCallback<rpc::ObjectLocationInfo> &callback));

  MOCK_METHOD1(AsyncGetAll, Status(
      const gcs::MultiItemCallback<rpc::ObjectLocationInfo> &callback));

  MOCK_METHOD3(AsyncAddLocation, Status(const ObjectID &object_id, const NodeID &node_id,
                                  const gcs::StatusCallback &callback));

  MOCK_METHOD3(AsyncAddSpilledUrl, Status(const ObjectID &object_id,
                                    const std::string &spilled_url,
                                    const gcs::StatusCallback &callback));

  MOCK_METHOD3(AsyncRemoveLocation, Status(const ObjectID &object_id, const NodeID &node_id,
                                     const gcs::StatusCallback &callback));

  MOCK_METHOD3(AsyncSubscribeToLocations, Status(
      const ObjectID &object_id,
      const gcs::SubscribeCallback<ObjectID, std::vector<rpc::ObjectLocationChange>>
          &subscribe,
      const gcs::StatusCallback &done));

  MOCK_METHOD1(AsyncUnsubscribeToLocations, Status(const ObjectID &object_id));

  MOCK_METHOD1(AsyncResubscribe, void(bool is_pubsub_server_restarted));

  MOCK_METHOD1(IsObjectUnsubscribed, bool(const ObjectID &object_id));
};

class LocalObjectManagerTest : public ::testing::Test {
 public:
  LocalObjectManagerTest()
    : worker_client(std::make_shared<MockWorkerClient>()),
      client_pool([&](const rpc::Address &addr) { return worker_client; }),
      manager(/*free_objects_batch_size=*/3,
        /*free_objects_period_ms=*/1000,
        worker_pool,
        object_table,
        client_pool,
        [&](const std::vector<ObjectID> &object_ids) {
          for (const auto &object_id : object_ids) {
            freed.insert(object_id);
          }
        }) {}

  std::shared_ptr<MockWorkerClient> worker_client;
  rpc::CoreWorkerClientPool client_pool;
  MockIOWorkerPool worker_pool;
  MockObjectInfoAccessor object_table;
  LocalObjectManager manager;

  std::unordered_set<ObjectID> freed;
};

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
