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

class MockIOWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void SpillObjects(const rpc::SpillObjectsRequest &request,
                            const rpc::ClientCallback<rpc::SpillObjectsReply> &callback) override {
    callbacks.push_back(callback);
  }

  bool ReplySpillObjects(Status status = Status::OK()) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::SpillObjectsReply();
    callback(status, reply);
    callbacks.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::SpillObjectsReply>> callbacks;
};

class MockIOWorker : public MockWorker {
  MockIOWorker(
    WorkerID worker_id, int port,
    std::shared_ptr<rpc::CoreWorkerClientInterface> io_worker)
  : MockWorker(worker_id, port),
    io_worker(io_worker) {}

  rpc::CoreWorkerClient *rpc_client() {
    return io_worker.get();
  }

  std::shared_ptr<rpc::CoreWorkerClientInterface> io_worker;
};

class MockIOWorkerPool : public IOWorkerPoolInterface {
  MOCK_METHOD1(PushIOWorker, void(const std::shared_ptr<WorkerInterface> &worker));

  void PopIOWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    callback(io_worker);
  }

  std::shared_ptr<rpc::CoreWorkerClientInterface> io_worker_client = std::make_shared<MockIOWorkerClient>();
  std::shared_ptr<WorkerInterface> io_worker = std::make_shared<MockIOWorker>(WorkerID::FromRandom(), 1234, io_worker_client);
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
    : owner_client(std::make_shared<MockWorkerClient>()),
      client_pool([&](const rpc::Address &addr) { return owner_client; }),
      manager(free_objects_batch_size,
        /*free_objects_period_ms=*/1000,
        worker_pool,
        object_table,
        client_pool,
        [&](const std::vector<ObjectID> &object_ids) {
          for (const auto &object_id : object_ids) {
            freed.insert(object_id);
          }
        }) {}

  size_t free_objects_batch_size = 3;
  std::shared_ptr<MockWorkerClient> owner_client;
  rpc::CoreWorkerClientPool client_pool;
  MockIOWorkerPool worker_pool;
  MockObjectInfoAccessor object_table;
  LocalObjectManager manager;

  std::unordered_set<ObjectID> freed;
};

TEST_F(LocalObjectManagerTest, TestPin) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    std::unique_ptr<RayObject> object(new RayObject(nullptr, meta_buffer, std::vector<ObjectID>()));
    objects.push_back(std::move(object));
  }
  manager.PinObjects(owner_address, object_ids, std::move(objects));

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
  }
  std::unordered_set<ObjectID> expected(object_ids.begin(), object_ids.end());
  ASSERT_EQ(freed, expected);
}

TEST_F(LocalObjectManagerTest, TestExplicitSpill) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    std::unique_ptr<RayObject> object(new RayObject(nullptr, meta_buffer, std::vector<ObjectID>()));
    objects.push_back(std::move(object));
  }
  manager.PinObjects(owner_address, object_ids, std::move(objects));

  manager.SpillObjects(object_ids, [&](const Status &status) {
      ASSERT_TRUE(status.ok());
      });
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
