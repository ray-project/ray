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

using ::testing::_;

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
  void SpillObjects(
      const rpc::SpillObjectsRequest &request,
      const rpc::ClientCallback<rpc::SpillObjectsReply> &callback) override {
    callbacks.push_back(callback);
  }

  bool ReplySpillObjects(std::vector<std::string> urls, Status status = Status::OK()) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::SpillObjectsReply();
    for (const auto &url : urls) {
      reply.add_spilled_objects_url(url);
    }
    callback(status, reply);
    callbacks.pop_front();
    return true;
  }

  void RestoreSpilledObjects(
      const rpc::RestoreSpilledObjectsRequest &request,
      const rpc::ClientCallback<rpc::RestoreSpilledObjectsReply> &callback) override {
    rpc::RestoreSpilledObjectsReply reply;
    callback(Status(), reply);
  }

  std::list<rpc::ClientCallback<rpc::SpillObjectsReply>> callbacks;
};

class MockIOWorker : public MockWorker {
 public:
  MockIOWorker(WorkerID worker_id, int port,
               std::shared_ptr<rpc::CoreWorkerClientInterface> io_worker)
      : MockWorker(worker_id, port), io_worker(io_worker) {}

  rpc::CoreWorkerClientInterface *rpc_client() { return io_worker.get(); }

  std::shared_ptr<rpc::CoreWorkerClientInterface> io_worker;
};

class MockIOWorkerPool : public IOWorkerPoolInterface {
 public:
  MOCK_METHOD1(PushIOWorker, void(const std::shared_ptr<WorkerInterface> &worker));

  void PopIOWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    callback(io_worker);
  }

  std::shared_ptr<MockIOWorkerClient> io_worker_client =
      std::make_shared<MockIOWorkerClient>();
  std::shared_ptr<WorkerInterface> io_worker =
      std::make_shared<MockIOWorker>(WorkerID::FromRandom(), 1234, io_worker_client);
};

class MockObjectInfoAccessor : public gcs::ObjectInfoAccessor {
 public:
  MOCK_METHOD2(
      AsyncGetLocations,
      Status(const ObjectID &object_id,
             const gcs::OptionalItemCallback<rpc::ObjectLocationInfo> &callback));

  MOCK_METHOD1(AsyncGetAll,
               Status(const gcs::MultiItemCallback<rpc::ObjectLocationInfo> &callback));

  MOCK_METHOD3(AsyncAddLocation, Status(const ObjectID &object_id, const NodeID &node_id,
                                        const gcs::StatusCallback &callback));

  Status AsyncAddSpilledUrl(const ObjectID &object_id, const std::string &spilled_url,
                            const gcs::StatusCallback &callback) {
    object_urls[object_id] = spilled_url;
    callback(Status());
    return Status();
  }

  MOCK_METHOD3(AsyncRemoveLocation,
               Status(const ObjectID &object_id, const NodeID &node_id,
                      const gcs::StatusCallback &callback));

  MOCK_METHOD3(AsyncSubscribeToLocations,
               Status(const ObjectID &object_id,
                      const gcs::SubscribeCallback<
                          ObjectID, std::vector<rpc::ObjectLocationChange>> &subscribe,
                      const gcs::StatusCallback &done));

  MOCK_METHOD1(AsyncUnsubscribeToLocations, Status(const ObjectID &object_id));

  MOCK_METHOD1(AsyncResubscribe, void(bool is_pubsub_server_restarted));

  MOCK_METHOD1(IsObjectUnsubscribed, bool(const ObjectID &object_id));

  std::unordered_map<ObjectID, std::string> object_urls;
};

class MockObjectBuffer : public Buffer {
 public:
  MockObjectBuffer(size_t size, ObjectID object_id,
                   std::shared_ptr<std::unordered_map<ObjectID, int>> unpins)
      : size_(size), id_(object_id), unpins_(unpins) {}

  MOCK_CONST_METHOD0(Data, uint8_t *());

  size_t Size() const { return size_; }

  MOCK_CONST_METHOD0(OwnsData, bool());

  MOCK_CONST_METHOD0(IsPlasmaBuffer, bool());

  ~MockObjectBuffer() { (*unpins_)[id_]++; }

  size_t size_;
  ObjectID id_;
  std::shared_ptr<std::unordered_map<ObjectID, int>> unpins_;
};

class LocalObjectManagerTest : public ::testing::Test {
 public:
  LocalObjectManagerTest()
      : owner_client(std::make_shared<MockWorkerClient>()),
        client_pool([&](const rpc::Address &addr) { return owner_client; }),
        manager(free_objects_batch_size,
                /*free_objects_period_ms=*/1000, worker_pool, object_table, client_pool,
                [&](const std::vector<ObjectID> &object_ids) {
                  for (const auto &object_id : object_ids) {
                    freed.insert(object_id);
                  }
                }),
        unpins(std::make_shared<std::unordered_map<ObjectID, int>>()) {
    RayConfig::instance().initialize({{"object_spilling_config", "mock_config"}});
  }

  size_t free_objects_batch_size = 3;
  std::shared_ptr<MockWorkerClient> owner_client;
  rpc::CoreWorkerClientPool client_pool;
  MockIOWorkerPool worker_pool;
  MockObjectInfoAccessor object_table;
  LocalObjectManager manager;

  std::unordered_set<ObjectID> freed;
  // This hashmap is incremented when objects are unpinned by destroying their
  // unique_ptr.
  std::shared_ptr<std::unordered_map<ObjectID, int>> unpins;
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
    std::unique_ptr<RayObject> object(
        new RayObject(nullptr, meta_buffer, std::vector<ObjectID>()));
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects));
  manager.WaitForObjectFree(owner_address, object_ids);

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
  }
  std::unordered_set<ObjectID> expected(object_ids.begin(), object_ids.end());
  ASSERT_EQ(freed, expected);
}

TEST_F(LocalObjectManagerTest, TestRestoreSpilledObject) {
  ObjectID object_id = ObjectID::FromRandom();
  std::string object_url("url");
  int num_times_fired = 0;
  EXPECT_CALL(worker_pool, PushIOWorker(_));
  manager.AsyncRestoreSpilledObject(object_id, object_url, [&](const Status &status) {
    ASSERT_TRUE(status.ok());
    num_times_fired++;
  });
  ASSERT_EQ(num_times_fired, 1);
}

TEST_F(LocalObjectManagerTest, TestExplicitSpill) {
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    std::unique_ptr<RayObject> object(
        new RayObject(data_buffer, nullptr, std::vector<ObjectID>()));
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects));

  int num_times_fired = 0;
  manager.SpillObjects(object_ids, [&](const Status &status) mutable {
    ASSERT_TRUE(status.ok());
    num_times_fired++;
  });
  ASSERT_EQ(num_times_fired, 0);
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  EXPECT_CALL(worker_pool, PushIOWorker(_));
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    urls.push_back("url" + std::to_string(i));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  ASSERT_EQ(num_times_fired, 1);
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_EQ(object_table.object_urls[object_ids[i]], urls[i]);
  }
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 1);
  }
}

TEST_F(LocalObjectManagerTest, TestDuplicateSpill) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    std::unique_ptr<RayObject> object(
        new RayObject(data_buffer, nullptr, std::vector<ObjectID>()));
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects));
  manager.WaitForObjectFree(owner_address, object_ids);

  int num_times_fired = 0;
  manager.SpillObjects(object_ids, [&](const Status &status) mutable {
    ASSERT_TRUE(status.ok());
    num_times_fired++;
  });
  // Spill the same objects again. The callback should only be fired once
  // total.
  manager.SpillObjects(object_ids,
                       [&](const Status &status) mutable { ASSERT_TRUE(!status.ok()); });
  ASSERT_EQ(num_times_fired, 0);
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    urls.push_back("url" + std::to_string(i));
  }
  EXPECT_CALL(worker_pool, PushIOWorker(_));
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  ASSERT_EQ(num_times_fired, 1);
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_EQ(object_table.object_urls[object_ids[i]], urls[i]);
  }
  ASSERT_FALSE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 1);
  }
}

TEST_F(LocalObjectManagerTest, TestSpillObjectsOfSize) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  int64_t total_size = 0;
  int64_t object_size = 1000;

  for (size_t i = 0; i < 3; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    total_size += object_size;
    std::unique_ptr<RayObject> object(
        new RayObject(data_buffer, nullptr, std::vector<ObjectID>()));
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects));

  int64_t num_bytes_required = manager.SpillObjectsOfSize(total_size / 2);
  ASSERT_EQ(num_bytes_required, -object_size / 2);
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  // Check that half the objects get spilled and the URLs get added to the
  // global object directory.
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size() / 2 + 1; i++) {
    urls.push_back("url" + std::to_string(i));
  }
  EXPECT_CALL(worker_pool, PushIOWorker(_));
  // Objects should get freed even though we didn't wait for the owner's notice
  // to evict.
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  ASSERT_EQ(object_table.object_urls.size(), object_ids.size() / 2 + 1);
  for (auto &object_url : object_table.object_urls) {
    auto it = std::find(urls.begin(), urls.end(), object_url.second);
    ASSERT_TRUE(it != urls.end());
    ASSERT_EQ((*unpins)[object_url.first], 1);
  }
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
