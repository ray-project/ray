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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/pubsub/subscriber.h"
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

class MockSubscriber : public pubsub::SubscriberInterface {
 public:
  void Subscribe(
      const rpc::ChannelType channel_type, const rpc::Address &owner_address,
      const std::string &message_id_binary,
      pubsub::SubscriptionCallback subscription_callback,
      pubsub::SubscriptionFailureCallback subscription_failure_callback) override {
    callbacks.push_back(
        std::make_pair(ObjectID::FromBinary(message_id_binary), subscription_callback));
  }

  bool PublishObjectEviction() {
    if (callbacks.empty()) {
      return false;
    }
    auto object_id = callbacks.front().first;
    auto callback = callbacks.front().second;
    auto msg = rpc::PubMessage();
    msg.set_message_id(object_id.Binary());
    msg.set_channel_type(channel_type_);
    auto *wait_for_object_eviction_msg = msg.mutable_wait_for_object_eviction_message();
    wait_for_object_eviction_msg->set_object_id(object_id.Binary());
    callback(msg);
    callbacks.pop_front();
    return true;
  }

  MOCK_METHOD3(Unsubscribe, bool(const rpc::ChannelType channel_type,
                                 const rpc::Address &publisher_address,
                                 const std::string &message_id_binary));

  bool CheckNoLeaks() const override { return true; }

  rpc::ChannelType channel_type_ = rpc::ChannelType::WAIT_FOR_OBJECT_EVICTION;
  std::deque<std::pair<ObjectID, pubsub::SubscriptionCallback>> callbacks;
};

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void SubscribeForObjectEviction(
      const rpc::SubscribeForObjectEvictionRequest &request,
      const rpc::ClientCallback<rpc::SubscribeForObjectEvictionReply> &callback)
      override {
    eviction_callbacks.push_back(callback);
  }

  bool ReplyObjectEviction(Status status = Status::OK()) {
    if (eviction_callbacks.empty()) {
      return false;
    }
    auto callback = eviction_callbacks.front();
    auto reply = rpc::SubscribeForObjectEvictionReply();
    callback(status, reply);
    eviction_callbacks.pop_front();
    return true;
  }

  void AddSpilledUrl(
      const rpc::AddSpilledUrlRequest &request,
      const rpc::ClientCallback<rpc::AddSpilledUrlReply> &callback) override {
    object_urls.emplace(ObjectID::FromBinary(request.object_id()), request.spilled_url());
    spilled_url_callbacks.push_back(callback);
  }

  bool ReplyAddSpilledUrl(Status status = Status::OK()) {
    if (spilled_url_callbacks.empty()) {
      return false;
    }
    auto callback = spilled_url_callbacks.front();
    auto reply = rpc::AddSpilledUrlReply();
    callback(status, reply);
    spilled_url_callbacks.pop_front();
    return true;
  }

  std::deque<rpc::ClientCallback<rpc::SubscribeForObjectEvictionReply>>
      eviction_callbacks;
  std::unordered_map<ObjectID, std::string> object_urls;
  std::deque<rpc::ClientCallback<rpc::AddSpilledUrlReply>> spilled_url_callbacks;
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
    if (status.ok()) {
      for (const auto &url : urls) {
        reply.add_spilled_objects_url(url);
      }
    }
    callback(status, reply);
    callbacks.pop_front();
    return true;
  }

  void RestoreSpilledObjects(
      const rpc::RestoreSpilledObjectsRequest &request,
      const rpc::ClientCallback<rpc::RestoreSpilledObjectsReply> &callback) override {
    restore_callbacks.push_back(callback);
  }

  bool ReplyRestoreObjects(int64_t bytes_restored, Status status = Status::OK()) {
    rpc::RestoreSpilledObjectsReply reply;
    reply.set_bytes_restored_total(bytes_restored);
    if (restore_callbacks.size() == 0) {
      return false;
    };
    auto callback = restore_callbacks.front();
    callback(status, reply);
    restore_callbacks.pop_front();
    return true;
  }

  void DeleteSpilledObjects(
      const rpc::DeleteSpilledObjectsRequest &request,
      const rpc::ClientCallback<rpc::DeleteSpilledObjectsReply> &callback) override {
    rpc::DeleteSpilledObjectsReply reply;
    delete_requests.push_back(request);
    delete_callbacks.push_back(callback);
  }

  /// Return the number of deleted urls.
  int ReplyDeleteSpilledObjects(Status status = Status::OK()) {
    if (delete_callbacks.size() == 0) {
      return 0;
    }

    auto callback = delete_callbacks.front();
    auto reply = rpc::DeleteSpilledObjectsReply();
    callback(status, reply);

    auto &request = delete_requests.front();
    int deleted_urls_size = request.spilled_objects_url_size();
    delete_callbacks.pop_front();
    delete_requests.pop_front();

    return deleted_urls_size;
  }

  std::list<rpc::ClientCallback<rpc::SpillObjectsReply>> callbacks;
  std::list<rpc::ClientCallback<rpc::DeleteSpilledObjectsReply>> delete_callbacks;
  std::list<rpc::ClientCallback<rpc::RestoreSpilledObjectsReply>> restore_callbacks;
  std::list<rpc::DeleteSpilledObjectsRequest> delete_requests;
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
  MOCK_METHOD1(PushUtilWorker, void(const std::shared_ptr<WorkerInterface> &worker));

  MOCK_METHOD1(PushSpillWorker, void(const std::shared_ptr<WorkerInterface> &worker));

  MOCK_METHOD1(PushRestoreWorker, void(const std::shared_ptr<WorkerInterface> &worker));

  MOCK_METHOD1(PushDeleteWorker, void(const std::shared_ptr<WorkerInterface> &worker));

  void PopUtilWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    callback(io_worker);
  }

  void PopSpillWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    callback(io_worker);
  }

  void PopRestoreWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    restoration_callbacks.push_back(callback);
  }

  void PopDeleteWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    callback(io_worker);
  }

  bool RestoreWorkerPushed() {
    if (restoration_callbacks.size() == 0) {
      return false;
    }
    const auto callback = restoration_callbacks.front();
    callback(io_worker);
    restoration_callbacks.pop_front();
    return true;
  }

  std::list<std::function<void(std::shared_ptr<WorkerInterface>)>> restoration_callbacks;
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

  MOCK_METHOD4(AsyncAddLocation,
               Status(const ObjectID &object_id, const NodeID &node_id,
                      size_t object_size, const gcs::StatusCallback &callback));

  Status AsyncAddSpilledUrl(const ObjectID &object_id, const std::string &spilled_url,
                            const NodeID &spilled_node_id, size_t object_size,
                            const gcs::StatusCallback &callback) {
    object_urls[object_id] = spilled_url;
    callbacks.push_back(callback);
    return Status();
  }

  bool ReplyAsyncAddSpilledUrl() {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    callback(Status::OK());
    callbacks.pop_front();
    return true;
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
  std::list<gcs::StatusCallback> callbacks;
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
      : subscriber_(std::make_shared<MockSubscriber>()),
        owner_client(std::make_shared<MockWorkerClient>()),
        client_pool([&](const rpc::Address &addr) { return owner_client; }),
        manager_node_id_(NodeID::FromRandom()),
        max_fused_object_count_(15),
        manager(manager_node_id_, "address", 1234, free_objects_batch_size,
                /*free_objects_period_ms=*/1000, worker_pool, object_table, client_pool,
                /*automatic_object_delete_enabled=*/true,
                /*max_io_workers=*/2,
                /*min_spilling_size=*/0,
                /*is_external_storage_type_fs=*/true,
                /*max_fused_object_count*/ max_fused_object_count_,
                /*on_objects_freed=*/
                [&](const std::vector<ObjectID> &object_ids) {
                  for (const auto &object_id : object_ids) {
                    freed.insert(object_id);
                  }
                },
                /*is_plasma_object_spillable=*/
                [&](const ray::ObjectID &object_id) {
                  return unevictable_objects_.count(object_id) == 0;
                },
                /*core_worker_subscriber=*/subscriber_),
        unpins(std::make_shared<std::unordered_map<ObjectID, int>>()) {
    RayConfig::instance().initialize("object_spilling_config,YQ==");
  }

  void TearDown() { unevictable_objects_.clear(); }

  std::string BuildURL(const std::string url, int offset = 0, int num_objects = 1) {
    return url + "?" + "num_objects=" + std::to_string(num_objects) +
           "&offset=" + std::to_string(offset);
  }

  instrumented_io_context io_service_;
  size_t free_objects_batch_size = 3;
  std::shared_ptr<MockSubscriber> subscriber_;
  std::shared_ptr<MockWorkerClient> owner_client;
  rpc::CoreWorkerClientPool client_pool;
  MockIOWorkerPool worker_pool;
  MockObjectInfoAccessor object_table;
  NodeID manager_node_id_;
  size_t max_fused_object_count_;
  LocalObjectManager manager;

  std::unordered_set<ObjectID> freed;
  // This hashmap is incremented when objects are unpinned by destroying their
  // unique_ptr.
  std::shared_ptr<std::unordered_map<ObjectID, int>> unpins;
  // Object ids in this field won't be evictable.
  std::unordered_set<ObjectID> unevictable_objects_;
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
    auto object =
        std::make_unique<RayObject>(nullptr, meta_buffer, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  manager.WaitForObjectFree(owner_address, object_ids);

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
    EXPECT_CALL(*subscriber_, Unsubscribe(_, _, object_ids[i].Binary()));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }
  std::unordered_set<ObjectID> expected(object_ids.begin(), object_ids.end());
  ASSERT_EQ(freed, expected);
}

TEST_F(LocalObjectManagerTest, TestRestoreSpilledObject) {
  // First, spill objects.
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);

  manager.SpillObjects(object_ids,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_TRUE(manager.GetSpilledObjectURL(object_ids[i]).empty());
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_FALSE(manager.GetSpilledObjectURL(object_ids[i]).empty());
  }
  for (size_t i = 0; i < object_ids.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }

  // Then try restoring objects from local.
  ObjectID object_id = object_ids[0];
  const auto url = urls[0];
  int num_times_fired = 0;
  EXPECT_CALL(worker_pool, PushRestoreWorker(_));
  // Subsequent calls should be deduped, so that only one callback should be fired.
  for (int i = 0; i < 10; i++) {
    manager.AsyncRestoreSpilledObject(object_id, url, [&](const Status &status) {
      ASSERT_TRUE(status.ok());
      num_times_fired++;
    });
  }
  ASSERT_EQ(num_times_fired, 0);

  // When restore workers are pushed, the request should be dedupped.
  for (int i = 0; i < 10; i++) {
    worker_pool.RestoreWorkerPushed();
    ASSERT_EQ(num_times_fired, 0);
  }
  worker_pool.io_worker_client->ReplyRestoreObjects(10);
  // The restore should've been invoked.
  ASSERT_EQ(num_times_fired, 1);
}

TEST_F(LocalObjectManagerTest, TestExplicitSpill) {
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);

  int num_times_fired = 0;
  manager.SpillObjects(object_ids, [&](const Status &status) mutable {
    ASSERT_TRUE(status.ok());
    num_times_fired++;
  });
  ASSERT_EQ(num_times_fired, 0);
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < object_ids.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }
  ASSERT_EQ(num_times_fired, 1);
  for (size_t i = 0; i < object_ids.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_EQ(owner_client->object_urls[object_ids[i]], urls[i]);
    } else {
      ASSERT_EQ(object_table.object_urls[object_ids[i]], urls[i]);
    }
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
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
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
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < object_ids.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }
  ASSERT_EQ(num_times_fired, 1);
  for (size_t i = 0; i < object_ids.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_EQ(owner_client->object_urls[object_ids[i]], urls[i]);
    } else {
      ASSERT_EQ(object_table.object_urls[object_ids[i]], urls[i]);
    }
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
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  ASSERT_TRUE(manager.SpillObjectsOfSize(total_size / 2));
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  // Check that half the objects get spilled and the URLs get added to the
  // global object directory.
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size() / 2 + 1; i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  // Objects should get freed even though we didn't wait for the owner's notice
  // to evict.
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < urls.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }
  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    ASSERT_EQ(owner_client->object_urls.size(), object_ids.size() / 2 + 1);
    for (auto &object_url : owner_client->object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  } else {
    ASSERT_EQ(object_table.object_urls.size(), object_ids.size() / 2 + 1);
    for (auto &object_url : object_table.object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  }

  // Make sure providing 0 bytes to SpillObjectsOfSize will spill one object.
  // This is important to cover min_spilling_size_== 0.
  ASSERT_TRUE(manager.SpillObjectsOfSize(0));
  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  const std::string url = BuildURL("url" + std::to_string(object_ids.size()));
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects({url}));
  urls.push_back(url);
  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    ASSERT_EQ(owner_client->object_urls.size(), 3);
    for (auto &object_url : owner_client->object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  } else {
    ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    ASSERT_EQ(object_table.object_urls.size(), 3);
    for (auto &object_url : object_table.object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  }

  // Since there's no more object to spill, this should fail.
  ASSERT_FALSE(manager.SpillObjectsOfSize(0));
}

TEST_F(LocalObjectManagerTest, TestSpillUptoMaxFuseCount) {
  ///
  /// Test objects are only fused up to max_fused_object_count.
  ///

  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  int64_t total_size = 0;
  int64_t object_size = 1000;

  for (size_t i = 0; i < max_fused_object_count_ + 5; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    total_size += object_size;
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  ASSERT_TRUE(manager.SpillObjectsOfSize(total_size));
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  // Make sure only max_fused_object_count_ objects are spilled although we requested to
  // spill as much as total size. It is because we limit the max fused number of objects
  // to be 10.
  std::vector<std::string> urls;
  for (size_t i = 0; i < max_fused_object_count_; i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  // Objects should get freed even though we didn't wait for the owner's notice
  // to evict.
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < urls.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }
  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    ASSERT_EQ(owner_client->object_urls.size(), max_fused_object_count_);
    for (auto &object_url : owner_client->object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  } else {
    ASSERT_EQ(object_table.object_urls.size(), max_fused_object_count_);
    for (auto &object_url : object_table.object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  }
}

TEST_F(LocalObjectManagerTest, TestSpillObjectNotEvictable) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  int64_t total_size = 0;
  int64_t object_size = 1000;

  const ObjectID object_id = ObjectID::FromRandom();
  object_ids.push_back(object_id);
  unevictable_objects_.emplace(object_id);
  auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
  total_size += object_size;
  auto object =
      std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
  objects.push_back(std::move(object));

  manager.PinObjects(object_ids, std::move(objects), owner_address);
  ASSERT_FALSE(manager.SpillObjectsOfSize(1000));
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  // Now object is evictable. Spill should succeed.
  unevictable_objects_.erase(object_id);
  ASSERT_TRUE(manager.SpillObjectsOfSize(1000));
}

TEST_F(LocalObjectManagerTest, TestSpillUptoMaxThroughput) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  int64_t object_size = 1000;
  size_t total_objects = 3;

  // Pin 3 objects.
  for (size_t i = 0; i < total_objects; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);

  // This will spill until 2 workers are occupied.
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_TRUE(manager.IsSpillingInProgress());
  // Spilling is still going on, meaning we can make the pace. So it should return true.
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_TRUE(manager.IsSpillingInProgress());
  // No object ids are spilled yet.
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  // Spill one object.
  std::vector<std::string> urls;
  urls.push_back(BuildURL("url" + std::to_string(0)));
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects({urls[0]}));
  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    // Make sure object is spilled.
    ASSERT_EQ(owner_client->object_urls.size(), 1);
    for (auto &object_url : owner_client->object_urls) {
      if (urls[0] == object_url.second) {
        ASSERT_EQ((*unpins)[object_url.first], 1);
      }
    }
  } else {
    ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    // Make sure object is spilled.
    ASSERT_EQ(object_table.object_urls.size(), 1);
    for (auto &object_url : object_table.object_urls) {
      if (urls[0] == object_url.second) {
        ASSERT_EQ((*unpins)[object_url.first], 1);
      }
    }
  }

  // Now, there's only one object that is current spilling.
  // SpillObjectUptoMaxThroughput will spill one more object (since one worker is
  // availlable).
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_TRUE(manager.IsSpillingInProgress());
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_TRUE(manager.IsSpillingInProgress());

  // Spilling is done for all objects.
  for (size_t i = 1; i < object_ids.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  for (size_t i = 1; i < urls.size(); i++) {
    ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects({urls[i]}));
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }
  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    ASSERT_EQ(owner_client->object_urls.size(), 3);
    for (auto &object_url : owner_client->object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  } else {
    ASSERT_EQ(object_table.object_urls.size(), 3);
    for (auto &object_url : object_table.object_urls) {
      auto it = std::find(urls.begin(), urls.end(), object_url.second);
      ASSERT_TRUE(it != urls.end());
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  }

  // We cannot spill anymore as there is no more pinned object.
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_FALSE(manager.IsSpillingInProgress());
}

TEST_F(LocalObjectManagerTest, TestSpillError) {
  // Check that we can spill an object again if there was a transient error
  // during the first attempt.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  ObjectID object_id = ObjectID::FromRandom();
  auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
  auto object = std::make_unique<RayObject>(std::move(data_buffer), nullptr,
                                            std::vector<ObjectID>());

  std::vector<std::unique_ptr<RayObject>> objects;
  objects.push_back(std::move(object));
  manager.PinObjects({object_id}, std::move(objects), owner_address);

  int num_times_fired = 0;
  manager.SpillObjects({object_id}, [&](const Status &status) mutable {
    ASSERT_FALSE(status.ok());
    num_times_fired++;
  });

  // Return an error from the IO worker during spill.
  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  ASSERT_TRUE(
      worker_pool.io_worker_client->ReplySpillObjects({}, Status::IOError("error")));
  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    ASSERT_FALSE(owner_client->ReplyAddSpilledUrl());
  } else {
    ASSERT_FALSE(object_table.ReplyAsyncAddSpilledUrl());
  }
  ASSERT_EQ(num_times_fired, 1);
  ASSERT_EQ((*unpins)[object_id], 0);

  // Try to spill the same object again.
  manager.SpillObjects({object_id}, [&](const Status &status) mutable {
    ASSERT_TRUE(status.ok());
    num_times_fired++;
  });
  std::string url = BuildURL("url");
  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects({url}));
  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    ASSERT_EQ(owner_client->object_urls[object_id], url);
  } else {
    ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    ASSERT_EQ(object_table.object_urls[object_id], url);
  }
  ASSERT_EQ(num_times_fired, 2);
  ASSERT_EQ((*unpins)[object_id], 1);
}

TEST_F(LocalObjectManagerTest, TestPartialSpillError) {
  // First, spill objects.
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  manager.SpillObjects(object_ids,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });

  EXPECT_CALL(worker_pool, PushSpillWorker(_));
  std::vector<std::string> urls;
  for (size_t i = 0; i < 2; i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    if (i < urls.size()) {
      ASSERT_EQ(urls[i], manager.GetSpilledObjectURL(object_ids[i]));
    } else {
      ASSERT_TRUE(manager.GetSpilledObjectURL(object_ids[i]).empty());
    }
  }
}

TEST_F(LocalObjectManagerTest, TestDeleteNoSpilledObjects) {
  // Make sure the delete queue won't delete any object when there are no spilled objects.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  // Make sure when there is no spilled object, nothing is deleted.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object = std::make_unique<RayObject>(std::move(data_buffer), nullptr,
                                              std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  manager.WaitForObjectFree(owner_address, object_ids);

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
    EXPECT_CALL(*subscriber_, Unsubscribe(_, _, object_ids[i].Binary()));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = worker_pool.io_worker_client->ReplyDeleteSpilledObjects();
  ASSERT_EQ(deleted_urls_size, 0);
}

TEST_F(LocalObjectManagerTest, TestDeleteSpilledObjects) {
  // Make sure spilled objects are deleted when the delete queue is processed.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  manager.WaitForObjectFree(owner_address, object_ids);

  // 2 Objects are spilled out of 3.
  std::vector<ObjectID> object_ids_to_spill;
  int spilled_urls_size = free_objects_batch_size - 1;
  for (int i = 0; i < spilled_urls_size; i++) {
    object_ids_to_spill.push_back(object_ids[i]);
  }
  manager.SpillObjects(object_ids_to_spill,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }

  // All objects are out of scope now.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
    EXPECT_CALL(*subscriber_, Unsubscribe(_, _, object_ids[i].Binary()));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  // // Make sure all spilled objects are deleted.
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = worker_pool.io_worker_client->ReplyDeleteSpilledObjects();
  ASSERT_EQ(deleted_urls_size, object_ids_to_spill.size());
}

TEST_F(LocalObjectManagerTest, TestDeleteURLRefCount) {
  // Make sure an url is deleted only when every object stored in that url is deleted
  // (when ref_count == 0).
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  // Objects are pinned.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  manager.WaitForObjectFree(owner_address, object_ids);

  // Every object is spilled.
  std::vector<ObjectID> object_ids_to_spill;
  int spilled_urls_size = free_objects_batch_size;
  for (int i = 0; i < spilled_urls_size; i++) {
    object_ids_to_spill.push_back(object_ids[i]);
  }
  manager.SpillObjects(object_ids_to_spill,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });
  std::vector<std::string> urls;
  // Note every object has the same url. It means all objects are fused.
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    // Simulate the situation where there's a single file that contains multiple objects.
    urls.push_back(BuildURL("unified_url",
                            /*offset=*/i,
                            /*num_objects*/ object_ids_to_spill.size()));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }

  // Everything is evicted except the last object. In this case, ref count is still > 0.
  for (size_t i = 0; i < free_objects_batch_size - 1; i++) {
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
    EXPECT_CALL(*subscriber_, Unsubscribe(_, _, object_ids[i].Binary()));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = worker_pool.io_worker_client->ReplyDeleteSpilledObjects();
  // Nothing is deleted yet because the ref count is > 0.
  ASSERT_EQ(deleted_urls_size, 0);

  // The last reference is deleted.
  ASSERT_TRUE(owner_client->ReplyObjectEviction());
  EXPECT_CALL(*subscriber_,
              Unsubscribe(_, _, object_ids[free_objects_batch_size - 1].Binary()));
  ASSERT_TRUE(subscriber_->PublishObjectEviction());
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  deleted_urls_size = worker_pool.io_worker_client->ReplyDeleteSpilledObjects();
  // Now the object is deleted.
  ASSERT_EQ(deleted_urls_size, 1);
}

TEST_F(LocalObjectManagerTest, TestDeleteSpillingObjectsBlocking) {
  // Make sure the object delete queue is blocked when there are spilling objects.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  // Objects are pinned.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  manager.WaitForObjectFree(owner_address, object_ids);

  // Objects are spilled.
  std::vector<ObjectID> object_ids_to_spill;
  int spilled_urls_size = free_objects_batch_size;
  for (int i = 0; i < spilled_urls_size; i++) {
    object_ids_to_spill.push_back(object_ids[i]);
  }
  manager.SpillObjects(object_ids_to_spill,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });

  std::vector<std::string> urls;
  // Only 1 object's spilling is done. Everything else is still spilling.
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < 1; i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }
  // Every object has gone out of scope.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
    EXPECT_CALL(*subscriber_, Unsubscribe(_, _, object_ids[i].Binary()));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }
  // // Now, deletion queue would process only the first object. Everything else won't be
  // deleted although it is out of scope because they are still spilling.
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = worker_pool.io_worker_client->ReplyDeleteSpilledObjects();
  // Only the first entry that is already spilled will be deleted.
  ASSERT_EQ(deleted_urls_size, 1);

  // Now spilling is completely done.
  std::vector<std::string> new_urls;
  for (size_t i = 1; i < object_ids_to_spill.size(); i++) {
    new_urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  for (size_t i = 1; i < object_ids_to_spill.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }

  // Every object is now deleted.
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  deleted_urls_size = worker_pool.io_worker_client->ReplyDeleteSpilledObjects();
  ASSERT_EQ(deleted_urls_size, object_ids_to_spill.size() - 1);
}

TEST_F(LocalObjectManagerTest, TestDeleteMaxObjects) {
  // Make sure deletion queue can only process upto X entries.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  // Make sure when there is no spilled object, nothing is deleted.
  for (size_t i = 0; i < free_objects_batch_size + 1; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object =
        std::make_unique<RayObject>(data_buffer, nullptr, std::vector<ObjectID>());
    objects.push_back(std::move(object));
  }
  manager.PinObjects(object_ids, std::move(objects), owner_address);
  manager.WaitForObjectFree(owner_address, object_ids);

  std::vector<ObjectID> object_ids_to_spill;
  int spilled_urls_size = free_objects_batch_size;
  for (int i = 0; i < spilled_urls_size; i++) {
    object_ids_to_spill.push_back(object_ids[i]);
  }

  // All the entries are spilled.
  manager.SpillObjects(object_ids_to_spill,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(worker_pool.io_worker_client->ReplySpillObjects(urls));
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    if (RayConfig::instance().ownership_based_object_directory_enabled()) {
      ASSERT_TRUE(owner_client->ReplyAddSpilledUrl());
    } else {
      ASSERT_TRUE(object_table.ReplyAsyncAddSpilledUrl());
    }
  }

  // Every reference has gone out of scope.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(owner_client->ReplyObjectEviction());
    EXPECT_CALL(*subscriber_, Unsubscribe(_, _, object_ids[i].Binary()));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  // The spilled objects should be deleted as number of spilled urls exceeds the batch
  // size.
  int deleted_urls_size = worker_pool.io_worker_client->ReplyDeleteSpilledObjects();
  ASSERT_EQ(deleted_urls_size, free_objects_batch_size);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
