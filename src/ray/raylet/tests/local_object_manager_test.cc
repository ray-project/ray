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

#include <deque>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/object_manager/ownership_object_directory.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/subscriber.h"
#include "ray/raylet/metrics.h"
#include "ray/raylet/object_spiller.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

namespace raylet {

using ::testing::_;

class MockSubscriber : public pubsub::SubscriberInterface {
 public:
  void Subscribe(
      const std::unique_ptr<rpc::SubMessage> sub_message,
      rpc::ChannelType channel_type,
      const rpc::Address &owner_address,
      const std::optional<std::string> &key_id_binary,
      pubsub::SubscribeDoneCallback subscribe_done_callback,
      pubsub::SubscriptionItemCallback subscription_callback,
      pubsub::SubscriptionFailureCallback subscription_failure_callback) override {
    auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
    callbacks[worker_id].push_back(
        std::make_pair(ObjectID::FromBinary(*key_id_binary), subscription_callback));
  }

  bool PublishObjectEviction(WorkerID worker_id = WorkerID::Nil()) {
    if (callbacks.empty()) {
      return false;
    }
    auto cbs = callbacks.begin();
    if (!worker_id.IsNil()) {
      cbs = callbacks.find(worker_id);
    }
    if (cbs == callbacks.end() || cbs->second.empty()) {
      return false;
    }
    auto object_id = cbs->second.front().first;
    auto callback = cbs->second.front().second;
    auto msg = rpc::PubMessage();
    msg.set_key_id(object_id.Binary());
    msg.set_channel_type(channel_type_);
    auto *object_eviction_msg = msg.mutable_worker_object_eviction_message();
    object_eviction_msg->set_object_id(object_id.Binary());
    callback(std::move(msg));
    cbs->second.pop_front();
    if (cbs->second.empty()) {
      callbacks.erase(cbs);
    }
    return true;
  }

  MOCK_METHOD3(Unsubscribe,
               void(rpc::ChannelType channel_type,
                    const rpc::Address &publisher_address,
                    const std::optional<std::string> &key_id_binary));

  MOCK_CONST_METHOD3(IsSubscribed,
                     bool(rpc::ChannelType channel_type,
                          const rpc::Address &publisher_address,
                          const std::string &key_id_binary));

  MOCK_CONST_METHOD0(DebugString, std::string());

  rpc::ChannelType channel_type_ = rpc::ChannelType::WORKER_OBJECT_EVICTION;
  std::unordered_map<WorkerID,
                     std::deque<std::pair<ObjectID, pubsub::SubscriptionItemCallback>>>
      callbacks;
};

class MockWorkerClient : public rpc::FakeCoreWorkerClient {
 public:
  void UpdateObjectLocationBatch(
      rpc::UpdateObjectLocationBatchRequest &&request,
      const rpc::ClientCallback<rpc::UpdateObjectLocationBatchReply> &callback) override {
    for (const auto &object_location_update : request.object_location_updates()) {
      ASSERT_TRUE(object_location_update.has_spilled_location_update());
      object_urls.emplace(ObjectID::FromBinary(object_location_update.object_id()),
                          object_location_update.spilled_location_update().spilled_url());
    }
    update_object_location_batch_callbacks.push_back(callback);
  }

  bool ReplyUpdateObjectLocationBatch(Status status = Status::OK()) {
    if (update_object_location_batch_callbacks.empty()) {
      return false;
    }
    auto callback = update_object_location_batch_callbacks.front();
    auto reply = rpc::UpdateObjectLocationBatchReply();
    callback(status, std::move(reply));
    update_object_location_batch_callbacks.pop_front();
    return true;
  }

  absl::flat_hash_map<ObjectID, std::string> object_urls;
  std::deque<rpc::ClientCallback<rpc::UpdateObjectLocationBatchReply>>
      update_object_location_batch_callbacks;
};

class MockObjectSpiller : public ObjectSpillerInterface {
 public:
  void SpillObjects(const std::vector<ObjectID> &object_ids,
                    const std::vector<const RayObject *> &objects,
                    const std::vector<rpc::Address> &owner_addresses,
                    std::function<void(const Status &, std::vector<std::string> urls)>
                        callback) override {
    spill_request_object_counts.push_back(object_ids.size());
    spill_callbacks.push_back(std::move(callback));
  }

  void RestoreSpilledObject(
      const ObjectID &object_id,
      const std::string &object_url,
      std::function<void(const Status &, int64_t bytes_restored)> callback) override {
    restore_callbacks.push_back(std::move(callback));
  }

  void DeleteSpilledObjects(const std::vector<std::string> &urls,
                            std::function<void(const Status &)> callback) override {
    delete_request_urls.push_back(urls);
    delete_callbacks.push_back(std::move(callback));
  }

  bool FlushSpillCallbacks(std::vector<std::string> urls, Status status = Status::OK()) {
    if (spill_callbacks.empty()) {
      return false;
    }
    auto cb = std::move(spill_callbacks.front());
    spill_callbacks.pop_front();
    cb(status, std::move(urls));
    return true;
  }

  bool FlushRestoreCallback(int64_t bytes_restored, Status status = Status::OK()) {
    if (restore_callbacks.empty()) {
      return false;
    }
    auto cb = std::move(restore_callbacks.front());
    restore_callbacks.pop_front();
    cb(status, bytes_restored);
    return true;
  }

  int FlushDeleteCallback(Status status = Status::OK()) {
    if (delete_callbacks.empty()) {
      return 0;
    }
    auto cb = std::move(delete_callbacks.front());
    delete_callbacks.pop_front();
    auto &urls = delete_request_urls.front();
    int size = urls.size();
    delete_request_urls.pop_front();
    cb(status);
    return size;
  }

  int FailDeleteCallback(Status status = Status::IOError("io error")) {
    return FlushDeleteCallback(status);
  }

  std::vector<int> spill_request_object_counts;
  std::deque<std::function<void(const Status &, std::vector<std::string>)>>
      spill_callbacks;
  std::deque<std::function<void(const Status &, int64_t)>> restore_callbacks;
  std::deque<std::function<void(const Status &)>> delete_callbacks;
  std::deque<std::vector<std::string>> delete_request_urls;
};

class MockObjectBuffer : public Buffer {
 public:
  MockObjectBuffer(size_t size,
                   ObjectID object_id,
                   std::shared_ptr<absl::flat_hash_map<ObjectID, int>> unpins)
      : size_(size), id_(object_id), unpins_(unpins) {}

  MOCK_CONST_METHOD0(Data, uint8_t *());

  size_t Size() const { return size_; }

  MOCK_CONST_METHOD0(OwnsData, bool());

  MOCK_CONST_METHOD0(IsPlasmaBuffer, bool());

  ~MockObjectBuffer() { (*unpins_)[id_]++; }

  size_t size_;
  ObjectID id_;
  std::shared_ptr<absl::flat_hash_map<ObjectID, int>> unpins_;
};

class LocalObjectManagerTestWithMinSpillingSize {
 public:
  LocalObjectManagerTestWithMinSpillingSize(int64_t min_spilling_size,
                                            int64_t max_fused_object_count,
                                            int64_t max_spilling_file_size_bytes = -1)
      : subscriber_(std::make_shared<MockSubscriber>()),
        owner_client(std::make_shared<MockWorkerClient>()),
        client_pool([&](const rpc::Address &addr) { return owner_client; }),
        manager_node_id_(NodeID::FromRandom()),
        max_fused_object_count_(max_fused_object_count),
        gcs_client_(std::make_unique<gcs::MockGcsClient>()),
        object_directory_(std::make_unique<OwnershipBasedObjectDirectory>(
            io_service_,
            *gcs_client_,
            subscriber_.get(),
            &client_pool,
            [](const ObjectID &object_id, const rpc::ErrorType &error_type) {})),
        manager(
            manager_node_id_,
            "address",
            1234,
            io_service_,
            free_objects_batch_size,
            /*free_objects_period_ms=*/1000,
            mock_spiller,
            client_pool,
            /*max_io_workers=*/2,
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
            /*core_worker_subscriber=*/subscriber_.get(),
            object_directory_.get(),
            /*object_store_memory_gauge=*/fake_object_store_memory_gauge_,
            /*spill_manager_metrics=*/spill_manager_metrics_),
        unpins(std::make_shared<absl::flat_hash_map<ObjectID, int>>()) {
    RayConfig::instance().initialize(R"({"object_spilling_config": "dummy"})");
    manager.min_spilling_size_ = min_spilling_size;
    manager.max_spilling_file_size_bytes_ = max_spilling_file_size_bytes;
  }

  int64_t NumBytesPendingSpill() { return manager.num_bytes_pending_spill_; }

  int64_t GetCurrentSpilledBytes() { return manager.spilled_bytes_current_; }

  size_t GetCurrentSpilledCount() { return manager.spilled_objects_url_.size(); }

  void AssertNoLeaks() {
    // TODO(swang): Assert this for all tests.
    ASSERT_EQ(manager.pinned_objects_size_, 0);
    ASSERT_TRUE(manager.pinned_objects_.empty());
    ASSERT_TRUE(manager.spilled_objects_url_.empty());
    ASSERT_TRUE(manager.objects_pending_spill_.empty());
    ASSERT_TRUE(manager.url_ref_count_.empty());
    ASSERT_TRUE(manager.local_objects_.empty());
    ASSERT_TRUE(manager.spilled_object_pending_delete_.empty());
    ASSERT_FALSE(manager.IsSpillingInProgress());
  }

  void TearDown() { unevictable_objects_.clear(); }

  std::string BuildURL(const std::string url, int offset = 0, int num_objects = 1) {
    return url + "?" + "num_objects=" + std::to_string(num_objects) +
           "&offset=" + std::to_string(offset);
  }

  void AssertSpillerDoesSpill(size_t num_objects, size_t num_batches) {
    std::vector<std::string> urls;
    for (size_t i = 0; i < num_objects; i++) {
      urls.push_back(BuildURL("url" + std::to_string(i)));
    }
    ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
    for (size_t i = 0; i < num_batches; i++) {
      ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
    }
  }

  instrumented_io_context io_service_;
  size_t free_objects_batch_size = 3;
  size_t object_size = 4;
  std::shared_ptr<MockSubscriber> subscriber_;
  std::shared_ptr<MockWorkerClient> owner_client;
  rpc::CoreWorkerClientPool client_pool;
  MockObjectSpiller mock_spiller;
  NodeID manager_node_id_;
  size_t max_fused_object_count_;
  std::unique_ptr<gcs::GcsClient> gcs_client_;
  std::unique_ptr<IObjectDirectory> object_directory_;
  ray::observability::FakeGauge fake_object_store_memory_gauge_;
  ray::observability::FakeGauge fake_spill_manager_objects_gauge_;
  ray::observability::FakeGauge fake_spill_manager_objects_bytes_gauge_;
  ray::observability::FakeGauge fake_spill_manager_request_total_gauge_;
  ray::observability::FakeGauge fake_spill_manager_throughput_mb_gauge_;
  ray::raylet::SpillManagerMetrics spill_manager_metrics_{
      fake_spill_manager_objects_gauge_,
      fake_spill_manager_objects_bytes_gauge_,
      fake_spill_manager_request_total_gauge_,
      fake_spill_manager_throughput_mb_gauge_};
  LocalObjectManager manager;

  std::unordered_set<ObjectID> freed;
  // This hashmap is incremented when objects are unpinned by destroying their
  // unique_ptr.
  std::shared_ptr<absl::flat_hash_map<ObjectID, int>> unpins;
  // Object ids in this field won't be evictable.
  std::unordered_set<ObjectID> unevictable_objects_;
};

class LocalObjectManagerTest : public LocalObjectManagerTestWithMinSpillingSize,
                               public ::testing::Test {
 public:
  LocalObjectManagerTest() : LocalObjectManagerTestWithMinSpillingSize(0, 1) {}
};

class LocalObjectManagerFusedTest : public LocalObjectManagerTestWithMinSpillingSize,
                                    public ::testing::Test {
 public:
  LocalObjectManagerFusedTest() : LocalObjectManagerTestWithMinSpillingSize(100, 15) {}
};

class LocalObjectManagerMaxFileSizeFusedTest
    : public LocalObjectManagerTestWithMinSpillingSize,
      public ::testing::Test {
 public:
  LocalObjectManagerMaxFileSizeFusedTest()
      : LocalObjectManagerTestWithMinSpillingSize(
            /*min_spilling_size=*/60,
            /*max_fused_object_count=*/15,
            /*max_spilling_file_size_bytes=*/60) {}
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
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
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
    std::shared_ptr<Buffer> data_buffer =
        std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    std::unique_ptr<RayObject> object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  manager.SpillObjects(object_ids,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });

  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_TRUE(manager.GetLocalSpilledObjectURL(object_ids[i]).empty());
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));

  // Spilled
  ASSERT_EQ(GetCurrentSpilledCount(), object_ids.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), object_ids.size() * object_size);

  // The first update is sent out immediately and the remaining ones are batched
  // since the first one is still in-flight.
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_FALSE(manager.GetLocalSpilledObjectURL(object_ids[i]).empty());
    ASSERT_EQ(owner_client->object_urls[object_ids[i]], urls[i]);
  }

  // Then try restoring objects from local.
  ObjectID object_id = object_ids[0];
  const auto url = urls[0];
  int num_times_fired = 0;
  // Subsequent calls should be deduped, so that only one callback should be fired.
  for (int i = 0; i < 10; i++) {
    manager.AsyncRestoreSpilledObject(
        object_id, object_size, url, [&](const Status &status) {
          ASSERT_TRUE(status.ok());
          num_times_fired++;
        });
  }
  ASSERT_EQ(num_times_fired, 0);

  // Only one restore was submitted to the spiller.
  mock_spiller.FlushRestoreCallback(10);
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
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  int num_times_fired = 0;
  manager.SpillObjects(object_ids, [&](const Status &status) mutable {
    ASSERT_TRUE(status.ok());
    num_times_fired++;
  });
  ASSERT_EQ(num_times_fired, 0);
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }
  ASSERT_EQ(num_times_fired, 1);
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_EQ(owner_client->object_urls[object_ids[i]], urls[i]);
  }
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 1);
  }

  ASSERT_EQ(GetCurrentSpilledCount(), object_ids.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), object_ids.size() * object_size);
}

TEST_F(LocalObjectManagerTest, TestDuplicateSpill) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

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
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }
  ASSERT_EQ(num_times_fired, 1);
  for (size_t i = 0; i < object_ids.size(); i++) {
    ASSERT_EQ(owner_client->object_urls[object_ids[i]], urls[i]);
  }
  ASSERT_FALSE(mock_spiller.FlushSpillCallbacks({}));
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 1);
  }

  // Only spilled once
  ASSERT_EQ(GetCurrentSpilledCount(), object_ids.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), object_ids.size() * object_size);
}

TEST_F(LocalObjectManagerTest, TestTryToSpillObjectsZero) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  int64_t object_size = 1000;

  for (size_t i = 0; i < 3; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);
  // Make sure providing 0 bytes as min_spilling_size_ will spill one object.
  manager.min_spilling_size_ = 0;
  ASSERT_TRUE(manager.TryToSpillObjects());
  const std::string url = BuildURL("url" + std::to_string(object_ids.size()));
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks({url}));
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_FALSE(mock_spiller.FlushSpillCallbacks({}));
  ASSERT_EQ(GetCurrentSpilledCount(), 1);
  ASSERT_EQ(GetCurrentSpilledBytes(), 1 * object_size);
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
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);
  manager.min_spilling_size_ = total_size;
  ASSERT_TRUE(manager.TryToSpillObjects());
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
  // Objects should get freed even though we didn't wait for the owner's notice
  // to evict.
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(owner_client->object_urls.size(), max_fused_object_count_);
  for (auto &object_url : owner_client->object_urls) {
    auto it = std::find(urls.begin(), urls.end(), object_url.second);
    ASSERT_TRUE(it != urls.end());
    ASSERT_EQ((*unpins)[object_url.first], 1);
  }
  ASSERT_EQ(GetCurrentSpilledCount(), max_fused_object_count_);
  ASSERT_EQ(GetCurrentSpilledBytes(), max_fused_object_count_ * object_size);
}

TEST_F(LocalObjectManagerTest, TestSpillObjectNotEvictable) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  int64_t object_size = 1000;

  const ObjectID object_id = ObjectID::FromRandom();
  object_ids.push_back(object_id);
  unevictable_objects_.emplace(object_id);
  auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
  auto object = std::make_unique<RayObject>(
      data_buffer, nullptr, std::vector<rpc::ObjectReference>());
  objects.push_back(std::move(object));

  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);
  manager.min_spilling_size_ = 1000;
  ASSERT_FALSE(manager.TryToSpillObjects());
  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  // Now object is evictable. Spill should succeed.
  unevictable_objects_.erase(object_id);
  ASSERT_TRUE(manager.TryToSpillObjects());

  AssertSpillerDoesSpill(/*num_objects*/ 1, /*num_batches*/ 1);
  ASSERT_EQ(GetCurrentSpilledCount(), 1);
  ASSERT_EQ(GetCurrentSpilledBytes(), 1 * object_size);
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
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

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
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks({urls[0]}));
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  // Make sure object is spilled.
  ASSERT_EQ(owner_client->object_urls.size(), 1);
  for (auto &object_url : owner_client->object_urls) {
    if (urls[0] == object_url.second) {
      ASSERT_EQ((*unpins)[object_url.first], 1);
    }
  }
  ASSERT_EQ(GetCurrentSpilledCount(), 1);
  ASSERT_EQ(GetCurrentSpilledBytes(), 1 * object_size);

  // Now, there's only one object that is current spilling.
  // SpillObjectUptoMaxThroughput will spill one more object (since one worker is
  // available).
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_TRUE(manager.IsSpillingInProgress());
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_TRUE(manager.IsSpillingInProgress());

  // Spilling is done for all objects.
  for (size_t i = 1; i < object_ids.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  for (size_t i = 1; i < urls.size(); i++) {
    ASSERT_TRUE(mock_spiller.FlushSpillCallbacks({urls[i]}));
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }
  ASSERT_EQ(GetCurrentSpilledCount(), object_ids.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size * object_ids.size());

  ASSERT_EQ(owner_client->object_urls.size(), 3);
  for (auto &object_url : owner_client->object_urls) {
    auto it = std::find(urls.begin(), urls.end(), object_url.second);
    ASSERT_TRUE(it != urls.end());
    ASSERT_EQ((*unpins)[object_url.first], 1);
  }

  // We cannot spill anymore as there is no more pinned object.
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_FALSE(mock_spiller.FlushSpillCallbacks({}));
  ASSERT_FALSE(manager.IsSpillingInProgress());
}

TEST_F(LocalObjectManagerTest, TestSpillError) {
  // Check that we can spill an object again if there was a transient error
  // during the first attempt.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  ObjectID object_id = ObjectID::FromRandom();
  auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
  auto object = std::make_unique<RayObject>(
      std::move(data_buffer), nullptr, std::vector<rpc::ObjectReference>());

  std::vector<std::unique_ptr<RayObject>> objects;
  objects.push_back(std::move(object));
  manager.PinObjectsAndWaitForFree({object_id}, std::move(objects), owner_address);

  int num_times_fired = 0;
  manager.SpillObjects({object_id}, [&](const Status &status) mutable {
    ASSERT_FALSE(status.ok());
    num_times_fired++;
  });

  // Return an error from the spiller.
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks({}, Status::IOError("error")));
  ASSERT_FALSE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(num_times_fired, 1);
  ASSERT_EQ((*unpins)[object_id], 0);

  // Make sure no spilled
  ASSERT_EQ(GetCurrentSpilledCount(), 0);
  ASSERT_EQ(GetCurrentSpilledBytes(), 0);

  // Try to spill the same object again.
  manager.SpillObjects({object_id}, [&](const Status &status) mutable {
    ASSERT_TRUE(status.ok());
    num_times_fired++;
  });
  std::string url = BuildURL("url");
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks({url}));
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(owner_client->object_urls[object_id], url);
  ASSERT_EQ(num_times_fired, 2);
  ASSERT_EQ((*unpins)[object_id], 1);

  // Now spill happened
  ASSERT_EQ(GetCurrentSpilledCount(), 1);
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size);
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
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);
  manager.SpillObjects(object_ids,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });

  std::vector<std::string> urls;
  for (size_t i = 0; i < 2; i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));

  // only tracking 2 spilled objected
  ASSERT_EQ(GetCurrentSpilledCount(), 2);
  ASSERT_EQ(GetCurrentSpilledBytes(), 2 * object_size);

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    if (i < urls.size()) {
      ASSERT_EQ(urls[i], manager.GetLocalSpilledObjectURL(object_ids[i]));
    } else {
      ASSERT_TRUE(manager.GetLocalSpilledObjectURL(object_ids[i]).empty());
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
    auto object = std::make_unique<RayObject>(
        std::move(data_buffer), nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
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
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());

    auto data_buffer = std::make_shared<MockObjectBuffer>(0, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, meta_buffer, std::vector<rpc::ObjectReference>());

    objects.push_back(std::move(object));
  }

  // 2 objects should be spilled out of 3.
  int64_t total_spill_size = 0;
  std::vector<ObjectID> object_ids_to_spill;
  int spilled_urls_size = free_objects_batch_size - 1;
  for (int i = 0; i < spilled_urls_size; i++) {
    object_ids_to_spill.push_back(object_ids[i]);
    total_spill_size += objects[i]->GetSize();
  }

  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);
  manager.SpillObjects(object_ids_to_spill,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });

  AssertSpillerDoesSpill(/*num_objects*/ object_ids_to_spill.size(), /*num_batches*/ 2);

  ASSERT_EQ(GetCurrentSpilledCount(), object_ids_to_spill.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), total_spill_size);

  // All objects are out of scope now.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  // Make sure all spilled objects are deleted.
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  ASSERT_EQ(deleted_urls_size, object_ids_to_spill.size());
  ASSERT_EQ(GetCurrentSpilledCount(), 0);
  ASSERT_EQ(GetCurrentSpilledBytes(), 0);
}

TEST_F(LocalObjectManagerTest, TestDeleteURLRefCount) {
  // Make sure an url is deleted only when every object stored in that url is deleted
  // (when ref_count == 0).
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  // Objects are pinned.
  size_t total_spill_size = object_size * free_objects_batch_size;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

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
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }

  ASSERT_EQ(GetCurrentSpilledCount(), object_ids_to_spill.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), total_spill_size);

  // Everything is evicted except the last object. In this case, ref count is still > 0.
  for (size_t i = 0; i < free_objects_batch_size - 1; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  // Nothing is deleted yet because the ref count is > 0.
  ASSERT_EQ(deleted_urls_size, 0);

  // Only 1 spilled object left
  ASSERT_EQ(GetCurrentSpilledCount(), 1);
  ASSERT_EQ(GetCurrentSpilledBytes(), 1 * object_size);

  // The last reference is deleted.
  EXPECT_CALL(*subscriber_,
              Unsubscribe(_,
                          _,
                          std::make_optional<std::string>(
                              object_ids[free_objects_batch_size - 1].Binary())));
  ASSERT_TRUE(subscriber_->PublishObjectEviction());
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  deleted_urls_size = mock_spiller.FlushDeleteCallback();
  // Now the object is deleted.
  ASSERT_EQ(deleted_urls_size, 1);

  ASSERT_EQ(GetCurrentSpilledCount(), 0);
  ASSERT_EQ(GetCurrentSpilledBytes(), 0);
}

TEST_F(LocalObjectManagerTest, TestDeleteSpillingObjectsBlocking) {
  // Make sure the object delete queue is blocked when there are spilling objects.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  size_t spilled_urls_size = 2;

  // Objects are pinned.
  for (size_t i = 0; i < spilled_urls_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Objects are spilled.
  std::vector<ObjectID> spill_set_1;
  std::vector<ObjectID> spill_set_2;
  size_t spill_set_1_size = spilled_urls_size / 2;
  size_t spill_set_2_size = spilled_urls_size - spill_set_1_size;
  size_t spill_set_1_bytes = spill_set_1_size * object_size;
  size_t spill_set_2_bytes = spill_set_2_size * object_size;

  for (size_t i = 0; i < spill_set_1_size; i++) {
    spill_set_1.push_back(object_ids[i]);
  }
  for (size_t i = spill_set_1_size; i < spilled_urls_size; i++) {
    spill_set_2.push_back(object_ids[i]);
  }
  manager.SpillObjects(spill_set_1,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });
  manager.SpillObjects(spill_set_2,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });

  std::vector<std::string> urls_spill_set_1;
  std::vector<std::string> urls_spill_set_2;
  for (size_t i = 0; i < spill_set_1_size; i++) {
    urls_spill_set_1.push_back(BuildURL("url" + std::to_string(i)));
  }
  for (size_t i = spill_set_1_size; i < spilled_urls_size; i++) {
    urls_spill_set_2.push_back(BuildURL("url" + std::to_string(i)));
  }

  // Spillset 1 objects are spilled.
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls_spill_set_1));
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(GetCurrentSpilledCount(), spill_set_1_size);
  ASSERT_EQ(GetCurrentSpilledBytes(), spill_set_1_bytes);

  // Every object has gone out of scope.
  for (size_t i = 0; i < spilled_urls_size; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }
  // Now, deletion queue would process only the first spill set. Everything else won't be
  // deleted although it is out of scope because they are still spilling.
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  // Only the first entry that is already spilled will be deleted.
  ASSERT_EQ(deleted_urls_size, spill_set_1_size);

  // Now spilling is completely done.
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls_spill_set_2));
  // These fail because the object is already freed, so the raylet does not
  // send the RPC.
  ASSERT_FALSE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(GetCurrentSpilledCount(), spill_set_2_size);
  ASSERT_EQ(GetCurrentSpilledBytes(), spill_set_2_bytes);

  // Every object is now deleted.
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  deleted_urls_size = mock_spiller.FlushDeleteCallback();
  ASSERT_EQ(deleted_urls_size, spill_set_2_size);
  ASSERT_EQ(GetCurrentSpilledCount(), 0);
  ASSERT_EQ(GetCurrentSpilledBytes(), 0);
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
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

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
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }

  ASSERT_EQ(GetCurrentSpilledCount(), spilled_urls_size);
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size * spilled_urls_size);

  // Every reference has gone out of scope.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  // The spilled objects should be deleted as number of spilled urls exceeds the batch
  // size.
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  ASSERT_EQ(deleted_urls_size, free_objects_batch_size);
}

TEST_F(LocalObjectManagerTest, TestDeleteURLRefCountRaceCondition) {
  ///
  /// Test the edge case where ref count URL is not properly deleted.
  /// https://github.com/ray-project/ray/pull/16153
  ///
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  // Objects are pinned.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Every object is spilled.
  std::vector<ObjectID> object_ids_to_spill;
  int spilled_urls_size = free_objects_batch_size;
  for (int i = 0; i < spilled_urls_size; i++) {
    object_ids_to_spill.push_back(object_ids[i]);
  }
  manager.SpillObjects(object_ids_to_spill,
                       [&](const Status &status) mutable { ASSERT_TRUE(status.ok()); });
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids_to_spill.size(); i++) {
    // Simulate the situation where there's a single file that contains multiple objects.
    urls.push_back(BuildURL("unified_url",
                            /*offset=*/i,
                            /*num_objects*/ object_ids_to_spill.size()));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));

  ASSERT_EQ(GetCurrentSpilledCount(), object_ids_to_spill.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size * object_ids_to_spill.size());

  EXPECT_CALL(*subscriber_,
              Unsubscribe(_, _, std::make_optional<std::string>(object_ids[0].Binary())));
  ASSERT_TRUE(subscriber_->PublishObjectEviction());
  // Delete operation is called. In this case, the file with the url should not be
  // deleted.
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  ASSERT_EQ(deleted_urls_size, 0);

  // But 1 spilled object should be deleted
  ASSERT_EQ(GetCurrentSpilledCount(), free_objects_batch_size - 1);
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size * (free_objects_batch_size - 1));

  // Everything else is now deleted.
  for (size_t i = 1; i < free_objects_batch_size; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  deleted_urls_size = mock_spiller.FlushDeleteCallback();
  // Nothing is deleted yet because the ref count is > 0.
  ASSERT_EQ(deleted_urls_size, 1);

  ASSERT_EQ(GetCurrentSpilledCount(), 0);
  ASSERT_EQ(GetCurrentSpilledBytes(), 0);
}

TEST_F(LocalObjectManagerTest, TestDuplicatePin) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
  }

  std::vector<std::unique_ptr<RayObject>> objects;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Receive a duplicate pin with the same owner. Same objects should not get
  // pinned again.
  objects.clear();
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Receive a duplicate pin with a different owner.
  objects.clear();
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  rpc::Address owner_address2;
  owner_address2.set_worker_id(WorkerID::FromRandom().Binary());
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address2);
  // No subscribe to the second owner.
  auto owner_id2 = WorkerID::FromBinary(owner_address2.worker_id());
  ASSERT_FALSE(subscriber_->PublishObjectEviction(owner_id2));

  // Free on messages from the original owner.
  auto owner_id1 = WorkerID::FromBinary(owner_address.worker_id());
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction(owner_id1));
  }
  std::unordered_set<ObjectID> expected(object_ids.begin(), object_ids.end());
  ASSERT_EQ(freed, expected);

  AssertNoLeaks();
}

TEST_F(LocalObjectManagerTest, TestDuplicatePinAndSpill) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
  }

  std::vector<std::unique_ptr<RayObject>> objects;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  bool spilled = false;
  manager.SpillObjects(object_ids, [&](const Status &status) {
    RAY_CHECK_OK(status);
    spilled = true;
  });
  ASSERT_FALSE(spilled);

  // Free on messages from the original owner.
  auto owner_id1 = WorkerID::FromBinary(owner_address.worker_id());
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ASSERT_TRUE(freed.empty());
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction(owner_id1));
  }
  std::unordered_set<ObjectID> expected(object_ids.begin(), object_ids.end());
  ASSERT_EQ(freed, expected);

  // Duplicate pin message from same owner.
  objects.clear();
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  manager.SpillObjects(object_ids,
                       [&](const Status &status) { RAY_CHECK(!status.ok()); });

  // Complete the first spill. Objects were freed, so OnObjectSpilled skips
  // owner updates, but objects are recorded as spilled.
  std::vector<std::string> urls;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  // Second spill was not submitted.
  ASSERT_FALSE(mock_spiller.FlushSpillCallbacks({}));
  ASSERT_TRUE(spilled);

  // Clean up spilled objects that were freed.
  manager.ProcessSpilledObjectsDeleteQueue(free_objects_batch_size);
  mock_spiller.FlushDeleteCallback();

  manager.FlushFreeObjects();
  AssertNoLeaks();
}

TEST_F(LocalObjectManagerTest, TestRetryDeleteSpilledObjects) {
  std::vector<std::string> urls_to_delete{{"url1"}};
  manager.DeleteSpilledObjects(urls_to_delete, /*num_retries*/ 1);
  ASSERT_EQ(1, mock_spiller.FailDeleteCallback());
  io_service_.run_one();
  // assert the request is retried.
  ASSERT_EQ(1, mock_spiller.FailDeleteCallback());
  // retry exhausted.
  io_service_.run_one();
  ASSERT_EQ(0, mock_spiller.FailDeleteCallback());
}

TEST_F(LocalObjectManagerFusedTest, TestMinSpillingSize) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  int64_t object_size = 52;

  for (size_t i = 0; i < 3; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);
  manager.SpillObjectUptoMaxThroughput();
  // Only 2 of the objects should be spilled (one batch).
  // The third object is below min_spilling_size and other spills are in progress.
  ASSERT_EQ(mock_spiller.spill_callbacks.size(), 1);
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_EQ(mock_spiller.spill_callbacks.size(), 1);

  for (const auto &id : object_ids) {
    ASSERT_EQ((*unpins)[id], 0);
  }

  // Check that half the objects get spilled and the URLs get added to the
  // global object directory.
  std::vector<std::string> urls;
  urls.push_back(BuildURL("url1"));
  urls.push_back(BuildURL("url2"));
  // Objects should get freed even though we didn't wait for the owner's notice
  // to evict.
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }

  // Spilled 2 objects
  ASSERT_EQ(GetCurrentSpilledCount(), 2);
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size * 2);

  ASSERT_EQ(owner_client->object_urls.size(), 2);
  int num_unpinned = 0;
  for (const auto &id : object_ids) {
    if ((*unpins)[id] == 1) {
      num_unpinned++;
    }
  }
  ASSERT_EQ(num_unpinned, 2);

  // We will spill the last object, even though we're under the min spilling
  // size, because they are the only spillable objects.
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_EQ(mock_spiller.spill_callbacks.size(), 1);
}

TEST_F(LocalObjectManagerFusedTest, TestMinSpillingSizeMaxFusionCount) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;
  // 20 of these objects are needed to hit the min spilling size, but
  // max_fused_object_count=15.
  int64_t object_size = 5;

  for (size_t i = 0; i < 40; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    auto data_buffer = std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    auto object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);
  manager.SpillObjectUptoMaxThroughput();
  // First two spill batches succeed because they have at least 15 objects.
  ASSERT_EQ(mock_spiller.spill_callbacks.size(), 2);

  std::vector<std::string> urls;
  for (int i = 0; i < 15; i++) {
    urls.push_back(BuildURL("url", i));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }
  // Spilled first 2 batches
  ASSERT_EQ(GetCurrentSpilledCount(), 30);
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size * 30);

  // We will spill the last objects even though we're under the min spilling
  // size because they are the only spillable objects.
  manager.SpillObjectUptoMaxThroughput();
  ASSERT_EQ(mock_spiller.spill_callbacks.size(), 1);

  urls.clear();
  for (int i = 15; i < 25; i++) {
    urls.push_back(BuildURL("url", i));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());

  // Spilled all objects
  ASSERT_EQ(GetCurrentSpilledCount(), 40);
  ASSERT_EQ(GetCurrentSpilledBytes(), object_size * 40);
}

TEST_F(LocalObjectManagerMaxFileSizeFusedTest, TestMaxSpillingFileSizeMaxFusionCount) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  // 40 objects * 10 bytes = 400 bytes.
  // max_spilling_file_size_bytes=60 -> batches: 6,6,6,6,6,6,4 (total 7 batches).
  const int64_t object_size = 10;
  for (int i = 0; i < 40; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
    std::shared_ptr<Buffer> data_buffer =
        std::make_shared<MockObjectBuffer>(object_size, object_id, unpins);
    std::unique_ptr<RayObject> object = std::make_unique<RayObject>(
        data_buffer, nullptr, std::vector<rpc::ObjectReference>());
    objects.push_back(std::move(object));
  }
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Spill all objects and verify the spilled batches.
  const std::vector<int> expected_batches = {6, 6, 6, 6, 6, 6, 4};

  // Spill may take multiple rounds to complete, so we need to loop until all
  // batches are spilled.
  size_t batch_idx = 0;
  while (batch_idx < expected_batches.size()) {
    manager.SpillObjectUptoMaxThroughput();

    // Reply to all in-flight spill requests.
    while (!mock_spiller.spill_callbacks.empty() && batch_idx < expected_batches.size()) {
      const int n = expected_batches[batch_idx];

      std::vector<std::string> urls;
      urls.reserve(n);
      for (int i = 0; i < n; i++) {
        urls.push_back(BuildURL("url", static_cast<int>(batch_idx * 100 + i)));
      }
      ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));

      while (owner_client->ReplyUpdateObjectLocationBatch()) {
      }

      batch_idx++;
    }
  }

  ASSERT_EQ(mock_spiller.spill_request_object_counts.size(), expected_batches.size());
  for (size_t i = 0; i < expected_batches.size(); i++) {
    ASSERT_EQ(mock_spiller.spill_request_object_counts[i], expected_batches[i]);
  }

  ASSERT_EQ(GetCurrentSpilledCount(), 40);
  ASSERT_EQ(GetCurrentSpilledBytes(), 40 * object_size);
}

TEST_F(LocalObjectManagerMaxFileSizeFusedTest,
       TestMaxSpillingFileSizeAllowsOversizedObject) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  const int64_t large_object_size = 70;

  std::vector<ObjectID> object_ids;
  std::vector<std::unique_ptr<RayObject>> objects;

  ObjectID object_id = ObjectID::FromRandom();
  object_ids.push_back(object_id);

  std::shared_ptr<Buffer> data_buffer =
      std::make_shared<MockObjectBuffer>(large_object_size, object_id, unpins);
  std::unique_ptr<RayObject> object = std::make_unique<RayObject>(
      data_buffer, nullptr, std::vector<rpc::ObjectReference>());
  objects.push_back(std::move(object));

  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  manager.SpillObjectUptoMaxThroughput();

  ASSERT_EQ(mock_spiller.spill_request_object_counts.size(), 1);
  ASSERT_EQ(mock_spiller.spill_request_object_counts[0], 1);

  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks({BuildURL("url", 0)}));
  while (owner_client->ReplyUpdateObjectLocationBatch()) {
  }

  ASSERT_EQ(GetCurrentSpilledCount(), 1);
  ASSERT_EQ(GetCurrentSpilledBytes(), large_object_size);
}

TEST_F(LocalObjectManagerTest, TestPinBytes) {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  // Prepare data for objects.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
  }

  std::vector<std::unique_ptr<RayObject>> objects;
  size_t total_objects_size = 0;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    total_objects_size += object->GetSize();
    objects.push_back(std::move(object));
  }

  // There is no pinned object yet.
  ASSERT_EQ(manager.GetPrimaryBytes(), 0);

  // Pin objects.
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Pinned object memory should be reported.
  ASSERT_GT(manager.GetPrimaryBytes(), 0);

  // Spill all objects.
  bool spilled = false;
  manager.SpillObjects(object_ids, [&](const Status &status) {
    RAY_CHECK_OK(status);
    spilled = true;
  });
  ASSERT_FALSE(spilled);
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }

  // Pinned object memory should be reported as nonzero while there are objects
  // being spilled.
  ASSERT_GT(manager.GetPrimaryBytes(), 0);

  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  for (size_t i = 0; i < 2; i++) {
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  }
  ASSERT_TRUE(spilled);

  ASSERT_EQ(GetCurrentSpilledCount(), object_ids.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), total_objects_size);

  // With all objects spilled, the pinned bytes would be 0.
  ASSERT_EQ(manager.GetPrimaryBytes(), 0);
  ASSERT_TRUE(manager.HasLocallySpilledObjects());

  // Delete all (spilled) objects.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }
  manager.ProcessSpilledObjectsDeleteQueue(/* max_batch_size */ 30);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  ASSERT_EQ(deleted_urls_size, object_ids.size());

  ASSERT_EQ(GetCurrentSpilledCount(), 0);
  ASSERT_EQ(GetCurrentSpilledBytes(), 0);

  // With no pinned or spilled object, the pinned bytes should be 0.
  ASSERT_EQ(manager.GetPrimaryBytes(), 0);
  ASSERT_FALSE(manager.HasLocallySpilledObjects());

  AssertNoLeaks();
}

TEST_F(LocalObjectManagerTest, TestConcurrentSpillAndDelete1) {
  // Test when object is deleted while the spiller is spilling it.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  // Prepare data for objects.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
  }

  std::vector<std::unique_ptr<RayObject>> objects;
  size_t total_size = 0;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    total_size += object->GetSize();
    objects.push_back(std::move(object));
  }

  // There is no pinned object yet.
  ASSERT_EQ(manager.GetPrimaryBytes(), 0);

  // Pin objects.
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Pinned object memory should be reported.
  ASSERT_GT(manager.GetPrimaryBytes(), 0);

  // Spill all objects.
  bool spilled = false;
  manager.SpillObjects(object_ids, [&](const Status &status) {
    RAY_CHECK_OK(status);
    spilled = true;
  });
  ASSERT_FALSE(spilled);

  // Delete all objects while they're being spilled.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  // Spill finishes, objects are recorded as spilled but owner is not notified
  // (because objects were freed).
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  ASSERT_FALSE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_TRUE(spilled);

  ASSERT_EQ(GetCurrentSpilledCount(), object_ids.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), total_size);

  manager.ProcessSpilledObjectsDeleteQueue(free_objects_batch_size);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  ASSERT_EQ(deleted_urls_size, object_ids.size());
  ASSERT_EQ(manager.GetPrimaryBytes(), 0);
  ASSERT_EQ(NumBytesPendingSpill(), 0);

  AssertNoLeaks();
}

TEST_F(LocalObjectManagerTest, TestConcurrentSpillAndDelete2) {
  // Test when object is deleted while a spill is in flight. In the new architecture
  // (direct object spiller), the spill is submitted immediately, so this test verifies
  // that freed objects are properly cleaned up after the spill completes.
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  std::vector<ObjectID> object_ids;
  // Prepare data for objects.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.push_back(object_id);
  }

  std::vector<std::unique_ptr<RayObject>> objects;
  size_t total_size = 0;
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    auto object = std::make_unique<RayObject>(
        nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
    total_size += object->GetSize();
    objects.push_back(std::move(object));
  }

  // There is no pinned object yet.
  ASSERT_EQ(manager.GetPrimaryBytes(), 0);

  // Pin objects.
  manager.PinObjectsAndWaitForFree(object_ids, std::move(objects), owner_address);

  // Pinned object memory should be reported.
  ASSERT_GT(manager.GetPrimaryBytes(), 0);

  // No spill reported
  ASSERT_EQ(GetCurrentSpilledBytes(), 0);

  // Spill all objects.
  bool spilled = false;
  manager.SpillObjects(object_ids, [&](const Status &status) {
    RAY_CHECK_OK(status);
    spilled = true;
  });
  ASSERT_FALSE(spilled);

  // Delete all objects while the spill is in flight.
  for (size_t i = 0; i < free_objects_batch_size; i++) {
    EXPECT_CALL(
        *subscriber_,
        Unsubscribe(_, _, std::make_optional<std::string>(object_ids[i].Binary())));
    ASSERT_TRUE(subscriber_->PublishObjectEviction());
  }

  // Complete the spill. Objects are recorded as spilled but owner is not
  // notified since they were freed.
  std::vector<std::string> urls;
  for (size_t i = 0; i < object_ids.size(); i++) {
    urls.push_back(BuildURL("url" + std::to_string(i)));
  }
  ASSERT_TRUE(mock_spiller.FlushSpillCallbacks(urls));
  ASSERT_FALSE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_TRUE(spilled);

  ASSERT_EQ(GetCurrentSpilledCount(), object_ids.size());
  ASSERT_EQ(GetCurrentSpilledBytes(), total_size);

  // Clean up: process the delete queue.
  manager.ProcessSpilledObjectsDeleteQueue(free_objects_batch_size);
  int deleted_urls_size = mock_spiller.FlushDeleteCallback();
  ASSERT_EQ(deleted_urls_size, object_ids.size());
  ASSERT_EQ(manager.GetPrimaryBytes(), 0);
  ASSERT_EQ(NumBytesPendingSpill(), 0);

  AssertNoLeaks();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
