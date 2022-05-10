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

#include "ray/object_manager/ownership_based_object_directory.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/pubsub/mock_pubsub.h"

// clang-format off
#include "mock/ray/gcs/gcs_client/accessor.h"
// clang-format on

namespace ray {

using ::testing::_;
using ::testing::Return;

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void UpdateObjectLocationBatch(
      const rpc::UpdateObjectLocationBatchRequest &request,
      const rpc::ClientCallback<rpc::UpdateObjectLocationBatchReply> &callback) override {
    const auto &worker_id = WorkerID::FromBinary(request.intended_worker_id());
    const auto &object_location_updates = request.object_location_updates();

    for (const auto &object_location_update : object_location_updates) {
      const auto &object_id = ObjectID::FromBinary(object_location_update.object_id());

      buffered_object_locations_[worker_id][object_id] = object_location_update;
      object_location_updates_.emplace_back(object_location_update);
    }
    batch_sent++;
    callbacks.push_back(callback);
  }

  bool ReplyUpdateObjectLocationBatch(Status status = Status::OK()) {
    if (callbacks.empty()) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::UpdateObjectLocationBatchReply();
    callback(status, reply);
    callback_invoked++;
    callbacks.pop_front();
    return true;
  }

  void AssertObjectPlasmaLocationUpdate(const WorkerID &worker_id,
                                        const ObjectID &object_id,
                                        rpc::ObjectPlasmaLocationUpdate update) {
    auto it = buffered_object_locations_.find(worker_id);
    RAY_CHECK(it != buffered_object_locations_.end())
        << "Worker ID " << worker_id << " wasn't updated.";
    auto object_it = it->second.find(object_id);
    RAY_CHECK(object_it->second.has_plasma_location_update());
    RAY_CHECK_EQ(object_it->second.plasma_location_update(), update);
  }

  void Reset() {
    buffered_object_locations_.clear();
    object_location_updates_.clear();
    callbacks.clear();
    callback_invoked = 0;
    batch_sent = 0;
  }

  absl::flat_hash_map<WorkerID, absl::flat_hash_map<ObjectID, rpc::ObjectLocationUpdate>>
      buffered_object_locations_;
  std::vector<rpc::ObjectLocationUpdate> object_location_updates_;
  std::deque<rpc::ClientCallback<rpc::UpdateObjectLocationBatchReply>> callbacks;
  int callback_invoked = 0;
  int batch_sent = 0;
};

class MockGcsClient : public gcs::GcsClient {
 public:
  MockGcsClient(gcs::GcsClientOptions options,
                gcs::MockNodeInfoAccessor *node_info_accessor)
      : gcs::GcsClient(options) {
    node_accessor_.reset(node_info_accessor);
  }

  gcs::NodeInfoAccessor &Nodes() {
    RAY_CHECK(node_accessor_ != nullptr);
    return *node_accessor_;
  }

  MOCK_METHOD1(Connect, Status(instrumented_io_context &io_service));

  MOCK_METHOD0(Disconnect, void());
};

class OwnershipBasedObjectDirectoryTest : public ::testing::Test {
 public:
  OwnershipBasedObjectDirectoryTest()
      : options_("localhost:6973"),
        node_info_accessor_(new gcs::MockNodeInfoAccessor()),
        gcs_client_mock_(new MockGcsClient(options_, node_info_accessor_)),
        subscriber_(std::make_shared<mock_pubsub::MockSubscriber>()),
        owner_client(std::make_shared<MockWorkerClient>()),
        client_pool([&](const rpc::Address &addr) { return owner_client; }),
        obod_(io_service_,
              gcs_client_mock_,
              subscriber_.get(),
              &client_pool,
              /*max_object_report_batch_size=*/20,
              [this](const ObjectID &object_id, const rpc::ErrorType &error_type) {
                MarkAsFailed(object_id, error_type);
              }) {}

  void TearDown() { owner_client->Reset(); }

  void MarkAsFailed(const ObjectID &object_id, const rpc::ErrorType &error_type) {
    RAY_LOG(INFO) << "Object Failed";
  }

  ObjectInfo CreateNewObjectInfo(const WorkerID &worker_id) {
    auto id = ObjectID::FromRandom();
    while (used_ids_.count(id) > 0) {
      id = ObjectID::FromRandom();
    }
    used_ids_.insert(id);
    ray::ObjectInfo info;
    info.object_id = id;
    info.data_size = 12;
    info.owner_raylet_id = NodeID::FromRandom();
    info.owner_ip_address = "124.2.3.4";
    info.owner_port = 6739;
    info.owner_worker_id = worker_id;
    return info;
  }

  void AssertObjectPlasmaLocationUpdate(const WorkerID &worker_id,
                                        const ObjectID &object_id,
                                        rpc::ObjectPlasmaLocationUpdate update) {
    owner_client->AssertObjectPlasmaLocationUpdate(worker_id, object_id, update);
  }

  void AssertNoLeak() {
    RAY_CHECK(obod_.in_flight_requests_.size() == 0)
        << "There are " << obod_.in_flight_requests_.size() << " in flight requests.";
    RAY_CHECK(obod_.location_buffers_.size() == 0)
        << "There are " << obod_.location_buffers_.size() << " buffered locations.";
  }

  int NumBatchRequestSent() { return owner_client->batch_sent; }

  int NumBatchReplied() { return owner_client->callback_invoked; }

  void SendDummyBatch(const WorkerID &owner_id) {
    // Send a dummy batch. It is needed because when the object report happens for the
    // first time, batch RPC is always sent.
    auto dummy_info = CreateNewObjectInfo(owner_id);
    obod_.ReportObjectAdded(dummy_info.object_id, current_node_id, dummy_info);
    RAY_LOG(INFO) << "First batch sent.";
  }

  void HandleMessage(const rpc::WorkerObjectLocationsPubMessage &location_info,
                     const ObjectID &object_id,
                     bool location_lookup_failed = false) {
    // Mock for receiving a message from the pubsub layer.
    obod_.ObjectLocationSubscriptionCallback(
        location_info, object_id, location_lookup_failed);
  }

  int64_t max_batch_size = 20;
  instrumented_io_context io_service_;
  gcs::GcsClientOptions options_;
  gcs::MockNodeInfoAccessor *node_info_accessor_;
  std::shared_ptr<gcs::GcsClient> gcs_client_mock_;
  std::shared_ptr<mock_pubsub::MockSubscriber> subscriber_;
  std::shared_ptr<MockWorkerClient> owner_client;
  rpc::CoreWorkerClientPool client_pool;
  OwnershipBasedObjectDirectory obod_;
  std::unordered_set<ObjectID> used_ids_;
  const NodeID current_node_id = NodeID::FromRandom();
};

TEST_F(OwnershipBasedObjectDirectoryTest, TestLocationUpdateBatchBasic) {
  const auto owner_id = WorkerID::FromRandom();

  {
    RAY_LOG(INFO) << "Object added basic.";
    auto object_info_added = CreateNewObjectInfo(owner_id);
    obod_.ReportObjectAdded(
        object_info_added.object_id, current_node_id, object_info_added);
    AssertObjectPlasmaLocationUpdate(object_info_added.owner_worker_id,
                                     object_info_added.object_id,
                                     rpc::ObjectPlasmaLocationUpdate::ADDED);
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
    ASSERT_EQ(NumBatchRequestSent(), 1);
    ASSERT_EQ(NumBatchReplied(), 1);
    AssertNoLeak();
  }

  {
    RAY_LOG(INFO) << "Object removed basic.";
    auto object_info_removed = CreateNewObjectInfo(owner_id);
    obod_.ReportObjectRemoved(
        object_info_removed.object_id, current_node_id, object_info_removed);
    AssertObjectPlasmaLocationUpdate(object_info_removed.owner_worker_id,
                                     object_info_removed.object_id,
                                     rpc::ObjectPlasmaLocationUpdate::REMOVED);
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
    ASSERT_EQ(NumBatchRequestSent(), 2);
    ASSERT_EQ(NumBatchReplied(), 2);
    AssertNoLeak();
  }

  {
    RAY_LOG(INFO) << "Object spilled basic.";
    auto object_info_spilled = CreateNewObjectInfo(owner_id);
    rpc::Address owner_address;
    owner_address.set_worker_id(object_info_spilled.owner_worker_id.Binary());
    obod_.ReportObjectSpilled(
        object_info_spilled.object_id, current_node_id, owner_address, "url1", true);
    rpc::ObjectLocationUpdate update =
        owner_client->buffered_object_locations_.at(object_info_spilled.owner_worker_id)
            .at(object_info_spilled.object_id);
    ASSERT_EQ(update.spilled_location_update().spilled_url(), "url1");
    ASSERT_EQ(update.spilled_location_update().spilled_to_local_storage(), true);
    ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
    ASSERT_EQ(NumBatchRequestSent(), 3);
    ASSERT_EQ(NumBatchReplied(), 3);
    AssertNoLeak();
  }
}

TEST_F(OwnershipBasedObjectDirectoryTest, TestLocationUpdateFIFOOrder) {
  const auto owner_id = WorkerID::FromRandom();
  SendDummyBatch(owner_id);

  auto object_info_1 = CreateNewObjectInfo(owner_id);
  auto object_info_2 = CreateNewObjectInfo(owner_id);
  auto object_info_3 = CreateNewObjectInfo(owner_id);
  obod_.ReportObjectAdded(object_info_1.object_id, current_node_id, object_info_1);
  obod_.ReportObjectAdded(object_info_3.object_id, current_node_id, object_info_3);
  obod_.ReportObjectAdded(object_info_2.object_id, current_node_id, object_info_2);
  obod_.ReportObjectRemoved(object_info_1.object_id, current_node_id, object_info_1);
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchReplied(), 1);
  ASSERT_EQ(NumBatchRequestSent(), 2);

  ASSERT_EQ(owner_client->object_location_updates_.size(), 4);
  ASSERT_EQ(owner_client->object_location_updates_[1].object_id(),
            object_info_1.object_id.Binary());
  ASSERT_EQ(owner_client->object_location_updates_[2].object_id(),
            object_info_3.object_id.Binary());
  ASSERT_EQ(owner_client->object_location_updates_[3].object_id(),
            object_info_2.object_id.Binary());

  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchReplied(), 2);
  AssertNoLeak();
}

TEST_F(OwnershipBasedObjectDirectoryTest, TestLocationUpdateBufferedUpdate) {
  const auto owner_id = WorkerID::FromRandom();
  SendDummyBatch(owner_id);

  auto object_info = CreateNewObjectInfo(owner_id);
  obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
  obod_.ReportObjectRemoved(object_info.object_id, current_node_id, object_info);
  rpc::Address owner_address;
  owner_address.set_worker_id(object_info.owner_worker_id.Binary());
  obod_.ReportObjectSpilled(
      object_info.object_id, current_node_id, owner_address, "url1", true);
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchReplied(), 1);

  ASSERT_EQ(NumBatchRequestSent(), 2);
  // For the same object ID, it should report the latest result (which is REMOVED).
  rpc::ObjectLocationUpdate update =
      owner_client->buffered_object_locations_.at(object_info.owner_worker_id)
          .at(object_info.object_id);
  AssertObjectPlasmaLocationUpdate(object_info.owner_worker_id,
                                   object_info.object_id,
                                   rpc::ObjectPlasmaLocationUpdate::REMOVED);
  ASSERT_EQ(update.spilled_location_update().spilled_url(), "url1");
  ASSERT_EQ(update.spilled_location_update().spilled_to_local_storage(), true);

  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchReplied(), 2);
  AssertNoLeak();
}

TEST_F(OwnershipBasedObjectDirectoryTest,
       TestLocationUpdateBufferedMultipleObjectBuffered) {
  const auto owner_id = WorkerID::FromRandom();
  SendDummyBatch(owner_id);

  auto object_info = CreateNewObjectInfo(owner_id);
  obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
  obod_.ReportObjectRemoved(object_info.object_id, current_node_id, object_info);

  auto object_info_2 = CreateNewObjectInfo(owner_id);
  obod_.ReportObjectRemoved(object_info_2.object_id, current_node_id, object_info_2);
  obod_.ReportObjectAdded(object_info_2.object_id, current_node_id, object_info_2);

  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchRequestSent(), 2);
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchReplied(), 2);
  // For the same object ID, it should report the latest result (which is REMOVED).
  AssertObjectPlasmaLocationUpdate(object_info.owner_worker_id,
                                   object_info.object_id,
                                   rpc::ObjectPlasmaLocationUpdate::REMOVED);
  AssertObjectPlasmaLocationUpdate(object_info_2.owner_worker_id,
                                   object_info_2.object_id,
                                   rpc::ObjectPlasmaLocationUpdate::ADDED);
  AssertNoLeak();
}

TEST_F(OwnershipBasedObjectDirectoryTest, TestLocationUpdateBufferedMultipleOwners) {
  const auto owner_1 = WorkerID::FromRandom();
  const auto owner_2 = WorkerID::FromRandom();
  SendDummyBatch(owner_1);
  SendDummyBatch(owner_2);

  auto object_info = CreateNewObjectInfo(owner_1);
  obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
  obod_.ReportObjectRemoved(object_info.object_id, current_node_id, object_info);

  auto object_info_2 = CreateNewObjectInfo(owner_2);
  obod_.ReportObjectRemoved(object_info_2.object_id, current_node_id, object_info_2);
  obod_.ReportObjectAdded(object_info_2.object_id, current_node_id, object_info_2);

  // Only dummy batch is sent.
  ASSERT_EQ(NumBatchRequestSent(), 2);
  // owner_1 batch replied
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  // owner_2 batch replied
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  // Requests are sent to owner 1 and 2.
  ASSERT_EQ(NumBatchRequestSent(), 4);
  ASSERT_EQ(NumBatchReplied(), 2);
  // For the same object ID, it should report the latest result (which is REMOVED).
  AssertObjectPlasmaLocationUpdate(object_info.owner_worker_id,
                                   object_info.object_id,
                                   rpc::ObjectPlasmaLocationUpdate::REMOVED);
  AssertObjectPlasmaLocationUpdate(object_info_2.owner_worker_id,
                                   object_info_2.object_id,
                                   rpc::ObjectPlasmaLocationUpdate::ADDED);

  // Clean up reply and check assert.
  // owner_1 batch replied
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  // owner_2 batch replied
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchReplied(), 4);
  AssertNoLeak();
}

TEST_F(OwnershipBasedObjectDirectoryTest, TestLocationUpdateOneInFlightRequest) {
  // Make sure there's only one in-flight request.
  const auto owner_1 = WorkerID::FromRandom();
  SendDummyBatch(owner_1);

  auto object_info = CreateNewObjectInfo(owner_1);
  for (int i = 0; i < 10; i++) {
    obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
    obod_.ReportObjectRemoved(object_info.object_id, current_node_id, object_info);
  }

  // Until the in-flight request is replied, batch requests are not sent.
  ASSERT_EQ(NumBatchRequestSent(), 1);

  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchRequestSent(), 2);
  AssertObjectPlasmaLocationUpdate(object_info.owner_worker_id,
                                   object_info.object_id,
                                   rpc::ObjectPlasmaLocationUpdate::REMOVED);

  // After it is replied, if there's no more entry in the buffer, it doesn't send a new
  // request.
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchRequestSent(), 2);
  AssertNoLeak();
}

TEST_F(OwnershipBasedObjectDirectoryTest, TestLocationUpdateMaxBatchSize) {
  // Make sure there's only one in-flight request.
  const auto owner_1 = WorkerID::FromRandom();
  SendDummyBatch(owner_1);

  std::vector<ObjectInfo> object_infos;
  for (int i = 0; i < max_batch_size + 1; i++) {
    auto object_info = CreateNewObjectInfo(owner_1);
    object_infos.emplace_back(object_info);
    obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
    obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
    obod_.ReportObjectRemoved(object_info.object_id, current_node_id, object_info);
  }

  // The dummy batch is replied, and the batch is sent as many as max_batch_size.
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchRequestSent(), 2);

  // The second batch is replied, and since there was max_batch_size + 1 entries, the
  // request is sent again.
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchRequestSent(), 3);

  // Once the next batch is replied, there's no more requests.
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchRequestSent(), 3);

  // Check if object id states are updated properly.
  for (const auto &object_info : object_infos) {
    AssertObjectPlasmaLocationUpdate(object_info.owner_worker_id,
                                     object_info.object_id,
                                     rpc::ObjectPlasmaLocationUpdate::REMOVED);
  }
  AssertNoLeak();
}

TEST_F(OwnershipBasedObjectDirectoryTest, TestOwnerFailed) {
  // Make sure there's only one in-flight request.
  const auto owner_1 = WorkerID::FromRandom();
  SendDummyBatch(owner_1);

  auto object_info = CreateNewObjectInfo(owner_1);
  obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);

  // The dummy batch is replied, and the new batch is sent.
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch());
  ASSERT_EQ(NumBatchRequestSent(), 2);

  // Buffer is filled.
  for (int i = 0; i < max_batch_size + 1; i++) {
    object_info = CreateNewObjectInfo(owner_1);
    obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
    obod_.ReportObjectAdded(object_info.object_id, current_node_id, object_info);
    obod_.ReportObjectRemoved(object_info.object_id, current_node_id, object_info);
  }

  // The second batch is replied, but failed.
  ASSERT_TRUE(owner_client->ReplyUpdateObjectLocationBatch(ray::Status::Invalid("")));
  // Requests are not sent anymore.
  ASSERT_EQ(NumBatchRequestSent(), 2);
  // Make sure metadata is cleaned up properly.
  AssertNoLeak();
}

TEST_F(OwnershipBasedObjectDirectoryTest, TestNotifyOnUpdate) {
  UniqueID callback_id = UniqueID::FromRandom();
  ObjectID obj_id = ObjectID::FromRandom();
  int num_callbacks = 0;
  EXPECT_CALL(*subscriber_, Subscribe(_, _, _, _, _, _, _)).WillOnce(Return(true));
  ASSERT_TRUE(
      obod_
          .SubscribeObjectLocations(callback_id,
                                    obj_id,
                                    rpc::Address(),
                                    [&](const ObjectID &object_id,
                                        const std::unordered_set<NodeID> &client_ids,
                                        const std::string &spilled_url,
                                        const NodeID &spilled_node_id,
                                        bool pending_creation,
                                        size_t object_size) { num_callbacks++; })
          .ok());
  ASSERT_EQ(num_callbacks, 0);

  // Object pending, no other metadata. This is the same as the initial state,
  // so no callbacks triggered.
  rpc::WorkerObjectLocationsPubMessage location_info;
  location_info.set_pending_creation(true);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 0);

  // Setting object size triggers callback once.
  location_info.set_object_size(100);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 1);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 1);

  // Adding object location triggers callback once.
  location_info.add_node_ids(NodeID::FromRandom().Binary());
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 2);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 2);

  // Removing object location triggers callback once.
  location_info.mutable_node_ids()->Clear();
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 3);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 3);

  // Adding spilled location triggers callback once.
  location_info.set_spilled_url("1234");
  location_info.set_spilled_node_id(NodeID::FromRandom().Binary());
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 4);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 4);

  // Setting pending creation back to false (happens during reconstruction)
  // triggers callback.
  location_info.set_pending_creation(false);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 5);
  HandleMessage(location_info, obj_id);
  ASSERT_EQ(num_callbacks, 5);

  // Make sure metadata is cleaned up properly.
  AssertNoLeak();
}

}  // namespace ray
