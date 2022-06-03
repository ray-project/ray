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

#include "ray/core_worker/reference_count.h"

#include <vector>

#include "absl/functional/bind_front.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/pubsub/mock_pubsub.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"

namespace ray {
namespace core {

static const rpc::Address empty_borrower;
static const ReferenceCounter::ReferenceTableProto empty_refs;

class ReferenceCountTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> rc;
  virtual void SetUp() {
    rpc::Address addr;
    publisher_ = std::make_shared<mock_pubsub::MockPublisher>();
    subscriber_ = std::make_shared<mock_pubsub::MockSubscriber>();
    rc = std::make_unique<ReferenceCounter>(
        addr, publisher_.get(), subscriber_.get(), [](const NodeID &node_id) {
          return true;
        });
  }

  virtual void TearDown() {
    AssertNoLeaks();
    publisher_.reset();
    subscriber_.reset();
    rc.reset();
  }

  void AssertNoLeaks() { ASSERT_EQ(rc->NumObjectIDsInScope(), 0); }

  std::shared_ptr<mock_pubsub::MockPublisher> publisher_;
  std::shared_ptr<mock_pubsub::MockSubscriber> subscriber_;
};

class ReferenceCountLineageEnabledTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> rc;
  virtual void SetUp() {
    rpc::Address addr;
    publisher_ = std::make_shared<mock_pubsub::MockPublisher>();
    subscriber_ = std::make_shared<mock_pubsub::MockSubscriber>();
    rc = std::make_unique<ReferenceCounter>(
        addr,
        publisher_.get(),
        subscriber_.get(),
        [](const NodeID &node_id) { return true; },
        /*lineage_pinning_enabled=*/true);
  }

  virtual void TearDown() {
    publisher_.reset();
    subscriber_.reset();
    rc.reset();
  }

  std::shared_ptr<mock_pubsub::MockPublisher> publisher_;
  std::shared_ptr<mock_pubsub::MockSubscriber> subscriber_;
};

/// The 2 classes below are implemented to support distributed mock test using
/// MockWorkerClient.
/// How it works? if Publish is called, the corresponding callback from
/// the Subscriber is called.
class MockDistributedSubscriber;
class MockDistributedPublisher;

using ObjectToCallbackMap =
    absl::flat_hash_map<ObjectID, pubsub::SubscriptionItemCallback>;
using ObjectToFailureCallbackMap =
    absl::flat_hash_map<ObjectID, pubsub::SubscriptionFailureCallback>;
using SubscriptionCallbackMap = absl::flat_hash_map<std::string, ObjectToCallbackMap>;
using SubscriptionFailureCallbackMap =
    absl::flat_hash_map<std::string, ObjectToFailureCallbackMap>;

// static maps are used to simulate distirubted environment.
static SubscriptionCallbackMap subscription_callback_map;
static SubscriptionFailureCallbackMap subscription_failure_callback_map;
static pubsub::pub_internal::SubscriptionIndex directory(
    rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL);

static std::string GenerateID(UniqueID publisher_id, UniqueID subscriber_id) {
  return publisher_id.Binary() + subscriber_id.Binary();
}

class MockCoreWorkerClientInterface : public rpc::CoreWorkerClientInterface {
 public:
  ~MockCoreWorkerClientInterface() = default;
  virtual void WaitForRefRemoved(const ObjectID object_id,
                                 const ObjectID contained_in_id,
                                 rpc::Address owner_address) = 0;
};

using PublisherFactoryFn =
    std::function<std::shared_ptr<MockCoreWorkerClientInterface>(const rpc::Address &)>;

class MockDistributedSubscriber : public pubsub::SubscriberInterface {
 public:
  MockDistributedSubscriber(
      pubsub::pub_internal::SubscriptionIndex *directory,
      SubscriptionCallbackMap *subscription_callback_map,
      SubscriptionFailureCallbackMap *subscription_failure_callback_map,
      pubsub::SubscriberID subscriber_id,
      PublisherFactoryFn client_factory)
      : directory_(directory),
        subscription_callback_map_(subscription_callback_map),
        subscription_failure_callback_map_(subscription_failure_callback_map),
        subscriber_id_(subscriber_id),
        subscriber_(std::make_unique<pubsub::pub_internal::SubscriberState>(
            subscriber_id,
            /*get_time_ms=*/[]() { return 1.0; },
            /*subscriber_timeout_ms=*/1000,
            /*publish_batch_size=*/1000)),
        client_factory_(client_factory) {}

  ~MockDistributedSubscriber() = default;

  bool Subscribe(
      const std::unique_ptr<rpc::SubMessage> sub_message,
      const rpc::ChannelType channel_type,
      const rpc::Address &publisher_address,
      const std::string &key_id_binary,
      pubsub::SubscribeDoneCallback subscribe_done_callback,
      pubsub::SubscriptionItemCallback subscription_callback,
      pubsub::SubscriptionFailureCallback subscription_failure_callback) override {
    const auto &request = sub_message->worker_ref_removed_message();
    // Register the borrower callback first. It will be flushable by
    // FlushBorrowerCallbacks from mock core worker client.
    const auto object_id = ObjectID::FromBinary(request.reference().object_id());
    const auto contained_in_id = ObjectID::FromBinary(request.contained_in_id());
    const auto owner_address = request.reference().owner_address();
    if (client_factory_) {
      client_factory_(publisher_address)
          ->WaitForRefRemoved(object_id, contained_in_id, owner_address);
    }
    // Due to the test env, there are times that the same message id from the same
    // subscriber is subscribed twice. We should just no-op in this case.
    if (!(directory_->HasKeyId(key_id_binary) &&
          directory_->HasSubscriber(subscriber_id_))) {
      directory_->AddEntry(key_id_binary, subscriber_.get());
    }
    const auto publisher_id = UniqueID::FromBinary(publisher_address.worker_id());
    const auto id = GenerateID(publisher_id, subscriber_id_);
    auto callback_it = subscription_callback_map_->find(id);
    if (callback_it == subscription_callback_map_->end()) {
      callback_it = subscription_callback_map_->emplace(id, ObjectToCallbackMap()).first;
    }

    auto failure_callback_it = subscription_failure_callback_map_->find(id);
    if (failure_callback_it == subscription_failure_callback_map_->end()) {
      failure_callback_it =
          subscription_failure_callback_map_->emplace(id, ObjectToFailureCallbackMap())
              .first;
    }

    const auto oid = ObjectID::FromBinary(key_id_binary);
    callback_it->second.emplace(oid, subscription_callback);
    return failure_callback_it->second.emplace(oid, subscription_failure_callback).second;
  }

  bool SubscribeChannel(
      const std::unique_ptr<rpc::SubMessage> sub_message,
      const rpc::ChannelType channel_type,
      const rpc::Address &publisher_address,
      pubsub::SubscribeDoneCallback subscribe_done_callback,
      pubsub::SubscriptionItemCallback subscription_callback,
      pubsub::SubscriptionFailureCallback subscription_failure_callback) override {
    RAY_LOG(FATAL) << "Unimplemented!";
    return false;
  }

  bool Unsubscribe(const rpc::ChannelType channel_type,
                   const rpc::Address &publisher_address,
                   const std::string &key_id_binary) override {
    return true;
  }

  bool UnsubscribeChannel(const rpc::ChannelType channel_type,
                          const rpc::Address &publisher_address) override {
    return true;
  }

  bool IsSubscribed(const rpc::ChannelType channel_type,
                    const rpc::Address &publisher_address,
                    const std::string &key_id_binary) const override {
    return directory_->HasKeyId(key_id_binary) &&
           directory_->HasSubscriber(subscriber_id_);
  }

  std::string DebugString() const override {
    RAY_LOG(FATAL) << "No need to implement it for testing.";
    return "";
  }

  pubsub::pub_internal::SubscriptionIndex *directory_;
  SubscriptionCallbackMap *subscription_callback_map_;
  SubscriptionFailureCallbackMap *subscription_failure_callback_map_;
  pubsub::SubscriberID subscriber_id_;
  std::unique_ptr<pubsub::pub_internal::SubscriberState> subscriber_;
  PublisherFactoryFn client_factory_;
};

class MockDistributedPublisher : public pubsub::PublisherInterface {
 public:
  MockDistributedPublisher(
      pubsub::pub_internal::SubscriptionIndex *directory,
      SubscriptionCallbackMap *subscription_callback_map,
      SubscriptionFailureCallbackMap *subscription_failure_callback_map,
      WorkerID publisher_id)
      : directory_(directory),
        subscription_callback_map_(subscription_callback_map),
        subscription_failure_callback_map_(subscription_failure_callback_map),
        publisher_id_(publisher_id) {}
  ~MockDistributedPublisher() = default;

  bool RegisterSubscription(const rpc::ChannelType channel_type,
                            const pubsub::SubscriberID &subscriber_id,
                            const std::optional<std::string> &key_id_binary) {
    RAY_CHECK(false) << "No need to implement it for testing.";
    return false;
  }

  void PublishFailure(const rpc::ChannelType channel_type,
                      const std::string &key_id_binary) {}

  void Publish(const rpc::PubMessage &pub_message) {
    if (pub_message.channel_type() == rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL) {
      // TODO(swang): Test object locations pubsub too.
      return;
    }
    const auto subscribers = directory_->GetSubscriberIdsByKeyId(pub_message.key_id());
    const auto oid = ObjectID::FromBinary(pub_message.key_id());
    for (const auto &subscriber_id : subscribers) {
      const auto id = GenerateID(publisher_id_, subscriber_id);
      const auto it = subscription_callback_map_->find(id);
      if (it != subscription_callback_map_->end()) {
        const auto callback_it = it->second.find(oid);
        RAY_CHECK(callback_it != it->second.end());
        callback_it->second(pub_message);
      }
    }
  }

  bool UnregisterSubscription(const rpc::ChannelType channel_type,
                              const pubsub::SubscriberID &subscriber_id,
                              const std::optional<std::string> &key_id_binary) {
    return true;
  }

  pubsub::pub_internal::SubscriptionIndex *directory_;
  SubscriptionCallbackMap *subscription_callback_map_;
  SubscriptionFailureCallbackMap *subscription_failure_callback_map_;
  WorkerID publisher_id_;
};

class MockWorkerClient : public MockCoreWorkerClientInterface {
 public:
  // Helper function to generate a random address.
  static rpc::Address CreateRandomAddress(const std::string &addr) {
    rpc::Address address;
    address.set_ip_address(addr);
    address.set_raylet_id(NodeID::FromRandom().Binary());
    address.set_worker_id(WorkerID::FromRandom().Binary());
    return address;
  }

  MockWorkerClient(const std::string &addr, PublisherFactoryFn client_factory = nullptr)
      : address_(CreateRandomAddress(addr)),
        publisher_(std::make_shared<MockDistributedPublisher>(
            &directory,
            &subscription_callback_map,
            &subscription_failure_callback_map,
            WorkerID::FromBinary(address_.worker_id()))),
        subscriber_(std::make_shared<MockDistributedSubscriber>(
            &directory,
            &subscription_callback_map,
            &subscription_failure_callback_map,
            WorkerID::FromBinary(address_.worker_id()),
            client_factory)),
        rc_(
            rpc::WorkerAddress(address_),
            publisher_.get(),
            subscriber_.get(),
            [](const NodeID &node_id) { return true; },
            /*lineage_pinning_enabled=*/false,
            client_factory) {}

  ~MockWorkerClient() override {
    if (!failed_) {
      AssertNoLeaks();
    }
  }

  void WaitForRefRemoved(const ObjectID object_id,
                         const ObjectID contained_in_id,
                         rpc::Address owner_address) override {
    auto r = num_requests_;

    auto borrower_callback = [=]() {
      auto ref_removed_callback =
          absl::bind_front(&ReferenceCounter::HandleRefRemoved, &rc_);
      rc_.SetRefRemovedCallback(
          object_id, contained_in_id, owner_address, ref_removed_callback);
    };
    borrower_callbacks_[r] = borrower_callback;

    num_requests_++;
  }

  bool FlushBorrowerCallbacks() {
    // Flush all the borrower callbacks. This means that after this function is invoked,
    // all of ref_counts will be tracked.
    if (borrower_callbacks_.empty()) {
      return false;
    } else {
      // Copy borrower callbacks in case we modify during the callbacks.
      auto borrower_callbacks_copy = borrower_callbacks_;
      borrower_callbacks_.clear();
      for (auto &callback : borrower_callbacks_copy) {
        callback.second();
      }
      return true;
    }
  }

  void FailAllWaitForRefRemovedRequests() {
    // Invoke all failure callbacks so that we can simulate the borrower failure scenario.
    for (const auto &it : subscription_failure_callback_map) {
      auto &callback_map = it.second;
      for (const auto &callback_it : callback_map) {
        const auto object_id = callback_it.first;
        const auto failure_callback = callback_it.second;
        failure_callback(object_id.Binary(), Status::UnknownError("Test failure"));
      }
    }
    subscription_failure_callback_map.clear();
    failed_ = true;
  }

  // The below methods mirror a core worker's operations, e.g., `Put` simulates
  // a ray.put().
  void Put(const ObjectID &object_id) {
    rc_.AddOwnedObject(object_id, {}, address_, "", 0, false, /*add_local_ref=*/true);
  }

  void PutWithForeignOwner(const ObjectID &object_id, const rpc::Address &owner_address) {
    rc_.AddLocalReference(object_id, "");
    rc_.AddBorrowedObject(object_id, {}, owner_address, /*foreign=*/true);
  }

  void PutWrappedId(const ObjectID outer_id, const ObjectID &inner_id) {
    rc_.AddOwnedObject(outer_id,
                       {inner_id},
                       address_,
                       "",
                       0,
                       false,
                       /*add_local_ref=*/true);
  }

  void GetSerializedObjectId(const ObjectID outer_id,
                             const ObjectID &inner_id,
                             const rpc::Address &owner_address) {
    rc_.AddLocalReference(inner_id, "");
    rc_.AddBorrowedObject(inner_id, outer_id, owner_address);
  }

  void ExecuteTaskWithArg(const ObjectID &arg_id,
                          const ObjectID &inner_id,
                          const rpc::Address &owner_address) {
    // Add a sentinel reference to keep the argument ID in scope even though
    // the frontend won't have a reference.
    rc_.AddLocalReference(arg_id, "");
    GetSerializedObjectId(arg_id, inner_id, owner_address);
  }

  ObjectID SubmitTaskWithArg(const ObjectID &arg_id) {
    ObjectID return_id = ObjectID::FromRandom();
    if (!arg_id.IsNil()) {
      rc_.UpdateSubmittedTaskReferences({return_id}, {arg_id});
    }
    rc_.AddOwnedObject(return_id, {}, address_, "", 0, false, /*add_local_ref=*/true);
    return_ids_.push_back(return_id);
    return return_id;
  }

  ReferenceCounter::ReferenceTableProto FinishExecutingTask(
      const ObjectID &arg_id,
      const ObjectID &return_id,
      const ObjectID *return_wrapped_id = nullptr,
      const rpc::WorkerAddress *owner_address = nullptr) {
    if (return_wrapped_id) {
      rc_.AddNestedObjectIds(return_id, {*return_wrapped_id}, *owner_address);
    }

    ReferenceCounter::ReferenceTableProto refs;
    if (!arg_id.IsNil()) {
      rc_.PopAndClearLocalBorrowers({arg_id}, &refs, nullptr);
    }
    return refs;
  }

  void HandleSubmittedTaskFinished(
      const ObjectID &return_id,
      const ObjectID &arg_id,
      const absl::flat_hash_map<ObjectID, std::vector<ObjectID>> &nested_return_ids = {},
      const rpc::Address &borrower_address = empty_borrower,
      const ReferenceCounter::ReferenceTableProto &borrower_refs = empty_refs) {
    std::vector<ObjectID> arguments;
    for (const auto &pair : nested_return_ids) {
      // NOTE(swang): https://github.com/ray-project/ray/issues/17553.
      rc_.AddNestedObjectIds(pair.first, pair.second, address_);
    }
    if (!arg_id.IsNil()) {
      arguments.push_back(arg_id);
    }
    rc_.UpdateFinishedTaskReferences(
        {return_id}, arguments, false, borrower_address, borrower_refs, nullptr);
  }

  WorkerID GetID() const { return WorkerID::FromBinary(address_.worker_id()); }

  void AssertNoLeaks() {
    for (const auto &return_id : return_ids_) {
      if (rc_.HasReference(return_id)) {
        rc_.RemoveLocalReference(return_id, nullptr);
      }
    }
    for (const auto &id : rc_.GetAllInScopeObjectIDs()) {
      RAY_LOG(INFO) << id;
    }
    ASSERT_EQ(rc_.NumObjectIDsInScope(), 0);
  }

  // Global map from Worker ID -> MockWorkerClient.
  // Global map from Object ID -> owner worker ID, list of objects that it depends on,
  // worker address that it's scheduled on. Worker map of pending return IDs.

  rpc::Address address_;
  std::shared_ptr<MockDistributedPublisher> publisher_;
  std::shared_ptr<MockDistributedSubscriber> subscriber_;
  // The ReferenceCounter at the "client".
  ReferenceCounter rc_;
  absl::flat_hash_map<int, std::function<void()>> borrower_callbacks_;
  int num_requests_ = 0;
  std::vector<ObjectID> return_ids_;
  bool failed_ = false;
};

// Tests basic incrementing/decrementing of direct/submitted task reference counts. An
// entry should only be removed once both of its reference counts reach zero.
TEST_F(ReferenceCountTest, TestBasic) {
  std::vector<ObjectID> out;

  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID return_id1 = ObjectID::FromRandom();
  ObjectID return_id2 = ObjectID::FromRandom();

  // Local references.
  rc->AddLocalReference(id1, "");
  rc->AddLocalReference(id1, "");
  rc->AddLocalReference(id2, "");
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();

  // Submitted task references.
  rc->AddLocalReference(return_id1, "");
  rc->AddLocalReference(return_id2, "");
  ASSERT_FALSE(rc->IsObjectPendingCreation(return_id1));
  ASSERT_FALSE(rc->IsObjectPendingCreation(return_id2));
  rc->UpdateSubmittedTaskReferences({return_id1}, {id1});
  rc->UpdateSubmittedTaskReferences({return_id2}, {id1, id2});
  ASSERT_TRUE(rc->IsObjectPendingCreation(return_id1));
  ASSERT_TRUE(rc->IsObjectPendingCreation(return_id2));

  ASSERT_EQ(rc->NumObjectIDsInScope(), 4);
  rc->UpdateFinishedTaskReferences(
      {return_id1}, {id1}, false, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 4);
  ASSERT_EQ(out.size(), 0);
  rc->UpdateFinishedTaskReferences(
      {return_id2}, {id2}, false, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 3);
  ASSERT_EQ(out.size(), 1);
  rc->UpdateFinishedTaskReferences(
      {return_id2}, {id1}, false, empty_borrower, empty_refs, &out);
  ASSERT_EQ(out.size(), 2);
  ASSERT_FALSE(rc->IsObjectPendingCreation(return_id1));
  ASSERT_FALSE(rc->IsObjectPendingCreation(return_id2));
  rc->RemoveLocalReference(return_id1, &out);
  rc->RemoveLocalReference(return_id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  out.clear();

  // Local & submitted task references.
  rc->AddLocalReference(id1, "");
  rc->UpdateSubmittedTaskReferences({return_id1}, {id1, id2});
  rc->AddLocalReference(id2, "");
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->UpdateFinishedTaskReferences(
      {return_id1}, {id2}, false, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 2);
  ASSERT_EQ(out.size(), 0);
  rc->UpdateFinishedTaskReferences(
      {return_id1}, {id1}, false, empty_borrower, empty_refs, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();

  // Submitted task with inlined references.
  rc->UpdateSubmittedTaskReferences({return_id1}, {id1});
  rc->UpdateSubmittedTaskReferences({return_id1}, {id2}, {id1}, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 1);
  rc->UpdateSubmittedTaskReferences({return_id1}, {}, {id2}, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 2);
  out.clear();
}

TEST_F(ReferenceCountTest, TestUnreconstructableObjectOutOfScope) {
  ObjectID id = ObjectID::FromRandom();
  rpc::Address address;
  address.set_ip_address("1234");

  auto out_of_scope = std::make_shared<bool>(false);
  auto callback = [&](const ObjectID &object_id) { *out_of_scope = true; };

  // The object goes out of scope once it has no more refs.
  std::vector<ObjectID> out;
  ASSERT_FALSE(rc->SetDeleteCallback(id, callback));
  rc->AddOwnedObject(id, {}, address, "", 0, false, /*add_local_ref=*/true);
  ASSERT_TRUE(rc->SetDeleteCallback(id, callback));
  ASSERT_FALSE(*out_of_scope);
  rc->RemoveLocalReference(id, &out);
  ASSERT_TRUE(*out_of_scope);

  // Unreconstructable objects go out of scope even if they have a nonzero
  // lineage ref count.
  *out_of_scope = false;
  ASSERT_FALSE(rc->SetDeleteCallback(id, callback));
  rc->AddOwnedObject(id, {}, address, "", 0, false, /*add_local_ref=*/false);
  ASSERT_TRUE(rc->SetDeleteCallback(id, callback));
  rc->UpdateSubmittedTaskReferences({}, {id});
  ASSERT_FALSE(*out_of_scope);
  rc->UpdateFinishedTaskReferences({}, {id}, false, empty_borrower, empty_refs, &out);
  ASSERT_TRUE(*out_of_scope);
}

// Tests call site tracking and ability to update object size.
TEST_F(ReferenceCountTest, TestReferenceStats) {
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  rpc::Address address;
  address.set_ip_address("1234");

  rc->AddLocalReference(id1, "file.py:42");
  rc->UpdateObjectSize(id1, 200);

  rpc::CoreWorkerStats stats;
  rc->AddObjectRefStats({}, &stats);
  ASSERT_EQ(stats.object_refs_size(), 1);
  ASSERT_EQ(stats.object_refs(0).object_id(), id1.Binary());
  ASSERT_EQ(stats.object_refs(0).local_ref_count(), 1);
  ASSERT_EQ(stats.object_refs(0).object_size(), 200);
  ASSERT_EQ(stats.object_refs(0).call_site(), "file.py:42");
  rc->RemoveLocalReference(id1, nullptr);

  rc->AddOwnedObject(id2, {}, address, "file2.py:43", 100, false, /*add_local_ref=*/true);
  rpc::CoreWorkerStats stats2;
  rc->AddObjectRefStats({}, &stats2);
  ASSERT_EQ(stats2.object_refs_size(), 1);
  ASSERT_EQ(stats2.object_refs(0).object_id(), id2.Binary());
  ASSERT_EQ(stats2.object_refs(0).local_ref_count(), 1);
  ASSERT_EQ(stats2.object_refs(0).object_size(), 100);
  ASSERT_EQ(stats2.object_refs(0).call_site(), "file2.py:43");
  rc->RemoveLocalReference(id2, nullptr);
}

TEST_F(ReferenceCountTest, TestHandleObjectSpilled) {
  ObjectID obj1 = ObjectID::FromRandom();
  NodeID node1 = NodeID::FromRandom();
  rpc::Address address;
  address.set_ip_address("1234");

  int64_t object_size = 100;
  rc->AddOwnedObject(obj1,
                     {},
                     address,
                     "file1.py:42",
                     object_size,
                     false,
                     /*add_local_ref=*/true,
                     absl::optional<NodeID>(node1));
  rc->HandleObjectSpilled(obj1, "url1", node1);
  rpc::WorkerObjectLocationsPubMessage object_info;
  Status status = rc->FillObjectInformation(obj1, &object_info);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(object_info.object_size(), object_size);
  ASSERT_EQ(object_info.spilled_url(), "url1");
  ASSERT_EQ(object_info.spilled_node_id(), node1.Binary());
  rc->RemoveLocalReference(obj1, nullptr);
}

// Tests fetching of locality data from reference table.
TEST_F(ReferenceCountTest, TestGetLocalityData) {
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  NodeID node1 = NodeID::FromRandom();
  NodeID node2 = NodeID::FromRandom();
  rpc::Address address;
  address.set_ip_address("1234");

  // Owned object with defined object size and pinned node location should return valid
  // locality data.
  int64_t object_size = 100;
  rc->AddOwnedObject(obj1,
                     {},
                     address,
                     "file2.py:42",
                     object_size,
                     false,
                     /*add_local_ref=*/true,
                     absl::optional<NodeID>(node1));
  auto locality_data_obj1 = rc->GetLocalityData(obj1);
  ASSERT_TRUE(locality_data_obj1.has_value());
  ASSERT_EQ(locality_data_obj1->object_size, object_size);
  ASSERT_EQ(locality_data_obj1->nodes_containing_object,
            absl::flat_hash_set<NodeID>{node1});

  // Owned object with defined object size and at least one node location should return
  // valid locality data.
  rc->AddObjectLocation(obj1, node2);
  locality_data_obj1 = rc->GetLocalityData(obj1);
  ASSERT_TRUE(locality_data_obj1.has_value());
  ASSERT_EQ(locality_data_obj1->object_size, object_size);
  ASSERT_EQ(locality_data_obj1->nodes_containing_object,
            absl::flat_hash_set<NodeID>({node1, node2}));
  rc->RemoveObjectLocation(obj1, node2);
  locality_data_obj1 = rc->GetLocalityData(obj1);
  ASSERT_EQ(locality_data_obj1->nodes_containing_object,
            absl::flat_hash_set<NodeID>({node1}));

  // When node2 is dead, reference table should remove it from obj1's locations.
  // And then GetLocalityData should only return node1.
  rc->AddObjectLocation(obj1, node2);
  locality_data_obj1 = rc->GetLocalityData(obj1);
  ASSERT_TRUE(locality_data_obj1.has_value());
  ASSERT_EQ(locality_data_obj1->object_size, object_size);
  ASSERT_EQ(locality_data_obj1->nodes_containing_object,
            absl::flat_hash_set<NodeID>({node1, node2}));
  rc->ResetObjectsOnRemovedNode(node2);
  locality_data_obj1 = rc->GetLocalityData(obj1);
  ASSERT_EQ(locality_data_obj1->nodes_containing_object,
            absl::flat_hash_set<NodeID>({node1}));

  // Include spilled locations in locality data.
  rc->RemoveObjectLocation(obj1, node1);
  rc->HandleObjectSpilled(obj1, "spill_loc", node1);
  locality_data_obj1 = rc->GetLocalityData(obj1);
  ASSERT_EQ(locality_data_obj1->nodes_containing_object,
            absl::flat_hash_set<NodeID>({node1}));

  // Borrowed object with defined object size and at least one node location should
  // return valid locality data.
  rc->AddLocalReference(obj2, "file.py:43");
  rc->AddBorrowedObject(obj2, ObjectID::Nil(), address);
  rc->ReportLocalityData(obj2, absl::flat_hash_set<NodeID>({node2}), object_size);
  auto locality_data_obj2 = rc->GetLocalityData(obj2);
  ASSERT_TRUE(locality_data_obj2.has_value());
  ASSERT_EQ(locality_data_obj2->object_size, object_size);
  ASSERT_EQ(locality_data_obj2->nodes_containing_object,
            absl::flat_hash_set<NodeID>({node2}));
  rc->RemoveLocalReference(obj2, nullptr);

  // Fetching locality data for an object that doesn't have a reference in the table
  // should return a null optional.
  auto locality_data_obj2_not_exist = rc->GetLocalityData(obj2);
  ASSERT_FALSE(locality_data_obj2_not_exist.has_value());

  // Fetching locality data for an object that doesn't have a pinned node location
  // defined should return empty locations.
  rc->AddLocalReference(obj2, "file.py:43");
  rc->UpdateObjectSize(obj2, 200);
  auto locality_data_obj2_no_pinned_raylet = rc->GetLocalityData(obj2);
  ASSERT_TRUE(locality_data_obj2_no_pinned_raylet.has_value());
  ASSERT_EQ(locality_data_obj2_no_pinned_raylet->nodes_containing_object.size(), 0);
  rc->RemoveLocalReference(obj2, nullptr);

  // Fetching locality data for an object that doesn't have an object size defined
  // should return a null optional.
  rc->AddOwnedObject(obj2,
                     {},
                     address,
                     "file2.py:43",
                     -1,
                     false,
                     /*add_local_ref=*/true,
                     absl::optional<NodeID>(node2));
  auto locality_data_obj2_no_object_size = rc->GetLocalityData(obj2);
  ASSERT_FALSE(locality_data_obj2_no_object_size.has_value());

  // Primary copy location is always returned
  // even if it's not in-memory (i.e. spilled).
  rc->AddOwnedObject(obj3,
                     {},
                     address,
                     "file2.py:43",
                     -1,
                     false,
                     /*add_local_ref=*/true);
  rc->UpdateObjectSize(obj3, 101);
  rc->UpdateObjectPinnedAtRaylet(obj3, node1);
  auto locality_data_obj3 = rc->GetLocalityData(obj3);
  ASSERT_TRUE(locality_data_obj3.has_value());
  ASSERT_EQ(locality_data_obj3->nodes_containing_object,
            absl::flat_hash_set<NodeID>({node1}));

  rc->RemoveLocalReference(obj1, nullptr);
  rc->RemoveLocalReference(obj2, nullptr);
  rc->RemoveLocalReference(obj3, nullptr);
}

// Tests that we can get the owner address correctly for objects that we own,
// objects that we borrowed via a serialized object ID, and objects whose
// origin we do not know.
TEST_F(ReferenceCountTest, TestOwnerAddress) {
  auto object_id = ObjectID::FromRandom();
  rpc::Address address;
  address.set_ip_address("1234");
  rc->AddOwnedObject(object_id, {}, address, "", 0, false, /*add_local_ref=*/true);

  TaskID added_id;
  rpc::Address added_address;
  ASSERT_TRUE(rc->GetOwner(object_id, &added_address));
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id2 = ObjectID::FromRandom();
  address.set_ip_address("5678");
  rc->AddOwnedObject(object_id2, {}, address, "", 0, false, /*add_local_ref=*/true);
  ASSERT_TRUE(rc->GetOwner(object_id2, &added_address));
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id3 = ObjectID::FromRandom();
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_address));
  rc->AddLocalReference(object_id3, "");
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_address));

  rc->RemoveLocalReference(object_id, nullptr);
  rc->RemoveLocalReference(object_id2, nullptr);
  rc->RemoveLocalReference(object_id3, nullptr);
}

// Tests that the ref counts are properly integrated into the local
// object memory store.
TEST(MemoryStoreIntegrationTest, TestSimple) {
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  uint8_t data[] = {1, 2, 3, 4, 5, 6, 7, 8};
  RayObject buffer(std::make_shared<LocalMemoryBuffer>(data, sizeof(data)), nullptr, {});

  auto publisher = std::make_shared<mock_pubsub::MockPublisher>();
  auto subscriber = std::make_shared<mock_pubsub::MockSubscriber>();
  auto rc = std::shared_ptr<ReferenceCounter>(
      new ReferenceCounter(rpc::WorkerAddress(rpc::Address()),
                           publisher.get(),
                           subscriber.get(),
                           [](const NodeID &node_id) { return true; }));
  CoreWorkerMemoryStore store(rc);

  // Tests putting an object with no references is ignored.
  RAY_CHECK(store.Put(buffer, id2));
  ASSERT_EQ(store.Size(), 0);

  // Tests ref counting overrides remove after get option.
  rc->AddLocalReference(id1, "");
  RAY_CHECK(store.Put(buffer, id1));
  ASSERT_EQ(store.Size(), 1);
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::Nil());
  RAY_CHECK_OK(store.Get({id1},
                         /*num_objects*/ 1,
                         /*timeout_ms*/ -1,
                         ctx,
                         /*remove_after_get*/ true,
                         &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(store.Size(), 1);
}

// A borrower is given a reference to an object ID, submits a task, waits for
// it to finish, then returns.
//
// @ray.remote
// def borrower(inner_ids):
//     inner_id = inner_ids[0]
//     ray.get(foo.remote(inner_id))
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestNoBorrow) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  auto return_id1 = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto return_id2 = borrower->SubmitTaskWithArg(inner_id);
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The borrower waits for the task to finish before returning to the owner.
  borrower->HandleSubmittedTaskFinished(return_id2, inner_id);
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  // Check that the borrower's ref count is now 0 for all objects.
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id1, outer_id, {}, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner's ref count is now 0 for all objects.
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
}

// A borrower is given a reference to an object ID, submits a task, does not
// wait for it to finish.
//
// @ray.remote
// def borrower(inner_ids):
//     inner_id = inner_ids[0]
//     foo.remote(inner_id)
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestSimpleBorrower) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  auto return_id1 =
      owner->SubmitTaskWithArg(outer_id);  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto return_id2 = borrower->SubmitTaskWithArg(inner_id);
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  // ASSERT_FALSE(borrower->rc_.HasReference(outer_id));
  // Check that the borrower's ref count for inner_id > 0 because of the
  // pending task.
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id1, outer_id, {}, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));

  // The task submitted by the borrower returns. Everyone's ref count should go
  // to 0.
  borrower->HandleSubmittedTaskFinished(return_id2, inner_id);
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
}

// A borrower is given a reference to an object ID, submits a task, does not
// wait for it to finish. The borrower then fails before the task finishes.
//
// @ray.remote
// def borrower(inner_ids):
//     inner_id = inner_ids[0]
//     foo.remote(inner_id)
//     # Process exits before task finishes.
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestSimpleBorrowerFailure) {
  // We need to clean up the failure callback map, so that we can properly test failure
  // scenario.
  subscription_failure_callback_map.clear();
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  auto return_id1 = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  // The borrower submits a task that depends on the inner object.
  borrower->SubmitTaskWithArg(inner_id);
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  // ASSERT_FALSE(borrower->rc_.HasReference(outer_id));
  // Check that the borrower's ref count for inner_id > 0 because of the
  // pending task.
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id1, outer_id, {}, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));

  // The borrower fails. The owner's ref count should go to 0.
  borrower->FailAllWaitForRefRemovedRequests();
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
}

// A borrower is given a reference to an object ID, keeps the reference past
// the task's lifetime, then deletes the reference before it hears from the
// owner.
//
// @ray.remote
// class Borrower:
//     def __init__(self, inner_ids):
//        self.inner_id = inner_ids[0]
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = Borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestSimpleBorrowerReferenceRemoved) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  auto return_id = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The borrower task returns to the owner while still using inner_id.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id));
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id, outer_id, {}, borrower->address_, borrower_refs);
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));

  // The borrower is no longer using inner_id, but it hasn't received the
  // message from the owner yet.
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower receives the owner's wait message. It should return a reply
  // to the owner immediately saying that it is no longer using inner_id.
  borrower->FlushBorrowerCallbacks();
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// A borrower is given a reference to an object ID, passes the reference to
// another borrower by submitting a task, and does not wait for it to finish.
//
// @ray.remote
// def borrower2(inner_ids):
//     pass
//
// @ray.remote
// def borrower(inner_ids):
//     borrower2.remote(inner_ids)
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestBorrowerTree) {
  auto borrower1 = std::make_shared<MockWorkerClient>("1");
  auto borrower2 = std::make_shared<MockWorkerClient>("2");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  auto return_id1 = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for both objects.
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Borrower 1 is given a reference to the inner object.
  borrower1->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto outer_id2 = ObjectID::FromRandom();
  borrower1->PutWrappedId(outer_id2, inner_id);
  auto return_id2 = borrower1->SubmitTaskWithArg(outer_id2);
  borrower1->rc_.RemoveLocalReference(inner_id, nullptr);
  borrower1->rc_.RemoveLocalReference(outer_id2, nullptr);
  ASSERT_TRUE(borrower1->rc_.HasReference(inner_id));
  ASSERT_TRUE(borrower1->rc_.HasReference(outer_id2));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower1->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_TRUE(borrower1->rc_.HasReference(inner_id));
  ASSERT_TRUE(borrower1->rc_.HasReference(outer_id2));
  ASSERT_FALSE(borrower1->rc_.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id1, outer_id, {}, borrower1->address_, borrower_refs);
  borrower1->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));

  // Borrower 2 starts executing. It is given a reference to the inner object
  // when it gets outer_id2 as an argument.
  borrower2->ExecuteTaskWithArg(outer_id2, inner_id, owner->address_);
  ASSERT_TRUE(borrower2->rc_.HasReference(inner_id));
  // Borrower 2 finishes but it is still using inner_id.
  borrower_refs = borrower2->FinishExecutingTask(outer_id2, ObjectID::Nil());
  ASSERT_TRUE(borrower2->rc_.HasReference(inner_id));
  ASSERT_FALSE(borrower2->rc_.HasReference(outer_id2));
  ASSERT_FALSE(borrower2->rc_.HasReference(outer_id));

  borrower1->HandleSubmittedTaskFinished(
      return_id2, outer_id2, {}, borrower2->address_, borrower_refs);
  borrower2->FlushBorrowerCallbacks();
  // Borrower 1 no longer has a reference to any objects.
  ASSERT_FALSE(borrower1->rc_.HasReference(inner_id));
  ASSERT_FALSE(borrower1->rc_.HasReference(outer_id2));
  // The owner should now have borrower 2 in its count.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  borrower2->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower2->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// A task is given a reference to an object ID, whose value contains another
// object ID. The task gets a reference to the innermost object ID, but deletes
// it by the time the task finishes.
//
// @ray.remote
// def borrower(mid_ids):
//     inner_id = ray.get(mid_ids[0])
//     del inner_id
//
// inner_id = ray.put(1)
// mid_id = ray.put([inner_id])
// outer_id = ray.put([mid_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestNestedObjectNoBorrow) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto mid_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(mid_id, inner_id);
  owner->PutWrappedId(outer_id, mid_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to mid_id.
  auto return_id = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(mid_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for all objects.
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(mid_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is given a reference to the middle object.
  borrower->ExecuteTaskWithArg(outer_id, mid_id, owner->address_);
  ASSERT_TRUE(borrower->rc_.HasReference(mid_id));
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));

  // The borrower unwraps the inner object with ray.get.
  borrower->GetSerializedObjectId(mid_id, inner_id, owner->address_);
  borrower->rc_.RemoveLocalReference(mid_id, nullptr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));
  // The borrower's reference to inner_id goes out of scope.
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);

  // The borrower task returns to the owner.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id));
  ASSERT_FALSE(borrower->rc_.HasReference(mid_id));
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id, outer_id, {}, borrower->address_, borrower_refs);
  // Check that owner now has nothing in scope.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
  ASSERT_FALSE(owner->rc_.HasReference(mid_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// A task is given a reference to an object ID, whose value contains another
// object ID. The task gets a reference to the innermost object ID, and is
// still borrowing it by the time the task finishes.
//
// @ray.remote
// def borrower(mid_ids):
//     inner_id = ray.get(mid_ids[0])
//     foo.remote(inner_id)
//
// inner_id = ray.put(1)
// mid_id = ray.put([inner_id])
// outer_id = ray.put([mid_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestNestedObject) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto mid_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(mid_id, inner_id);
  owner->PutWrappedId(outer_id, mid_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to mid_id.
  auto return_id = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(mid_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  // The owner's ref count > 0 for all objects.
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(mid_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is given a reference to the middle object.
  borrower->ExecuteTaskWithArg(outer_id, mid_id, owner->address_);
  ASSERT_TRUE(borrower->rc_.HasReference(mid_id));
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));

  // The borrower unwraps the inner object with ray.get.
  borrower->GetSerializedObjectId(mid_id, inner_id, owner->address_);
  borrower->rc_.RemoveLocalReference(mid_id, nullptr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The borrower task returns to the owner while still using inner_id.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id));
  ASSERT_FALSE(borrower->rc_.HasReference(mid_id));
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id, outer_id, {}, borrower->address_, borrower_refs);
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Check that owner's ref count for outer and mid are 0 since the borrower
  // task returned and there were no local references to outer_id.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
  ASSERT_FALSE(owner->rc_.HasReference(mid_id));

  // The borrower receives the owner's wait message. It should return a reply
  // to the owner immediately saying that it is no longer using inner_id.
  borrower->FlushBorrowerCallbacks();
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is no longer using inner_id, but it hasn't received the
  // message from the owner yet.
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// A borrower is given a reference to an object ID, whose value contains
// another object ID. The borrower passes the reference again to another
// borrower and waits for it to finish. The nested borrower unwraps the outer
// object and gets a reference to the innermost ID.
//
// @ray.remote
// def borrower2(owner_id2):
//     owner_id1 = ray.get(owner_id2[0])[0]
//     foo.remote(owner_id1)
//
// @ray.remote
// def borrower1(owner_id2):
//     ray.get(borrower2.remote(owner_id2))
//
// owner_id1 = ray.put(1)
// owner_id2 = ray.put([owner_id1])
// owner_id3 = ray.put([owner_id2])
// res = borrower1.remote(owner_id3)
TEST(DistributedReferenceCountTest, TestNestedObjectDifferentOwners) {
  auto borrower1 = std::make_shared<MockWorkerClient>("1");
  auto borrower2 = std::make_shared<MockWorkerClient>("2");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });

  // The owner creates an inner object and wraps it.
  auto owner_id1 = ObjectID::FromRandom();
  auto owner_id2 = ObjectID::FromRandom();
  auto owner_id3 = ObjectID::FromRandom();
  owner->Put(owner_id1);
  owner->PutWrappedId(owner_id2, owner_id1);
  owner->PutWrappedId(owner_id3, owner_id2);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to owner_id2.
  auto return_id2 = owner->SubmitTaskWithArg(owner_id3);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(owner_id1, nullptr);
  owner->rc_.RemoveLocalReference(owner_id2, nullptr);
  owner->rc_.RemoveLocalReference(owner_id3, nullptr);

  // The borrower is given a reference to the middle object.
  borrower1->ExecuteTaskWithArg(owner_id3, owner_id2, owner->address_);
  ASSERT_TRUE(borrower1->rc_.HasReference(owner_id2));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id1));

  // The borrower wraps the object ID again.
  auto borrower_id = ObjectID::FromRandom();
  borrower1->PutWrappedId(borrower_id, owner_id2);
  borrower1->rc_.RemoveLocalReference(owner_id2, nullptr);

  // Borrower 1 submits a task that depends on the wrapped object. The task
  // will be given a reference to owner_id2.
  auto return_id1 = borrower1->SubmitTaskWithArg(borrower_id);
  borrower1->rc_.RemoveLocalReference(borrower_id, nullptr);
  borrower2->ExecuteTaskWithArg(borrower_id, owner_id2, owner->address_);

  // The nested task returns while still using owner_id1.
  borrower2->GetSerializedObjectId(owner_id2, owner_id1, owner->address_);
  borrower2->rc_.RemoveLocalReference(owner_id2, nullptr);
  auto borrower_refs = borrower2->FinishExecutingTask(borrower_id, ObjectID::Nil());
  ASSERT_TRUE(borrower2->rc_.HasReference(owner_id1));
  ASSERT_FALSE(borrower2->rc_.HasReference(owner_id2));

  // Borrower 1 should now know that borrower 2 is borrowing the inner object
  // ID.
  borrower1->HandleSubmittedTaskFinished(
      return_id1, borrower_id, {}, borrower2->address_, borrower_refs);
  ASSERT_TRUE(borrower1->rc_.HasReference(owner_id1));

  // Borrower 1 finishes. It should not have any references now because all
  // state has been merged into the owner.
  borrower_refs = borrower1->FinishExecutingTask(owner_id3, ObjectID::Nil());
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id1));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id2));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id3));
  ASSERT_FALSE(borrower1->rc_.HasReference(borrower_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id2, owner_id3, {}, borrower1->address_, borrower_refs);
  // Check that owner now has borrower2 in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(owner_id1));
  ASSERT_FALSE(owner->rc_.HasReference(owner_id2));
  ASSERT_FALSE(owner->rc_.HasReference(owner_id3));

  // The borrower receives the owner's wait message.
  borrower2->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner->rc_.HasReference(owner_id1));
  borrower2->rc_.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_FALSE(borrower2->rc_.HasReference(owner_id1));
  ASSERT_FALSE(owner->rc_.HasReference(owner_id1));
}

// A borrower is given a reference to an object ID, whose value contains
// another object ID. The borrower passes the reference again to another
// borrower but does not wait for it to finish. The nested borrower unwraps the
// outer object and gets a reference to the innermost ID.
//
// @ray.remote
// def borrower2(owner_id2):
//     owner_id1 = ray.get(owner_id2[0])[0]
//     foo.remote(owner_id1)
//
// @ray.remote
// def borrower1(owner_id2):
//     borrower2.remote(owner_id2)
//
// owner_id1 = ray.put(1)
// owner_id2 = ray.put([owner_id1])
// owner_id3 = ray.put([owner_id2])
// res = borrower1.remote(owner_id3)
TEST(DistributedReferenceCountTest, TestNestedObjectDifferentOwners2) {
  auto borrower1 = std::make_shared<MockWorkerClient>("1");
  auto borrower2 = std::make_shared<MockWorkerClient>("2");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });

  // The owner creates an inner object and wraps it.
  auto owner_id1 = ObjectID::FromRandom();
  auto owner_id2 = ObjectID::FromRandom();
  auto owner_id3 = ObjectID::FromRandom();
  owner->Put(owner_id1);
  owner->PutWrappedId(owner_id2, owner_id1);
  owner->PutWrappedId(owner_id3, owner_id2);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to owner_id2.
  auto return_id2 = owner->SubmitTaskWithArg(owner_id3);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(owner_id1, nullptr);
  owner->rc_.RemoveLocalReference(owner_id2, nullptr);
  owner->rc_.RemoveLocalReference(owner_id3, nullptr);

  // The borrower is given a reference to the middle object.
  borrower1->ExecuteTaskWithArg(owner_id3, owner_id2, owner->address_);
  ASSERT_TRUE(borrower1->rc_.HasReference(owner_id2));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id1));

  // The borrower wraps the object ID again.
  auto borrower_id = ObjectID::FromRandom();
  borrower1->PutWrappedId(borrower_id, owner_id2);
  borrower1->rc_.RemoveLocalReference(owner_id2, nullptr);

  // Borrower 1 submits a task that depends on the wrapped object. The task
  // will be given a reference to owner_id2.
  auto return_id1 = borrower1->SubmitTaskWithArg(borrower_id);
  borrower2->ExecuteTaskWithArg(borrower_id, owner_id2, owner->address_);

  // The nested task returns while still using owner_id1.
  borrower2->GetSerializedObjectId(owner_id2, owner_id1, owner->address_);
  borrower2->rc_.RemoveLocalReference(owner_id2, nullptr);
  auto borrower_refs = borrower2->FinishExecutingTask(borrower_id, ObjectID::Nil());
  ASSERT_TRUE(borrower2->rc_.HasReference(owner_id1));
  ASSERT_FALSE(borrower2->rc_.HasReference(owner_id2));

  // Borrower 1 should now know that borrower 2 is borrowing the inner object
  // ID.
  borrower1->HandleSubmittedTaskFinished(
      return_id1, borrower_id, {}, borrower2->address_, borrower_refs);
  ASSERT_TRUE(borrower1->rc_.HasReference(owner_id1));
  ASSERT_TRUE(borrower1->rc_.HasReference(owner_id2));

  // Borrower 1 finishes. It should only have its reference to owner_id2 now.
  borrower_refs = borrower1->FinishExecutingTask(owner_id3, ObjectID::Nil());
  ASSERT_TRUE(borrower1->rc_.HasReference(owner_id2));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id3));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id2, owner_id3, {}, borrower1->address_, borrower_refs);
  // Check that owner now has borrower2 in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(owner_id1));
  ASSERT_TRUE(owner->rc_.HasReference(owner_id2));
  ASSERT_FALSE(owner->rc_.HasReference(owner_id3));

  // The borrower receives the owner's wait message.
  borrower2->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner->rc_.HasReference(owner_id1));
  borrower2->rc_.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_FALSE(borrower2->rc_.HasReference(owner_id1));
  ASSERT_TRUE(owner->rc_.HasReference(owner_id1));

  // The borrower receives the owner's wait message.
  borrower1->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner->rc_.HasReference(owner_id2));
  borrower1->rc_.RemoveLocalReference(borrower_id, nullptr);
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id2));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id1));
  ASSERT_FALSE(owner->rc_.HasReference(owner_id2));
}

// A borrower is given a reference to an object ID and passes the reference to
// another task. The nested task executes on the object's owner.
//
// @ray.remote
// def executes_on_owner(inner_ids):
//     inner_id = inner_ids[0]
//
// @ray.remote
// def borrower(inner_ids):
//     outer_id2 = ray.put(inner_ids)
//     executes_on_owner.remote(outer_id2)
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = borrower.remote(outer_id)
TEST(DistributedReferenceCountTest, TestBorrowerPingPong) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>("2", [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == borrower->address_.ip_address());
    return borrower;
  });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  auto return_id1 = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);

  // Borrower 1 is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto outer_id2 = ObjectID::FromRandom();
  borrower->PutWrappedId(outer_id2, inner_id);
  auto return_id2 = borrower->SubmitTaskWithArg(outer_id2);
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  borrower->rc_.RemoveLocalReference(outer_id2, nullptr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));
  ASSERT_TRUE(borrower->rc_.HasReference(outer_id2));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));
  ASSERT_TRUE(borrower->rc_.HasReference(outer_id2));
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id));

  // The owner receives the borrower's reply and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id1, outer_id, {}, borrower->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has a borrower for inner.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));

  // Owner starts executing the submitted task. It is given a second reference
  // to the inner object when it gets outer_id2 as an argument.
  owner->ExecuteTaskWithArg(outer_id2, inner_id, owner->address_);
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Owner finishes but it is still using inner_id.
  borrower_refs = owner->FinishExecutingTask(outer_id2, ObjectID::Nil());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  borrower->HandleSubmittedTaskFinished(
      return_id2, outer_id2, {}, owner->address_, borrower_refs);
  borrower->FlushBorrowerCallbacks();
  // Borrower no longer has a reference to any objects.
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id2));
  // The owner should now have borrower 2 in its count.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// A borrower is given two references to the same object ID. `task` and `Actor`
// execute on the same process.
//
// @ray.remote
// def task(inner_ids):
//     foo.remote(inner_ids[0])
//
// @ray.remote
// class Actor:
//     def __init__(self, inner_ids):
//         self.inner_id = inner_ids[0]
//
// inner_id = ray.put(1)
// outer_id = ray.put([inner_id])
// res = task.remote(outer_id)
// Actor.remote(outer_id)
TEST(DistributedReferenceCountTest, TestDuplicateBorrower) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // The owner creates an inner object and wraps it.
  auto inner_id = ObjectID::FromRandom();
  auto outer_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  owner->PutWrappedId(outer_id, inner_id);

  // The owner submits a task that depends on the outer object. The task will
  // be given a reference to inner_id.
  auto return_id1 = owner->SubmitTaskWithArg(outer_id);
  // The owner's references go out of scope.
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower is given a reference to the inner object.
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  // The borrower submits a task that depends on the inner object.
  auto return_id2 = borrower->SubmitTaskWithArg(inner_id);
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The borrower task returns to the owner without waiting for its submitted
  // task to finish.
  auto borrower_refs1 = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());
  // Check that the borrower's ref count for inner_id > 0 because of the
  // pending task.
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // The borrower is given a 2nd reference to the inner object.
  auto return_id3 = owner->SubmitTaskWithArg(outer_id);
  owner->rc_.RemoveLocalReference(outer_id, nullptr);
  borrower->ExecuteTaskWithArg(outer_id, inner_id, owner->address_);
  auto borrower_refs2 = borrower->FinishExecutingTask(outer_id, ObjectID::Nil());

  // The owner receives the borrower's replies and merges the borrower's ref
  // count into its own.
  owner->HandleSubmittedTaskFinished(
      return_id1, outer_id, {}, borrower->address_, borrower_refs1);
  owner->HandleSubmittedTaskFinished(
      return_id3, outer_id, {}, borrower->address_, borrower_refs2);
  borrower->FlushBorrowerCallbacks();
  // Check that owner now has borrower in inner's borrowers list.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  // Check that owner's ref count for outer == 0 since the borrower task
  // returned and there were no local references to outer_id.
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));

  // The task submitted by the borrower returns and its second reference goes
  // out of scope. Everyone's ref count should go to 0.
  borrower->HandleSubmittedTaskFinished(return_id2, inner_id);
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(borrower->rc_.HasReference(outer_id));
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
}

// Two tasks execute on the same worker. After the inner object id returned is
// transited twice on the same worker, a WaitForRefRemoved RPC is still able
// to retrieve the right containment metadata about the inner id.
//
// This unit test covers scenarios from test_dataset.py::test_callable_classes
// and test_dataset_pipeline.py::test_pipeline_actors.
//
// @ray.remote
// def owner_task1():
//     inner_id = ray.put(data, _owner=owner)
//     return inner_id
//
// @ray.remote
// def owner_task2(x):
//     ray.put(data, _owner=owner)
//
// return_id = owner_task1.remote()
// inner_id = ray.get(outer_id)[0]
// return_id2 = owner_task2.remote(inner_id)
//
TEST(DistributedReferenceCountTest, TestForeignOwner) {
  auto caller = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>("2");
  auto foreign_owner =
      std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
        if (addr.ip_address() == owner->address_.ip_address()) {
          return owner;
        } else
          return caller;
      });

  //
  // Phase 1 -- submit and execute owner_task1()
  //
  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());
  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->PutWithForeignOwner(inner_id, foreign_owner->address_);
  rpc::WorkerAddress addr(caller->address_);
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
  // Caller receives the owner's message, but inner_id is still in scope
  // because caller has a reference to return_id.
  caller->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  ASSERT_TRUE(caller->rc_.HasReference(inner_id));

  //
  // Phase 2 -- submit and execute owner_task2(x)
  //
  auto return_id2 = caller->SubmitTaskWithArg(return_id);
  caller->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  ASSERT_TRUE(caller->rc_.HasReference(inner_id));
  caller->rc_.RemoveLocalReference(return_id2, nullptr);
  // Owner receives a reference to inner_id. It still has a reference when
  // the task returns.
  owner->ExecuteTaskWithArg(return_id, inner_id, caller->address_);
  auto refs2 = owner->FinishExecutingTask(return_id, return_id2);
  // owner merges ref count into the caller.
  caller->HandleSubmittedTaskFinished(return_id2, return_id, {}, owner->address_, refs2);
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(return_id));
  ASSERT_FALSE(caller->rc_.HasReference(return_id));
  ASSERT_FALSE(owner->rc_.HasReference(return_id2));
  ASSERT_FALSE(caller->rc_.HasReference(return_id2));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  //
  // Phase 3 -- foreign owner gets ref removed information.
  //
  // Emulate ref removed callback.
  foreign_owner->rc_.AddOwnedObject(inner_id,
                                    {},
                                    foreign_owner->address_,
                                    "",
                                    0,
                                    false,
                                    /*add_local_ref=*/false);
  foreign_owner->rc_.AddBorrowerAddress(inner_id, owner->address_);

  // Foreign owner waits on owner.
  ASSERT_TRUE(owner->FlushBorrowerCallbacks());
  ASSERT_TRUE(foreign_owner->rc_.HasReference(inner_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(caller->FlushBorrowerCallbacks());
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  caller->rc_.RemoveLocalReference(inner_id, nullptr);

  // Foreign owner waits on caller next.
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(foreign_owner->rc_.HasReference(inner_id));
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
}

// A borrower is given references to 2 different objects, which each contain a
// reference to an object ID. The borrower unwraps both objects and receives a
// duplicate reference to the inner ID.
TEST(DistributedReferenceCountTest, TestDuplicateNestedObject) {
  auto borrower1 = std::make_shared<MockWorkerClient>("1");
  auto borrower2 = std::make_shared<MockWorkerClient>("2");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    if (addr.ip_address() == borrower1->address_.ip_address()) {
      return borrower1;
    } else {
      return borrower2;
    }
  });

  // The owner creates an inner object and wraps it.
  auto owner_id1 = ObjectID::FromRandom();
  auto owner_id2 = ObjectID::FromRandom();
  auto owner_id3 = ObjectID::FromRandom();
  owner->Put(owner_id1);
  owner->PutWrappedId(owner_id2, owner_id1);
  owner->PutWrappedId(owner_id3, owner_id2);

  auto return_id1 = owner->SubmitTaskWithArg(owner_id3);
  auto return_id2 = owner->SubmitTaskWithArg(owner_id2);
  owner->rc_.RemoveLocalReference(owner_id1, nullptr);
  owner->rc_.RemoveLocalReference(owner_id2, nullptr);
  owner->rc_.RemoveLocalReference(owner_id3, nullptr);

  borrower2->ExecuteTaskWithArg(owner_id3, owner_id2, owner->address_);
  borrower2->GetSerializedObjectId(owner_id2, owner_id1, owner->address_);
  borrower2->rc_.RemoveLocalReference(owner_id2, nullptr);
  // The nested task returns while still using owner_id1.
  auto borrower_refs = borrower2->FinishExecutingTask(owner_id3, ObjectID::Nil());
  owner->HandleSubmittedTaskFinished(
      return_id1, owner_id3, {}, borrower2->address_, borrower_refs);
  ASSERT_TRUE(borrower2->FlushBorrowerCallbacks());

  // The owner submits a task that is given a reference to owner_id1.
  borrower1->ExecuteTaskWithArg(owner_id2, owner_id1, owner->address_);
  // The borrower wraps the object ID again.
  auto borrower_id = ObjectID::FromRandom();
  borrower1->PutWrappedId(borrower_id, owner_id1);
  borrower1->rc_.RemoveLocalReference(owner_id1, nullptr);
  // Borrower 1 submits a task that depends on the wrapped object. The task
  // will be given a reference to owner_id1.
  auto return_id3 = borrower1->SubmitTaskWithArg(borrower_id);
  borrower1->rc_.RemoveLocalReference(borrower_id, nullptr);
  borrower2->ExecuteTaskWithArg(borrower_id, owner_id1, owner->address_);
  // The nested task returns while still using owner_id1.
  // It should now have 2 local references to owner_id1, one from the owner and
  // one from the borrower.
  borrower_refs = borrower2->FinishExecutingTask(borrower_id, ObjectID::Nil());
  borrower1->HandleSubmittedTaskFinished(
      return_id3, borrower_id, {}, borrower2->address_, borrower_refs);

  // Borrower 1 finishes. It should not have any references now because all
  // state has been merged into the owner.
  borrower_refs = borrower1->FinishExecutingTask(owner_id2, ObjectID::Nil());
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id1));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id2));
  ASSERT_FALSE(borrower1->rc_.HasReference(owner_id3));
  ASSERT_FALSE(borrower1->rc_.HasReference(borrower_id));
  // Borrower 1 should not have merge any refs into the owner because borrower 2's ref was
  // already merged into the owner.
  owner->HandleSubmittedTaskFinished(
      return_id2, owner_id2, {}, borrower1->address_, borrower_refs);

  // The borrower receives the owner's wait message.
  borrower2->FlushBorrowerCallbacks();
  ASSERT_TRUE(owner->rc_.HasReference(owner_id1));
  borrower2->rc_.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_TRUE(owner->rc_.HasReference(owner_id1));
  borrower2->rc_.RemoveLocalReference(owner_id1, nullptr);
  ASSERT_FALSE(borrower2->rc_.HasReference(owner_id1));
  ASSERT_FALSE(owner->rc_.HasReference(owner_id1));
}

// We submit a task and immediately delete the reference to the return ID. The
// submitted task returns an object ID.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// returns_id.remote()
TEST(DistributedReferenceCountTest, TestReturnObjectIdNoBorrow) {
  auto caller = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == caller->address_.ip_address());
    return caller;
  });

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Caller's ref to the task's return ID goes out of scope before it hears
  // from the owner of inner_id.
  caller->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  caller->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(caller->rc_.HasReference(return_id));
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));

  // Caller should respond to the owner's message immediately.
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// We submit a task and keep the reference to the return ID. The submitted task
// returns an object ID.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// return_id = returns_id.remote()
TEST(DistributedReferenceCountTest, TestReturnObjectIdBorrow) {
  auto caller = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == caller->address_.ip_address());
    return caller;
  });

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Caller receives the owner's message, but inner_id is still in scope
  // because caller has a reference to return_id.
  caller->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Caller's reference to return_id goes out of scope. The caller should
  // respond to the owner of inner_id so that inner_id can be deleted.
  caller->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(caller->rc_.HasReference(return_id));
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// We submit a task and submit another task that depends on the return ID. The
// submitted task returns an object ID, which will get borrowed by the second
// task.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// return_id = returns_id.remote()
// borrow.remote(return_id)
TEST(DistributedReferenceCountTest, TestReturnObjectIdBorrowChain) {
  auto caller = std::make_shared<MockWorkerClient>("1");
  auto borrower = std::make_shared<MockWorkerClient>("2");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    if (addr.ip_address() == caller->address_.ip_address()) {
      return caller;
    } else {
      return borrower;
    }
  });

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Caller receives the owner's message, but inner_id is still in scope
  // because caller has a reference to return_id.
  caller->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  auto return_id2 = caller->SubmitTaskWithArg(return_id);
  caller->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Borrower receives a reference to inner_id. It still has a reference when
  // the task returns.
  borrower->ExecuteTaskWithArg(return_id, inner_id, owner->address_);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));
  auto borrower_refs = borrower->FinishExecutingTask(return_id, return_id);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // Borrower merges ref count into the caller.
  caller->HandleSubmittedTaskFinished(
      return_id2, return_id, {}, borrower->address_, borrower_refs);
  // The caller should not have a ref count anymore because it was merged into
  // the owner.
  ASSERT_FALSE(caller->rc_.HasReference(return_id));
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower's receives the owner's message and its reference goes out of
  // scope.
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower->rc_.HasReference(return_id));
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// We submit a task and submit another task that depends on the return ID. The
// first submitted task returns an object ID, which will get borrowed by the second
// task. The second task returns the borrowed ID.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// @ray.remote
// def returns_borrowed_id(inner_ids):
//     return inner_ids
//
// return_id = returns_id.remote()
// returns_borrowed_id.remote(return_id)
TEST(DistributedReferenceCountTest, TestReturnBorrowedId) {
  auto caller = std::make_shared<MockWorkerClient>("1");
  auto borrower = std::make_shared<MockWorkerClient>("2");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    if (addr.ip_address() == caller->address_.ip_address()) {
      return caller;
    } else {
      return borrower;
    }
  });

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Caller receives the owner's message, but inner_id is still in scope
  // because caller has a reference to return_id.
  caller->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  auto borrower_return_id = caller->SubmitTaskWithArg(return_id);
  caller->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Borrower receives a reference to inner_id. It returns the inner_id as its
  // return value.
  borrower->ExecuteTaskWithArg(return_id, inner_id, owner->address_);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));
  auto borrower_refs =
      borrower->FinishExecutingTask(return_id, borrower_return_id, &inner_id, &addr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // Borrower merges ref count into the caller.
  caller->HandleSubmittedTaskFinished(borrower_return_id,
                                      return_id,
                                      {{borrower_return_id, {inner_id}}},
                                      borrower->address_,
                                      borrower_refs);
  // The caller should still have a ref count because it has a reference to
  // borrower_return_id.
  ASSERT_FALSE(caller->rc_.HasReference(return_id));
  ASSERT_TRUE(caller->rc_.HasReference(borrower_return_id));
  ASSERT_TRUE(caller->rc_.HasReference(inner_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower's receives the owner's message and its reference goes out of
  // scope.
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower->rc_.HasReference(borrower_return_id));
  ASSERT_FALSE(borrower->rc_.HasReference(return_id));
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));

  // The caller's reference to the borrower's return value goes out of scope.
  caller->rc_.RemoveLocalReference(borrower_return_id, nullptr);
  ASSERT_FALSE(caller->rc_.HasReference(borrower_return_id));
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
  // The owner should still have the object ID in scope because it hasn't heard
  // from borrower yet.
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// We submit a task and submit another task that depends on the return ID. The
// first submitted task returns an object ID, which will get borrowed by the second
// task. The second task returns the borrowed ID. The driver gets the value of
// the second task and now has a reference to the inner object ID.
//
// @ray.remote
// def returns_id():
//     inner_id = ray.put()
//     return inner_id
//
// @ray.remote
// def returns_borrowed_id(inner_ids):
//     return inner_ids
//
// return_id = returns_id.remote()
// inner_id = ray.get(returns_borrowed_id.remote(return_id))[0]
TEST(DistributedReferenceCountTest, TestReturnBorrowedIdDeserialize) {
  auto caller = std::make_shared<MockWorkerClient>("1");
  auto borrower = std::make_shared<MockWorkerClient>("2");
  auto owner = std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
    if (addr.ip_address() == caller->address_.ip_address()) {
      return caller;
    } else {
      return borrower;
    }
  });

  // Caller submits a task.
  auto return_id = caller->SubmitTaskWithArg(ObjectID::Nil());

  // Task returns inner_id as its return value.
  auto inner_id = ObjectID::FromRandom();
  owner->Put(inner_id);
  rpc::WorkerAddress addr(caller->address_);
  auto refs = owner->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(refs.empty());
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Caller receives the owner's message, but inner_id is still in scope
  // because caller has a reference to return_id.
  caller->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  auto borrower_return_id = caller->SubmitTaskWithArg(return_id);
  caller->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Borrower receives a reference to inner_id. It returns the inner_id as its
  // return value.
  borrower->ExecuteTaskWithArg(return_id, inner_id, owner->address_);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));
  auto borrower_refs =
      borrower->FinishExecutingTask(return_id, borrower_return_id, &inner_id, &addr);
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // Borrower merges ref count into the caller.
  caller->HandleSubmittedTaskFinished(borrower_return_id,
                                      return_id,
                                      {{borrower_return_id, {inner_id}}},
                                      borrower->address_,
                                      borrower_refs);
  // The caller should still have a ref count because it has a reference to
  // borrower_return_id.
  ASSERT_FALSE(caller->rc_.HasReference(return_id));
  ASSERT_TRUE(caller->rc_.HasReference(borrower_return_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  caller->GetSerializedObjectId(borrower_return_id, inner_id, owner->address_);
  caller->rc_.RemoveLocalReference(borrower_return_id, nullptr);
  ASSERT_TRUE(caller->FlushBorrowerCallbacks());
  caller->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(caller->rc_.HasReference(return_id));
  ASSERT_FALSE(caller->rc_.HasReference(borrower_return_id));
  ASSERT_FALSE(caller->rc_.HasReference(inner_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // The borrower's receives the owner's message and its reference goes out of
  // scope.
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_FALSE(borrower->rc_.HasReference(borrower_return_id));
  ASSERT_FALSE(borrower->rc_.HasReference(return_id));
  ASSERT_FALSE(borrower->rc_.HasReference(inner_id));
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// Recursively returning IDs. We submit a task, which submits another task and
// returns the submitted task's return ID. The nested task creates an object
// and returns that ID.
//
// @ray.remote
// def nested_worker():
//     inner_id = ray.put()
//     return inner_id
//
// @ray.remote
// def worker():
//     return nested_worker.remote()
//
// return_id = worker.remote()
// nested_return_id = ray.get(return_id)
// inner_id = ray.get(nested_return_id)
TEST(DistributedReferenceCountTest, TestReturnIdChain) {
  auto root = std::make_shared<MockWorkerClient>("1");
  auto worker = std::make_shared<MockWorkerClient>("2", [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == root->address_.ip_address());
    return root;
  });
  auto nested_worker =
      std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
        RAY_CHECK(addr.ip_address() == worker->address_.ip_address());
        return worker;
      });

  // Root submits a task.
  auto return_id = root->SubmitTaskWithArg(ObjectID::Nil());

  // Task submits a nested task and returns the return ID.
  auto nested_return_id = worker->SubmitTaskWithArg(ObjectID::Nil());
  rpc::WorkerAddress addr(root->address_);
  auto refs =
      worker->FinishExecutingTask(ObjectID::Nil(), return_id, &nested_return_id, &addr);

  // The nested task returns an ObjectID that it owns.
  auto inner_id = ObjectID::FromRandom();
  nested_worker->Put(inner_id);
  rpc::WorkerAddress worker_addr(worker->address_);
  auto nested_refs = nested_worker->FinishExecutingTask(
      ObjectID::Nil(), nested_return_id, &inner_id, &worker_addr);
  nested_worker->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));

  // All task execution replies are received.
  root->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {nested_return_id}}});
  worker->HandleSubmittedTaskFinished(
      nested_return_id, ObjectID::Nil(), {{nested_return_id, {inner_id}}});
  root->FlushBorrowerCallbacks();
  worker->FlushBorrowerCallbacks();

  // The reference only goes out of scope once the other workers' references to
  // their submitted tasks' return ID go out of scope.
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));
  worker->rc_.RemoveLocalReference(nested_return_id, nullptr);
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));
  root->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(nested_worker->rc_.HasReference(inner_id));
}

// Recursively returning a borrowed object ID. We submit a task, which submits
// another task, calls ray.get() on the return ID and returns the value.  The
// nested task creates an object and returns that ID.
//
// @ray.remote
// def nested_worker():
//     inner_id = ray.put()
//     return inner_id
//
// @ray.remote
// def worker():
//     return ray.get(nested_worker.remote())
//
// return_id = worker.remote()
// inner_id = ray.get(return_id)
TEST(DistributedReferenceCountTest, TestReturnBorrowedIdChain) {
  auto root = std::make_shared<MockWorkerClient>("1");
  auto worker = std::make_shared<MockWorkerClient>("2", [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == root->address_.ip_address());
    return root;
  });
  auto nested_worker =
      std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
        if (addr.ip_address() == root->address_.ip_address()) {
          return root;
        } else {
          return worker;
        }
      });

  // Root submits a task.
  auto return_id = root->SubmitTaskWithArg(ObjectID::Nil());

  // Task submits a nested task.
  auto nested_return_id = worker->SubmitTaskWithArg(ObjectID::Nil());

  // The nested task returns an ObjectID that it owns.
  auto inner_id = ObjectID::FromRandom();
  nested_worker->Put(inner_id);
  rpc::WorkerAddress worker_addr(worker->address_);
  auto nested_refs = nested_worker->FinishExecutingTask(
      ObjectID::Nil(), nested_return_id, &inner_id, &worker_addr);
  nested_worker->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));

  // Worker receives the reply from the nested task.
  worker->HandleSubmittedTaskFinished(
      nested_return_id, ObjectID::Nil(), {{nested_return_id, {inner_id}}});
  worker->FlushBorrowerCallbacks();
  // Worker deserializes the inner_id and returns it.
  worker->GetSerializedObjectId(nested_return_id, inner_id, nested_worker->address_);
  rpc::WorkerAddress addr(root->address_);
  auto refs = worker->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);

  // Worker no longer borrowers the inner ID.
  worker->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(worker->rc_.HasReference(inner_id));
  worker->rc_.RemoveLocalReference(nested_return_id, nullptr);
  ASSERT_FALSE(worker->rc_.HasReference(inner_id));
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));

  // Root receives worker's reply, then the WaitForRefRemovedRequest from
  // nested_worker.
  root->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  root->FlushBorrowerCallbacks();
  // Object is still in scope because root now knows that return_id contains
  // inner_id.
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));

  root->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(root->rc_.HasReference(return_id));
  ASSERT_FALSE(root->rc_.HasReference(inner_id));
  ASSERT_FALSE(nested_worker->rc_.HasReference(inner_id));
}

// Recursively returning a borrowed object ID. We submit a task, which submits
// another task, calls ray.get() on the return ID and returns the value.  The
// nested task creates an object and returns that ID.
//
// This test is the same as above, except that it reorders messages so that the
// driver receives the WaitForRefRemovedRequest from nested_worker BEFORE it
// receives the reply from worker indicating that return_id contains inner_id.
//
// @ray.remote
// def nested_worker():
//     inner_id = ray.put()
//     return inner_id
//
// @ray.remote
// def worker():
//     return ray.get(nested_worker.remote())
//
// return_id = worker.remote()
// inner_id = ray.get(return_id)
TEST(DistributedReferenceCountTest, TestReturnBorrowedIdChainOutOfOrder) {
  auto root = std::make_shared<MockWorkerClient>("1");
  auto worker = std::make_shared<MockWorkerClient>("2", [&](const rpc::Address &addr) {
    RAY_CHECK(addr.ip_address() == root->address_.ip_address());
    return root;
  });
  auto nested_worker =
      std::make_shared<MockWorkerClient>("3", [&](const rpc::Address &addr) {
        if (addr.ip_address() == root->address_.ip_address()) {
          return root;
        } else {
          return worker;
        }
      });

  // Root submits a task.
  auto return_id = root->SubmitTaskWithArg(ObjectID::Nil());

  // Task submits a nested task.
  auto nested_return_id = worker->SubmitTaskWithArg(ObjectID::Nil());

  // The nested task returns an ObjectID that it owns.
  auto inner_id = ObjectID::FromRandom();
  nested_worker->Put(inner_id);
  rpc::WorkerAddress worker_addr(worker->address_);
  auto nested_refs = nested_worker->FinishExecutingTask(
      ObjectID::Nil(), nested_return_id, &inner_id, &worker_addr);
  nested_worker->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));

  // Worker receives the reply from the nested task.
  worker->HandleSubmittedTaskFinished(
      nested_return_id, ObjectID::Nil(), {{nested_return_id, {inner_id}}});
  worker->FlushBorrowerCallbacks();
  // Worker deserializes the inner_id and returns it.
  worker->GetSerializedObjectId(nested_return_id, inner_id, nested_worker->address_);
  rpc::WorkerAddress addr(root->address_);
  auto refs = worker->FinishExecutingTask(ObjectID::Nil(), return_id, &inner_id, &addr);

  // Worker no longer borrowers the inner ID.
  worker->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(worker->rc_.HasReference(inner_id));
  worker->rc_.RemoveLocalReference(nested_return_id, nullptr);
  ASSERT_FALSE(worker->rc_.HasReference(inner_id));
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));

  // Root receives the WaitForRefRemovedRequest from nested_worker BEFORE the
  // reply from worker.
  root->FlushBorrowerCallbacks();
  ASSERT_TRUE(nested_worker->rc_.HasReference(inner_id));

  root->HandleSubmittedTaskFinished(
      return_id, ObjectID::Nil(), {{return_id, {inner_id}}});
  root->rc_.RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(root->rc_.HasReference(return_id));
  ASSERT_FALSE(root->rc_.HasReference(inner_id));
  ASSERT_FALSE(nested_worker->rc_.HasReference(inner_id));
}

// TODO: Test Pop and Merge individually.

TEST_F(ReferenceCountLineageEnabledTest, TestUnreconstructableObjectOutOfScope) {
  ObjectID id = ObjectID::FromRandom();
  ObjectID return_id = ObjectID::FromRandom();
  rpc::Address address;
  address.set_ip_address("1234");

  auto out_of_scope = std::make_shared<bool>(false);
  auto callback = [&](const ObjectID &object_id) { *out_of_scope = true; };

  // The object goes out of scope once it has no more refs.
  std::vector<ObjectID> out;
  ASSERT_FALSE(rc->SetDeleteCallback(id, callback));
  rc->AddOwnedObject(id, {}, address, "", 0, false, /*add_local_ref=*/true);
  ASSERT_TRUE(rc->SetDeleteCallback(id, callback));
  ASSERT_FALSE(*out_of_scope);
  ASSERT_FALSE(*out_of_scope);
  rc->RemoveLocalReference(id, &out);
  ASSERT_TRUE(*out_of_scope);

  rc->AddLocalReference(return_id, "");

  // Unreconstructable objects stay in scope if they have a nonzero lineage ref
  // count.
  *out_of_scope = false;
  ASSERT_FALSE(rc->SetDeleteCallback(id, callback));
  rc->AddOwnedObject(id, {}, address, "", 0, false, /*add_local_ref=*/false);
  ASSERT_TRUE(rc->SetDeleteCallback(id, callback));
  rc->UpdateSubmittedTaskReferences({return_id}, {id});
  ASSERT_TRUE(rc->IsObjectPendingCreation(return_id));
  ASSERT_FALSE(*out_of_scope);
  rc->UpdateFinishedTaskReferences(
      {return_id}, {id}, false, empty_borrower, empty_refs, &out);
  ASSERT_FALSE(rc->IsObjectPendingCreation(return_id));
  ASSERT_FALSE(*out_of_scope);

  // Unreconstructable objects go out of scope once their lineage ref count
  // reaches 0.
  rc->UpdateResubmittedTaskReferences({return_id}, {id});
  ASSERT_TRUE(rc->IsObjectPendingCreation(return_id));
  rc->UpdateFinishedTaskReferences(
      {return_id}, {id}, true, empty_borrower, empty_refs, &out);
  ASSERT_FALSE(rc->IsObjectPendingCreation(return_id));
  ASSERT_TRUE(*out_of_scope);
}

// Test to make sure that we call the lineage released callback correctly.
TEST_F(ReferenceCountLineageEnabledTest, TestBasicLineage) {
  std::vector<ObjectID> out;
  std::vector<ObjectID> lineage_deleted;

  ObjectID id = ObjectID::FromRandom();

  rc->SetReleaseLineageCallback(
      [&](const ObjectID &object_id, std::vector<ObjectID> *ids_to_release) {
        lineage_deleted.push_back(object_id);
        return 0;
      });

  // We should not keep lineage for borrowed objects.
  rc->AddLocalReference(id, "");
  ASSERT_TRUE(rc->HasReference(id));
  rc->RemoveLocalReference(id, nullptr);
  ASSERT_TRUE(lineage_deleted.empty());

  // We should keep lineage for owned objects.
  rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, false, /*add_local_ref=*/true);
  ASSERT_TRUE(rc->HasReference(id));
  rc->RemoveLocalReference(id, nullptr);
  ASSERT_EQ(lineage_deleted.size(), 1);
}

// Test for pinning the lineage of an object, where the lineage is a chain of
// tasks that each depend on the previous. The previous objects should already
// have gone out of scope, but their Reference entry is pinned until the final
// object goes out of scope.
TEST_F(ReferenceCountLineageEnabledTest, TestPinLineageRecursive) {
  std::vector<ObjectID> out;
  std::vector<ObjectID> lineage_deleted;

  std::vector<ObjectID> ids;
  for (int i = 0; i < 3; i++) {
    ObjectID id = ObjectID::FromRandom();
    ids.push_back(id);
    rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, true, /*add_local_ref=*/false);
  }

  rc->SetReleaseLineageCallback(
      [&](const ObjectID &object_id, std::vector<ObjectID> *ids_to_release) {
        lineage_deleted.push_back(object_id);
        // Simulate releasing objects in downstream_id's lineage.
        size_t i = 0;
        for (; i < ids.size(); i++) {
          if (ids[i] == object_id) {
            break;
          }
        }
        RAY_CHECK(i < ids.size());
        if (i > 0) {
          ids_to_release->push_back(ids[i - 1]);
        }
        return 0;
      });

  for (size_t i = 0; i < ids.size() - 1; i++) {
    auto id = ids[i];
    // Submit a dependent task on id.
    ASSERT_TRUE(rc->HasReference(id));
    rc->UpdateSubmittedTaskReferences({}, {id});
    rc->RemoveLocalReference(id, nullptr);

    // The task finishes but is retryable.
    rc->UpdateFinishedTaskReferences({}, {id}, false, empty_borrower, empty_refs, &out);
    // We should fail to set the deletion callback because the object has
    // already gone out of scope.
    ASSERT_FALSE(rc->SetDeleteCallback(
        id, [&](const ObjectID &object_id) { ASSERT_FALSE(true); }));

    ASSERT_EQ(out.size(), 1);
    out.clear();
    ASSERT_TRUE(lineage_deleted.empty());
    ASSERT_TRUE(rc->HasReference(id));
  }

  // The task return ID goes out of scope.
  rc->AddLocalReference(ids.back(), "");
  rc->RemoveLocalReference(ids.back(), nullptr);
  // The removal of the last return ID should recursively delete all
  // references.
  ASSERT_EQ(lineage_deleted.size(), ids.size());
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
}

TEST_F(ReferenceCountLineageEnabledTest, TestEvictLineage) {
  std::vector<ObjectID> ids;
  for (int i = 0; i < 3; i++) {
    ObjectID id = ObjectID::FromRandom();
    ids.push_back(id);
    rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, true, /*add_local_ref=*/true);
  }
  std::vector<ObjectID> lineage_deleted;
  rc->SetReleaseLineageCallback(
      [&](const ObjectID &object_id, std::vector<ObjectID> *ids_to_release) {
        lineage_deleted.push_back(object_id);
        if (object_id == ids[1]) {
          // ID1 depends on ID0.
          ids_to_release->push_back(ids[0]);
        }

        return 10;
      });

  // ID1 depends on ID0.
  rc->UpdateSubmittedTaskReferences({ids[1]}, {ids[0]});
  rc->RemoveLocalReference(ids[0], nullptr);
  rc->UpdateFinishedTaskReferences(
      {ids[1]}, {ids[0]}, /*release_lineage=*/false, empty_borrower, empty_refs, nullptr);

  bool lineage_evicted = false;
  for (const auto &id : ids) {
    ASSERT_TRUE(rc->IsObjectReconstructable(id, &lineage_evicted));
    ASSERT_FALSE(lineage_evicted);
  }

  // IDs 0 and 1 should be evicted because they were created before ID2, and
  // ID1 depends on ID0.
  auto bytes_evicted = rc->EvictLineage(10);
  ASSERT_EQ(bytes_evicted, 20);
  ASSERT_EQ(lineage_deleted.size(), 2);
  ASSERT_FALSE(rc->HasReference(ids[0]));
  ASSERT_TRUE(rc->HasReference(ids[1]));
  ASSERT_TRUE(rc->HasReference(ids[2]));
  // ID1 is no longer reconstructable due to lineage eviction.
  ASSERT_FALSE(rc->IsObjectReconstructable(ids[1], &lineage_evicted));
  ASSERT_TRUE(lineage_evicted);
  ASSERT_TRUE(rc->IsObjectReconstructable(ids[2], &lineage_evicted));
  ASSERT_FALSE(lineage_evicted);
}

TEST_F(ReferenceCountLineageEnabledTest, TestResubmittedTask) {
  std::vector<ObjectID> out;
  std::vector<ObjectID> lineage_deleted;

  ObjectID id = ObjectID::FromRandom();
  rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, true, /*add_local_ref=*/true);

  rc->SetReleaseLineageCallback(
      [&](const ObjectID &object_id, std::vector<ObjectID> *ids_to_release) {
        lineage_deleted.push_back(object_id);
        return 0;
      });

  // Local references.
  ASSERT_TRUE(rc->HasReference(id));

  // Submit 2 dependent tasks.
  rc->UpdateSubmittedTaskReferences({}, {id});
  rc->UpdateSubmittedTaskReferences({}, {id});
  rc->RemoveLocalReference(id, nullptr);
  ASSERT_TRUE(rc->HasReference(id));

  // Both tasks finish, 1 is retryable.
  rc->UpdateFinishedTaskReferences({}, {id}, true, empty_borrower, empty_refs, &out);
  rc->UpdateFinishedTaskReferences({}, {id}, false, empty_borrower, empty_refs, &out);
  // The dependency is no longer in scope, but we still keep a reference to it
  // because it is in the lineage of the retryable task.
  ASSERT_EQ(out.size(), 1);
  ASSERT_TRUE(rc->HasReference(id));

  // Simulate retrying the task.
  rc->UpdateResubmittedTaskReferences({}, {id});
  rc->UpdateFinishedTaskReferences({}, {id}, true, empty_borrower, empty_refs, &out);
  ASSERT_FALSE(rc->HasReference(id));
  ASSERT_EQ(lineage_deleted.size(), 1);
}

TEST_F(ReferenceCountLineageEnabledTest, TestPlasmaLocation) {
  auto deleted = std::make_shared<std::unordered_set<ObjectID>>();
  auto callback = [&](const ObjectID &object_id) { deleted->insert(object_id); };

  ObjectID borrowed_id = ObjectID::FromRandom();
  rc->AddLocalReference(borrowed_id, "");
  bool owned_by_us = false;
  NodeID pinned_at;
  bool spilled = false;
  ASSERT_TRUE(
      rc->IsPlasmaObjectPinnedOrSpilled(borrowed_id, &owned_by_us, &pinned_at, &spilled));
  ASSERT_FALSE(owned_by_us);

  ObjectID id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();
  rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, true, /*add_local_ref=*/true);
  ASSERT_TRUE(rc->SetDeleteCallback(id, callback));
  ASSERT_TRUE(rc->IsPlasmaObjectPinnedOrSpilled(id, &owned_by_us, &pinned_at, &spilled));
  ASSERT_TRUE(owned_by_us);
  ASSERT_TRUE(pinned_at.IsNil());
  rc->UpdateObjectPinnedAtRaylet(id, node_id);
  ASSERT_TRUE(rc->IsPlasmaObjectPinnedOrSpilled(id, &owned_by_us, &pinned_at, &spilled));
  ASSERT_TRUE(owned_by_us);
  ASSERT_FALSE(pinned_at.IsNil());
  ASSERT_TRUE(rc->GetObjectLocations(id)->empty());

  rc->RemoveLocalReference(id, nullptr);
  ASSERT_FALSE(rc->IsPlasmaObjectPinnedOrSpilled(id, &owned_by_us, &pinned_at, &spilled));
  ASSERT_TRUE(deleted->count(id) > 0);
  deleted->clear();

  rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, true, /*add_local_ref=*/true);
  ASSERT_TRUE(rc->SetDeleteCallback(id, callback));
  rc->UpdateObjectPinnedAtRaylet(id, node_id);
  rc->ResetObjectsOnRemovedNode(node_id);
  auto objects = rc->FlushObjectsToRecover();
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(objects[0], id);
  ASSERT_TRUE(rc->IsPlasmaObjectPinnedOrSpilled(id, &owned_by_us, &pinned_at, &spilled));
  ASSERT_TRUE(owned_by_us);
  ASSERT_TRUE(pinned_at.IsNil());
  ASSERT_TRUE(deleted->count(id) > 0);
  deleted->clear();
}

TEST_F(ReferenceCountTest, TestFree) {
  auto deleted = std::make_shared<std::unordered_set<ObjectID>>();
  auto callback = [&](const ObjectID &object_id) { deleted->insert(object_id); };

  ObjectID id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();

  // Test free before receiving information about where the object is pinned.
  rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, true, /*add_local_ref=*/true);
  ASSERT_FALSE(rc->IsPlasmaObjectFreed(id));
  rc->FreePlasmaObjects({id});
  ASSERT_TRUE(rc->IsPlasmaObjectFreed(id));
  ASSERT_FALSE(rc->SetDeleteCallback(id, callback));
  ASSERT_EQ(deleted->count(id), 0);
  rc->UpdateObjectPinnedAtRaylet(id, node_id);
  bool owned_by_us;
  NodeID pinned_at;
  bool spilled;
  ASSERT_TRUE(rc->IsPlasmaObjectPinnedOrSpilled(id, &owned_by_us, &pinned_at, &spilled));
  ASSERT_TRUE(owned_by_us);
  ASSERT_TRUE(pinned_at.IsNil());
  ASSERT_TRUE(rc->IsPlasmaObjectFreed(id));
  rc->RemoveLocalReference(id, nullptr);
  ASSERT_FALSE(rc->IsPlasmaObjectFreed(id));

  // Test free after receiving information about where the object is pinned.
  rc->AddOwnedObject(id, {}, rpc::Address(), "", 0, true, /*add_local_ref=*/true);
  ASSERT_TRUE(rc->SetDeleteCallback(id, callback));
  rc->UpdateObjectPinnedAtRaylet(id, node_id);
  ASSERT_FALSE(rc->IsPlasmaObjectFreed(id));
  rc->FreePlasmaObjects({id});
  ASSERT_TRUE(rc->IsPlasmaObjectFreed(id));
  ASSERT_TRUE(deleted->count(id) > 0);
  ASSERT_TRUE(rc->IsPlasmaObjectPinnedOrSpilled(id, &owned_by_us, &pinned_at, &spilled));
  ASSERT_TRUE(owned_by_us);
  ASSERT_TRUE(pinned_at.IsNil());
  rc->RemoveLocalReference(id, nullptr);
  ASSERT_FALSE(rc->IsPlasmaObjectFreed(id));
}

TEST_F(ReferenceCountTest, TestGetObjectStatusReplyDelayed) {
  // https://github.com/ray-project/ray/issues/18557.
  // Check that we track an ObjectRef nested inside another borrowed ObjectRef.
  ObjectID outer_id = ObjectID::FromRandom();
  ObjectID inner_id = ObjectID::FromRandom();

  // We have a reference to the borrowed ObjectRef.
  rpc::Address owner_address(MockWorkerClient::CreateRandomAddress("1234"));
  rc->AddLocalReference(outer_id, "");
  rc->AddBorrowedObject(outer_id, ObjectID::Nil(), owner_address);
  ASSERT_TRUE(rc->HasReference(outer_id));
  // Task finishes and our local ref to the outer ObjectRef is deleted. We
  // return borrower information to the owner.
  ReferenceCounter::ReferenceTableProto refs_proto;
  rc->PopAndClearLocalBorrowers({outer_id}, &refs_proto, nullptr);
  ASSERT_FALSE(rc->HasReference(outer_id));
  // Future resolution is async, so we may receive information about the inner
  // ObjectRef after we deleted the outer ObjectRef. Check that we do not leak
  // the inner Reference info.
  rc->AddBorrowedObject(inner_id, outer_id, owner_address);
  ASSERT_FALSE(rc->HasReference(inner_id));

  // Now we do it again but the future is resolved while the outer ObjectRef is
  // still in scope.
  rc->AddLocalReference(outer_id, "");
  rc->AddBorrowedObject(outer_id, ObjectID::Nil(), owner_address);
  ASSERT_TRUE(rc->HasReference(outer_id));
  // Future is resolved and we receive information about the inner ObjectRef.
  // This time we keep the Reference information.
  rc->AddBorrowedObject(inner_id, outer_id, owner_address);
  ASSERT_TRUE(rc->HasReference(inner_id));
  refs_proto.Clear();
  rc->PopAndClearLocalBorrowers({outer_id}, &refs_proto, nullptr);
  // Inner ObjectRef info gets popped with the outer ObjectRef.
  ASSERT_FALSE(rc->HasReference(outer_id));
  ASSERT_FALSE(rc->HasReference(inner_id));
}

TEST_F(ReferenceCountTest, TestDelayedWaitForRefRemoved) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // Owner owns a nested object ref, borrower is using the outer ObjectRef.
  ObjectID outer_id = ObjectID::FromRandom();
  ObjectID inner_id = ObjectID::FromRandom();
  owner->rc_.AddOwnedObject(outer_id,
                            {},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/false);
  owner->rc_.AddBorrowerAddress(outer_id, borrower->address_);
  owner->rc_.AddOwnedObject(inner_id,
                            {},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/true);
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  borrower->rc_.AddLocalReference(outer_id, "");
  borrower->rc_.AddBorrowedObject(outer_id, ObjectID::Nil(), owner->address_);
  // Borrower deserializes the inner ObjectRef.
  borrower->rc_.AddLocalReference(inner_id, "");
  borrower->rc_.AddBorrowedObject(inner_id, outer_id, owner->address_);
  ASSERT_TRUE(borrower->rc_.HasReference(outer_id));
  ASSERT_TRUE(borrower->rc_.HasReference(inner_id));

  // Borrower deletes the outer ObjectRef. Inner ObjectRef is still in scope.
  borrower->rc_.RemoveLocalReference(outer_id, nullptr);
  // WaitForRefRemoved RPC from owner arrives after outer object ref has been deleted.
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Inner ObjectRef is still in scope because the borrower is still using it.
  owner->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Delete all refs to the inner ObjectRef.
  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

TEST_F(ReferenceCountTest, TestRepeatedDeserialization) {
  auto borrower = std::make_shared<MockWorkerClient>("1");
  auto owner = std::make_shared<MockWorkerClient>(
      "2", [&](const rpc::Address &addr) { return borrower; });

  // Owner owns a nested object ref, borrower is using the outer ObjectRef.
  ObjectID outer_id = ObjectID::FromRandom();
  ObjectID middle_id = ObjectID::FromRandom();
  ObjectID inner_id = ObjectID::FromRandom();
  owner->rc_.AddOwnedObject(inner_id,
                            {},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/false);
  owner->rc_.AddOwnedObject(middle_id,
                            {inner_id},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/false);
  owner->rc_.AddOwnedObject(outer_id,
                            {middle_id},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/false);
  owner->rc_.AddBorrowerAddress(outer_id, borrower->address_);
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(middle_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  borrower->rc_.AddLocalReference(outer_id, "");
  borrower->rc_.AddBorrowedObject(outer_id, ObjectID::Nil(), owner->address_);
  borrower->rc_.AddLocalReference(middle_id, "");
  borrower->rc_.AddBorrowedObject(middle_id, outer_id, owner->address_);
  // Borrower receives the inlined inner ObjectRef.
  // This also simulates the case where the borrower deserializes the inner
  // ObjectRef, then deletes it.
  borrower->rc_.AddBorrowedObject(inner_id, middle_id, owner->address_);

  borrower->rc_.RemoveLocalReference(outer_id, nullptr);
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(middle_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Borrower deserializes the inner ObjectRef.
  borrower->rc_.AddLocalReference(inner_id, "");
  borrower->rc_.RemoveLocalReference(middle_id, nullptr);
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(middle_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  borrower->rc_.RemoveLocalReference(inner_id, nullptr);
  ASSERT_TRUE(borrower->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(inner_id));
}

// Matches test_reference_counting_2.py::test_forward_nested_ref.
TEST_F(ReferenceCountTest, TestForwardNestedRefs) {
  auto borrower1 = std::make_shared<MockWorkerClient>("1");
  auto borrower2 = std::make_shared<MockWorkerClient>("2");
  bool first_borrower = true;
  auto owner = std::make_shared<MockWorkerClient>("2", [&](const rpc::Address &addr) {
    return first_borrower ? borrower1 : borrower2;
  });

  // Owner owns a nested object ref, borrower1 is using the outer ObjectRef.
  ObjectID outer_id = ObjectID::FromRandom();
  ObjectID middle_id = ObjectID::FromRandom();
  ObjectID inner_id = ObjectID::FromRandom();
  owner->rc_.AddOwnedObject(inner_id,
                            {},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/false);
  owner->rc_.AddOwnedObject(middle_id,
                            {inner_id},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/false);
  owner->rc_.AddOwnedObject(outer_id,
                            {middle_id},
                            owner->address_,
                            "",
                            0,
                            false,
                            /*add_local_ref=*/false);
  owner->rc_.AddBorrowerAddress(outer_id, borrower1->address_);
  ASSERT_TRUE(owner->rc_.HasReference(outer_id));
  ASSERT_TRUE(owner->rc_.HasReference(middle_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  // Borrower 1 forwards the ObjectRef to borrower 2 via task submission.
  borrower1->rc_.AddLocalReference(outer_id, "");
  borrower1->rc_.AddBorrowedObject(outer_id, ObjectID::Nil(), owner->address_);
  auto return_id = borrower1->SubmitTaskWithArg(outer_id);

  // Borrower 2 executes the task, keeps ref to inner ref.
  borrower2->ExecuteTaskWithArg(outer_id, middle_id, owner->address_);
  borrower2->GetSerializedObjectId(middle_id, inner_id, owner->address_);
  borrower2->rc_.RemoveLocalReference(middle_id, nullptr);
  auto borrower_refs = borrower2->FinishExecutingTask(outer_id, ObjectID::Nil());
  borrower1->HandleSubmittedTaskFinished(
      return_id, outer_id, {}, borrower2->address_, borrower_refs);
  borrower1->rc_.RemoveLocalReference(outer_id, nullptr);

  // Now the owner should contact borrower 2.
  first_borrower = false;
  ASSERT_TRUE(borrower1->FlushBorrowerCallbacks());
  ASSERT_FALSE(owner->rc_.HasReference(outer_id));
  ASSERT_FALSE(owner->rc_.HasReference(middle_id));
  ASSERT_TRUE(owner->rc_.HasReference(inner_id));

  ASSERT_TRUE(borrower2->FlushBorrowerCallbacks());
  borrower2->rc_.RemoveLocalReference(inner_id, nullptr);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
