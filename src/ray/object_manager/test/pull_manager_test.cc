// Copyright 2020-2021 The Ray Authors.
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

#include "ray/object_manager/pull_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/common_protocol.h"
#include "ray/common/test_util.h"

namespace ray {

using ::testing::ElementsAre;

class PullManagerTestWithCapacity {
 public:
  PullManagerTestWithCapacity(size_t num_available_bytes)
      : self_node_id_(NodeID::FromRandom()),
        object_is_local_(false),
        num_send_pull_request_calls_(0),
        num_restore_spilled_object_calls_(0),
        fake_time_(0),
        pull_manager_(
            self_node_id_,
            [this](const ObjectID &object_id) { return object_is_local_; },
            [this](const ObjectID &object_id, const NodeID &node_id) {
              num_send_pull_request_calls_++;
            },
            [this](const ObjectID &object_id) { num_abort_calls_[object_id]++; },
            [this](const ObjectID &object_id) { timed_out_objects_.insert(object_id); },
            [this](const ObjectID &,
                   int64_t size,
                   const std::string &,
                   std::function<void(const ray::Status &)> callback) {
              num_restore_spilled_object_calls_++;
              restore_object_callback_ = callback;
            },
            [this]() { return fake_time_; },
            10000,
            num_available_bytes,
            [this](const ObjectID &object_id) { return PinReturn(); },
            [this](const ObjectID &object_id) {
              return GetLocalSpilledObjectURL(object_id);
            }) {}

  void AssertNoLeaks() {
    ASSERT_TRUE(pull_manager_.get_request_bundles_.Empty());
    ASSERT_TRUE(pull_manager_.wait_request_bundles_.Empty());
    ASSERT_TRUE(pull_manager_.task_argument_bundles_.Empty());
    ASSERT_EQ(pull_manager_.num_active_bundles_, 0);
    ASSERT_TRUE(pull_manager_.object_pull_requests_.empty());
    absl::MutexLock lock(&pull_manager_.active_objects_mu_);
    ASSERT_TRUE(pull_manager_.active_object_pull_requests_.empty());
    ASSERT_TRUE(pull_manager_.pinned_objects_.empty());
    ASSERT_EQ(pull_manager_.pinned_objects_size_, 0);
    // Most tests should not timeout any pull requests.
    ASSERT_TRUE(timed_out_objects_.empty());
  }

  int NumPinnedObjects() { return pull_manager_.pinned_objects_.size(); }

  std::unique_ptr<RayObject> PinReturn() {
    if (allow_pin_) {
      return std::make_unique<RayObject>(rpc::ErrorType::OBJECT_IN_PLASMA);
    } else {
      return nullptr;
    }
  }

  std::string GetLocalSpilledObjectURL(const ObjectID &oid) { return spilled_url_[oid]; }

  void ObjectSpilled(const ObjectID &oid, std::string url) { spilled_url_[oid] = url; }

  NodeID self_node_id_;
  bool object_is_local_;
  bool allow_pin_ = false;
  int num_send_pull_request_calls_;
  int num_restore_spilled_object_calls_;
  std::function<void(const ray::Status &)> restore_object_callback_;
  double fake_time_;
  PullManager pull_manager_;
  absl::flat_hash_map<ObjectID, int> num_abort_calls_;
  absl::flat_hash_map<ObjectID, std::string> spilled_url_;
  std::unordered_set<ObjectID> timed_out_objects_;
};

class PullManagerTest : public PullManagerTestWithCapacity,
                        public ::testing::TestWithParam<BundlePriority> {
 public:
  PullManagerTest() : PullManagerTestWithCapacity(1) {}

  void AssertNumActiveRequestsEquals(int64_t num_requests) {
    absl::MutexLock lock(&pull_manager_.active_objects_mu_);
    ASSERT_EQ(pull_manager_.object_pull_requests_.size(), num_requests);
    ASSERT_EQ(pull_manager_.active_object_pull_requests_.size(), num_requests);
  }

  int64_t NumBytesBeingPulled() { return pull_manager_.num_bytes_being_pulled_; }
};

class PullManagerWithAdmissionControlTest
    : public PullManagerTestWithCapacity,
      public ::testing::TestWithParam<BundlePriority> {
 public:
  PullManagerWithAdmissionControlTest() : PullManagerTestWithCapacity(10) {}

  void AssertNumActiveRequestsEquals(int64_t num_requests) {
    absl::MutexLock lock(&pull_manager_.active_objects_mu_);
    ASSERT_EQ(pull_manager_.active_object_pull_requests_.size(), num_requests);
  }

  void AssertNumActiveBundlesEquals(int64_t num_bundles) {
    ASSERT_EQ(pull_manager_.num_active_bundles_, num_bundles);
  }

  bool IsUnderCapacity(int64_t num_bytes_requested) {
    return num_bytes_requested <= pull_manager_.num_bytes_available_;
  }
};

std::vector<rpc::ObjectReference> CreateObjectRefs(int num_objs) {
  std::vector<rpc::ObjectReference> refs;
  for (int i = 0; i < num_objs; i++) {
    ObjectID obj = ObjectID::FromRandom();
    rpc::ObjectReference ref;
    ref.set_object_id(obj.Binary());
    refs.push_back(ref);
  }
  return refs;
}

TEST_P(PullManagerTest, TestStaleSubscription) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto oid = ObjectRefsToIds(refs)[0];
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(oid, client_ids, "", NodeID::Nil(), false, 0);
  AssertNumActiveRequestsEquals(1);

  // There are no client ids to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_TRUE(num_abort_calls_.empty());

  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));

  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  AssertNumActiveRequestsEquals(0);

  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(oid, client_ids, "", NodeID::Nil(), false, 0);

  // Now we're getting a notification about an object that was already cancelled.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_EQ(num_abort_calls_[oid], 1);

  AssertNoLeaks();
}

TEST_P(PullManagerWithAdmissionControlTest, TestPullObjectPendingCreation) {
  BundlePriority prio = GetParam();
  if (GetParam() == BundlePriority::GET_REQUEST) {
    // Get requests are unlimited.
    return;
  }

  auto refs_1 = CreateObjectRefs(1);
  auto refs_2 = CreateObjectRefs(1);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id_1 = pull_manager_.Pull(refs_1, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs_1));
  objects_to_locate.clear();
  auto req_id_2 = pull_manager_.Pull(refs_2, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs_2));
  AssertNumActiveRequestsEquals(0);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(
      ObjectRefToId(refs_1[0]), client_ids, "", NodeID::Nil(), false, 6);
  AssertNumActiveRequestsEquals(1);
  AssertNumActiveBundlesEquals(1);
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_TRUE(pull_manager_.IsObjectActive(ObjectRefToId(refs_1[0])));

  // The second request won't be activated since available memory is only 10.
  pull_manager_.OnLocationChange(
      ObjectRefToId(refs_2[0]), client_ids, "", NodeID::Nil(), false, 6);
  AssertNumActiveRequestsEquals(1);
  AssertNumActiveBundlesEquals(1);
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_FALSE(pull_manager_.IsObjectActive(ObjectRefToId(refs_2[0])));

  // The first object is lost and pending creation.
  // In this case, the first request will be deactivated and the second request will be
  // activated.
  pull_manager_.OnLocationChange(
      ObjectRefToId(refs_1[0]), {}, "", NodeID::Nil(), true, 6);
  AssertNumActiveRequestsEquals(1);
  AssertNumActiveBundlesEquals(1);
  ASSERT_EQ(num_send_pull_request_calls_, 2);
  ASSERT_EQ(num_abort_calls_[ObjectRefToId(refs_1[0])], 1);
  ASSERT_FALSE(pull_manager_.IsObjectActive(ObjectRefToId(refs_1[0])));
  ASSERT_TRUE(pull_manager_.IsObjectActive(ObjectRefToId(refs_2[0])));

  // The first object is recreated but the pull request will be inactive since there is no
  // available memory.
  pull_manager_.OnLocationChange(
      ObjectRefToId(refs_1[0]), client_ids, "", NodeID::Nil(), false, 6);
  AssertNumActiveRequestsEquals(1);
  AssertNumActiveBundlesEquals(1);
  ASSERT_EQ(num_send_pull_request_calls_, 2);
  ASSERT_EQ(num_abort_calls_[ObjectRefToId(refs_1[0])], 1);
  ASSERT_FALSE(pull_manager_.IsObjectActive(ObjectRefToId(refs_1[0])));
  ASSERT_TRUE(pull_manager_.IsObjectActive(ObjectRefToId(refs_2[0])));

  pull_manager_.CancelPull(req_id_2);
  // The first request is active now due to the available memory freed by the second
  // request.
  AssertNumActiveRequestsEquals(1);
  AssertNumActiveBundlesEquals(1);
  ASSERT_EQ(num_send_pull_request_calls_, 3);
  ASSERT_EQ(num_abort_calls_[ObjectRefToId(refs_1[0])], 1);
  ASSERT_TRUE(pull_manager_.IsObjectActive(ObjectRefToId(refs_1[0])));

  pull_manager_.CancelPull(req_id_1);

  AssertNoLeaks();
}

TEST_P(PullManagerWithAdmissionControlTest, TestPullOrder) {
  BundlePriority prio = GetParam();

  auto refs_1 = CreateObjectRefs(1);
  auto refs_2 = CreateObjectRefs(1);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id_1 = pull_manager_.Pull(refs_1, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs_1));
  objects_to_locate.clear();
  auto req_id_2 = pull_manager_.Pull(refs_2, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs_2));
  AssertNumActiveRequestsEquals(0);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  // Second pull request gets the location first,
  // so it will be pulled before the first request.
  pull_manager_.OnLocationChange(
      ObjectRefToId(refs_2[0]), client_ids, "", NodeID::Nil(), false, 0);
  AssertNumActiveRequestsEquals(1);
  AssertNumActiveBundlesEquals(1);
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_TRUE(pull_manager_.IsObjectActive(ObjectRefToId(refs_2[0])));
  ASSERT_FALSE(pull_manager_.IsObjectActive(ObjectRefToId(refs_1[0])));

  pull_manager_.OnLocationChange(
      ObjectRefToId(refs_1[0]), client_ids, "", NodeID::Nil(), false, 0);
  AssertNumActiveRequestsEquals(2);
  AssertNumActiveBundlesEquals(2);
  ASSERT_EQ(num_send_pull_request_calls_, 2);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_TRUE(pull_manager_.IsObjectActive(ObjectRefToId(refs_2[0])));
  ASSERT_TRUE(pull_manager_.IsObjectActive(ObjectRefToId(refs_1[0])));

  objects_to_locate.clear();
  // All the locations are known so the pull request is active immediately.
  auto req_id_3 = pull_manager_.Pull(
      std::vector<rpc::ObjectReference>{refs_1[0], refs_2[0]}, prio, &objects_to_locate);
  ASSERT_TRUE(objects_to_locate.empty());
  AssertNumActiveBundlesEquals(3);
  ASSERT_EQ(num_send_pull_request_calls_, 2);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  pull_manager_.CancelPull(req_id_1);
  pull_manager_.CancelPull(req_id_2);
  pull_manager_.CancelPull(req_id_3);
  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestRestoreSpilledObjectRemote) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "", NodeID::Nil(), false, 0);

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  NodeID node_that_object_spilled = NodeID::FromRandom();
  fake_time_ += 10.;
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", node_that_object_spilled, false, 0);

  // We request a remote pull to restore the object.
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // No retry yet.
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", node_that_object_spilled, false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // The call can be retried after a delay.
  client_ids.insert(node_that_object_spilled);
  fake_time_ += 10.;
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", node_that_object_spilled, false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 2);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // Don't restore an object if it's local.
  object_is_local_ = true;
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", NodeID::FromRandom(), false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 2);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(num_abort_calls_[obj1], 1);

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestRestoreSpilledObjectLocal) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "", NodeID::Nil(), false, 0);

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  fake_time_ += 10.;
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", self_node_id_, false, 0);

  // We request a local restore.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  // No retry yet.
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", self_node_id_, false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  // The call can be retried after a delay.
  fake_time_ += 10.;
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", self_node_id_, false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(num_abort_calls_[obj1], 1);

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestRestoreSpilledObjectOnLocalStorage) {
  /// Test the scneario where the object is spilled to local storage, like filesystems.
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "", NodeID::Nil(), false, 0);

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  fake_time_ += 10.;
  // Objects are spilled locally, but the remote object directory doesn't have the
  // information. It should still restore objects.
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(obj1, client_ids, "", self_node_id_, false, 0);

  // We request a local restore.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  // The call can be retried after a delay, and the url in the remote object directory is
  // updated now.
  fake_time_ += 10.;
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", self_node_id_, false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(num_abort_calls_[obj1], 1);

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestRestoreSpilledObjectOnExternalStorage) {
  /// Test the scneario where the object is spilled to external storages, such as S3.
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "", NodeID::Nil(), false, 0);

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  fake_time_ += 10.;
  // Objects are spilled to the empty URL locally if it is spilled to external storages.
  ObjectSpilled(obj1, "");
  // If objects are spilled to external storages, the node id should be Nil().
  // So this shouldn't invoke restoration.
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", self_node_id_, false, 0);

  // We request a local restore.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // Now Nil ID is properly updated.
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", NodeID::Nil(), false, 0);

  // We request a local restore.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  // The call can be retried after a delay.
  fake_time_ += 10.;
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", NodeID::Nil(), false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(num_abort_calls_[obj1], 1);

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestLoadBalancingRestorationRequest) {
  /* Make sure when the object copy is in other raylet, we pull object from there instead
   * of requesting the owner node to restore the object. */
  BundlePriority prio = GetParam();

  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumObjectPullRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumObjectPullRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  const auto copy_node1 = NodeID::FromRandom();
  const auto copy_node2 = NodeID::FromRandom();
  const auto remote_node_that_spilled_object = NodeID::FromRandom();
  client_ids.insert(copy_node1);
  client_ids.insert(copy_node2);
  ObjectSpilled(obj1, "remote_url/foo/bar");
  pull_manager_.OnLocationChange(
      obj1, client_ids, "remote_url/foo/bar", remote_node_that_spilled_object, false, 0);

  ASSERT_EQ(num_send_pull_request_calls_, 1);
  // Make sure the restore request wasn't sent since there are nodes that have a copied
  // object.
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_TRUE(num_abort_calls_.empty());
}

TEST_P(PullManagerTest, TestManyUpdates) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  for (int i = 0; i < 100; i++) {
    pull_manager_.OnLocationChange(obj1, client_ids, "", NodeID::Nil(), false, 0);
    AssertNumActiveRequestsEquals(1);
  }

  // Since no time has passed, only send a single pull request.
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(num_abort_calls_[obj1], 1);

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestRetryTimer) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  // We need to call OnLocationChange at least once, to population the list of nodes with
  // the object.
  pull_manager_.OnLocationChange(obj1, client_ids, "", NodeID::Nil(), false, 0);
  AssertNumActiveRequestsEquals(1);
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  for (; fake_time_ <= 7 * 10; fake_time_ += 1.) {
    pull_manager_.Tick();
  }

  // Location changes can trigger reset timer.
  for (; fake_time_ <= 120 * 10; fake_time_ += 1.) {
    pull_manager_.OnLocationChange(obj1, client_ids, "", NodeID::Nil(), false, 0);
  }

  // We should make a pull request every tick (even if it's a duplicate to a node we're
  // already pulling from).
  ASSERT_EQ(num_send_pull_request_calls_, 7);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // Don't retry an object if it's local.
  object_is_local_ = true;
  num_send_pull_request_calls_ = 0;
  for (; fake_time_ <= 127 * 10; fake_time_ += 1.) {
    pull_manager_.Tick();
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);

  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(num_abort_calls_[obj1], 1);

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestBasic) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  ASSERT_TRUE(pull_manager_.HasPullsQueued());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    ASSERT_FALSE(pull_manager_.IsObjectActive(oids[i]));
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 0);
  }
  for (size_t i = 0; i < oids.size(); i++) {
    ASSERT_TRUE(pull_manager_.IsObjectActive(oids[i]));
  }
  ASSERT_EQ(num_send_pull_request_calls_, oids.size());
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  AssertNumActiveRequestsEquals(oids.size());
  ASSERT_FALSE(pull_manager_.HasPullsQueued());

  // Don't pull an object if it's local.
  object_is_local_ = true;
  num_send_pull_request_calls_ = 0;
  fake_time_ += 10;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 0);
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);

  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, oids);
  AssertNumActiveRequestsEquals(0);
  for (auto &oid : oids) {
    ASSERT_EQ(num_abort_calls_[oid], 1);
    ASSERT_FALSE(pull_manager_.IsObjectActive(oid));
  }
  ASSERT_FALSE(pull_manager_.HasPullsQueued());

  // Don't pull a remote object if we've canceled.
  object_is_local_ = false;
  num_send_pull_request_calls_ = 0;
  fake_time_ += 10;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 0);
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_FALSE(pull_manager_.HasPullsQueued());

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestPinActiveObjects) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    ASSERT_FALSE(pull_manager_.IsObjectActive(oids[i]));
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 1);
  }
  for (size_t i = 0; i < oids.size(); i++) {
    ASSERT_TRUE(pull_manager_.IsObjectActive(oids[i]));
  }
  ASSERT_EQ(num_send_pull_request_calls_, oids.size());
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  AssertNumActiveRequestsEquals(oids.size());
  pull_manager_.UpdatePullsBasedOnAvailableMemory(4);

  // Check we pin objects belonging to active bundles.
  allow_pin_ = true;
  ASSERT_EQ(NumPinnedObjects(), 0);
  ASSERT_EQ(pull_manager_.RemainingQuota(), 1);
  pull_manager_.PinNewObjectIfNeeded(oids[0]);
  // Now we have more space (object manager should also report more space used),
  // so remaining quota would go back to 1 on the avail memory report.
  ASSERT_EQ(pull_manager_.RemainingQuota(), 2);
  ASSERT_EQ(NumPinnedObjects(), 1);
  pull_manager_.PinNewObjectIfNeeded(oids[0]);
  ASSERT_EQ(NumPinnedObjects(), 1);

  // Check do not pin objects belonging to inactive bundles.
  auto refs2 = CreateObjectRefs(1);
  auto oids2 = ObjectRefsToIds(refs2);
  auto req_id2 = pull_manager_.Pull(refs2, BundlePriority::TASK_ARGS, &objects_to_locate);
  for (size_t i = 0; i < oids2.size(); i++) {
    ASSERT_FALSE(pull_manager_.IsObjectActive(oids2[i]));
    pull_manager_.OnLocationChange(oids2[i], client_ids, "", NodeID::Nil(), false, 1000);
    ASSERT_FALSE(pull_manager_.IsObjectActive(oids2[i]));
  }
  pull_manager_.UpdatePullsBasedOnAvailableMemory(4);
  pull_manager_.PinNewObjectIfNeeded(oids2[0]);
  ASSERT_EQ(NumPinnedObjects(), 1);

  // The object is unpinned on cancel.
  pull_manager_.CancelPull(req_id);
  pull_manager_.CancelPull(req_id2);
  ASSERT_EQ(NumPinnedObjects(), 0);
  ASSERT_EQ(pull_manager_.RemainingQuota(), 4);

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestDeduplicateBundles) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id1 = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);

  objects_to_locate.clear();
  auto req_id2 = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_TRUE(objects_to_locate.empty());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    ASSERT_FALSE(pull_manager_.IsObjectActive(oids[i]));
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 0);
  }
  ASSERT_EQ(num_send_pull_request_calls_, oids.size());
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  AssertNumActiveRequestsEquals(oids.size());

  // Cancel one request.
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id1));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id1);
  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(objects_to_cancel.empty());
  // Objects should still be pulled because the other request is still open.
  AssertNumActiveRequestsEquals(oids.size());
  fake_time_ += 10;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 0);
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 0);
    ASSERT_EQ(num_send_pull_request_calls_, i + 1);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
    ASSERT_TRUE(pull_manager_.IsObjectActive(oids[i]));
  }

  // Cancel the other request.
  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id2));
  objects_to_cancel = pull_manager_.CancelPull(req_id2);
  ASSERT_EQ(objects_to_cancel, oids);
  AssertNumActiveRequestsEquals(0);
  for (auto &oid : oids) {
    ASSERT_FALSE(pull_manager_.IsObjectActive(oid));
    ASSERT_EQ(num_abort_calls_[oid], 1);
  }

  // Don't pull a remote object if we've canceled.
  object_is_local_ = false;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "", NodeID::Nil(), false, 0);
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);

  AssertNoLeaks();
}

// https://github.com/ray-project/ray/issues/15990
TEST_P(PullManagerTest, TestDuplicateObjectsInDuplicateRequests) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(2);
  // Duplicate an object id in the pull request.
  refs.push_back(refs[0]);
  auto oids = ObjectRefsToIds(refs);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id1 = pull_manager_.Pull(refs, prio, &objects_to_locate);
  // One object is duplicate, so there are only two requests total.
  objects_to_locate.clear();
  auto req_id2 = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_TRUE(objects_to_locate.empty());

  // Cancel one request. It should not check fail.
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id1));
  auto objects_to_cancel = pull_manager_.CancelPull(req_id1);
  ASSERT_TRUE(num_abort_calls_.empty());
  ASSERT_TRUE(objects_to_cancel.empty());

  // Cancel the remaining request.
  auto objects_to_cancel2 = pull_manager_.CancelPull(req_id2);
  ASSERT_EQ(objects_to_cancel2.size(), 2);
  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestDuplicateObjectsAreActivatedAndCleanedUp) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  // Duplicate an object id in the pull request.
  refs.push_back(refs[0]);
  auto oids = ObjectRefsToIds(refs);
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(oids[0], client_ids, "", NodeID::Nil(), false, 0);
  AssertNumActiveRequestsEquals(1);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  AssertNumActiveRequestsEquals(0);
  ASSERT_EQ(objects_to_cancel.size(), 1);
  AssertNoLeaks();
}

TEST_P(PullManagerWithAdmissionControlTest, TestBasic) {
  BundlePriority prio = GetParam();
  /// Test admission control for a single pull bundle request. We should
  /// activate the request when we are under the reported capacity and
  /// deactivate it when we are over.
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  size_t object_size = 2;
  AssertNumActiveRequestsEquals(0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  ASSERT_TRUE(pull_manager_.HasPullsQueued());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    ASSERT_FALSE(pull_manager_.IsObjectActive(oids[i]));
    pull_manager_.OnLocationChange(
        oids[i], client_ids, "", NodeID::Nil(), false, object_size);
  }
  ASSERT_EQ(num_send_pull_request_calls_, oids.size());
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  AssertNumActiveRequestsEquals(oids.size());
  ASSERT_TRUE(IsUnderCapacity(oids.size() * object_size));
  for (size_t i = 0; i < oids.size(); i++) {
    ASSERT_TRUE(pull_manager_.IsObjectActive(oids[i]));
  }
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));
  ASSERT_FALSE(pull_manager_.HasPullsQueued());

  // Reduce the available memory.
  ASSERT_TRUE(num_abort_calls_.empty());
  pull_manager_.UpdatePullsBasedOnAvailableMemory(oids.size() * object_size - 1);

  // In unlimited mode, we fulfill all ray.gets using the fallback allocator.
  if (GetParam() == BundlePriority::GET_REQUEST) {
    ASSERT_FALSE(pull_manager_.HasPullsQueued());
    AssertNumActiveRequestsEquals(3);
    AssertNumActiveBundlesEquals(1);
    return;
  }

  ASSERT_FALSE(pull_manager_.HasPullsQueued());
  AssertNumActiveRequestsEquals(3);
  for (auto &oid : oids) {
    ASSERT_TRUE(pull_manager_.IsObjectActive(oid));
    ASSERT_EQ(num_abort_calls_[oid], 0);
  }
  ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_id));

  pull_manager_.CancelPull(req_id);
  for (auto &oid : oids) {
    ASSERT_FALSE(pull_manager_.IsObjectActive(oid));
  }
  AssertNoLeaks();
}

TEST_P(PullManagerWithAdmissionControlTest, TestQueue) {
  BundlePriority prio = GetParam();
  /// Test admission control for a queue of pull bundle requests. We should
  /// activate as many requests as we can, subject to the reported capacity.
  int object_size = 2;
  int num_oids_per_request = 2;
  int num_requests = 3;

  std::vector<std::vector<ObjectID>> bundles;
  std::vector<int64_t> req_ids;
  for (int i = 0; i < num_requests; i++) {
    auto refs = CreateObjectRefs(num_oids_per_request);
    auto oids = ObjectRefsToIds(refs);
    std::vector<rpc::ObjectReference> objects_to_locate;
    auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
    ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);

    bundles.push_back(oids);
    req_ids.push_back(req_id);
  }

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (auto &oids : bundles) {
    for (size_t i = 0; i < oids.size(); i++) {
      pull_manager_.OnLocationChange(
          oids[i], client_ids, "", NodeID::Nil(), false, object_size);
    }
  }

  for (int capacity = 0; capacity < 20; capacity++) {
    int num_requests_quota =
        std::min(num_requests, capacity / (object_size * num_oids_per_request));
    int num_requests_expected = std::max(1, num_requests_quota);
    if (GetParam() == BundlePriority::GET_REQUEST) {
      num_requests_expected = num_requests;
    }
    pull_manager_.UpdatePullsBasedOnAvailableMemory(capacity);

    AssertNumActiveRequestsEquals(num_requests_expected * num_oids_per_request);
    // This is the maximum number of requests that can be served at once that
    // is under the capacity.
    if (num_requests_expected < num_requests) {
      ASSERT_FALSE(IsUnderCapacity((num_requests_expected + 1) * num_oids_per_request *
                                   object_size));
    }
    // Check that OOM was triggered.
    for (size_t i = 0; i < req_ids.size(); i++) {
      if ((int)i < num_requests_expected) {
        ASSERT_TRUE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_ids[i]));
      } else {
        ASSERT_FALSE(pull_manager_.PullRequestActiveOrWaitingForMetadata(req_ids[i]));
      }
    }
  }

  for (auto req_id : req_ids) {
    pull_manager_.CancelPull(req_id);
  }
  AssertNoLeaks();
}

TEST_P(PullManagerWithAdmissionControlTest, TestCancel) {
  BundlePriority prio = GetParam();
  if (GetParam() == BundlePriority::GET_REQUEST) {
    return;  // This case isn't meaningful to test.
  }
  /// Test admission control while requests are cancelled out-of-order. When an
  /// active request is cancelled, we should activate another request in the
  /// queue, if there is one that satisfies the reported capacity.
  auto test_cancel = [&](std::vector<int> object_sizes,
                         int capacity,
                         size_t cancel_idx,
                         int num_active_requests_expected_before,
                         int num_active_requests_expected_after) {
    pull_manager_.UpdatePullsBasedOnAvailableMemory(capacity);
    auto refs = CreateObjectRefs(object_sizes.size());
    auto oids = ObjectRefsToIds(refs);
    std::vector<int64_t> req_ids;
    for (auto &ref : refs) {
      std::vector<rpc::ObjectReference> objects_to_locate;
      auto req_id = pull_manager_.Pull({ref}, prio, &objects_to_locate);
      req_ids.push_back(req_id);
    }
    for (size_t i = 0; i < object_sizes.size(); i++) {
      pull_manager_.OnLocationChange(
          oids[i], {}, "", NodeID::Nil(), false, object_sizes[i]);
    }
    AssertNumActiveRequestsEquals(num_active_requests_expected_before);
    pull_manager_.CancelPull(req_ids[cancel_idx]);
    AssertNumActiveRequestsEquals(num_active_requests_expected_after);

    // Request is really canceled.
    pull_manager_.OnLocationChange(oids[cancel_idx],
                                   {NodeID::FromRandom()},
                                   "",
                                   NodeID::Nil(),
                                   false,
                                   object_sizes[cancel_idx]);
    ASSERT_EQ(num_send_pull_request_calls_, 0);

    // The expected number of requests at the head of the queue are pulled.
    int num_active = 0;
    for (size_t i = 0; i < refs.size() && num_active < num_active_requests_expected_after;
         i++) {
      pull_manager_.OnLocationChange(
          oids[i], {NodeID::FromRandom()}, "", NodeID::Nil(), false, object_sizes[i]);
      if (i != cancel_idx) {
        num_active++;
      }
    }
    ASSERT_EQ(num_send_pull_request_calls_, num_active_requests_expected_after);

    // Reset state.
    for (size_t i = 0; i < req_ids.size(); i++) {
      if (i != cancel_idx) {
        pull_manager_.CancelPull(req_ids[i]);
      }
    }
    num_send_pull_request_calls_ = 0;
  };

  // The next request in the queue is infeasible. If it is canceled, the
  // request after that is activated.
  test_cancel({1, 1, 2, 1}, 3, 2, 2, 3);

  // If an activated request is canceled, the next request is activated.
  test_cancel({1, 1, 2, 1}, 3, 0, 2, 2);
  test_cancel({1, 1, 2, 1}, 3, 1, 2, 2);

  // Cancellation of requests at the end of the queue has no effect.
  test_cancel({1, 1, 2, 1, 1}, 3, 3, 2, 2);

  // As many new requests as possible are activated when one is canceled.
  test_cancel({1, 2, 1, 1, 1}, 3, 1, 2, 3);

  AssertNoLeaks();
}

TEST_F(PullManagerWithAdmissionControlTest, TestPrioritizeWorkerRequests) {
  /// Test prioritizing worker requests over task argument requests during
  /// admission control, and gets over waits.
  int object_size = 2;
  std::vector<ObjectID> task_oids;
  std::vector<ObjectID> get_oids;
  std::vector<ObjectID> wait_oids;

  // First submit two task args requests that can be pulled at the same time.
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto refs = CreateObjectRefs(1);
  auto task_req_id1 =
      pull_manager_.Pull(refs, BundlePriority::TASK_ARGS, &objects_to_locate);
  task_oids.push_back(ObjectRefsToIds(refs)[0]);

  refs = CreateObjectRefs(1);
  auto task_req_id2 =
      pull_manager_.Pull(refs, BundlePriority::TASK_ARGS, &objects_to_locate);
  task_oids.push_back(ObjectRefsToIds(refs)[0]);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (auto &oid : task_oids) {
    pull_manager_.OnLocationChange(
        oid, client_ids, "", NodeID::Nil(), false, object_size);
  }

  // Two requests can be pulled at a time.
  pull_manager_.UpdatePullsBasedOnAvailableMemory(5);
  AssertNumActiveRequestsEquals(2);
  ASSERT_TRUE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(task_oids[1]));

  // A wait request comes in. It takes priority over the task requests.
  refs = CreateObjectRefs(1);
  auto wait_req_id =
      pull_manager_.Pull(refs, BundlePriority::WAIT_REQUEST, &objects_to_locate);
  wait_oids.push_back(ObjectRefsToIds(refs)[0]);
  pull_manager_.OnLocationChange(
      wait_oids[0], client_ids, "", NodeID::Nil(), false, object_size);
  AssertNumActiveRequestsEquals(2);
  ASSERT_TRUE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));

  // A worker request comes in.
  refs = CreateObjectRefs(1);
  auto get_req_id1 =
      pull_manager_.Pull(refs, BundlePriority::GET_REQUEST, &objects_to_locate);
  get_oids.push_back(ObjectRefsToIds(refs)[0]);
  // Nothing has changed yet because the size information for the worker's
  // request is not available.
  AssertNumActiveRequestsEquals(2);
  ASSERT_TRUE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));
  // Worker request takes priority over the wait and task requests once its size is
  // available.
  for (auto &oid : get_oids) {
    pull_manager_.OnLocationChange(
        oid, client_ids, "", NodeID::Nil(), false, object_size);
  }
  AssertNumActiveRequestsEquals(2);
  ASSERT_TRUE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));

  // Another worker request comes in. It takes priority over the wait request
  // once its size is available.
  refs = CreateObjectRefs(1);
  auto get_req_id2 =
      pull_manager_.Pull(refs, BundlePriority::GET_REQUEST, &objects_to_locate);
  get_oids.push_back(ObjectRefsToIds(refs)[0]);
  AssertNumActiveRequestsEquals(2);
  ASSERT_TRUE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));
  for (auto &oid : get_oids) {
    pull_manager_.OnLocationChange(
        oid, client_ids, "", NodeID::Nil(), false, object_size);
  }
  AssertNumActiveRequestsEquals(2);
  ASSERT_TRUE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(get_oids[1]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));

  // Only 1 request can run at a time. We should prioritize between requests of
  // the same type by FIFO order.
  pull_manager_.UpdatePullsBasedOnAvailableMemory(2);
  ASSERT_TRUE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(get_oids[1]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));

  pull_manager_.CancelPull(get_req_id1);
  ASSERT_FALSE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(get_oids[1]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));

  pull_manager_.CancelPull(get_req_id2);
  ASSERT_FALSE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(get_oids[1]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));

  pull_manager_.CancelPull(wait_req_id);
  ASSERT_FALSE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(get_oids[1]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[1]));

  pull_manager_.CancelPull(task_req_id1);
  ASSERT_FALSE(pull_manager_.IsObjectActive(get_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(get_oids[1]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(wait_oids[0]));
  ASSERT_FALSE(pull_manager_.IsObjectActive(task_oids[0]));
  ASSERT_TRUE(pull_manager_.IsObjectActive(task_oids[1]));

  pull_manager_.CancelPull(task_req_id2);
  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestTimeOut) {
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto oids = ObjectRefsToIds(refs);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  ASSERT_TRUE(pull_manager_.HasPullsQueued());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(oids[0], client_ids, "", NodeID::Nil(), false, 0);
  ASSERT_TRUE(pull_manager_.IsObjectActive(oids[0]));
  ASSERT_EQ(num_send_pull_request_calls_, 1);

  // Object has no locations now.
  fake_time_ += 10;
  pull_manager_.OnLocationChange(oids[0], {}, "", NodeID::Nil(), false, 0);
  ASSERT_FALSE(timed_out_objects_.count(oids[0]));
  fake_time_ += 601;
  pull_manager_.Tick();
  ASSERT_TRUE(timed_out_objects_.count(oids[0]));
  timed_out_objects_.clear();
  RAY_UNUSED(pull_manager_.CancelPull(req_id));

  req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  pull_manager_.OnLocationChange(oids[0], client_ids, "", NodeID::Nil(), false, 0);
  ASSERT_TRUE(pull_manager_.IsObjectActive(oids[0]));
  // Object has no locations now but it is pending execution.
  fake_time_ += 10;
  pull_manager_.OnLocationChange(oids[0], {}, "", NodeID::Nil(), true, 0);
  ASSERT_FALSE(timed_out_objects_.count(oids[0]));
  fake_time_ += 601;
  pull_manager_.Tick();
  ASSERT_FALSE(timed_out_objects_.count(oids[0]));
  // Object has no locations now and is no longer pending execution.
  pull_manager_.OnLocationChange(oids[0], {}, "", NodeID::Nil(), false, 0);
  fake_time_ += 601;
  pull_manager_.Tick();
  ASSERT_TRUE(timed_out_objects_.count(oids[0]));
  timed_out_objects_.clear();
  RAY_UNUSED(pull_manager_.CancelPull(req_id));

  AssertNoLeaks();
}

TEST_P(PullManagerTest, TestTimeOutAfterFailedPull) {
  // Check that a successful Pull resets the timer for failed fetches.
  BundlePriority prio = GetParam();
  auto refs = CreateObjectRefs(1);
  auto oids = ObjectRefsToIds(refs);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, prio, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  ASSERT_TRUE(pull_manager_.HasPullsQueued());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(oids[0], client_ids, "", NodeID::Nil(), false, 0);
  ASSERT_TRUE(pull_manager_.IsObjectActive(oids[0]));
  ASSERT_EQ(num_send_pull_request_calls_, 1);

  // Object has no locations.
  fake_time_ += 10;
  pull_manager_.OnLocationChange(oids[0], {}, "", NodeID::Nil(), false, 0);
  ASSERT_FALSE(timed_out_objects_.count(oids[0]));
  // Location added, we send a pull.
  pull_manager_.OnLocationChange(oids[0], client_ids, "", NodeID::Nil(), false, 0);
  ASSERT_EQ(num_send_pull_request_calls_, 2);
  // Object has no locations again. Object is not failed because the pull
  // reset the timer.
  fake_time_ += 601;
  pull_manager_.OnLocationChange(oids[0], {}, "", NodeID::Nil(), false, 0);
  ASSERT_FALSE(timed_out_objects_.count(oids[0]));

  // Still no locations, now the object is failed.
  fake_time_ += 601;
  pull_manager_.Tick();
  ASSERT_TRUE(timed_out_objects_.count(oids[0]));
  timed_out_objects_.clear();
  RAY_UNUSED(pull_manager_.CancelPull(req_id));

  AssertNoLeaks();
}

INSTANTIATE_TEST_SUITE_P(WorkerOrTaskRequests,
                         PullManagerTest,
                         testing::Values(BundlePriority::GET_REQUEST,
                                         BundlePriority::WAIT_REQUEST,
                                         BundlePriority::TASK_ARGS));

INSTANTIATE_TEST_SUITE_P(WorkerOrTaskRequests,
                         PullManagerWithAdmissionControlTest,
                         testing::Values(BundlePriority::GET_REQUEST,
                                         BundlePriority::WAIT_REQUEST,
                                         BundlePriority::TASK_ARGS));
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
