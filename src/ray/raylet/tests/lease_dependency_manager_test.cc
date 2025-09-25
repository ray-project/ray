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

#include "ray/raylet/lease_dependency_manager.h"

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/object_manager/object_manager.h"
#include "ray/common/test_utils.h"
#include "ray/observability/fake_metric.h"

namespace ray {

namespace raylet {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;

class CustomMockObjectManager : public MockObjectManager {
 public:
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs,
                BundlePriority prio,
                const TaskMetricsKey &task_key) override {
    if (prio == BundlePriority::GET_REQUEST) {
      active_get_requests.insert(req_id);
    } else if (prio == BundlePriority::WAIT_REQUEST) {
      active_wait_requests.insert(req_id);
    } else {
      active_lease_requests.insert(req_id);
    }
    return req_id++;
  }

  void CancelPull(uint64_t request_id) override {
    ASSERT_TRUE(active_get_requests.erase(request_id) ||
                active_wait_requests.erase(request_id) ||
                active_lease_requests.erase(request_id));
  }

  bool PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const override {
    return active_get_requests.count(request_id) ||
           active_wait_requests.count(request_id) ||
           active_lease_requests.count(request_id);
  }

  uint64_t req_id = 1;
  std::unordered_set<uint64_t> active_get_requests;
  std::unordered_set<uint64_t> active_wait_requests;
  std::unordered_set<uint64_t> active_lease_requests;
};

class LeaseDependencyManagerTest : public ::testing::Test {
 public:
  LeaseDependencyManagerTest()
      : object_manager_mock_(),
        fake_task_by_state_counter_(),
        lease_dependency_manager_(object_manager_mock_, fake_task_by_state_counter_) {}

  int64_t NumWaiting(const std::string &lease_name) {
    return lease_dependency_manager_.waiting_leases_counter_.Get({lease_name, false});
  }

  int64_t NumWaitingTotal() {
    return lease_dependency_manager_.waiting_leases_counter_.Total();
  }

  void AssertNoLeaks() {
    ASSERT_TRUE(lease_dependency_manager_.required_objects_.empty());
    ASSERT_TRUE(lease_dependency_manager_.queued_lease_requests_.empty());
    ASSERT_TRUE(lease_dependency_manager_.get_requests_.empty());
    ASSERT_TRUE(lease_dependency_manager_.wait_requests_.empty());
    ASSERT_EQ(lease_dependency_manager_.waiting_leases_counter_.Total(), 0);
    // All pull requests are canceled.
    ASSERT_TRUE(object_manager_mock_.active_lease_requests.empty());
    ASSERT_TRUE(object_manager_mock_.active_get_requests.empty());
    ASSERT_TRUE(object_manager_mock_.active_wait_requests.empty());
  }

  CustomMockObjectManager object_manager_mock_;
  ray::observability::FakeGauge fake_task_by_state_counter_;
  LeaseDependencyManager lease_dependency_manager_;
};

TEST_F(LeaseDependencyManagerTest, TestRecordMetrics) {
  auto obj_id = ObjectID::FromRandom();
  lease_dependency_manager_.RequestLeaseDependencies(
      LeaseID::FromRandom(), ObjectIdsToRefs({obj_id}), {"foo", false});
  lease_dependency_manager_.HandleObjectLocal(obj_id);
  lease_dependency_manager_.RecordMetrics();
  auto tag_to_value = fake_task_by_state_counter_.GetTagToValue();
  // 3 states: PENDING_NODE_ASSIGNMENT, PENDING_ARGS_FETCH, PENDING_OBJ_STORE_MEM_AVAIL
  ASSERT_EQ(tag_to_value.size(), 3);
  ASSERT_EQ(tag_to_value.begin()->first.at("Name"), "foo");
}

/// Test requesting the dependencies for a lease. The dependency manager should
/// return the lease ID as ready once all of its arguments are local.
TEST_F(LeaseDependencyManagerTest, TestSimpleLease) {
  // Create a lease with 3 arguments.
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(ObjectID::FromRandom());
  }
  LeaseID lease_id = LeaseID::FromRandom();
  bool ready = lease_dependency_manager_.RequestLeaseDependencies(
      lease_id, ObjectIdsToRefs(arguments), {"foo", false});
  ASSERT_FALSE(ready);
  ASSERT_EQ(NumWaiting("bar"), 0);
  ASSERT_EQ(NumWaiting("foo"), 1);
  ASSERT_EQ(NumWaitingTotal(), 1);

  // For each argument, tell the lease dependency manager that the argument is
  // local. All arguments should be canceled as they become available locally.
  auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(arguments[0]);
  ASSERT_TRUE(ready_lease_ids.empty());
  ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(arguments[1]);
  ASSERT_TRUE(ready_lease_ids.empty());
  // The lease is ready to run.
  ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(arguments[2]);
  ASSERT_EQ(ready_lease_ids.size(), 1);
  ASSERT_EQ(ready_lease_ids.front(), lease_id);
  ASSERT_EQ(NumWaiting("bar"), 0);
  ASSERT_EQ(NumWaiting("foo"), 0);
  ASSERT_EQ(NumWaitingTotal(), 0);

  // Remove the lease.
  lease_dependency_manager_.RemoveLeaseDependencies(lease_id);
  AssertNoLeaks();
}

/// Test multiple leases that depend on the same object. The dependency manager
/// should return all lease IDs as ready once the object is local.
TEST_F(LeaseDependencyManagerTest, TestMultipleLeases) {
  // Create 3 leases that are dependent on the same object.
  ObjectID argument_id = ObjectID::FromRandom();
  std::vector<LeaseID> dependent_leases;
  int num_dependent_leases = 3;
  for (int i = 0; i < num_dependent_leases; i++) {
    LeaseID lease_id = LeaseID::FromRandom();
    dependent_leases.push_back(lease_id);
    bool ready = lease_dependency_manager_.RequestLeaseDependencies(
        lease_id, ObjectIdsToRefs({argument_id}), {"foo", false});
    ASSERT_FALSE(ready);
    // The object should be requested from the object manager once for each lease.
    ASSERT_EQ(object_manager_mock_.active_lease_requests.size(), i + 1);
  }
  ASSERT_EQ(NumWaiting("bar"), 0);
  ASSERT_EQ(NumWaiting("foo"), 3);
  ASSERT_EQ(NumWaitingTotal(), 3);

  // Tell the lease dependency manager that the object is local.
  auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(argument_id);
  // Check that all leases are now ready to run.
  std::unordered_set<LeaseID> added_leases(dependent_leases.begin(),
                                           dependent_leases.end());
  for (auto &id : ready_lease_ids) {
    ASSERT_TRUE(added_leases.erase(id));
  }
  ASSERT_TRUE(added_leases.empty());

  for (auto &id : dependent_leases) {
    lease_dependency_manager_.RemoveLeaseDependencies(id);
  }
  AssertNoLeaks();
}

/// Test lease with multiple dependencies. The dependency manager should return
/// the lease ID as ready once all dependencies are local. If a dependency is
/// later evicted, the dependency manager should return the lease ID as waiting.
TEST_F(LeaseDependencyManagerTest, TestLeaseArgEviction) {
  // Add a lease with 3 arguments.
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(ObjectID::FromRandom());
  }
  LeaseID lease_id = LeaseID::FromRandom();
  bool ready = lease_dependency_manager_.RequestLeaseDependencies(
      lease_id, ObjectIdsToRefs(arguments), {"", false});
  ASSERT_FALSE(ready);

  // Tell the lease dependency manager that each of the arguments is now
  // available.
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<LeaseID> ready_leases;
    ready_leases = lease_dependency_manager_.HandleObjectLocal(arguments[i]);
    if (i == arguments.size() - 1) {
      ASSERT_EQ(ready_leases.size(), 1);
      ASSERT_EQ(ready_leases.front(), lease_id);
    } else {
      ASSERT_TRUE(ready_leases.empty());
    }
  }

  // Simulate each of the arguments getting evicted. Each object should now be
  // considered remote.
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<LeaseID> waiting_leases;
    waiting_leases = lease_dependency_manager_.HandleObjectMissing(arguments[i]);
    if (i == 0) {
      // The first eviction should cause the lease to go back to the waiting
      // state.
      ASSERT_EQ(waiting_leases.size(), 1);
      ASSERT_EQ(waiting_leases.front(), lease_id);
    } else {
      // The subsequent evictions shouldn't cause any more leases to go back to
      // the waiting state.
      ASSERT_TRUE(waiting_leases.empty());
    }
  }

  // Tell the lease dependency manager that each of the arguments is available
  // again.
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<LeaseID> ready_leases;
    ready_leases = lease_dependency_manager_.HandleObjectLocal(arguments[i]);
    if (i == arguments.size() - 1) {
      ASSERT_EQ(ready_leases.size(), 1);
      ASSERT_EQ(ready_leases.front(), lease_id);
    } else {
      ASSERT_TRUE(ready_leases.empty());
    }
  }

  lease_dependency_manager_.RemoveLeaseDependencies(lease_id);
  AssertNoLeaks();
}

/// Test `ray.get`. Worker calls ray.get on {oid1}, then {oid1, oid2}, then
/// {oid1, oid2, oid3}.
TEST_F(LeaseDependencyManagerTest, TestGet) {
  WorkerID worker_id = WorkerID::FromRandom();
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    // Add the new argument to the list of dependencies to subscribe to.
    ObjectID argument_id = ObjectID::FromRandom();
    arguments.push_back(argument_id);
    // Subscribe to the lease's dependencies. All arguments except the last are
    // duplicates of previous subscription calls. Each argument should only be
    // requested from the node manager once.
    auto prev_pull_reqs = object_manager_mock_.active_get_requests;
    lease_dependency_manager_.StartOrUpdateGetRequest(worker_id,
                                                      ObjectIdsToRefs(arguments));
    // Previous pull request for this get should be canceled upon each new
    // bundle.
    ASSERT_EQ(object_manager_mock_.active_get_requests.size(), 1);
    ASSERT_NE(object_manager_mock_.active_get_requests, prev_pull_reqs);
  }

  // Nothing happens if the same bundle is requested.
  auto prev_pull_reqs = object_manager_mock_.active_get_requests;
  lease_dependency_manager_.StartOrUpdateGetRequest(worker_id,
                                                    ObjectIdsToRefs(arguments));
  ASSERT_EQ(object_manager_mock_.active_get_requests, prev_pull_reqs);

  // Cancel the pull request once the worker cancels the `ray.get`.
  lease_dependency_manager_.CancelGetRequest(worker_id);
  AssertNoLeaks();
}

/// Test that when one of the objects becomes local after a `ray.wait` call,
/// all requests to remote nodes associated with the object are canceled.
TEST_F(LeaseDependencyManagerTest, TestWait) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> oids;
  for (int i = 0; i < num_objects; i++) {
    oids.push_back(ObjectID::FromRandom());
  }
  lease_dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_wait_requests.size(), num_objects);

  for (int i = 0; i < num_objects; i++) {
    // Object is local.
    auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(oids[i]);

    // Local object gets evicted. The `ray.wait` call should not be
    // reactivated.
    auto waiting_lease_ids = lease_dependency_manager_.HandleObjectMissing(oids[i]);
    ASSERT_TRUE(waiting_lease_ids.empty());
    ASSERT_EQ(object_manager_mock_.active_wait_requests.size(), num_objects - i - 1);
  }
  AssertNoLeaks();
}

/// Test that when no objects are locally available, a `ray.wait` call makes
/// the correct requests to remote nodes and correctly cancels the requests
/// when the `ray.wait` call is canceled.
TEST_F(LeaseDependencyManagerTest, TestWaitThenCancel) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> oids;
  for (int i = 0; i < num_objects; i++) {
    oids.push_back(ObjectID::FromRandom());
  }
  // Simulate a worker calling `ray.wait` on some objects.
  lease_dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_wait_requests.size(), num_objects);
  // Check that it's okay to call `ray.wait` on the same objects again. No new
  // calls should be made to try and make the objects local.
  lease_dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_wait_requests.size(), num_objects);
  // Cancel the worker's `ray.wait`.
  lease_dependency_manager_.CancelWaitRequest(worker_id);
  AssertNoLeaks();
}

/// Test that when one of the objects is already local at the time of the
/// `ray.wait` call, the `ray.wait` call does not trigger any requests to
/// remote nodes for that object.
TEST_F(LeaseDependencyManagerTest, TestWaitObjectLocal) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> oids;
  for (int i = 0; i < num_objects; i++) {
    oids.push_back(ObjectID::FromRandom());
  }
  // Simulate one of the objects becoming local. The later `ray.wait` call
  // should have no effect because the object is already local.
  const ObjectID local_object_id = std::move(oids.back());
  auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(local_object_id);
  ASSERT_TRUE(ready_lease_ids.empty());
  lease_dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_wait_requests.size(), num_objects - 1);
  // Simulate the local object getting evicted. The `ray.wait` call should not
  // be reactivated.
  auto waiting_lease_ids = lease_dependency_manager_.HandleObjectMissing(local_object_id);
  ASSERT_TRUE(waiting_lease_ids.empty());
  ASSERT_EQ(object_manager_mock_.active_wait_requests.size(), num_objects - 1);
  // Cancel the worker's `ray.wait`.
  lease_dependency_manager_.CancelWaitRequest(worker_id);
  AssertNoLeaks();
}

/// Test requesting the dependencies for a lease. The dependency manager should
/// return the lease ID as ready once all of its unique arguments are local.
TEST_F(LeaseDependencyManagerTest, TestDuplicateLeaseArgs) {
  // Create a lease with 3 arguments.
  int num_arguments = 3;
  auto obj_id = ObjectID::FromRandom();
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(obj_id);
  }
  LeaseID lease_id = LeaseID::FromRandom();
  bool ready = lease_dependency_manager_.RequestLeaseDependencies(
      lease_id, ObjectIdsToRefs(arguments), {"", false});
  ASSERT_FALSE(ready);
  ASSERT_EQ(object_manager_mock_.active_lease_requests.size(), 1);

  auto ready_lease_ids = lease_dependency_manager_.HandleObjectLocal(obj_id);
  ASSERT_EQ(ready_lease_ids.size(), 1);
  ASSERT_EQ(ready_lease_ids.front(), lease_id);
  lease_dependency_manager_.RemoveLeaseDependencies(lease_id);

  LeaseID lease_id2 = LeaseID::FromRandom();
  ready = lease_dependency_manager_.RequestLeaseDependencies(
      lease_id2, ObjectIdsToRefs(arguments), {"", false});
  ASSERT_TRUE(ready);
  ASSERT_EQ(object_manager_mock_.active_lease_requests.size(), 1);
  lease_dependency_manager_.RemoveLeaseDependencies(lease_id2);

  AssertNoLeaks();
}

/// Test that RemoveLeaseDependencies is called before objects
/// becoming local (e.g. the lease is cancelled).
TEST_F(LeaseDependencyManagerTest, TestRemoveLeaseDependenciesBeforeLocal) {
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(ObjectID::FromRandom());
  }
  LeaseID lease_id = LeaseID::FromRandom();
  bool ready = lease_dependency_manager_.RequestLeaseDependencies(
      lease_id, ObjectIdsToRefs(arguments), {"foo", false});
  ASSERT_FALSE(ready);
  ASSERT_EQ(NumWaiting("bar"), 0);
  ASSERT_EQ(NumWaiting("foo"), 1);
  ASSERT_EQ(NumWaitingTotal(), 1);

  // The lease is cancelled
  lease_dependency_manager_.RemoveLeaseDependencies(lease_id);
  ASSERT_EQ(NumWaiting("foo"), 0);
  ASSERT_EQ(NumWaitingTotal(), 0);
  AssertNoLeaks();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
