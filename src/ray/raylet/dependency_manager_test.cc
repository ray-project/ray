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

#include "ray/raylet/dependency_manager.h"

#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"

namespace ray {

namespace raylet {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;

class MockObjectManager : public ObjectManagerInterface {
 public:
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs) {
    active_requests.insert(req_id);
    return req_id++;
  }

  void CancelPull(uint64_t request_id) { ASSERT_TRUE(active_requests.erase(request_id)); }

  uint64_t req_id = 1;
  std::unordered_set<uint64_t> active_requests;
};

class MockReconstructionPolicy : public ReconstructionPolicyInterface {
 public:
  MOCK_METHOD2(ListenAndMaybeReconstruct,
               void(const ObjectID &object_id, const rpc::Address &owner_address));
  MOCK_METHOD1(Cancel, void(const ObjectID &object_id));
};

class DependencyManagerTest : public ::testing::Test {
 public:
  DependencyManagerTest()
      : object_manager_mock_(),
        reconstruction_policy_mock_(),
        dependency_manager_(object_manager_mock_, reconstruction_policy_mock_) {}

  void AssertNoLeaks() {
    ASSERT_TRUE(dependency_manager_.required_objects_.empty());
    ASSERT_TRUE(dependency_manager_.queued_task_requests_.empty());
    ASSERT_TRUE(dependency_manager_.get_requests_.empty());
    ASSERT_TRUE(dependency_manager_.wait_requests_.empty());
    // All pull requests are canceled.
    ASSERT_TRUE(object_manager_mock_.active_requests.empty());
  }

  MockObjectManager object_manager_mock_;
  MockReconstructionPolicy reconstruction_policy_mock_;
  DependencyManager dependency_manager_;
};

/// Test requesting the dependencies for a task. The dependency manager should
/// return the task ID as ready once all of its arguments are local.
TEST_F(DependencyManagerTest, TestSimpleTask) {
  // Create a task with 3 arguments.
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(ObjectID::FromRandom());
  }
  TaskID task_id = RandomTaskId();
  // No objects have been registered in the task dependency manager, so all
  // arguments should be remote.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id, _));
  }
  bool ready =
      dependency_manager_.RequestTaskDependencies(task_id, ObjectIdsToRefs(arguments));
  ASSERT_FALSE(ready);
  ASSERT_EQ(object_manager_mock_.active_requests.size(), 1);

  // For each argument, tell the task dependency manager that the argument is
  // local. All arguments should be canceled as they become available locally.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }
  auto ready_task_ids = dependency_manager_.HandleObjectLocal(arguments[0]);
  ASSERT_TRUE(ready_task_ids.empty());
  ready_task_ids = dependency_manager_.HandleObjectLocal(arguments[1]);
  ASSERT_TRUE(ready_task_ids.empty());
  // The task is ready to run.
  ready_task_ids = dependency_manager_.HandleObjectLocal(arguments[2]);
  ASSERT_EQ(ready_task_ids.size(), 1);
  ASSERT_EQ(ready_task_ids.front(), task_id);

  // Remove the task.
  dependency_manager_.RemoveTaskDependencies(task_id);
  AssertNoLeaks();
}

/// Test multiple tasks that depend on the same object. The dependency manager
/// should return all task IDs as ready once the object is local.
TEST_F(DependencyManagerTest, TestMultipleTasks) {
  // Create 3 tasks that are dependent on the same object.
  ObjectID argument_id = ObjectID::FromRandom();
  std::vector<TaskID> dependent_tasks;
  int num_dependent_tasks = 3;
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id, _));
  for (int i = 0; i < num_dependent_tasks; i++) {
    TaskID task_id = RandomTaskId();
    dependent_tasks.push_back(task_id);
    bool ready = dependency_manager_.RequestTaskDependencies(
        task_id, ObjectIdsToRefs({argument_id}));
    ASSERT_FALSE(ready);
    // The object should be requested from the object manager once for each task.
    ASSERT_EQ(object_manager_mock_.active_requests.size(), i + 1);
  }

  // Tell the task dependency manager that the object is local.
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  auto ready_task_ids = dependency_manager_.HandleObjectLocal(argument_id);
  // Check that all tasks are now ready to run.
  std::unordered_set<TaskID> added_tasks(dependent_tasks.begin(), dependent_tasks.end());
  for (auto &id : ready_task_ids) {
    ASSERT_TRUE(added_tasks.erase(id));
  }
  ASSERT_TRUE(added_tasks.empty());

  for (auto &id : dependent_tasks) {
    dependency_manager_.RemoveTaskDependencies(id);
  }
  AssertNoLeaks();
}

/// Test task with multiple dependencies. The dependency manager should return
/// the task ID as ready once all dependencies are local. If a dependency is
/// later evicted, the dependency manager should return the task ID as waiting.
TEST_F(DependencyManagerTest, TestTaskArgEviction) {
  // Add a task with 3 arguments.
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(ObjectID::FromRandom());
  }
  TaskID task_id = RandomTaskId();
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id, _));
  }
  bool ready =
      dependency_manager_.RequestTaskDependencies(task_id, ObjectIdsToRefs(arguments));
  ASSERT_FALSE(ready);

  // Tell the task dependency manager that each of the arguments is now
  // available.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<TaskID> ready_tasks;
    ready_tasks = dependency_manager_.HandleObjectLocal(arguments[i]);
    if (i == arguments.size() - 1) {
      ASSERT_EQ(ready_tasks.size(), 1);
      ASSERT_EQ(ready_tasks.front(), task_id);
    } else {
      ASSERT_TRUE(ready_tasks.empty());
    }
  }

  // Simulate each of the arguments getting evicted. Each object should now be
  // considered remote.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id, _));
  }
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<TaskID> waiting_tasks;
    waiting_tasks = dependency_manager_.HandleObjectMissing(arguments[i]);
    if (i == 0) {
      // The first eviction should cause the task to go back to the waiting
      // state.
      ASSERT_EQ(waiting_tasks.size(), 1);
      ASSERT_EQ(waiting_tasks.front(), task_id);
    } else {
      // The subsequent evictions shouldn't cause any more tasks to go back to
      // the waiting state.
      ASSERT_TRUE(waiting_tasks.empty());
    }
  }

  // Tell the task dependency manager that each of the arguments is available
  // again.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<TaskID> ready_tasks;
    ready_tasks = dependency_manager_.HandleObjectLocal(arguments[i]);
    if (i == arguments.size() - 1) {
      ASSERT_EQ(ready_tasks.size(), 1);
      ASSERT_EQ(ready_tasks.front(), task_id);
    } else {
      ASSERT_TRUE(ready_tasks.empty());
    }
  }

  dependency_manager_.RemoveTaskDependencies(task_id);
  AssertNoLeaks();
}

/// Test `ray.get`. Worker calls ray.get on {oid1}, then {oid1, oid2}, then
/// {oid1, oid2, oid3}.
TEST_F(DependencyManagerTest, TestGet) {
  WorkerID worker_id = WorkerID::FromRandom();
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    // Add the new argument to the list of dependencies to subscribe to.
    ObjectID argument_id = ObjectID::FromRandom();
    arguments.push_back(argument_id);
    // Subscribe to the task's dependencies. All arguments except the last are
    // duplicates of previous subscription calls. Each argument should only be
    // requested from the node manager once.
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id, _));
    auto prev_pull_reqs = object_manager_mock_.active_requests;
    dependency_manager_.StartOrUpdateGetRequest(worker_id, ObjectIdsToRefs(arguments));
    // Previous pull request for this worker should be canceled upon each new
    // bundle.
    ASSERT_EQ(object_manager_mock_.active_requests.size(), 1);
    ASSERT_NE(object_manager_mock_.active_requests, prev_pull_reqs);
  }

  // Nothing happens if the same bundle is requested.
  auto prev_pull_reqs = object_manager_mock_.active_requests;
  dependency_manager_.StartOrUpdateGetRequest(worker_id, ObjectIdsToRefs(arguments));
  ASSERT_EQ(object_manager_mock_.active_requests, prev_pull_reqs);

  // All arguments should be canceled as they become available locally.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }

  // Cancel the pull request once the worker cancels the `ray.get`.
  dependency_manager_.CancelGetRequest(worker_id);
  AssertNoLeaks();
}

/// Test that when one of the objects becomes local after a `ray.wait` call,
/// all requests to remote nodes associated with the object are canceled.
TEST_F(DependencyManagerTest, TestWait) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> oids;
  for (int i = 0; i < num_objects; i++) {
    oids.push_back(ObjectID::FromRandom());
  }
  // Simulate a worker calling `ray.wait` on some objects.
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(_, _))
      .Times(num_objects);
  dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_requests.size(), num_objects);

  for (int i = 0; i < num_objects; i++) {
    // Object is local.
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(oids[i]));
    auto ready_task_ids = dependency_manager_.HandleObjectLocal(oids[i]);

    // Local object gets evicted. The `ray.wait` call should not be
    // reactivated.
    auto waiting_task_ids = dependency_manager_.HandleObjectMissing(oids[i]);
    ASSERT_TRUE(waiting_task_ids.empty());
    ASSERT_EQ(object_manager_mock_.active_requests.size(), num_objects - i - 1);
  }
  AssertNoLeaks();
}

/// Test that when no objects are locally available, a `ray.wait` call makes
/// the correct requests to remote nodes and correctly cancels the requests
/// when the `ray.wait` call is canceled.
TEST_F(DependencyManagerTest, TestWaitThenCancel) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> oids;
  for (int i = 0; i < num_objects; i++) {
    oids.push_back(ObjectID::FromRandom());
  }
  // Simulate a worker calling `ray.wait` on some objects.
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(_, _))
      .Times(num_objects);
  dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_requests.size(), num_objects);
  auto prev_pull_reqs = object_manager_mock_.active_requests;
  // Check that it's okay to call `ray.wait` on the same objects again. No new
  // calls should be made to try and make the objects local.
  dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_requests, prev_pull_reqs);
  // Cancel the worker's `ray.wait`.
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(_)).Times(num_objects);
  dependency_manager_.CancelWaitRequest(worker_id);
  AssertNoLeaks();
}

/// Test that when one of the objects is already local at the time of the
/// `ray.wait` call, the `ray.wait` call does not trigger any requests to
/// remote nodes for that object.
TEST_F(DependencyManagerTest, TestWaitObjectLocal) {
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
  auto ready_task_ids = dependency_manager_.HandleObjectLocal(local_object_id);
  ASSERT_TRUE(ready_task_ids.empty());

  // Simulate a worker calling `ray.wait` on the objects. It should only make
  // requests for the objects that are not local.
  for (const auto &object_id : oids) {
    if (object_id != local_object_id) {
      EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(object_id, _));
    }
  }
  dependency_manager_.StartOrUpdateWaitRequest(worker_id, ObjectIdsToRefs(oids));
  ASSERT_EQ(object_manager_mock_.active_requests.size(), num_objects - 1);
  // Simulate the local object getting evicted. The `ray.wait` call should not
  // be reactivated.
  auto waiting_task_ids = dependency_manager_.HandleObjectMissing(local_object_id);
  ASSERT_TRUE(waiting_task_ids.empty());
  ASSERT_EQ(object_manager_mock_.active_requests.size(), num_objects - 1);
  // Cancel the worker's `ray.wait`.
  for (const auto &object_id : oids) {
    if (object_id != local_object_id) {
      EXPECT_CALL(reconstruction_policy_mock_, Cancel(object_id));
    }
  }
  dependency_manager_.CancelWaitRequest(worker_id);
  AssertNoLeaks();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
