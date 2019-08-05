#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "ray/common/task/task_util.h"
#include "ray/raylet/task_dependency_manager.h"
#include "ray/util/test_util.h"

namespace ray {

namespace raylet {

using ::testing::_;

const static JobID kDefaultJobId = JobID::FromInt(1);

const static TaskID kDefaultDriverTaskId = TaskID::ForDriverTask(kDefaultJobId);

class MockObjectManager : public ObjectManagerInterface {
 public:
  MOCK_METHOD1(Pull, ray::Status(const ObjectID &object_id));
  MOCK_METHOD1(CancelPull, void(const ObjectID &object_id));
};

class MockReconstructionPolicy : public ReconstructionPolicyInterface {
 public:
  MOCK_METHOD1(ListenAndMaybeReconstruct, void(const ObjectID &object_id));
  MOCK_METHOD1(Cancel, void(const ObjectID &object_id));
};

class MockGcs : public gcs::TableInterface<TaskID, TaskLeaseData> {
 public:
  MOCK_METHOD4(
      Add,
      ray::Status(const JobID &job_id, const TaskID &task_id,
                  const std::shared_ptr<TaskLeaseData> &task_data,
                  const gcs::TableInterface<TaskID, TaskLeaseData>::WriteCallback &done));
};

class TaskDependencyManagerTest : public ::testing::Test {
 public:
  TaskDependencyManagerTest()
      : object_manager_mock_(),
        reconstruction_policy_mock_(),
        io_service_(),
        gcs_mock_(),
        initial_lease_period_ms_(100),
        task_dependency_manager_(object_manager_mock_, reconstruction_policy_mock_,
                                 io_service_, ClientID::Nil(), initial_lease_period_ms_,
                                 gcs_mock_) {}

  void Run(uint64_t timeout_ms) {
    auto timer_period = boost::posix_time::milliseconds(timeout_ms);
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service_, timer_period);
    timer->async_wait([this](const boost::system::error_code &error) {
      ASSERT_FALSE(error);
      io_service_.stop();
    });
    io_service_.run();
    io_service_.reset();
  }

 protected:
  MockObjectManager object_manager_mock_;
  MockReconstructionPolicy reconstruction_policy_mock_;
  boost::asio::io_service io_service_;
  MockGcs gcs_mock_;
  int64_t initial_lease_period_ms_;
  TaskDependencyManager task_dependency_manager_;
};

static inline Task ExampleTask(const std::vector<ObjectID> &arguments,
                               uint64_t num_returns) {
  TaskSpecBuilder builder;
  builder.SetCommonTaskSpec(RandomTaskId(), Language::PYTHON, {"", "", ""}, JobID::Nil(),
                            RandomTaskId(), 0, num_returns, {}, {});
  for (const auto &arg : arguments) {
    builder.AddByRefArg(arg);
  }
  rpc::TaskExecutionSpec execution_spec_message;
  execution_spec_message.set_num_forwards(1);
  return Task(builder.Build(), TaskExecutionSpecification(execution_spec_message));
}

std::vector<Task> MakeTaskChain(int chain_size,
                                const std::vector<ObjectID> &initial_arguments,
                                int64_t num_returns) {
  std::vector<Task> task_chain;
  std::vector<ObjectID> arguments = initial_arguments;
  for (int i = 0; i < chain_size; i++) {
    auto task = ExampleTask(arguments, num_returns);
    task_chain.push_back(task);
    arguments.clear();
    for (int j = 0; j < task.GetTaskSpecification().NumReturns(); j++) {
      arguments.push_back(task.GetTaskSpecification().ReturnId(j));
    }
  }
  return task_chain;
}

TEST_F(TaskDependencyManagerTest, TestSimpleTask) {
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
    EXPECT_CALL(object_manager_mock_, Pull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id));
  }
  // Subscribe to the task's dependencies.
  bool ready = task_dependency_manager_.SubscribeGetDependencies(task_id, arguments);
  ASSERT_FALSE(ready);

  // All arguments should be canceled as they become available locally.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, CancelPull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }
  // For each argument except the last, tell the task dependency manager that
  // the argument is local.
  int i = 0;
  for (; i < num_arguments - 1; i++) {
    auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(arguments[i]);
    ASSERT_TRUE(ready_task_ids.empty());
  }
  // Tell the task dependency manager that the last argument is local. Now the
  // task should be ready to run.
  auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(arguments[i]);
  ASSERT_EQ(ready_task_ids.size(), 1);
  ASSERT_EQ(ready_task_ids.front(), task_id);
}

TEST_F(TaskDependencyManagerTest, TestDuplicateSubscribeGetDependencies) {
  // Create a task with 3 arguments.
  TaskID task_id = RandomTaskId();
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    // Add the new argument to the list of dependencies to subscribe to.
    ObjectID argument_id = ObjectID::FromRandom();
    arguments.push_back(argument_id);
    // Subscribe to the task's dependencies. All arguments except the last are
    // duplicates of previous subscription calls. Each argument should only be
    // requested from the node manager once.
    EXPECT_CALL(object_manager_mock_, Pull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id));
    bool ready = task_dependency_manager_.SubscribeGetDependencies(task_id, arguments);
    ASSERT_FALSE(ready);
  }

  // All arguments should be canceled as they become available locally.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, CancelPull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }
  // For each argument except the last, tell the task dependency manager that
  // the argument is local.
  int i = 0;
  for (; i < num_arguments - 1; i++) {
    auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(arguments[i]);
    ASSERT_TRUE(ready_task_ids.empty());
  }
  // Tell the task dependency manager that the last argument is local. Now the
  // task should be ready to run.
  auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(arguments[i]);
  ASSERT_EQ(ready_task_ids.size(), 1);
  ASSERT_EQ(ready_task_ids.front(), task_id);
}

TEST_F(TaskDependencyManagerTest, TestMultipleTasks) {
  // Create 3 tasks that are dependent on the same object.
  ObjectID argument_id = ObjectID::FromRandom();
  std::vector<TaskID> dependent_tasks;
  int num_dependent_tasks = 3;
  // The object should only be requested from the object manager once for all
  // three tasks.
  EXPECT_CALL(object_manager_mock_, Pull(argument_id));
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id));
  for (int i = 0; i < num_dependent_tasks; i++) {
    TaskID task_id = RandomTaskId();
    dependent_tasks.push_back(task_id);
    // Subscribe to each of the task's dependencies.
    bool ready =
        task_dependency_manager_.SubscribeGetDependencies(task_id, {argument_id});
    ASSERT_FALSE(ready);
  }

  // Tell the task dependency manager that the object is local.
  EXPECT_CALL(object_manager_mock_, CancelPull(argument_id));
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(argument_id);
  // Check that all tasks are now ready to run.
  ASSERT_EQ(ready_task_ids.size(), dependent_tasks.size());
  for (const auto &task_id : ready_task_ids) {
    ASSERT_NE(std::find(dependent_tasks.begin(), dependent_tasks.end(), task_id),
              dependent_tasks.end());
  }
}

TEST_F(TaskDependencyManagerTest, TestTaskChain) {
  // Create 3 tasks, each dependent on the previous. The first task has no
  // arguments.
  int num_tasks = 3;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  int num_ready_tasks = 1;
  int i = 0;
  // No objects should be remote or canceled since each task depends on a
  // locally queued task.
  EXPECT_CALL(object_manager_mock_, Pull(_)).Times(0);
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(_)).Times(0);
  EXPECT_CALL(object_manager_mock_, CancelPull(_)).Times(0);
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(_)).Times(0);
  for (const auto &task : tasks) {
    // Subscribe to each of the tasks' arguments.
    const auto &arguments = task.GetDependencies();
    bool ready = task_dependency_manager_.SubscribeGetDependencies(
        task.GetTaskSpecification().TaskId(), arguments);
    if (i < num_ready_tasks) {
      // The first task should be ready to run since it has no arguments.
      ASSERT_TRUE(ready);
    } else {
      // All remaining tasks depend on the previous task.
      ASSERT_FALSE(ready);
    }

    // Mark each task as pending. A lease entry should be added to the GCS for
    // each task.
    EXPECT_CALL(gcs_mock_, Add(_, task.GetTaskSpecification().TaskId(), _, _));
    task_dependency_manager_.TaskPending(task);

    i++;
  }

  // Simulate executing each task. Each task's completion should make the next
  // task runnable.
  while (!tasks.empty()) {
    auto task = tasks.front();
    tasks.erase(tasks.begin());
    TaskID task_id = task.GetTaskSpecification().TaskId();
    auto return_id = task.GetTaskSpecification().ReturnId(0);

    task_dependency_manager_.UnsubscribeGetDependencies(task_id);
    // Simulate the object notifications for the task's return values.
    auto ready_tasks = task_dependency_manager_.HandleObjectLocal(return_id);
    if (tasks.empty()) {
      // If there are no more tasks, then there should be no more tasks that
      // become ready to run.
      ASSERT_TRUE(ready_tasks.empty());
    } else {
      // If there are more tasks to run, then the next task in the chain should
      // now be ready to run.
      ASSERT_EQ(ready_tasks.size(), 1);
      ASSERT_EQ(ready_tasks.front(), tasks.front().GetTaskSpecification().TaskId());
    }
    // Simulate the task finishing execution.
    task_dependency_manager_.TaskCanceled(task_id);
  }
}

TEST_F(TaskDependencyManagerTest, TestDependentPut) {
  // Create a task with 3 arguments.
  auto task1 = ExampleTask({}, 0);
  ObjectID put_id = ObjectID::ForPut(task1.GetTaskSpecification().TaskId(), /*index=*/1,
                                     /*transport_type=*/0);
  auto task2 = ExampleTask({put_id}, 0);

  // No objects have been registered in the task dependency manager, so the put
  // object should be remote.
  EXPECT_CALL(object_manager_mock_, Pull(put_id));
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(put_id));
  // Subscribe to the task's dependencies.
  bool ready = task_dependency_manager_.SubscribeGetDependencies(
      task2.GetTaskSpecification().TaskId(), {put_id});
  ASSERT_FALSE(ready);

  // The put object should be considered local as soon as the task that creates
  // it is pending execution.
  EXPECT_CALL(object_manager_mock_, CancelPull(put_id));
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(put_id));
  EXPECT_CALL(gcs_mock_, Add(_, task1.GetTaskSpecification().TaskId(), _, _));
  task_dependency_manager_.TaskPending(task1);
}

TEST_F(TaskDependencyManagerTest, TestTaskForwarding) {
  // Create 2 tasks, one dependent on the other. The first has no arguments.
  int num_tasks = 2;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  for (const auto &task : tasks) {
    // Subscribe to each of the tasks' arguments.
    const auto &arguments = task.GetDependencies();
    static_cast<void>(task_dependency_manager_.SubscribeGetDependencies(
        task.GetTaskSpecification().TaskId(), arguments));
    EXPECT_CALL(gcs_mock_, Add(_, task.GetTaskSpecification().TaskId(), _, _));
    task_dependency_manager_.TaskPending(task);
  }

  // Get the first task.
  const auto task = tasks.front();
  TaskID task_id = task.GetTaskSpecification().TaskId();
  ObjectID return_id = task.GetTaskSpecification().ReturnId(0);
  // Simulate forwarding the first task to a remote node.
  task_dependency_manager_.UnsubscribeGetDependencies(task_id);
  // The object returned by the first task should be considered remote once we
  // cancel the forwarded task, since the second task depends on it.
  EXPECT_CALL(object_manager_mock_, Pull(return_id));
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(return_id));
  task_dependency_manager_.TaskCanceled(task_id);

  // Simulate the task executing on a remote node and its return value
  // appearing locally.
  EXPECT_CALL(object_manager_mock_, CancelPull(return_id));
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(return_id));
  auto ready_tasks = task_dependency_manager_.HandleObjectLocal(return_id);
  // Check that the task that we kept is now ready to run.
  ASSERT_EQ(ready_tasks.size(), 1);
  ASSERT_EQ(ready_tasks.front(), tasks.back().GetTaskSpecification().TaskId());
}

TEST_F(TaskDependencyManagerTest, TestEviction) {
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
    EXPECT_CALL(object_manager_mock_, Pull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id));
  }
  // Subscribe to the task's dependencies.
  bool ready = task_dependency_manager_.SubscribeGetDependencies(task_id, arguments);
  ASSERT_FALSE(ready);

  // Tell the task dependency manager that each of the arguments is now
  // available.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, CancelPull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<TaskID> ready_tasks;
    ready_tasks = task_dependency_manager_.HandleObjectLocal(arguments[i]);
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
    EXPECT_CALL(object_manager_mock_, Pull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(argument_id));
  }
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<TaskID> waiting_tasks;
    waiting_tasks = task_dependency_manager_.HandleObjectMissing(arguments[i]);
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
    EXPECT_CALL(object_manager_mock_, CancelPull(argument_id));
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(argument_id));
  }
  for (size_t i = 0; i < arguments.size(); i++) {
    std::vector<TaskID> ready_tasks;
    ready_tasks = task_dependency_manager_.HandleObjectLocal(arguments[i]);
    if (i == arguments.size() - 1) {
      ASSERT_EQ(ready_tasks.size(), 1);
      ASSERT_EQ(ready_tasks.front(), task_id);
    } else {
      ASSERT_TRUE(ready_tasks.empty());
    }
  }
}

TEST_F(TaskDependencyManagerTest, TestTaskLeaseRenewal) {
  // Mark a task as pending.
  auto task = ExampleTask({}, 0);
  // We expect an initial call to acquire the lease.
  EXPECT_CALL(gcs_mock_, Add(_, task.GetTaskSpecification().TaskId(), _, _));
  task_dependency_manager_.TaskPending(task);

  // Check that while the task is still pending, there is one call to renew the
  // lease for each lease period that passes. The lease period doubles with
  // each renewal.
  int num_expected_calls = 4;
  int64_t sleep_time = 0;
  for (int i = 1; i <= num_expected_calls; i++) {
    sleep_time += i * initial_lease_period_ms_;
  }
  EXPECT_CALL(gcs_mock_, Add(_, task.GetTaskSpecification().TaskId(), _, _))
      .Times(num_expected_calls);
  Run(sleep_time);
}

TEST_F(TaskDependencyManagerTest, TestRemoveTasksAndRelatedObjects) {
  // Create 3 tasks, each dependent on the previous. The first task has no
  // arguments.
  int num_tasks = 3;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  // No objects should be remote or canceled since each task depends on a
  // locally queued task.
  EXPECT_CALL(object_manager_mock_, Pull(_)).Times(0);
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(_)).Times(0);
  EXPECT_CALL(object_manager_mock_, CancelPull(_)).Times(0);
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(_)).Times(0);
  for (const auto &task : tasks) {
    // Subscribe to each of the tasks' arguments.
    const auto &arguments = task.GetDependencies();
    task_dependency_manager_.SubscribeGetDependencies(
        task.GetTaskSpecification().TaskId(), arguments);
    // Mark each task as pending. A lease entry should be added to the GCS for
    // each task.
    EXPECT_CALL(gcs_mock_, Add(_, task.GetTaskSpecification().TaskId(), _, _));
    task_dependency_manager_.TaskPending(task);
  }

  // Simulate executing the first task. This should make the second task
  // runnable.
  auto task = tasks.front();
  TaskID task_id = task.GetTaskSpecification().TaskId();
  auto return_id = task.GetTaskSpecification().ReturnId(0);
  task_dependency_manager_.UnsubscribeGetDependencies(task_id);
  // Simulate the object notifications for the task's return values.
  auto ready_tasks = task_dependency_manager_.HandleObjectLocal(return_id);
  // The second task should be ready to run.
  ASSERT_EQ(ready_tasks.size(), 1);
  // Simulate the task finishing execution.
  task_dependency_manager_.TaskCanceled(task_id);

  // Remove all tasks from the manager except the first task, which already
  // finished executing.
  std::unordered_set<TaskID> task_ids;
  for (const auto &task : tasks) {
    task_ids.insert(task.GetTaskSpecification().TaskId());
  }
  task_ids.erase(task_id);
  task_dependency_manager_.RemoveTasksAndRelatedObjects(task_ids);
  // Simulate evicting the return value of the first task. Make sure that this
  // does not return the second task, which should have been removed.
  auto waiting_tasks = task_dependency_manager_.HandleObjectMissing(return_id);
  ASSERT_TRUE(waiting_tasks.empty());

  // Simulate the object notifications for the second task's return values.
  // Make sure that this does not return the third task, which should have been
  // removed.
  return_id = tasks[1].GetTaskSpecification().ReturnId(0);
  ready_tasks = task_dependency_manager_.HandleObjectLocal(return_id);
  ASSERT_TRUE(ready_tasks.empty());
}

/// Test that when no objects are locally available, a `ray.wait` call makes
/// the correct requests to remote nodes and correctly cancels the requests
/// when the `ray.wait` call is canceled.
TEST_F(TaskDependencyManagerTest, TestWaitDependencies) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> wait_object_ids;
  for (int i = 0; i < num_objects; i++) {
    wait_object_ids.push_back(ObjectID::FromRandom());
  }
  // Simulate a worker calling `ray.wait` on some objects.
  EXPECT_CALL(object_manager_mock_, Pull(_)).Times(num_objects);
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(_))
      .Times(num_objects);
  task_dependency_manager_.SubscribeWaitDependencies(worker_id, wait_object_ids);
  // Check that it's okay to call `ray.wait` on the same objects again. No new
  // calls should be made to try and make the objects local.
  task_dependency_manager_.SubscribeWaitDependencies(worker_id, wait_object_ids);
  // Cancel the worker's `ray.wait`. calls.
  EXPECT_CALL(object_manager_mock_, CancelPull(_)).Times(num_objects);
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(_)).Times(num_objects);
  task_dependency_manager_.UnsubscribeWaitDependencies(worker_id);
}

/// Test that when one of the objects is already local at the time of the
/// `ray.wait` call, the `ray.wait` call does not trigger any requests to
/// remote nodes for that object.
TEST_F(TaskDependencyManagerTest, TestWaitDependenciesObjectLocal) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> wait_object_ids;
  for (int i = 0; i < num_objects; i++) {
    wait_object_ids.push_back(ObjectID::FromRandom());
  }
  // Simulate one of the objects becoming local. The later `ray.wait` call
  // should have no effect because the object is already local.
  const ObjectID local_object_id = std::move(wait_object_ids.back());
  auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(local_object_id);
  ASSERT_TRUE(ready_task_ids.empty());

  // Simulate a worker calling `ray.wait` on the objects. It should only make
  // requests for the objects that are not local.
  for (const auto &object_id : wait_object_ids) {
    if (object_id != local_object_id) {
      EXPECT_CALL(object_manager_mock_, Pull(object_id));
      EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(object_id));
    }
  }
  task_dependency_manager_.SubscribeWaitDependencies(worker_id, wait_object_ids);
  // Simulate the local object getting evicted. The `ray.wait` call should not
  // be reactivated.
  auto waiting_task_ids = task_dependency_manager_.HandleObjectMissing(local_object_id);
  ASSERT_TRUE(waiting_task_ids.empty());
  // Simulate a worker calling `ray.wait` on the objects. It should only make
  // requests for the objects that are not local.
  for (const auto &object_id : wait_object_ids) {
    if (object_id != local_object_id) {
      EXPECT_CALL(object_manager_mock_, CancelPull(object_id));
      EXPECT_CALL(reconstruction_policy_mock_, Cancel(object_id));
    }
  }
  task_dependency_manager_.UnsubscribeWaitDependencies(worker_id);
}

/// Test that when one of the objects becomes local after a `ray.wait` call,
/// all requests to remote nodes associated with the object are canceled.
TEST_F(TaskDependencyManagerTest, TestWaitDependenciesHandleObjectLocal) {
  // Generate a random worker and objects to wait on.
  WorkerID worker_id = WorkerID::FromRandom();
  int num_objects = 3;
  std::vector<ObjectID> wait_object_ids;
  for (int i = 0; i < num_objects; i++) {
    wait_object_ids.push_back(ObjectID::FromRandom());
  }
  // Simulate a worker calling `ray.wait` on some objects.
  EXPECT_CALL(object_manager_mock_, Pull(_)).Times(num_objects);
  EXPECT_CALL(reconstruction_policy_mock_, ListenAndMaybeReconstruct(_))
      .Times(num_objects);
  task_dependency_manager_.SubscribeWaitDependencies(worker_id, wait_object_ids);
  // Simulate one of the objects becoming local while the `ray.wait` calls is
  // active. The `ray.wait` call should be canceled.
  const ObjectID local_object_id = std::move(wait_object_ids.back());
  wait_object_ids.pop_back();
  EXPECT_CALL(object_manager_mock_, CancelPull(local_object_id));
  EXPECT_CALL(reconstruction_policy_mock_, Cancel(local_object_id));
  auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(local_object_id);
  ASSERT_TRUE(ready_task_ids.empty());
  // Simulate the local object getting evicted. The `ray.wait` call should not
  // be reactivated.
  auto waiting_task_ids = task_dependency_manager_.HandleObjectMissing(local_object_id);
  ASSERT_TRUE(waiting_task_ids.empty());
  // Cancel the worker's `ray.wait` calls. Only the objects that are still not
  // local should be canceled.
  for (const auto &object_id : wait_object_ids) {
    EXPECT_CALL(object_manager_mock_, CancelPull(object_id));
    EXPECT_CALL(reconstruction_policy_mock_, Cancel(object_id));
  }
  task_dependency_manager_.UnsubscribeWaitDependencies(worker_id);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
