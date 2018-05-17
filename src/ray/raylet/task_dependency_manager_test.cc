#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "ray/raylet/task_dependency_manager.h"

namespace ray {

namespace raylet {

using ::testing::_;

class MockObjectManager : public ObjectManagerInterface {
 public:
  MOCK_METHOD1(Pull, ray::Status(const ObjectID &object_id));
  MOCK_METHOD1(Cancel, ray::Status(const ObjectID &object_id));
};

class TaskDependencyManagerTest : public ::testing::Test {
 public:
  TaskDependencyManagerTest()
      : object_manager_mock_(), task_dependency_manager_(object_manager_mock_) {}

 protected:
  MockObjectManager object_manager_mock_;
  TaskDependencyManager task_dependency_manager_;
};

static inline Task ExampleTask(const std::vector<ObjectID> &arguments,
                               int64_t num_returns) {
  std::unordered_map<std::string, double> required_resources;
  std::vector<std::shared_ptr<TaskArgument>> task_arguments;
  for (auto &argument : arguments) {
    std::vector<ObjectID> references = {argument};
    task_arguments.emplace_back(std::make_shared<TaskArgumentByReference>(references));
  }
  auto spec = TaskSpecification(UniqueID::nil(), UniqueID::from_random(), 0,
                                UniqueID::from_random(), task_arguments, num_returns,
                                required_resources);
  auto execution_spec = TaskExecutionSpecification(std::vector<ObjectID>());
  execution_spec.IncrementNumForwards();
  Task task = Task(execution_spec, spec);
  return task;
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
    arguments.push_back(ObjectID::from_random());
  }
  TaskID task_id = TaskID::from_random();
  // No objects have been registered in the task dependency manager, so all
  // arguments should be remote.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, Pull(argument_id));
  }
  // Subscribe to the task's dependencies.
  bool ready = task_dependency_manager_.SubscribeDependencies(task_id, arguments);
  ASSERT_FALSE(ready);

  // All arguments should be canceled as they become available locally.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, Cancel(argument_id));
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

TEST_F(TaskDependencyManagerTest, TestDuplicateSubscribe) {
  // Create a task with 3 arguments.
  TaskID task_id = TaskID::from_random();
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    // Add the new argument to the list of dependencies to subscribe to.
    ObjectID argument_id = ObjectID::from_random();
    arguments.push_back(argument_id);
    // Subscribe to the task's dependencies. All arguments except the last are
    // duplicates of previous subscription calls. Each argument should only be
    // requested from the node manager once.
    EXPECT_CALL(object_manager_mock_, Pull(argument_id));
    bool ready = task_dependency_manager_.SubscribeDependencies(task_id, arguments);
    ASSERT_FALSE(ready);
  }

  // All arguments should be canceled as they become available locally.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, Cancel(argument_id));
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
  ObjectID argument_id = ObjectID::from_random();
  std::vector<TaskID> dependent_tasks;
  int num_dependent_tasks = 3;
  // The object should only be requested from the object manager once for all
  // three tasks.
  EXPECT_CALL(object_manager_mock_, Pull(argument_id));
  for (int i = 0; i < num_dependent_tasks; i++) {
    TaskID task_id = TaskID::from_random();
    dependent_tasks.push_back(task_id);
    // Subscribe to each of the task's dependencies.
    bool ready = task_dependency_manager_.SubscribeDependencies(task_id, {argument_id});
    ASSERT_FALSE(ready);
  }

  // Tell the task dependency manager that the object is local.
  EXPECT_CALL(object_manager_mock_, Cancel(argument_id));
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
  EXPECT_CALL(object_manager_mock_, Cancel(_)).Times(0);
  for (const auto &task : tasks) {
    // Subscribe to each of the tasks' arguments.
    auto arguments = task.GetDependencies();
    bool ready = task_dependency_manager_.SubscribeDependencies(
        task.GetTaskSpecification().TaskId(), arguments);
    if (i < num_ready_tasks) {
      // The first task should be ready to run since it has no arguments.
      ASSERT_TRUE(ready);
    } else {
      // All remaining tasks depend on the previous task.
      ASSERT_FALSE(ready);
    }

    // Mark each task as pending.
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

    task_dependency_manager_.UnsubscribeDependencies(task_id);
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
  ObjectID put_id = ComputePutId(task1.GetTaskSpecification().TaskId(), 1);
  auto task2 = ExampleTask({put_id}, 0);

  // No objects have been registered in the task dependency manager, so the put
  // object should be remote.
  EXPECT_CALL(object_manager_mock_, Pull(put_id));
  // Subscribe to the task's dependencies.
  bool ready = task_dependency_manager_.SubscribeDependencies(
      task2.GetTaskSpecification().TaskId(), {put_id});
  ASSERT_FALSE(ready);

  // The put object should be considered local as soon as the task that creates
  // it is pending execution.
  EXPECT_CALL(object_manager_mock_, Cancel(put_id));
  task_dependency_manager_.TaskPending(task1);
}

TEST_F(TaskDependencyManagerTest, TestTaskForwarding) {
  // Create 2 tasks, one dependent on the other. The first has no arguments.
  int num_tasks = 2;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  for (const auto &task : tasks) {
    // Subscribe to each of the tasks' arguments.
    auto arguments = task.GetDependencies();
    static_cast<void>(task_dependency_manager_.SubscribeDependencies(
        task.GetTaskSpecification().TaskId(), arguments));
    task_dependency_manager_.TaskPending(task);
  }

  // Get the first task.
  const auto task = tasks.front();
  TaskID task_id = task.GetTaskSpecification().TaskId();
  ObjectID return_id = task.GetTaskSpecification().ReturnId(0);
  // Simulate forwarding the first task to a remote node.
  task_dependency_manager_.UnsubscribeDependencies(task_id);
  // The object returned by the first task should be considered remote once we
  // cancel the forwarded task, since the second task depends on it.
  EXPECT_CALL(object_manager_mock_, Pull(return_id));
  task_dependency_manager_.TaskCanceled(task_id);

  // Simulate the task executing on a remote node and its return value
  // appearing locally.
  EXPECT_CALL(object_manager_mock_, Cancel(return_id));
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
    arguments.push_back(ObjectID::from_random());
  }
  TaskID task_id = TaskID::from_random();
  // No objects have been registered in the task dependency manager, so all
  // arguments should be remote.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, Pull(argument_id));
  }
  // Subscribe to the task's dependencies.
  bool ready = task_dependency_manager_.SubscribeDependencies(task_id, arguments);
  ASSERT_FALSE(ready);

  // Tell the task dependency manager that each of the arguments is now
  // available.
  for (const auto &argument_id : arguments) {
    EXPECT_CALL(object_manager_mock_, Cancel(argument_id));
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
    EXPECT_CALL(object_manager_mock_, Cancel(argument_id));
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

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
