#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "ray/raylet/task_dependency_manager.h"

namespace ray {

namespace raylet {

class TaskDependencyManagerTest : public ::testing::Test {
 public:
  TaskDependencyManagerTest() : task_dependency_manager_() {}

 protected:
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
  // Subscribe to the task's dependencies.
  std::vector<ObjectID> remote_objects;
  bool ready =
      task_dependency_manager_.SubscribeDependencies(task_id, arguments, remote_objects);
  ASSERT_FALSE(ready);
  // No objects have been registered in the task dependency manager, so all
  // arguments should be remote.
  ASSERT_EQ(remote_objects, arguments);

  int i = 0;
  // For each argument except the last, tell the task dependency manager that
  // the argument is local.
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
    // duplicates of previous subscription calls.
    std::vector<ObjectID> remote_objects;
    bool ready = task_dependency_manager_.SubscribeDependencies(task_id, arguments,
                                                                remote_objects);
    ASSERT_FALSE(ready);
    // No objects have been registered in the task dependency manager, so all
    // arguments should be remote.
    ASSERT_EQ(remote_objects.size(), 1);
    ASSERT_EQ(remote_objects.front(), argument_id);
  }

  int i = 0;
  // For each argument except the last, tell the task dependency manager that
  // the argument is local.
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
  for (int i = 0; i < num_dependent_tasks; i++) {
    TaskID task_id = TaskID::from_random();
    dependent_tasks.push_back(task_id);
    // Subscribe to each of the task's dependencies.
    std::vector<ObjectID> remote_objects;
    bool ready = task_dependency_manager_.SubscribeDependencies(task_id, {argument_id},
                                                                remote_objects);
    ASSERT_FALSE(ready);
    ASSERT_EQ(remote_objects.size(), 1);
    ASSERT_EQ(remote_objects.front(), argument_id);
  }
  // Tell the task dependency manager that the object is local.
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
  for (const auto &task : tasks) {
    // Subscribe to each of the tasks' arguments.
    std::vector<ObjectID> remote_objects;
    auto arguments = task.GetDependencies();
    bool ready = task_dependency_manager_.SubscribeDependencies(
        task.GetTaskSpecification().TaskId(), arguments, remote_objects);
    if (i < num_ready_tasks) {
      // The first task should be ready to run since it has no arguments.
      ASSERT_TRUE(ready);
    } else {
      // All remaining tasks depend on the previous task.
      ASSERT_FALSE(ready);
    }
    // No objects should be remote since each task depends on a locally queued
    // task.
    ASSERT_TRUE(remote_objects.empty());

    // Mark each task as pending.
    std::vector<ObjectID> canceled_objects;
    task_dependency_manager_.TaskPending(task, canceled_objects);
    ASSERT_TRUE(canceled_objects.empty());

    i++;
  }

  // Simulate executing each task. Each task's completion should make the next
  // task runnable.
  while (!tasks.empty()) {
    auto task = tasks.front();
    tasks.erase(tasks.begin());
    TaskID task_id = task.GetTaskSpecification().TaskId();
    auto return_id = task.GetTaskSpecification().ReturnId(0);

    std::vector<ObjectID> canceled_objects;
    task_dependency_manager_.UnsubscribeDependencies(task_id, canceled_objects);
    ASSERT_EQ(canceled_objects, task.GetDependencies());
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
    std::vector<ObjectID> remote_objects;
    task_dependency_manager_.TaskCanceled(task_id, remote_objects);
    ASSERT_TRUE(remote_objects.empty());
  }
}

TEST_F(TaskDependencyManagerTest, TestTaskForwarding) {
  // Create 2 tasks, one dependent on the other. The first has no arguments.
  int num_tasks = 2;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  for (const auto &task : tasks) {
    // Subscribe to each of the tasks' arguments.
    std::vector<ObjectID> null;
    auto arguments = task.GetDependencies();
    (void)task_dependency_manager_.SubscribeDependencies(
        task.GetTaskSpecification().TaskId(), arguments, null);
    task_dependency_manager_.TaskPending(task, null);
  }

  // Get the first task.
  const auto task = tasks.front();
  TaskID task_id = task.GetTaskSpecification().TaskId();
  ObjectID return_id = task.GetTaskSpecification().ReturnId(0);
  // Simulate forwarding the first task to a remote node.
  std::vector<ObjectID> canceled_objects;
  task_dependency_manager_.UnsubscribeDependencies(task_id, canceled_objects);
  ASSERT_EQ(canceled_objects, task.GetDependencies());
  std::vector<ObjectID> remote_objects;
  task_dependency_manager_.TaskCanceled(task_id, remote_objects);
  // The object returned by the first task should now be considered remote,
  // since we forwarded the first task and the second task in the chain depends
  // on it.
  ASSERT_EQ(remote_objects.size(), 1);
  ASSERT_EQ(remote_objects.front(), return_id);

  // Simulate the task executing on a remote node and its return value
  // appearing locally.
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
  // Subscribe to the task's dependencies.
  std::vector<ObjectID> remote_objects;
  bool ready =
      task_dependency_manager_.SubscribeDependencies(task_id, arguments, remote_objects);
  ASSERT_FALSE(ready);
  // No objects have been registered in the task dependency manager, so the all
  // arguments should be remote.
  ASSERT_EQ(remote_objects, arguments);

  // Tell the task dependency manager that each of the arguments is now
  // available.
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

  // Simulate each of the arguments getting evicted.
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
