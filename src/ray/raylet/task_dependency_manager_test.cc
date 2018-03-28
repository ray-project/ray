#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "ray/raylet/task_dependency_manager.h"

namespace ray {

namespace raylet {

class TaskDependencyManagerTest : public ::testing::Test {
 public:
  TaskDependencyManagerTest()
      : task_dependency_manager_(
            [this](const ObjectID &object_id) { ObjectRemote(object_id); },
            [this](const TaskID &task_id) { TaskReady(task_id); },
            [this](const TaskID &task_id) { TaskWaiting(task_id); }) {}

  void ObjectRemote(const ObjectID &object_id) { remote_objects_.insert(object_id); }

  void TaskReady(const TaskID &task_id) {
    ready_tasks_.insert(task_id);
    waiting_tasks_.erase(task_id);
  }

  void TaskWaiting(const TaskID &task_id) {
    waiting_tasks_.insert(task_id);
    ready_tasks_.erase(task_id);
  }

 protected:
  TaskDependencyManager task_dependency_manager_;
  std::unordered_set<ObjectID, UniqueIDHasher> remote_objects_;
  std::unordered_set<TaskID, UniqueIDHasher> ready_tasks_;
  std::unordered_set<TaskID, UniqueIDHasher> waiting_tasks_;
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
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(ObjectID::from_random());
  }
  Task task = ExampleTask(arguments, 0);
  task_dependency_manager_.SubscribeTaskReady(task);
  ASSERT_EQ(remote_objects_.size(), num_arguments);
  ASSERT_EQ(ready_tasks_.size(), 0);
  ASSERT_EQ(waiting_tasks_.size(), 1);

  task_dependency_manager_.HandleObjectReady(arguments.front());
  ASSERT_EQ(remote_objects_.size(), num_arguments);
  ASSERT_EQ(ready_tasks_.size(), 0);
  ASSERT_EQ(waiting_tasks_.size(), 1);

  for (int i = 1; i < num_arguments; i++) {
    task_dependency_manager_.HandleObjectReady(arguments[i]);
  }
  ASSERT_EQ(remote_objects_.size(), num_arguments);
  ASSERT_EQ(ready_tasks_.size(), 1);
  ASSERT_EQ(waiting_tasks_.size(), 0);
}

TEST_F(TaskDependencyManagerTest, TestTaskChain) {
  int num_tasks = 3;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  for (const auto &task : tasks) {
    task_dependency_manager_.SubscribeTaskReady(task);
  }
  int num_ready_tasks = 1;
  ASSERT_EQ(remote_objects_.size(), 0);
  ASSERT_EQ(ready_tasks_.size(), num_ready_tasks);
  ASSERT_EQ(waiting_tasks_.size(), num_tasks - num_ready_tasks);

  while (tasks.size() > 1) {
    auto task = tasks.front();
    tasks.erase(tasks.begin());
    TaskID task_id = task.GetTaskSpecification().TaskId();
    auto return_id = task.GetTaskSpecification().ReturnId(0);

    // Simulate the object notifications from the task creating its outputs.
    task_dependency_manager_.HandleObjectReady(return_id);
    // Simulate the task finishing execution
    task_dependency_manager_.UnsubscribeTaskReady(task_id);

    num_ready_tasks++;
    ASSERT_EQ(remote_objects_.size(), 0);
    ASSERT_EQ(ready_tasks_.size(), num_ready_tasks);
    ASSERT_EQ(waiting_tasks_.size(), num_tasks - num_ready_tasks);
  }
}

TEST_F(TaskDependencyManagerTest, TestLateObjectNotification) {
  int num_tasks = 3;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  for (const auto &task : tasks) {
    task_dependency_manager_.SubscribeTaskReady(task);
  }
  int num_ready_tasks = 1;
  ASSERT_EQ(remote_objects_.size(), 0);
  ASSERT_EQ(ready_tasks_.size(), num_ready_tasks);
  ASSERT_EQ(waiting_tasks_.size(), num_tasks - num_ready_tasks);

  while (tasks.size() > 1) {
    auto task = tasks.front();
    tasks.erase(tasks.begin());
    TaskID task_id = task.GetTaskSpecification().TaskId();
    auto return_id = task.GetTaskSpecification().ReturnId(0);

    // Simulate the task finishing execution.
    task_dependency_manager_.UnsubscribeTaskReady(task_id);
    // Send the object notification after the task finishes execution. This
    // simulates a late notification from the object store and should not
    // trigger the remote object callback.
    task_dependency_manager_.HandleObjectReady(return_id);

    num_ready_tasks++;
    ASSERT_EQ(remote_objects_.size(), 0);
    ASSERT_EQ(ready_tasks_.size(), num_ready_tasks);
    ASSERT_EQ(waiting_tasks_.size(), num_tasks - num_ready_tasks);
  }
}

TEST_F(TaskDependencyManagerTest, TestTaskForwarding) {
  int num_tasks = 3;
  auto tasks = MakeTaskChain(num_tasks, {}, 1);
  for (const auto &task : tasks) {
    task_dependency_manager_.SubscribeTaskReady(task);
  }
  int num_ready_tasks = 1;
  ASSERT_EQ(remote_objects_.size(), 0);
  ASSERT_EQ(ready_tasks_.size(), num_ready_tasks);
  ASSERT_EQ(waiting_tasks_.size(), num_tasks - num_ready_tasks);

  ObjectID return_id;
  while (tasks.size() > 1) {
    auto task = tasks.front();
    tasks.erase(tasks.begin());
    TaskID task_id = task.GetTaskSpecification().TaskId();
    return_id = task.GetTaskSpecification().ReturnId(0);

    // Simulate forwarding the task to a remote node.
    ready_tasks_.erase(task_id);
    waiting_tasks_.erase(task_id);
    task_dependency_manager_.UnsubscribeTaskReady(task_id);

    ASSERT_EQ(remote_objects_.count(return_id), 1);
    ASSERT_EQ(ready_tasks_.size(), 0);
    ASSERT_EQ(waiting_tasks_.size(), tasks.size());
  }

  task_dependency_manager_.HandleObjectReady(return_id);
  ASSERT_EQ(ready_tasks_.size(), 1);
  ASSERT_EQ(waiting_tasks_.size(), 0);
}

TEST_F(TaskDependencyManagerTest, TestEviction) {
  int num_arguments = 3;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < num_arguments; i++) {
    arguments.push_back(ObjectID::from_random());
  }
  Task task = ExampleTask(arguments, 0);
  task_dependency_manager_.SubscribeTaskReady(task);

  for (const auto &argument_id : arguments) {
    task_dependency_manager_.HandleObjectReady(argument_id);
  }
  ASSERT_EQ(ready_tasks_.size(), 1);
  ASSERT_EQ(waiting_tasks_.size(), 0);

  // Simulate an object being evicted from the local node.
  ObjectID evicted_argument_id = arguments[1];
  task_dependency_manager_.HandleObjectMissing(evicted_argument_id);
  ASSERT_EQ(ready_tasks_.size(), 0);
  ASSERT_EQ(waiting_tasks_.size(), 1);

  task_dependency_manager_.HandleObjectReady(evicted_argument_id);
  ASSERT_EQ(ready_tasks_.size(), 1);
  ASSERT_EQ(waiting_tasks_.size(), 0);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
