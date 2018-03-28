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
            [this](const ObjectID &object_id) { ObjectMissing(object_id); },
            [this](const TaskID &task_id) { TaskReady(task_id); },
            [this](const TaskID &task_id) { TaskWaiting(task_id); }) {}

  void ObjectMissing(const ObjectID &object_id) { missing_objects_.insert(object_id); }

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
  std::unordered_set<ObjectID, UniqueIDHasher> missing_objects_;
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
  ASSERT_EQ(missing_objects_.size(), num_arguments);
  ASSERT_EQ(ready_tasks_.size(), 0);
  ASSERT_EQ(waiting_tasks_.size(), 1);

  task_dependency_manager_.HandleObjectReady(arguments.front());
  ASSERT_EQ(missing_objects_.size(), num_arguments);
  ASSERT_EQ(ready_tasks_.size(), 0);
  ASSERT_EQ(waiting_tasks_.size(), 1);

  for (int i = 1; i < num_arguments; i++) {
    task_dependency_manager_.HandleObjectReady(arguments[i]);
  }
  ASSERT_EQ(missing_objects_.size(), num_arguments);
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
  ASSERT_EQ(missing_objects_.size(), 0);
  ASSERT_EQ(ready_tasks_.size(), num_ready_tasks);
  ASSERT_EQ(waiting_tasks_.size(), num_tasks - num_ready_tasks);

  while (tasks.size() > 1) {
    auto task = tasks.front();
    tasks.erase(tasks.begin());
    auto return_id = task.GetTaskSpecification().ReturnId(0);
    task_dependency_manager_.HandleObjectReady(return_id);
    num_ready_tasks++;
    ASSERT_EQ(missing_objects_.size(), 0);
    ASSERT_EQ(ready_tasks_.size(), num_ready_tasks);
    ASSERT_EQ(waiting_tasks_.size(), num_tasks - num_ready_tasks);
  }
}

TEST_F(TaskDependencyManagerTest, TestEviction) {}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
