#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/raylet/lineage_cache.h"
#include "ray/raylet/task.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

class MockGcs : virtual public gcs::Storage<TaskID, TaskFlatbuffer>,
                virtual public gcs::Storage<ObjectID, ObjectTableData> {
 public:
  MockGcs(){};
  Status Add(const JobID &job_id, const TaskID &task_id, std::shared_ptr<TaskT> task_data,
             const gcs::Storage<TaskID, TaskFlatbuffer>::Callback &done) {
    task_table_[task_id] = task_data;
    task_callbacks_.push_back(std::pair<gcs::TaskTable::Callback, TaskID>(done, task_id));
    return ray::Status::OK();
  };

  Status Add(const JobID &job_id, const ObjectID &object_id,
             std::shared_ptr<ObjectTableDataT> object_data,
             const gcs::Storage<ObjectID, ObjectTableData>::Callback &done) {
    object_table_[object_id] = object_data;
    object_callbacks_.push_back(
        std::pair<gcs::ObjectTable::Callback, ObjectID>(done, object_id));
    return ray::Status::OK();
  };

  void Flush() {
    for (const auto &callback : task_callbacks_) {
      callback.first(NULL, callback.second, nullptr);
    }
    task_callbacks_.clear();
    for (const auto &callback : object_callbacks_) {
      callback.first(NULL, callback.second, nullptr);
    }
    object_callbacks_.clear();
  };

  std::unordered_map<TaskID, std::shared_ptr<TaskT>, UniqueIDHasher> &TaskTable() { return task_table_; }
  std::unordered_map<ObjectID, std::shared_ptr<ObjectTableDataT>, UniqueIDHasher>
      &ObjectTable() { return object_table_; }

 private:
  std::unordered_map<TaskID, std::shared_ptr<TaskT>, UniqueIDHasher> task_table_;
  std::unordered_map<ObjectID, std::shared_ptr<ObjectTableDataT>, UniqueIDHasher>
      object_table_;
  std::vector<std::pair<gcs::TaskTable::Callback, TaskID>> task_callbacks_;
  std::vector<std::pair<gcs::ObjectTable::Callback, ObjectID>> object_callbacks_;
};

class LineageCacheTest : public ::testing::Test {
 public:
  LineageCacheTest()
      : mock_gcs_(), lineage_cache_(ClientID::from_random(), mock_gcs_, mock_gcs_) {}

 protected:
  MockGcs mock_gcs_;
  LineageCache lineage_cache_;
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
  Task task = Task(execution_spec, spec);
  return task;
}

std::vector<ObjectID> InsertTaskChain(LineageCache &lineage_cache,
                                      std::vector<Task> &inserted_tasks,
                                      int chain_size,
                                      const std::vector<ObjectID> &initial_arguments,
                                      int64_t num_returns) {
  Lineage empty_lineage;
  std::vector<ObjectID> arguments = initial_arguments;
  for (int i = 0; i < chain_size; i++) {
    auto task = ExampleTask(arguments, num_returns);
    lineage_cache.AddWaitingTask(task, empty_lineage);
    inserted_tasks.push_back(task);
    arguments.clear();
    for (int j = 0; j < task.GetTaskSpecification().NumReturns(); j++) {
      arguments.push_back(task.GetTaskSpecification().ReturnId(j));
    }
  }
  return arguments;
}

TEST_F(LineageCacheTest, TestGetUncommittedLineage) {
  // Insert two independent chains of tasks.
  std::vector<Task> tasks1;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks1, 3, std::vector<ObjectID>(), 1);
  std::vector<TaskID> task_ids1;
  for (const auto &task : tasks1) {
    task_ids1.push_back(task.GetTaskSpecification().TaskId());
  }

  std::vector<Task> tasks2;
  auto return_values2 =
      InsertTaskChain(lineage_cache_, tasks2, 2, std::vector<ObjectID>(), 2);
  std::vector<TaskID> task_ids2;
  for (const auto &task : tasks2) {
    task_ids2.push_back(task.GetTaskSpecification().TaskId());
  }

  // Get the uncommitted lineage for the last task (the leaf) of one of the
  // chains.
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_ids1.back());
  // Check that every task in that chain is in the uncommitted lineage.
  for (auto &task_id : task_ids1) {
    ASSERT_TRUE(uncommitted_lineage.GetEntry(task_id));
  }
  // Check that every task in the independent chain is not in the uncommitted
  // lineage.
  for (auto &task_id : task_ids2) {
    ASSERT_FALSE(uncommitted_lineage.GetEntry(task_id));
  }
  // Check that every entry in the uncommitted lineage is a task in the chain
  // or is an object created by a task in the chain.
  for (auto &entry : uncommitted_lineage.GetEntries()) {
    auto task_id = ComputeTaskId(entry.first);
    ASSERT_TRUE(std::find(task_ids1.begin(), task_ids1.end(), task_id) !=
                task_ids1.end());
  }

  // Insert one task that is dependent on the previous chains of tasks.
  std::vector<Task> combined_tasks = tasks1;
  combined_tasks.insert(combined_tasks.end(), tasks2.begin(), tasks2.end());
  std::vector<ObjectID> combined_arguments = return_values1;
  combined_arguments.insert(combined_arguments.end(), return_values2.begin(),
                            return_values2.end());
  InsertTaskChain(lineage_cache_, combined_tasks, 1, combined_arguments, 1);
  std::vector<TaskID> combined_task_ids;
  for (const auto &task : combined_tasks) {
    combined_task_ids.push_back(task.GetTaskSpecification().TaskId());
  }

  // Get the uncommitted lineage for the inserted task.
  uncommitted_lineage = lineage_cache_.GetUncommittedLineage(combined_task_ids.back());
  // Check that every task inserted so far is in the uncommitted lineage.
  for (auto &task_id : combined_task_ids) {
    ASSERT_TRUE(uncommitted_lineage.GetEntry(task_id));
  }
  // Check that every entry in the uncommitted lineage is an inserted task or
  // is an object created by an inserted task.
  for (auto &entry : uncommitted_lineage.GetEntries()) {
    auto task_id = ComputeTaskId(entry.first);
    ASSERT_TRUE(std::find(combined_task_ids.begin(), combined_task_ids.end(), task_id) !=
                combined_task_ids.end());
  }
}

void CheckFlush(LineageCache &lineage_cache, MockGcs &mock_gcs, size_t num_tasks_flushed, size_t num_objects_flushed) {
  RAY_CHECK_OK(lineage_cache.Flush());
  ASSERT_EQ(mock_gcs.TaskTable().size(), num_tasks_flushed);
  ASSERT_EQ(mock_gcs.ObjectTable().size(), num_objects_flushed);
}

void MarkTaskReturnValuesReady(LineageCache &lineage_cache, const Task &task) {
  for (int64_t i = 0; i < task.GetTaskSpecification().NumReturns(); i++) {
    auto return_id = task.GetTaskSpecification().ReturnId(i);
    lineage_cache.AddReadyObject(return_id, false);
  }
}

TEST_F(LineageCacheTest, TestWritebackNoneReady) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  size_t num_objects_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Check that when no tasks have been marked as ready, we do not flush any
  // entries.
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);
}

TEST_F(LineageCacheTest, TestWritebackReady) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  size_t num_objects_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Check that after marking the first task as ready, we flush only that task.
  lineage_cache_.AddReadyTask(tasks.front());
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);

  // Check that after marking the first task's return values as ready, we flush
  // those objects.
  MarkTaskReturnValuesReady(lineage_cache_, tasks.front());
  num_objects_flushed += tasks.front().GetTaskSpecification().NumReturns();
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);
}

TEST_F(LineageCacheTest, TestWritebackOrder) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  size_t num_objects_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Mark all tasks and objects as ready.
  for (const auto &task : tasks) {
    lineage_cache_.AddReadyTask(task);
    MarkTaskReturnValuesReady(lineage_cache_, task);
  }
  // Check that we write back the tasks in order of data dependencies.
  for (size_t i = 0; i < tasks.size(); i++) {
    num_tasks_flushed++;
    num_objects_flushed++;
    CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);
    // Flush acknowledgements. The next task and object should be able to be
    // written.
    mock_gcs_.Flush();
  }
}

TEST_F(LineageCacheTest, TestWritebackPartiallyReady) {
  // Insert a task T1 with two return values and a task T2 that is dependent on
  // both values.
  size_t num_tasks_flushed = 0;
  size_t num_objects_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 2, std::vector<ObjectID>(), 2);

  // Mark T1 and T2 as ready, but only mark one return value of T1 ready.
  for (const auto &task : tasks) {
    lineage_cache_.AddReadyTask(task);
    lineage_cache_.AddReadyObject(task.GetTaskSpecification().ReturnId(0), false);
  }
  // Check that only T1 and its return value are flushed.
  num_tasks_flushed++;
  num_objects_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);

  // Flush acknowledgements. T2 should still not be flushed since the other
  // dependency is not committed yet.
  mock_gcs_.Flush();
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);

  // Mark all other objects, including T2's other dependency as ready.
  for (const auto &task : tasks) {
    for (int64_t j = 1; j < task.GetTaskSpecification().NumReturns(); j++) {
      lineage_cache_.AddReadyObject(task.GetTaskSpecification().ReturnId(j), false);
    }
  }
  // Check that T2's other dependency gets flushed, but not T2.
  num_objects_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);

  // Flush acknowledgements. T2 and its return values should now be flushed.
  mock_gcs_.Flush();
  num_tasks_flushed++;
  num_objects_flushed += 2;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed, num_objects_flushed);
}

TEST_F(LineageCacheTest, TestRemoveWaitingTask) {
  // Insert a chain of dependent tasks.
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  auto task_to_remove = tasks[0];
  auto task_id_to_remove = task_to_remove.GetTaskSpecification().TaskId();
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id_to_remove);
  flatbuffers::FlatBufferBuilder fbb;
  auto uncommitted_lineage_message =
      uncommitted_lineage.ToFlatbuffer(fbb, task_id_to_remove);
  fbb.Finish(uncommitted_lineage_message);
  uncommitted_lineage =
      Lineage(*flatbuffers::GetRoot<ForwardTaskRequest>(fbb.GetBufferPointer()));

  lineage_cache_.RemoveWaitingTask(task_id_to_remove);
  lineage_cache_.AddWaitingTask(task_to_remove, uncommitted_lineage);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
