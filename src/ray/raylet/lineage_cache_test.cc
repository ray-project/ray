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
    for (auto &callback : task_callbacks_) {
      callback.first(NULL, callback.second, nullptr);
    }
    for (auto &callback : object_callbacks_) {
      callback.first(NULL, callback.second, nullptr);
    }
  };

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
      : client_id_(ObjectID::from_random()),
        mock_gcs_(),
        lineage_cache_(client_id_, mock_gcs_, mock_gcs_) {}

 private:
  ClientID client_id_;
  MockGcs mock_gcs_;

 protected:
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
                                      std::vector<TaskID> &inserted_task_ids,
                                      int chain_size,
                                      const std::vector<ObjectID> &initial_arguments) {
  Lineage empty_lineage;
  std::vector<ObjectID> arguments = initial_arguments;
  for (int i = 0; i < chain_size; i++) {
    auto task = ExampleTask(arguments, i + 1);
    lineage_cache.AddWaitingTask(task, empty_lineage);
    inserted_task_ids.push_back(task.GetTaskSpecification().TaskId());
    arguments.clear();
    for (int j = 0; j < task.GetTaskSpecification().NumReturns(); j++) {
      arguments.push_back(task.GetTaskSpecification().ReturnId(j));
    }
  }
  return arguments;
}

TEST_F(LineageCacheTest, TestGetUncommittedLineage) {
  // Insert two independent chains of tasks.
  std::vector<TaskID> task_ids1;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, task_ids1, 3, std::vector<ObjectID>());
  std::vector<TaskID> task_ids2;
  auto return_values2 =
      InsertTaskChain(lineage_cache_, task_ids2, 2, std::vector<ObjectID>());

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
  std::vector<TaskID> combined_task_ids = task_ids1;
  combined_task_ids.insert(combined_task_ids.end(), task_ids2.begin(), task_ids2.end());
  std::vector<ObjectID> combined_arguments = return_values1;
  combined_arguments.insert(combined_arguments.end(), return_values2.begin(),
                            return_values2.end());
  InsertTaskChain(lineage_cache_, combined_task_ids, 1, combined_arguments);

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

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
