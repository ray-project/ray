#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/lineage_cache.h"
#include "ray/raylet/task.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

class MockGcs : public gcs::TableInterface<TaskID, protocol::Task>,
                public gcs::PubsubInterface<TaskID> {
 public:
  MockGcs() {}

  void Subscribe(const gcs::raylet::TaskTable::WriteCallback &notification_callback) {
    notification_callback_ = notification_callback;
  }

  Status Add(const JobID &job_id, const TaskID &task_id,
             std::shared_ptr<protocol::TaskT> task_data,
             const gcs::TableInterface<TaskID, protocol::Task>::WriteCallback &done) {
    task_table_[task_id] = task_data;
    callbacks_.push_back(
        std::pair<gcs::raylet::TaskTable::WriteCallback, TaskID>(done, task_id));
    return ray::Status::OK();
  }

  Status RemoteAdd(const TaskID &task_id, std::shared_ptr<protocol::TaskT> task_data) {
    task_table_[task_id] = task_data;
    // Send a notification after the add if the lineage cache requested
    // notifications for this key.
    bool send_notification = (subscribed_tasks_.count(task_id) == 1);
    auto callback = [this, send_notification](ray::gcs::AsyncGcsClient *client,
                                              const TaskID &task_id,
                                              std::shared_ptr<protocol::TaskT> data) {
      if (send_notification) {
        notification_callback_(client, task_id, data);
      }
    };
    return Add(JobID::nil(), task_id, task_data, callback);
  }

  Status RequestNotifications(const JobID &job_id, const TaskID &task_id,
                              const ClientID &client_id) {
    subscribed_tasks_.insert(task_id);
    if (task_table_.count(task_id) == 1) {
      callbacks_.push_back({notification_callback_, task_id});
    }
    return ray::Status::OK();
  }

  Status CancelNotifications(const JobID &job_id, const TaskID &task_id,
                             const ClientID &client_id) {
    subscribed_tasks_.erase(task_id);
    return ray::Status::OK();
  }

  void Flush() {
    for (const auto &callback : callbacks_) {
      callback.first(NULL, callback.second, task_table_[callback.second]);
    }
    callbacks_.clear();
  }

  const std::unordered_map<TaskID, std::shared_ptr<protocol::TaskT>> &TaskTable() const {
    return task_table_;
  }

  const std::unordered_set<TaskID> &SubscribedTasks() const { return subscribed_tasks_; }

 private:
  std::unordered_map<TaskID, std::shared_ptr<protocol::TaskT>> task_table_;
  std::vector<std::pair<gcs::raylet::TaskTable::WriteCallback, TaskID>> callbacks_;
  gcs::raylet::TaskTable::WriteCallback notification_callback_;
  std::unordered_set<TaskID> subscribed_tasks_;
};

class LineageCacheTest : public ::testing::Test {
 public:
  LineageCacheTest()
      : mock_gcs_(), lineage_cache_(ClientID::from_random(), mock_gcs_, mock_gcs_) {
    mock_gcs_.Subscribe([this](ray::gcs::AsyncGcsClient *client, const TaskID &task_id,
                               std::shared_ptr<ray::protocol::TaskT> data) {
      lineage_cache_.HandleEntryCommitted(task_id);
    });
  }

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
  execution_spec.IncrementNumForwards();
  Task task = Task(execution_spec, spec);
  return task;
}

std::vector<ObjectID> InsertTaskChain(LineageCache &lineage_cache,
                                      std::vector<Task> &inserted_tasks, int chain_size,
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
  // Check that the uncommitted lineage is exactly equal to the first chain of
  // tasks.
  ASSERT_EQ(task_ids1.size(), uncommitted_lineage.GetEntries().size());
  for (auto &task_id : task_ids1) {
    ASSERT_TRUE(uncommitted_lineage.GetEntry(task_id));
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
  // Check that the uncommitted lineage is exactly equal to the entire set of
  // tasks inserted so far.
  ASSERT_EQ(combined_task_ids.size(), uncommitted_lineage.GetEntries().size());
  for (auto &task_id : combined_task_ids) {
    ASSERT_TRUE(uncommitted_lineage.GetEntry(task_id));
  }
}

void CheckFlush(LineageCache &lineage_cache, MockGcs &mock_gcs,
                size_t num_tasks_flushed) {
  RAY_CHECK_OK(lineage_cache.Flush());
  ASSERT_EQ(mock_gcs.TaskTable().size(), num_tasks_flushed);
}

TEST_F(LineageCacheTest, TestWritebackNoneReady) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Check that when no tasks have been marked as ready, we do not flush any
  // entries.
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);
}

TEST_F(LineageCacheTest, TestWritebackReady) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Check that after marking the first task as ready, we flush only that task.
  lineage_cache_.AddReadyTask(tasks.front());
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);
}

TEST_F(LineageCacheTest, TestWritebackOrder) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Mark all tasks as ready.
  for (const auto &task : tasks) {
    lineage_cache_.AddReadyTask(task);
  }
  // Check that we write back the tasks in order of data dependencies.
  for (size_t i = 0; i < tasks.size(); i++) {
    num_tasks_flushed++;
    CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);
    // Flush acknowledgements. The next task should be able to be written.
    mock_gcs_.Flush();
  }
}

TEST_F(LineageCacheTest, TestWritebackPartiallyReady) {
  // Create two independent tasks, task1 and task2, and a dependent task
  // that depends on both tasks.
  size_t num_tasks_flushed = 0;
  auto task1 = ExampleTask({}, 1);
  auto task2 = ExampleTask({}, 1);
  std::vector<ObjectID> returns;
  for (int64_t i = 0; i < task1.GetTaskSpecification().NumReturns(); i++) {
    returns.push_back(task1.GetTaskSpecification().ReturnId(i));
  }
  for (int64_t i = 0; i < task2.GetTaskSpecification().NumReturns(); i++) {
    returns.push_back(task2.GetTaskSpecification().ReturnId(i));
  }
  auto dependent_task = ExampleTask(returns, 1);
  auto dependencies = dependent_task.GetDependencies();

  // Insert all tasks as waiting for execution.
  lineage_cache_.AddWaitingTask(task1, Lineage());
  lineage_cache_.AddWaitingTask(task2, Lineage());
  lineage_cache_.AddWaitingTask(dependent_task, Lineage());

  // Mark one of the independent tasks and the dependent task as ready.
  lineage_cache_.AddReadyTask(task1);
  lineage_cache_.AddReadyTask(dependent_task);
  // Check that only the first independent task is flushed.
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);

  // Flush acknowledgements. The dependent task should still not be flushed
  // since task2 is not committed yet.
  mock_gcs_.Flush();
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);

  // Mark the other independent task as ready.
  lineage_cache_.AddReadyTask(task2);
  // Check that the other independent task gets flushed.
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);

  // Flush acknowledgements. The dependent task should now be able to be
  // written.
  mock_gcs_.Flush();
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);
}

TEST_F(LineageCacheTest, TestForwardTaskRoundTrip) {
  // Insert a chain of dependent tasks.
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Simulate removing the task and forwarding it to another node.
  auto forwarded_task = tasks[1];
  auto task_id_to_remove = forwarded_task.GetTaskSpecification().TaskId();
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id_to_remove);
  lineage_cache_.RemoveWaitingTask(task_id_to_remove);

  // Simulate receiving the task again.
  flatbuffers::FlatBufferBuilder fbb;
  auto uncommitted_lineage_message =
      uncommitted_lineage.ToFlatbuffer(fbb, task_id_to_remove);
  fbb.Finish(uncommitted_lineage_message);
  uncommitted_lineage = Lineage(
      *flatbuffers::GetRoot<protocol::ForwardTaskRequest>(fbb.GetBufferPointer()));
  lineage_cache_.AddWaitingTask(forwarded_task, uncommitted_lineage);
}

TEST_F(LineageCacheTest, TestForwardTask) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  auto return_values1 =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Simulate removing the task and forwarding it to another node.
  auto it = tasks.begin() + 1;
  auto forwarded_task = *it;
  tasks.erase(it);
  auto task_id_to_remove = forwarded_task.GetTaskSpecification().TaskId();
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id_to_remove);
  lineage_cache_.RemoveWaitingTask(task_id_to_remove);

  // Simulate executing the remaining tasks.
  for (const auto &task : tasks) {
    lineage_cache_.AddReadyTask(task);
  }
  // Check that the first task, which has no dependencies can be flushed. The
  // last task cannot be flushed since one of its dependencies has not been
  // added by the remote node yet.
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);

  // Simulate executing the task on a remote node and adding it to the GCS.
  auto task_data = std::make_shared<protocol::TaskT>();
  RAY_CHECK_OK(
      mock_gcs_.RemoteAdd(forwarded_task.GetTaskSpecification().TaskId(), task_data));
  // Check that the remote task is flushed.
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);
  ASSERT_EQ(mock_gcs_.SubscribedTasks().size(), 1);

  // Check that once we receive the callback for the remote task, we can now
  // flush the last task.
  mock_gcs_.Flush();
  num_tasks_flushed++;
  CheckFlush(lineage_cache_, mock_gcs_, num_tasks_flushed);
  ASSERT_EQ(mock_gcs_.SubscribedTasks().size(), 0);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
