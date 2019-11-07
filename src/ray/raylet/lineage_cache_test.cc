#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/task/task.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"

#include "ray/gcs/callback.h"

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/lineage_cache.h"

#include "ray/util/test_util.h"

namespace ray {

namespace raylet {

const static JobID kDefaultJobId = JobID::FromInt(1);

const static TaskID kDefaultDriverTaskId = TaskID::ForDriverTask(kDefaultJobId);

class MockGcs : public gcs::TableInterface<TaskID, TaskTableData>,
                public gcs::PubsubInterface<TaskID> {
 public:
  MockGcs() {}

  void Subscribe(const gcs::raylet::TaskTable::WriteCallback &notification_callback) {
    notification_callback_ = notification_callback;
  }

  Status Add(const JobID &job_id, const TaskID &task_id,
             const std::shared_ptr<TaskTableData> &task_data,
             const gcs::TableInterface<TaskID, TaskTableData>::WriteCallback &done) {
    task_table_[task_id] = task_data;
    auto callback = done;
    // If we requested notifications for this task ID, send the notification as
    // part of the callback.
    if (subscribed_tasks_.count(task_id) == 1) {
      callback = [this, done](ray::gcs::RedisGcsClient *client, const TaskID &task_id,
                              const TaskTableData &data) {
        done(client, task_id, data);
        // If we're subscribed to the task to be added, also send a
        // subscription notification.
        notification_callback_(client, task_id, data);
      };
    }

    callbacks_.push_back(
        std::pair<gcs::raylet::TaskTable::WriteCallback, TaskID>(callback, task_id));
    num_task_adds_++;
    return ray::Status::OK();
  }

  Status RemoteAdd(const TaskID &task_id, std::shared_ptr<TaskTableData> task_data) {
    task_table_[task_id] = task_data;
    // Send a notification after the add if the lineage cache requested
    // notifications for this key.
    bool send_notification = (subscribed_tasks_.count(task_id) == 1);
    auto callback = [this, send_notification](ray::gcs::RedisGcsClient *client,
                                              const TaskID &task_id,
                                              const TaskTableData &data) {
      if (send_notification) {
        notification_callback_(client, task_id, data);
      }
    };
    return Add(JobID::Nil(), task_id, task_data, callback);
  }

  Status RequestNotifications(const JobID &job_id, const TaskID &task_id,
                              const ClientID &client_id,
                              const gcs::StatusCallback &done) {
    subscribed_tasks_.insert(task_id);
    if (task_table_.count(task_id) == 1) {
      callbacks_.push_back({notification_callback_, task_id});
    }
    num_requested_notifications_ += 1;
    return ray::Status::OK();
  }

  Status CancelNotifications(const JobID &job_id, const TaskID &task_id,
                             const ClientID &client_id, const gcs::StatusCallback &done) {
    subscribed_tasks_.erase(task_id);
    return ray::Status::OK();
  }

  void Flush() {
    auto callbacks = std::move(callbacks_);
    callbacks_.clear();
    for (const auto &callback : callbacks) {
      callback.first(NULL, callback.second, *task_table_[callback.second]);
    }
  }

  const std::unordered_map<TaskID, std::shared_ptr<TaskTableData>> &TaskTable() const {
    return task_table_;
  }

  const std::unordered_set<TaskID> &SubscribedTasks() const { return subscribed_tasks_; }

  const int NumRequestedNotifications() const { return num_requested_notifications_; }

  const int NumTaskAdds() const { return num_task_adds_; }

 private:
  std::unordered_map<TaskID, std::shared_ptr<TaskTableData>> task_table_;
  std::vector<std::pair<gcs::raylet::TaskTable::WriteCallback, TaskID>> callbacks_;
  gcs::raylet::TaskTable::WriteCallback notification_callback_;
  std::unordered_set<TaskID> subscribed_tasks_;
  int num_requested_notifications_ = 0;
  int num_task_adds_ = 0;
};

class LineageCacheTest : public ::testing::Test {
 public:
  LineageCacheTest()
      : max_lineage_size_(10),
        num_notifications_(0),
        mock_gcs_(),
        lineage_cache_(ClientID::FromRandom(), mock_gcs_, mock_gcs_, max_lineage_size_) {
    mock_gcs_.Subscribe([this](ray::gcs::RedisGcsClient *client, const TaskID &task_id,
                               const TaskTableData &data) {
      lineage_cache_.HandleEntryCommitted(task_id);
      num_notifications_++;
    });
  }

 protected:
  uint64_t max_lineage_size_;
  uint64_t num_notifications_;
  MockGcs mock_gcs_;
  LineageCache lineage_cache_;
};

static inline Task ExampleTask(const std::vector<ObjectID> &arguments,
                               uint64_t num_returns) {
  TaskSpecBuilder builder;
  builder.SetCommonTaskSpec(RandomTaskId(), Language::PYTHON, {"", "", ""}, JobID::Nil(),
                            RandomTaskId(), 0, RandomTaskId(), num_returns, {}, {});
  for (const auto &arg : arguments) {
    builder.AddByRefArg(arg);
  }
  rpc::TaskExecutionSpec execution_spec_message;
  execution_spec_message.set_num_forwards(1);
  return Task(builder.Build(), TaskExecutionSpecification(execution_spec_message));
}

/// Helper method to create a Lineage object with a single task.
Lineage CreateSingletonLineage(const Task &task) {
  Lineage singleton_lineage;
  singleton_lineage.SetEntry(task, GcsStatus::UNCOMMITTED);
  return singleton_lineage;
}

std::vector<ObjectID> InsertTaskChain(LineageCache &lineage_cache,
                                      std::vector<Task> &inserted_tasks, int chain_size,
                                      const std::vector<ObjectID> &initial_arguments,
                                      int64_t num_returns) {
  std::vector<ObjectID> arguments = initial_arguments;
  for (int i = 0; i < chain_size; i++) {
    auto task = ExampleTask(arguments, num_returns);
    Lineage lineage = CreateSingletonLineage(task);
    lineage_cache.AddUncommittedLineage(task.GetTaskSpecification().TaskId(), lineage);
    inserted_tasks.push_back(task);
    arguments.clear();
    for (size_t j = 0; j < task.GetTaskSpecification().NumReturns(); j++) {
      arguments.push_back(task.GetTaskSpecification().ReturnIdForPlasma(j));
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

  // Get the uncommitted lineage for the last task (the leaf) of one of the chains.
  auto uncommitted_lineage =
      lineage_cache_.GetUncommittedLineage(task_ids1.back(), ClientID::Nil());
  // Check that the uncommitted lineage is exactly equal to the first chain of tasks.
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
  uncommitted_lineage =
      lineage_cache_.GetUncommittedLineage(combined_task_ids.back(), ClientID::Nil());
  // Check that the uncommitted lineage is exactly equal to the entire set of
  // tasks inserted so far.
  ASSERT_EQ(combined_task_ids.size(), uncommitted_lineage.GetEntries().size());
  for (auto &task_id : combined_task_ids) {
    ASSERT_TRUE(uncommitted_lineage.GetEntry(task_id));
  }
}

TEST_F(LineageCacheTest, TestDuplicateUncommittedLineage) {
  // Insert a chain of tasks.
  std::vector<Task> tasks;
  auto return_values =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);
  std::vector<TaskID> task_ids;
  for (const auto &task : tasks) {
    task_ids.push_back(task.GetTaskSpecification().TaskId());
  }
  // Check that we subscribed to each of the uncommitted tasks.
  ASSERT_EQ(mock_gcs_.NumRequestedNotifications(), task_ids.size());

  // Check that if we add the same tasks as UNCOMMITTED again, we do not issue
  // duplicate subscribe requests.
  Lineage duplicate_lineage;
  for (const auto &task : tasks) {
    duplicate_lineage.SetEntry(task, GcsStatus::UNCOMMITTED);
  }
  lineage_cache_.AddUncommittedLineage(task_ids.back(), duplicate_lineage);
  ASSERT_EQ(mock_gcs_.NumRequestedNotifications(), task_ids.size());

  // Check that if we commit one of the tasks, we still do not issue any
  // duplicate subscribe requests.
  lineage_cache_.CommitTask(tasks.front());
  lineage_cache_.AddUncommittedLineage(task_ids.back(), duplicate_lineage);
  ASSERT_EQ(mock_gcs_.NumRequestedNotifications(), task_ids.size());
}

TEST_F(LineageCacheTest, TestMarkTaskAsForwarded) {
  // Insert chain of tasks.
  std::vector<Task> tasks;
  auto return_values =
      InsertTaskChain(lineage_cache_, tasks, 4, std::vector<ObjectID>(), 1);
  std::vector<TaskID> task_ids;
  for (const auto &task : tasks) {
    task_ids.push_back(task.GetTaskSpecification().TaskId());
  }

  auto node_id = ClientID::FromRandom();
  auto node_id2 = ClientID::FromRandom();
  auto forwarded_task_id = task_ids[task_ids.size() - 2];
  auto remaining_task_id = task_ids[task_ids.size() - 1];
  lineage_cache_.MarkTaskAsForwarded(forwarded_task_id, node_id);

  auto uncommitted_lineage =
      lineage_cache_.GetUncommittedLineage(remaining_task_id, node_id);
  auto uncommitted_lineage_all =
      lineage_cache_.GetUncommittedLineage(remaining_task_id, node_id2);

  ASSERT_EQ(1, uncommitted_lineage.GetEntries().size());
  ASSERT_EQ(4, uncommitted_lineage_all.GetEntries().size());
  ASSERT_TRUE(uncommitted_lineage.GetEntry(remaining_task_id));

  // Check that lineage of requested task includes itself, regardless of whether
  // it has been forwarded before.
  auto uncommitted_lineage_forwarded =
      lineage_cache_.GetUncommittedLineage(forwarded_task_id, node_id);
  ASSERT_EQ(1, uncommitted_lineage_forwarded.GetEntries().size());
}

TEST_F(LineageCacheTest, TestWritebackReady) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);

  // Check that when no tasks have been marked as ready, we do not flush any
  // entries.
  ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);

  // Check that after marking the first task as ready, we flush only that task.
  ASSERT_TRUE(lineage_cache_.CommitTask(tasks.front()));
  num_tasks_flushed++;
  ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
}

TEST_F(LineageCacheTest, TestWritebackOrder) {
  // Insert a chain of dependent tasks.
  std::vector<Task> tasks;
  InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);
  size_t num_tasks_flushed = tasks.size();

  // Mark all tasks as ready. All tasks should be flushed.
  for (const auto &task : tasks) {
    ASSERT_TRUE(lineage_cache_.CommitTask(task));
  }

  ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
}

TEST_F(LineageCacheTest, TestEvictChain) {
  // Create a chain of 3 tasks.
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < 3; i++) {
    auto task = ExampleTask(arguments, 1);
    tasks.push_back(task);
    arguments = {task.GetTaskSpecification().ReturnIdForPlasma(0)};
  }

  Lineage uncommitted_lineage;
  for (const auto &task : tasks) {
    uncommitted_lineage.SetEntry(task, GcsStatus::UNCOMMITTED);
  }
  // Mark the last task as ready to flush.
  lineage_cache_.AddUncommittedLineage(tasks.back().GetTaskSpecification().TaskId(),
                                       uncommitted_lineage);
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), tasks.size());
  ASSERT_TRUE(lineage_cache_.CommitTask(tasks.back()));
  num_tasks_flushed++;
  ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
  // Flush acknowledgements. The lineage cache should receive the commit for
  // the flushed task, but its lineage should not be evicted yet.
  mock_gcs_.Flush();
  ASSERT_EQ(lineage_cache_
                .GetUncommittedLineage(tasks.back().GetTaskSpecification().TaskId(),
                                       ClientID::Nil())
                .GetEntries()
                .size(),
            tasks.size());
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), tasks.size());

  // Simulate executing the task on a remote node and adding it to the GCS.
  auto task_data = std::make_shared<TaskTableData>();
  RAY_CHECK_OK(
      mock_gcs_.RemoteAdd(tasks.at(1).GetTaskSpecification().TaskId(), task_data));
  mock_gcs_.Flush();
  ASSERT_EQ(lineage_cache_
                .GetUncommittedLineage(tasks.back().GetTaskSpecification().TaskId(),
                                       ClientID::Nil())
                .GetEntries()
                .size(),
            tasks.size());
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), tasks.size());

  // Simulate executing the task on a remote node and adding it to the GCS.
  RAY_CHECK_OK(
      mock_gcs_.RemoteAdd(tasks.at(0).GetTaskSpecification().TaskId(), task_data));
  mock_gcs_.Flush();
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), 0);
  ASSERT_EQ(lineage_cache_.GetLineage().GetChildrenSize(), 0);
}

TEST_F(LineageCacheTest, TestEvictManyParents) {
  // Create some independent tasks.
  std::vector<Task> parent_tasks;
  std::vector<ObjectID> arguments;
  for (int i = 0; i < 10; i++) {
    auto task = ExampleTask({}, 1);
    parent_tasks.push_back(task);
    arguments.push_back(task.GetTaskSpecification().ReturnIdForPlasma(0));
    auto lineage = CreateSingletonLineage(task);
    lineage_cache_.AddUncommittedLineage(task.GetTaskSpecification().TaskId(), lineage);
  }
  // Create a child task that is dependent on all of the previous tasks.
  auto child_task = ExampleTask(arguments, 1);
  auto lineage = CreateSingletonLineage(child_task);
  lineage_cache_.AddUncommittedLineage(child_task.GetTaskSpecification().TaskId(),
                                       lineage);

  // Flush the child task. Make sure that it remains in the cache, since none
  // of its parents have been committed yet, and that the uncommitted lineage
  // still includes all of the parent tasks.
  size_t total_tasks = parent_tasks.size() + 1;
  lineage_cache_.CommitTask(child_task);
  mock_gcs_.Flush();
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), total_tasks);
  ASSERT_EQ(lineage_cache_
                .GetUncommittedLineage(child_task.GetTaskSpecification().TaskId(),
                                       ClientID::Nil())
                .GetEntries()
                .size(),
            total_tasks);

  // Flush each parent task and check for eviction safety.
  for (const auto &parent_task : parent_tasks) {
    lineage_cache_.CommitTask(parent_task);
    mock_gcs_.Flush();
    total_tasks--;
    if (total_tasks > 1) {
      // Each task should be evicted as soon as its commit is acknowledged,
      // since the parent tasks have no dependencies.
      ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), total_tasks);
      ASSERT_EQ(lineage_cache_
                    .GetUncommittedLineage(child_task.GetTaskSpecification().TaskId(),
                                           ClientID::Nil())
                    .GetEntries()
                    .size(),
                total_tasks);
    } else {
      // After the last task has been committed, then the child task should
      // also be evicted. The lineage cache should now be empty.
      ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), 0);
    }
  }
  ASSERT_EQ(lineage_cache_.GetLineage().GetChildrenSize(), 0);
}

TEST_F(LineageCacheTest, TestEviction) {
  // Insert a chain of dependent tasks.
  uint64_t lineage_size = max_lineage_size_ + 1;
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  InsertTaskChain(lineage_cache_, tasks, lineage_size, std::vector<ObjectID>(), 1);

  // Check that the last task in the chain still has all tasks in its
  // uncommitted lineage.
  const auto last_task_id = tasks.back().GetTaskSpecification().TaskId();
  auto uncommitted_lineage =
      lineage_cache_.GetUncommittedLineage(last_task_id, ClientID::Nil());
  ASSERT_EQ(uncommitted_lineage.GetEntries().size(), lineage_size);

  // Simulate executing the first task on a remote node and adding it to the
  // GCS.
  auto task_data = std::make_shared<TaskTableData>();
  auto it = tasks.begin();
  RAY_CHECK_OK(mock_gcs_.RemoteAdd(it->GetTaskSpecification().TaskId(), task_data));
  it++;
  // Check that the remote task is flushed.
  num_tasks_flushed++;
  mock_gcs_.Flush();
  ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
  // Check that the last task in the chain still has all tasks in its
  // uncommitted lineage.
  ASSERT_EQ(uncommitted_lineage.GetEntries().size(), lineage_size);
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(),
            lineage_size - num_tasks_flushed);

  // Simulate executing all the rest of the tasks except the last one on a
  // remote node and adding them to the GCS.
  tasks.pop_back();
  for (; it != tasks.end(); it++) {
    RAY_CHECK_OK(mock_gcs_.RemoteAdd(it->GetTaskSpecification().TaskId(), task_data));
    // Check that the remote task is flushed.
    num_tasks_flushed++;
    mock_gcs_.Flush();
    ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
    ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(),
              lineage_size - num_tasks_flushed);
  }
  // All tasks have now been flushed. Check that enough lineage has been
  // evicted that the uncommitted lineage is now less than the maximum size.
  uncommitted_lineage =
      lineage_cache_.GetUncommittedLineage(last_task_id, ClientID::Nil());
  ASSERT_TRUE(uncommitted_lineage.GetEntries().size() < max_lineage_size_);
  // The remaining task should have no uncommitted lineage.
  ASSERT_EQ(uncommitted_lineage.GetEntries().size(), 1);
  ASSERT_EQ(lineage_cache_.GetLineage().GetChildrenSize(), 1);
}

TEST_F(LineageCacheTest, TestOutOfOrderEviction) {
  // Insert a chain of dependent tasks that is more than twice as long as the
  // maximum lineage size. This will ensure that we request notifications for
  // at most 2 remote tasks.
  uint64_t lineage_size = (2 * max_lineage_size_) + 2;
  size_t num_tasks_flushed = 0;
  std::vector<Task> tasks;
  InsertTaskChain(lineage_cache_, tasks, lineage_size, std::vector<ObjectID>(), 1);

  // Check that the last task in the chain still has all tasks in its
  // uncommitted lineage.
  const auto last_task_id = tasks.back().GetTaskSpecification().TaskId();
  auto uncommitted_lineage =
      lineage_cache_.GetUncommittedLineage(last_task_id, ClientID::Nil());
  ASSERT_EQ(uncommitted_lineage.GetEntries().size(), lineage_size);
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), lineage_size);

  // Simulate executing the rest of the tasks on a remote node and receiving
  // the notifications from the GCS in reverse order of execution.
  auto last_task = tasks.front();
  tasks.erase(tasks.begin());
  for (auto it = tasks.rbegin(); it != tasks.rend(); it++) {
    auto task_data = std::make_shared<TaskTableData>();
    RAY_CHECK_OK(mock_gcs_.RemoteAdd(it->GetTaskSpecification().TaskId(), task_data));
    // Check that the remote task is flushed.
    num_tasks_flushed++;
    mock_gcs_.Flush();
    ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
    ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), lineage_size);
  }
  // Flush the last task. The lineage should not get evicted until this task's
  // commit is received.
  auto task_data = std::make_shared<TaskTableData>();
  RAY_CHECK_OK(mock_gcs_.RemoteAdd(last_task.GetTaskSpecification().TaskId(), task_data));
  num_tasks_flushed++;
  mock_gcs_.Flush();
  ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), 0);
  ASSERT_EQ(lineage_cache_.GetLineage().GetChildrenSize(), 0);
}

TEST_F(LineageCacheTest, TestEvictionUncommittedChildren) {
  // Insert a chain of dependent tasks.
  size_t num_tasks_flushed = 0;
  uint64_t lineage_size = max_lineage_size_ + 1;
  std::vector<Task> tasks;
  InsertTaskChain(lineage_cache_, tasks, lineage_size, std::vector<ObjectID>(), 1);

  // Add more tasks to the lineage cache that will remain local. Each of these
  // tasks is dependent one of the tasks that was forwarded above.
  for (const auto &task : tasks) {
    auto return_id = task.GetTaskSpecification().ReturnIdForPlasma(0);
    auto dependent_task = ExampleTask({return_id}, 1);
    auto lineage = CreateSingletonLineage(dependent_task);
    lineage_cache_.AddUncommittedLineage(dependent_task.GetTaskSpecification().TaskId(),
                                         lineage);
    ASSERT_TRUE(lineage_cache_.CommitTask(dependent_task));
    // Once the forwarded tasks are evicted from the lineage cache, we expect
    // each of these dependent tasks to be flushed, since all of their
    // dependencies have been committed.
    num_tasks_flushed++;
  }

  // Simulate executing the tasks on the remote node in reverse order and
  // adding them to the GCS. Lineage at the local node should not get evicted
  // until after the final remote task is executed, since a task can only be
  // evicted once all of its ancestors have been committed.
  for (auto it = tasks.rbegin(); it != tasks.rend(); it++) {
    auto task_data = std::make_shared<TaskTableData>();
    ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), lineage_size * 2);
    RAY_CHECK_OK(mock_gcs_.RemoteAdd(it->GetTaskSpecification().TaskId(), task_data));
    num_tasks_flushed++;
    mock_gcs_.Flush();
    ASSERT_EQ(mock_gcs_.TaskTable().size(), num_tasks_flushed);
  }
  // Check that after the final remote task is executed, all local lineage is
  // now evicted.
  ASSERT_EQ(lineage_cache_.GetLineage().GetEntries().size(), 0);
  ASSERT_EQ(lineage_cache_.GetLineage().GetChildrenSize(), 0);
}

TEST_F(LineageCacheTest, TestFlushAllUncommittedTasks) {
  // Insert a chain of tasks.
  std::vector<Task> tasks;
  auto return_values =
      InsertTaskChain(lineage_cache_, tasks, 3, std::vector<ObjectID>(), 1);
  std::vector<TaskID> task_ids;
  for (const auto &task : tasks) {
    task_ids.push_back(task.GetTaskSpecification().TaskId());
  }
  // Check that we subscribed to each of the uncommitted tasks.
  ASSERT_EQ(mock_gcs_.NumRequestedNotifications(), task_ids.size());

  // Flush all uncommitted tasks and make sure we add all tasks to
  // the task table.
  lineage_cache_.FlushAllUncommittedTasks();
  ASSERT_EQ(mock_gcs_.NumTaskAdds(), tasks.size());
  // Flush again and make sure there are no new tasks added to the
  // task table.
  lineage_cache_.FlushAllUncommittedTasks();
  ASSERT_EQ(mock_gcs_.NumTaskAdds(), tasks.size());

  // Flush all GCS notifications.
  mock_gcs_.Flush();
  // Make sure that we unsubscribed to the uncommitted tasks before
  // we flushed them.
  ASSERT_EQ(num_notifications_, 0);

  // Flush again and make sure there are no new tasks added to the
  // task table.
  lineage_cache_.FlushAllUncommittedTasks();
  ASSERT_EQ(mock_gcs_.NumTaskAdds(), tasks.size());
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
