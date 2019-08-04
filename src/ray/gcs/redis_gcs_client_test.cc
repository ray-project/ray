#include "gtest/gtest.h"

// TODO(pcm): get rid of this and replace with the type safe plasma event loop
extern "C" {
#include "ray/thirdparty/hiredis/hiredis.h"
}

#include "ray/common/ray_config.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

/* Flush redis. */
static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

/// A helper function to generate an unique JobID.
inline JobID NextJobID() {
  static int32_t counter = 0;
  return JobID::FromInt(++counter);
}

class TestGcs : public ::testing::Test {
 public:
  TestGcs(CommandType command_type) : num_callbacks_(0), command_type_(command_type) {
    GcsClientOptions options("127.0.0.1", 6379, command_type_);
    client_ = std::make_shared<gcs::RedisGcsClient>(options);
    job_id_ = NextJobID();
  }

  virtual ~TestGcs() {
    // Clear all keys in the GCS.
    flushall_redis();
  };

  virtual void Start() = 0;

  virtual void Stop() = 0;

  uint64_t NumCallbacks() const { return num_callbacks_; }

  void IncrementNumCallbacks() { num_callbacks_++; }

 protected:
  uint64_t num_callbacks_;
  gcs::CommandType command_type_;
  std::shared_ptr<gcs::RedisGcsClient> client_;
  JobID job_id_;
};

TestGcs *test;

class TestGcsWithAsio : public TestGcs {
 public:
  TestGcsWithAsio(CommandType command_type)
      : TestGcs(command_type), io_service_(), work_(io_service_) {
    RAY_CHECK_OK(client_->Connect(io_service_));
  }

  TestGcsWithAsio() : TestGcsWithAsio(CommandType::kRegular) {}

  ~TestGcsWithAsio() {
    // Destroy the client first since it has a reference to the event loop.
    client_->Disconnect();
    client_.reset();
  }
  void Start() override { io_service_.run(); }
  void Stop() override { io_service_.stop(); }

 private:
  boost::asio::io_service io_service_;
  // Give the event loop some work so that it's forced to run until Stop() is
  // called.
  boost::asio::io_service::work work_;
};

class TestGcsWithChainAsio : public TestGcsWithAsio {
 public:
  TestGcsWithChainAsio() : TestGcsWithAsio(gcs::CommandType::kChain){};
};

/// A helper function that creates a GCS `TaskTableData` object.
std::shared_ptr<TaskTableData> CreateTaskTableData(const TaskID &task_id,
                                                   uint64_t num_returns = 0) {
  auto data = std::make_shared<TaskTableData>();
  data->mutable_task()->mutable_task_spec()->set_task_id(task_id.Binary());
  data->mutable_task()->mutable_task_spec()->set_num_returns(num_returns);
  return data;
}

/// A helper function that compare wether 2 `TaskTableData` objects are equal.
/// Note, this function only compares fields set by `CreateTaskTableData`.
bool TaskTableDataEqual(const TaskTableData &data1, const TaskTableData &data2) {
  const auto &spec1 = data1.task().task_spec();
  const auto &spec2 = data2.task().task_spec();
  return (spec1.task_id() == spec2.task_id() &&
          spec1.num_returns() == spec2.num_returns());
}

void TestTableLookup(const JobID &job_id, std::shared_ptr<gcs::RedisGcsClient> client) {
  const auto task_id = TaskID::FromRandom();
  const auto data = CreateTaskTableData(task_id);

  // Check that we added the correct task.
  auto add_callback = [task_id, data](gcs::RedisGcsClient *client, const TaskID &id,
                                      const TaskTableData &d) {
    ASSERT_EQ(id, task_id);
    ASSERT_TRUE(TaskTableDataEqual(*data, d));
  };

  // Check that the lookup returns the added task.
  auto lookup_callback = [task_id, data](gcs::RedisGcsClient *client, const TaskID &id,
                                         const TaskTableData &d) {
    ASSERT_EQ(id, task_id);
    ASSERT_TRUE(TaskTableDataEqual(*data, d));
    test->Stop();
  };

  // Check that the lookup does not return an empty entry.
  auto failure_callback = [](gcs::RedisGcsClient *client, const TaskID &id) {
    RAY_CHECK(false);
  };

  // Add the task, then do a lookup.
  RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id, data, add_callback));
  RAY_CHECK_OK(client->raylet_task_table().Lookup(job_id, task_id, lookup_callback,
                                                  failure_callback));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
}

// Convenient macro to test across {ae, asio} x {regular, chain} x {the tests}.
// Undefined at the end.
#define TEST_MACRO(FIXTURE, TEST) \
  TEST_F(FIXTURE, TEST) {         \
    test = this;                  \
    TEST(job_id_, client_);       \
  }

TEST_MACRO(TestGcsWithAsio, TestTableLookup);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAsio, TestTableLookup);
#endif

void TestLogLookup(const JobID &job_id, std::shared_ptr<gcs::RedisGcsClient> client) {
  // Append some entries to the log at an object ID.
  TaskID task_id = TaskID::FromRandom();
  std::vector<std::string> node_manager_ids = {"abc", "def", "ghi"};
  for (auto &node_manager_id : node_manager_ids) {
    auto data = std::make_shared<TaskReconstructionData>();
    data->set_node_manager_id(node_manager_id);
    // Check that we added the correct object entries.
    auto add_callback = [task_id, data](gcs::RedisGcsClient *client, const TaskID &id,
                                        const TaskReconstructionData &d) {
      ASSERT_EQ(id, task_id);
      ASSERT_EQ(data->node_manager_id(), d.node_manager_id());
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Append(job_id, task_id, data, add_callback));
  }

  // Check that lookup returns the added object entries.
  auto lookup_callback = [task_id, node_manager_ids](
                             gcs::RedisGcsClient *client, const TaskID &id,
                             const std::vector<TaskReconstructionData> &data) {
    ASSERT_EQ(id, task_id);
    for (const auto &entry : data) {
      ASSERT_EQ(entry.node_manager_id(), node_manager_ids[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == node_manager_ids.size()) {
      test->Stop();
    }
  };

  // Do a lookup at the object ID.
  RAY_CHECK_OK(
      client->task_reconstruction_log().Lookup(job_id, task_id, lookup_callback));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), node_manager_ids.size());
}

TEST_F(TestGcsWithAsio, TestLogLookup) {
  test = this;
  TestLogLookup(job_id_, client_);
}

void TestTableLookupFailure(const JobID &job_id,
                            std::shared_ptr<gcs::RedisGcsClient> client) {
  TaskID task_id = TaskID::FromRandom();

  // Check that the lookup does not return data.
  auto lookup_callback = [](gcs::RedisGcsClient *client, const TaskID &id,
                            const TaskTableData &d) { RAY_CHECK(false); };

  // Check that the lookup returns an empty entry.
  auto failure_callback = [task_id](gcs::RedisGcsClient *client, const TaskID &id) {
    ASSERT_EQ(id, task_id);
    test->Stop();
  };

  // Lookup the task. We have not done any writes, so the key should be empty.
  RAY_CHECK_OK(client->raylet_task_table().Lookup(job_id, task_id, lookup_callback,
                                                  failure_callback));
  // Run the event loop. The loop will only stop if the failure callback is
  // called (or an assertion failure).
  test->Start();
}

TEST_MACRO(TestGcsWithAsio, TestTableLookupFailure);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAsio, TestTableLookupFailure);
#endif

void TestLogAppendAt(const JobID &job_id, std::shared_ptr<gcs::RedisGcsClient> client) {
  TaskID task_id = TaskID::FromRandom();
  std::vector<std::string> node_manager_ids = {"A", "B"};
  std::vector<std::shared_ptr<TaskReconstructionData>> data_log;
  for (const auto &node_manager_id : node_manager_ids) {
    auto data = std::make_shared<TaskReconstructionData>();
    data->set_node_manager_id(node_manager_id);
    data_log.push_back(data);
  }

  // Check that we added the correct task.
  auto failure_callback = [task_id](gcs::RedisGcsClient *client, const TaskID &id,
                                    const TaskReconstructionData &d) {
    ASSERT_EQ(id, task_id);
    test->IncrementNumCallbacks();
  };

  // Will succeed.
  RAY_CHECK_OK(client->task_reconstruction_log().Append(job_id, task_id, data_log.front(),
                                                        /*done callback=*/nullptr));
  // Append at index 0 will fail.
  RAY_CHECK_OK(client->task_reconstruction_log().AppendAt(
      job_id, task_id, data_log[1],
      /*done callback=*/nullptr, failure_callback, /*log_length=*/0));

  // Append at index 2 will fail.
  RAY_CHECK_OK(client->task_reconstruction_log().AppendAt(
      job_id, task_id, data_log[1],
      /*done callback=*/nullptr, failure_callback, /*log_length=*/2));

  // Append at index 1 will succeed.
  RAY_CHECK_OK(client->task_reconstruction_log().AppendAt(
      job_id, task_id, data_log[1],
      /*done callback=*/nullptr, failure_callback, /*log_length=*/1));

  auto lookup_callback = [node_manager_ids](
                             gcs::RedisGcsClient *client, const TaskID &id,
                             const std::vector<TaskReconstructionData> &data) {
    std::vector<std::string> appended_managers;
    for (const auto &entry : data) {
      appended_managers.push_back(entry.node_manager_id());
    }
    ASSERT_EQ(appended_managers, node_manager_ids);
    test->Stop();
  };
  RAY_CHECK_OK(
      client->task_reconstruction_log().Lookup(job_id, task_id, lookup_callback));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), 2);
}

TEST_F(TestGcsWithAsio, TestLogAppendAt) {
  test = this;
  TestLogAppendAt(job_id_, client_);
}

void TestSet(const JobID &job_id, std::shared_ptr<gcs::RedisGcsClient> client) {
  // Add some entries to the set at an object ID.
  ObjectID object_id = ObjectID::FromRandom();
  std::vector<std::string> managers = {"abc", "def", "ghi"};
  for (auto &manager : managers) {
    auto data = std::make_shared<ObjectTableData>();
    data->set_manager(manager);
    // Check that we added the correct object entries.
    auto add_callback = [object_id, data](gcs::RedisGcsClient *client, const ObjectID &id,
                                          const ObjectTableData &d) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data->manager(), d.manager());
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, add_callback));
  }

  // Check that lookup returns the added object entries.
  auto lookup_callback = [object_id, managers](gcs::RedisGcsClient *client,
                                               const ObjectID &id,
                                               const std::vector<ObjectTableData> &data) {
    ASSERT_EQ(id, object_id);
    ASSERT_EQ(data.size(), managers.size());
    test->IncrementNumCallbacks();
  };

  // Do a lookup at the object ID.
  RAY_CHECK_OK(client->object_table().Lookup(job_id, object_id, lookup_callback));

  for (auto &manager : managers) {
    auto data = std::make_shared<ObjectTableData>();
    data->set_manager(manager);
    // Check that we added the correct object entries.
    auto remove_entry_callback = [object_id, data](gcs::RedisGcsClient *client,
                                                   const ObjectID &id,
                                                   const ObjectTableData &d) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data->manager(), d.manager());
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->object_table().Remove(job_id, object_id, data, remove_entry_callback));
  }

  // Check that the entries are removed.
  auto lookup_callback2 = [object_id, managers](
                              gcs::RedisGcsClient *client, const ObjectID &id,
                              const std::vector<ObjectTableData> &data) {
    ASSERT_EQ(id, object_id);
    ASSERT_EQ(data.size(), 0);
    test->IncrementNumCallbacks();
    test->Stop();
  };

  // Do a lookup at the object ID.
  RAY_CHECK_OK(client->object_table().Lookup(job_id, object_id, lookup_callback2));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), managers.size() * 2 + 2);
}

TEST_F(TestGcsWithAsio, TestSet) {
  test = this;
  TestSet(job_id_, client_);
}

void TestDeleteKeysFromLog(
    const JobID &job_id, std::shared_ptr<gcs::RedisGcsClient> client,
    std::vector<std::shared_ptr<TaskReconstructionData>> &data_vector) {
  std::vector<TaskID> ids;
  TaskID task_id;
  for (auto &data : data_vector) {
    task_id = TaskID::FromRandom();
    ids.push_back(task_id);
    // Check that we added the correct object entries.
    auto add_callback = [task_id, data](gcs::RedisGcsClient *client, const TaskID &id,
                                        const TaskReconstructionData &d) {
      ASSERT_EQ(id, task_id);
      ASSERT_EQ(data->node_manager_id(), d.node_manager_id());
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Append(job_id, task_id, data, add_callback));
  }
  for (const auto &task_id : ids) {
    // Check that lookup returns the added object entries.
    auto lookup_callback = [task_id, data_vector](
                               gcs::RedisGcsClient *client, const TaskID &id,
                               const std::vector<TaskReconstructionData> &data) {
      ASSERT_EQ(id, task_id);
      ASSERT_EQ(data.size(), 1);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Lookup(job_id, task_id, lookup_callback));
  }
  if (ids.size() == 1) {
    client->task_reconstruction_log().Delete(job_id, ids[0]);
  } else {
    client->task_reconstruction_log().Delete(job_id, ids);
  }
  for (const auto &task_id : ids) {
    auto lookup_callback = [task_id](gcs::RedisGcsClient *client, const TaskID &id,
                                     const std::vector<TaskReconstructionData> &data) {
      ASSERT_EQ(id, task_id);
      ASSERT_TRUE(data.size() == 0);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Lookup(job_id, task_id, lookup_callback));
  }
}

void TestDeleteKeysFromTable(const JobID &job_id,
                             std::shared_ptr<gcs::RedisGcsClient> client,
                             std::vector<std::shared_ptr<TaskTableData>> &data_vector,
                             bool stop_at_end) {
  std::vector<TaskID> ids;
  TaskID task_id;
  for (auto &data : data_vector) {
    task_id = TaskID::FromRandom();
    ids.push_back(task_id);
    // Check that we added the correct object entries.
    auto add_callback = [task_id, data](gcs::RedisGcsClient *client, const TaskID &id,
                                        const TaskTableData &d) {
      ASSERT_EQ(id, task_id);
      ASSERT_TRUE(TaskTableDataEqual(*data, d));
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id, data, add_callback));
  }
  for (const auto &task_id : ids) {
    auto task_lookup_callback = [task_id](gcs::RedisGcsClient *client, const TaskID &id,
                                          const TaskTableData &data) {
      ASSERT_EQ(id, task_id);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->raylet_task_table().Lookup(job_id, task_id, task_lookup_callback,
                                                    nullptr));
  }
  if (ids.size() == 1) {
    client->raylet_task_table().Delete(job_id, ids[0]);
  } else {
    client->raylet_task_table().Delete(job_id, ids);
  }
  auto expected_failure_callback = [](RedisGcsClient *client, const TaskID &id) {
    ASSERT_TRUE(true);
    test->IncrementNumCallbacks();
  };
  auto undesired_callback = [](gcs::RedisGcsClient *client, const TaskID &id,
                               const TaskTableData &data) { ASSERT_TRUE(false); };
  for (size_t i = 0; i < ids.size(); ++i) {
    RAY_CHECK_OK(client->raylet_task_table().Lookup(job_id, task_id, undesired_callback,
                                                    expected_failure_callback));
  }
  if (stop_at_end) {
    auto stop_callback = [](RedisGcsClient *client, const TaskID &id) { test->Stop(); };
    RAY_CHECK_OK(
        client->raylet_task_table().Lookup(job_id, ids[0], nullptr, stop_callback));
  }
}

void TestDeleteKeysFromSet(const JobID &job_id,
                           std::shared_ptr<gcs::RedisGcsClient> client,
                           std::vector<std::shared_ptr<ObjectTableData>> &data_vector) {
  std::vector<ObjectID> ids;
  ObjectID object_id;
  for (auto &data : data_vector) {
    object_id = ObjectID::FromRandom();
    ids.push_back(object_id);
    // Check that we added the correct object entries.
    auto add_callback = [object_id, data](gcs::RedisGcsClient *client, const ObjectID &id,
                                          const ObjectTableData &d) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data->manager(), d.manager());
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, add_callback));
  }
  for (const auto &object_id : ids) {
    // Check that lookup returns the added object entries.
    auto lookup_callback = [object_id, data_vector](
                               gcs::RedisGcsClient *client, const ObjectID &id,
                               const std::vector<ObjectTableData> &data) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data.size(), 1);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Lookup(job_id, object_id, lookup_callback));
  }
  if (ids.size() == 1) {
    client->object_table().Delete(job_id, ids[0]);
  } else {
    client->object_table().Delete(job_id, ids);
  }
  for (const auto &object_id : ids) {
    auto lookup_callback = [object_id](gcs::RedisGcsClient *client, const ObjectID &id,
                                       const std::vector<ObjectTableData> &data) {
      ASSERT_EQ(id, object_id);
      ASSERT_TRUE(data.size() == 0);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Lookup(job_id, object_id, lookup_callback));
  }
}

// Test delete function for keys of Log or Table.
void TestDeleteKeys(const JobID &job_id, std::shared_ptr<gcs::RedisGcsClient> client) {
  // Test delete function for keys of Log.
  std::vector<std::shared_ptr<TaskReconstructionData>> task_reconstruction_vector;
  auto AppendTaskReconstructionData = [&task_reconstruction_vector](size_t add_count) {
    for (size_t i = 0; i < add_count; ++i) {
      auto data = std::make_shared<TaskReconstructionData>();
      data->set_node_manager_id(ObjectID::FromRandom().Hex());
      task_reconstruction_vector.push_back(data);
    }
  };
  // Test one element case.
  AppendTaskReconstructionData(1);
  ASSERT_EQ(task_reconstruction_vector.size(), 1);
  TestDeleteKeysFromLog(job_id, client, task_reconstruction_vector);
  // Test the case for more than one elements and less than
  // maximum_gcs_deletion_batch_size.
  AppendTaskReconstructionData(RayConfig::instance().maximum_gcs_deletion_batch_size() /
                               2);
  ASSERT_GT(task_reconstruction_vector.size(), 1);
  ASSERT_LT(task_reconstruction_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromLog(job_id, client, task_reconstruction_vector);
  // Test the case for more than maximum_gcs_deletion_batch_size.
  // The Delete function will split the data into two commands.
  AppendTaskReconstructionData(RayConfig::instance().maximum_gcs_deletion_batch_size() /
                               2);
  ASSERT_GT(task_reconstruction_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromLog(job_id, client, task_reconstruction_vector);

  // Test delete function for keys of Table.
  std::vector<std::shared_ptr<TaskTableData>> task_vector;
  auto AppendTaskData = [&task_vector](size_t add_count) {
    for (size_t i = 0; i < add_count; ++i) {
      task_vector.push_back(CreateTaskTableData(TaskID::FromRandom()));
    }
  };
  AppendTaskData(1);
  ASSERT_EQ(task_vector.size(), 1);
  TestDeleteKeysFromTable(job_id, client, task_vector, false);

  AppendTaskData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(task_vector.size(), 1);
  ASSERT_LT(task_vector.size(), RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromTable(job_id, client, task_vector, false);

  AppendTaskData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(task_vector.size(), RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromTable(job_id, client, task_vector, true);

  test->Start();
  ASSERT_GT(test->NumCallbacks(),
            9 * RayConfig::instance().maximum_gcs_deletion_batch_size());

  // Test delete function for keys of Set.
  std::vector<std::shared_ptr<ObjectTableData>> object_vector;
  auto AppendObjectData = [&object_vector](size_t add_count) {
    for (size_t i = 0; i < add_count; ++i) {
      auto data = std::make_shared<ObjectTableData>();
      data->set_manager(ObjectID::FromRandom().Hex());
      object_vector.push_back(data);
    }
  };
  // Test one element case.
  AppendObjectData(1);
  ASSERT_EQ(object_vector.size(), 1);
  TestDeleteKeysFromSet(job_id, client, object_vector);
  // Test the case for more than one elements and less than
  // maximum_gcs_deletion_batch_size.
  AppendObjectData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(object_vector.size(), 1);
  ASSERT_LT(object_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromSet(job_id, client, object_vector);
  // Test the case for more than maximum_gcs_deletion_batch_size.
  // The Delete function will split the data into two commands.
  AppendObjectData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(object_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromSet(job_id, client, object_vector);
}

TEST_F(TestGcsWithAsio, TestDeleteKey) {
  test = this;
  TestDeleteKeys(job_id_, client_);
}

void TestLogSubscribeAll(const JobID &job_id,
                         std::shared_ptr<gcs::RedisGcsClient> client) {
  std::vector<JobID> job_ids;
  for (int i = 0; i < 3; i++) {
    job_ids.emplace_back(NextJobID());
  }
  // Callback for a notification.
  auto notification_callback = [job_ids](gcs::RedisGcsClient *client, const JobID &id,
                                         const std::vector<JobTableData> data) {
    ASSERT_EQ(id, job_ids[test->NumCallbacks()]);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.job_id(), job_ids[test->NumCallbacks()].Binary());
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == job_ids.size()) {
      test->Stop();
    }
  };

  // Callback for subscription success. We are guaranteed to receive
  // notifications after this is called.
  auto subscribe_callback = [job_ids](gcs::RedisGcsClient *client) {
    // We have subscribed. Do the writes to the table.
    for (size_t i = 0; i < job_ids.size(); i++) {
      RAY_CHECK_OK(
          client->job_table().AppendJobData(job_ids[i], false, 0, "localhost", 1));
    }
  };

  // Subscribe to all driver table notifications. Once we have successfully
  // subscribed, we will append to the key several times and check that we get
  // notified for each.
  RAY_CHECK_OK(client->job_table().Subscribe(job_id, ClientID::Nil(),
                                             notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one notification callback for each write.
  ASSERT_EQ(test->NumCallbacks(), job_ids.size());
}

TEST_F(TestGcsWithAsio, TestLogSubscribeAll) {
  test = this;
  TestLogSubscribeAll(job_id_, client_);
}

void TestSetSubscribeAll(const JobID &job_id,
                         std::shared_ptr<gcs::RedisGcsClient> client) {
  std::vector<ObjectID> object_ids;
  for (int i = 0; i < 3; i++) {
    object_ids.emplace_back(ObjectID::FromRandom());
  }
  std::vector<std::string> managers = {"abc", "def", "ghi"};

  // Callback for a notification.
  auto notification_callback = [object_ids, managers](
                                   gcs::RedisGcsClient *client, const ObjectID &id,
                                   const GcsChangeMode change_mode,
                                   const std::vector<ObjectTableData> data) {
    if (test->NumCallbacks() < 3 * 3) {
      ASSERT_EQ(change_mode, GcsChangeMode::APPEND_OR_ADD);
    } else {
      ASSERT_EQ(change_mode, GcsChangeMode::REMOVE);
    }
    ASSERT_EQ(id, object_ids[test->NumCallbacks() / 3 % 3]);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.manager(), managers[test->NumCallbacks() % 3]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == object_ids.size() * 3 * 2) {
      test->Stop();
    }
  };

  // Callback for subscription success. We are guaranteed to receive
  // notifications after this is called.
  auto subscribe_callback = [job_id, object_ids, managers](gcs::RedisGcsClient *client) {
    // We have subscribed. Do the writes to the table.
    for (size_t i = 0; i < object_ids.size(); i++) {
      for (size_t j = 0; j < managers.size(); j++) {
        auto data = std::make_shared<ObjectTableData>();
        data->set_manager(managers[j]);
        for (int k = 0; k < 3; k++) {
          // Add the same entry several times.
          // Expect no notification if the entry already exists.
          RAY_CHECK_OK(client->object_table().Add(job_id, object_ids[i], data, nullptr));
        }
      }
    }
    for (size_t i = 0; i < object_ids.size(); i++) {
      for (size_t j = 0; j < managers.size(); j++) {
        auto data = std::make_shared<ObjectTableData>();
        data->set_manager(managers[j]);
        for (int k = 0; k < 3; k++) {
          // Remove the same entry several times.
          // Expect no notification if the entry doesn't exist.
          RAY_CHECK_OK(
              client->object_table().Remove(job_id, object_ids[i], data, nullptr));
        }
      }
    }
  };

  // Subscribe to all driver table notifications. Once we have successfully
  // subscribed, we will append to the key several times and check that we get
  // notified for each.
  RAY_CHECK_OK(client->object_table().Subscribe(
      job_id, ClientID::Nil(), notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one notification callback for each write.
  ASSERT_EQ(test->NumCallbacks(), object_ids.size() * 3 * 2);
}

TEST_F(TestGcsWithAsio, TestSetSubscribeAll) {
  test = this;
  TestSetSubscribeAll(job_id_, client_);
}

void TestTableSubscribeId(const JobID &job_id,
                          std::shared_ptr<gcs::RedisGcsClient> client) {
  int num_modifications = 3;

  // Add a table entry.
  TaskID task_id1 = TaskID::FromRandom();

  // Add a table entry at a second key.
  TaskID task_id2 = TaskID::FromRandom();

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [task_id2, num_modifications](gcs::RedisGcsClient *client,
                                                             const TaskID &id,
                                                             const TaskTableData &data) {
    // Check that we only get notifications for the requested key.
    ASSERT_EQ(id, task_id2);
    // Check that we get notifications in the same order as the writes.
    ASSERT_TRUE(
        TaskTableDataEqual(data, *CreateTaskTableData(task_id2, test->NumCallbacks())));
    test->IncrementNumCallbacks();
    if (test->NumCallbacks() == num_modifications) {
      test->Stop();
    }
  };

  // The failure callback should be called once since both keys start as empty.
  bool failure_notification_received = false;
  auto failure_callback = [task_id2, &failure_notification_received](
                              gcs::RedisGcsClient *client, const TaskID &id) {
    ASSERT_EQ(id, task_id2);
    // The failure notification should be the first notification received.
    ASSERT_EQ(test->NumCallbacks(), 0);
    failure_notification_received = true;
  };

  // The callback for subscription success. Once we've subscribed, request
  // notifications for only one of the keys, then write to both keys.
  auto subscribe_callback = [job_id, task_id1, task_id2,
                             num_modifications](gcs::RedisGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        job_id, task_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    for (uint64_t i = 0; i < num_modifications; i++) {
      auto data = CreateTaskTableData(task_id1, i);
      RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id1, data, nullptr));
    }
    for (uint64_t i = 0; i < num_modifications; i++) {
      auto data = CreateTaskTableData(task_id2, i);
      RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id2, data, nullptr));
    }
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->raylet_task_table().Subscribe(
      job_id, client->client_table().GetLocalClientId(), notification_callback,
      failure_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that the failure callback was called since the key was initially
  // empty.
  ASSERT_TRUE(failure_notification_received);
  // Check that we received one notification callback for each write to the
  // requested key.
  ASSERT_EQ(test->NumCallbacks(), num_modifications);
}

TEST_MACRO(TestGcsWithAsio, TestTableSubscribeId);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAsio, TestTableSubscribeId);
#endif

void TestLogSubscribeId(const JobID &job_id,
                        std::shared_ptr<gcs::RedisGcsClient> client) {
  // Add a log entry.
  JobID job_id1 = NextJobID();
  std::vector<std::string> job_ids1 = {"abc", "def", "ghi"};
  auto data1 = std::make_shared<JobTableData>();
  data1->set_job_id(job_ids1[0]);
  RAY_CHECK_OK(client->job_table().Append(job_id, job_id1, data1, nullptr));

  // Add a log entry at a second key.
  JobID job_id2 = NextJobID();
  std::vector<std::string> job_ids2 = {"jkl", "mno", "pqr"};
  auto data2 = std::make_shared<JobTableData>();
  data2->set_job_id(job_ids2[0]);
  RAY_CHECK_OK(client->job_table().Append(job_id, job_id2, data2, nullptr));

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [job_id2, job_ids2](
                                   gcs::RedisGcsClient *client, const JobID &id,
                                   const std::vector<JobTableData> &data) {
    // Check that we only get notifications for the requested key.
    ASSERT_EQ(id, job_id2);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.job_id(), job_ids2[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == job_ids2.size()) {
      test->Stop();
    }
  };

  // The callback for subscription success. Once we've subscribed, request
  // notifications for only one of the keys, then write to both keys.
  auto subscribe_callback = [job_id, job_id1, job_id2, job_ids1,
                             job_ids2](gcs::RedisGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->job_table().RequestNotifications(
        job_id, job_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    auto remaining = std::vector<std::string>(++job_ids1.begin(), job_ids1.end());
    for (const auto &job_id_it : remaining) {
      auto data = std::make_shared<JobTableData>();
      data->set_job_id(job_id_it);
      RAY_CHECK_OK(client->job_table().Append(job_id, job_id1, data, nullptr));
    }
    remaining = std::vector<std::string>(++job_ids2.begin(), job_ids2.end());
    for (const auto &job_id_it : remaining) {
      auto data = std::make_shared<JobTableData>();
      data->set_job_id(job_id_it);
      RAY_CHECK_OK(client->job_table().Append(job_id, job_id2, data, nullptr));
    }
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->job_table().Subscribe(job_id,
                                             client->client_table().GetLocalClientId(),
                                             notification_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received one notification callback for each write to the
  // requested key.
  ASSERT_EQ(test->NumCallbacks(), job_ids2.size());
}

TEST_F(TestGcsWithAsio, TestLogSubscribeId) {
  test = this;
  TestLogSubscribeId(job_id_, client_);
}

void TestSetSubscribeId(const JobID &job_id,
                        std::shared_ptr<gcs::RedisGcsClient> client) {
  // Add a set entry.
  ObjectID object_id1 = ObjectID::FromRandom();
  std::vector<std::string> managers1 = {"abc", "def", "ghi"};
  auto data1 = std::make_shared<ObjectTableData>();
  data1->set_manager(managers1[0]);
  RAY_CHECK_OK(client->object_table().Add(job_id, object_id1, data1, nullptr));

  // Add a set entry at a second key.
  ObjectID object_id2 = ObjectID::FromRandom();
  std::vector<std::string> managers2 = {"jkl", "mno", "pqr"};
  auto data2 = std::make_shared<ObjectTableData>();
  data2->set_manager(managers2[0]);
  RAY_CHECK_OK(client->object_table().Add(job_id, object_id2, data2, nullptr));

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [object_id2, managers2](
                                   gcs::RedisGcsClient *client, const ObjectID &id,
                                   const GcsChangeMode change_mode,
                                   const std::vector<ObjectTableData> &data) {
    ASSERT_EQ(change_mode, GcsChangeMode::APPEND_OR_ADD);
    // Check that we only get notifications for the requested key.
    ASSERT_EQ(id, object_id2);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.manager(), managers2[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == managers2.size()) {
      test->Stop();
    }
  };

  // The callback for subscription success. Once we've subscribed, request
  // notifications for only one of the keys, then write to both keys.
  auto subscribe_callback = [job_id, object_id1, object_id2, managers1,
                             managers2](gcs::RedisGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    auto remaining = std::vector<std::string>(++managers1.begin(), managers1.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableData>();
      data->set_manager(manager);
      RAY_CHECK_OK(client->object_table().Add(job_id, object_id1, data, nullptr));
    }
    remaining = std::vector<std::string>(++managers2.begin(), managers2.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableData>();
      data->set_manager(manager);
      RAY_CHECK_OK(client->object_table().Add(job_id, object_id2, data, nullptr));
    }
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(
      client->object_table().Subscribe(job_id, client->client_table().GetLocalClientId(),
                                       notification_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received one notification callback for each write to the
  // requested key.
  ASSERT_EQ(test->NumCallbacks(), managers2.size());
}

TEST_F(TestGcsWithAsio, TestSetSubscribeId) {
  test = this;
  TestSetSubscribeId(job_id_, client_);
}

void TestTableSubscribeCancel(const JobID &job_id,
                              std::shared_ptr<gcs::RedisGcsClient> client) {
  // Add a table entry.
  const auto task_id = TaskID::FromRandom();
  const int num_modifications = 3;
  const auto data = CreateTaskTableData(task_id, 0);
  RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id, data, nullptr));

  // The failure callback should not be called since all keys are non-empty
  // when notifications are requested.
  auto failure_callback = [](gcs::RedisGcsClient *client, const TaskID &id) {
    RAY_CHECK(false);
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [task_id](gcs::RedisGcsClient *client, const TaskID &id,
                                         const TaskTableData &data) {
    ASSERT_EQ(id, task_id);
    // Check that we only get notifications for the first and last writes,
    // since notifications are canceled in between.
    if (test->NumCallbacks() == 0) {
      ASSERT_TRUE(TaskTableDataEqual(data, *CreateTaskTableData(task_id, 0)));
    } else {
      ASSERT_TRUE(
          TaskTableDataEqual(data, *CreateTaskTableData(task_id, num_modifications - 1)));
    }
    test->IncrementNumCallbacks();
    if (test->NumCallbacks() == num_modifications - 1) {
      test->Stop();
    }
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto subscribe_callback = [job_id, task_id](gcs::RedisGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        job_id, task_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->raylet_task_table().CancelNotifications(
        job_id, task_id, client->client_table().GetLocalClientId()));
    // Write to the key. Since we canceled notifications, we should not receive
    // a notification for these writes.
    for (uint64_t i = 1; i < num_modifications; i++) {
      auto data = CreateTaskTableData(task_id, i);
      RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id, data, nullptr));
    }
    // Request notifications again. We should receive a notification for the
    // current value at the key.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        job_id, task_id, client->client_table().GetLocalClientId()));
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->raylet_task_table().Subscribe(
      job_id, client->client_table().GetLocalClientId(), notification_callback,
      failure_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received a notification callback for the first and least
  // writes to the key, since notifications are canceled in between.
  ASSERT_EQ(test->NumCallbacks(), 2);
}

TEST_MACRO(TestGcsWithAsio, TestTableSubscribeCancel);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAsio, TestTableSubscribeCancel);
#endif

void TestLogSubscribeCancel(const JobID &job_id,
                            std::shared_ptr<gcs::RedisGcsClient> client) {
  // Add a log entry.
  JobID random_job_id = NextJobID();
  std::vector<std::string> job_ids = {"jkl", "mno", "pqr"};
  auto data = std::make_shared<JobTableData>();
  data->set_job_id(job_ids[0]);
  RAY_CHECK_OK(client->job_table().Append(job_id, random_job_id, data, nullptr));

  // The callback for a notification from the object table. This should only be
  // received for the object that we requested notifications for.
  auto notification_callback = [random_job_id, job_ids](
                                   gcs::RedisGcsClient *client, const JobID &id,
                                   const std::vector<JobTableData> &data) {
    ASSERT_EQ(id, random_job_id);
    // Check that we get a duplicate notification for the first write. We get a
    // duplicate notification because the log is append-only and notifications
    // are canceled after the first write, then requested again.
    auto job_ids_copy = job_ids;
    job_ids_copy.insert(job_ids_copy.begin(), job_ids_copy.front());
    for (const auto &entry : data) {
      ASSERT_EQ(entry.job_id(), job_ids_copy[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == job_ids_copy.size()) {
      test->Stop();
    }
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto subscribe_callback = [job_id, random_job_id,
                             job_ids](gcs::RedisGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->job_table().RequestNotifications(
        job_id, random_job_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->job_table().CancelNotifications(
        job_id, random_job_id, client->client_table().GetLocalClientId()));
    // Append to the key. Since we canceled notifications, we should not
    // receive a notification for these writes.
    auto remaining = std::vector<std::string>(++job_ids.begin(), job_ids.end());
    for (const auto &remaining_job_id : remaining) {
      auto data = std::make_shared<JobTableData>();
      data->set_job_id(remaining_job_id);
      RAY_CHECK_OK(client->job_table().Append(job_id, random_job_id, data, nullptr));
    }
    // Request notifications again. We should receive a notification for the
    // current values at the key.
    RAY_CHECK_OK(client->job_table().RequestNotifications(
        job_id, random_job_id, client->client_table().GetLocalClientId()));
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->job_table().Subscribe(job_id,
                                             client->client_table().GetLocalClientId(),
                                             notification_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received a notification callback for the first append to the
  // key, then a notification for all of the appends, because we cancel
  // notifications in between.
  ASSERT_EQ(test->NumCallbacks(), job_ids.size() + 1);
}

TEST_F(TestGcsWithAsio, TestLogSubscribeCancel) {
  test = this;
  TestLogSubscribeCancel(job_id_, client_);
}

void TestSetSubscribeCancel(const JobID &job_id,
                            std::shared_ptr<gcs::RedisGcsClient> client) {
  // Add a set entry.
  ObjectID object_id = ObjectID::FromRandom();
  std::vector<std::string> managers = {"jkl", "mno", "pqr"};
  auto data = std::make_shared<ObjectTableData>();
  data->set_manager(managers[0]);
  RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, nullptr));

  // The callback for a notification from the object table. This should only be
  // received for the object that we requested notifications for.
  auto notification_callback = [object_id, managers](
                                   gcs::RedisGcsClient *client, const ObjectID &id,
                                   const GcsChangeMode change_mode,
                                   const std::vector<ObjectTableData> &data) {
    ASSERT_EQ(change_mode, GcsChangeMode::APPEND_OR_ADD);
    ASSERT_EQ(id, object_id);
    // Check that we get a duplicate notification for the first write. We get a
    // duplicate notification because notifications
    // are canceled after the first write, then requested again.
    if (data.size() == 1) {
      // first notification
      ASSERT_EQ(data[0].manager(), managers[0]);
      test->IncrementNumCallbacks();
    } else {
      // second notification
      ASSERT_EQ(data.size(), managers.size());
      std::unordered_set<std::string> managers_set(managers.begin(), managers.end());
      std::unordered_set<std::string> data_managers_set;
      for (const auto &entry : data) {
        data_managers_set.insert(entry.manager());
        test->IncrementNumCallbacks();
      }
      ASSERT_EQ(managers_set, data_managers_set);
    }
    if (test->NumCallbacks() == managers.size() + 1) {
      test->Stop();
    }
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto subscribe_callback = [job_id, object_id, managers](gcs::RedisGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->object_table().CancelNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
    // Add to the key. Since we canceled notifications, we should not
    // receive a notification for these writes.
    auto remaining = std::vector<std::string>(++managers.begin(), managers.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableData>();
      data->set_manager(manager);
      RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, nullptr));
    }
    // Request notifications again. We should receive a notification for the
    // current values at the key.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(
      client->object_table().Subscribe(job_id, client->client_table().GetLocalClientId(),
                                       notification_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received a notification callback for the first append to the
  // key, then a notification for all of the appends, because we cancel
  // notifications in between.
  ASSERT_EQ(test->NumCallbacks(), managers.size() + 1);
}

TEST_F(TestGcsWithAsio, TestSetSubscribeCancel) {
  test = this;
  TestSetSubscribeCancel(job_id_, client_);
}

void ClientTableNotification(gcs::RedisGcsClient *client, const ClientID &client_id,
                             const GcsNodeInfo &data, bool is_alive) {
  ClientID added_id = client->client_table().GetLocalClientId();
  ASSERT_EQ(client_id, added_id);
  ASSERT_EQ(ClientID::FromBinary(data.node_id()), added_id);
  ASSERT_EQ(data.state() == GcsNodeInfo::ALIVE, is_alive);

  GcsNodeInfo cached_client;
  client->client_table().GetClient(added_id, cached_client);
  ASSERT_EQ(ClientID::FromBinary(cached_client.node_id()), added_id);
  ASSERT_EQ(cached_client.state() == GcsNodeInfo::ALIVE, is_alive);
}

void TestClientTableConnect(const JobID &job_id,
                            std::shared_ptr<gcs::RedisGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::RedisGcsClient *client, const ClientID &id, const GcsNodeInfo &data) {
        ClientTableNotification(client, id, data, true);
        test->Stop();
      });

  // Connect and disconnect to client table. We should receive notifications
  // for the addition and removal of our own entry.
  GcsNodeInfo local_node_info = client->client_table().GetLocalClient();
  local_node_info.set_node_manager_address("127.0.0.1");
  local_node_info.set_node_manager_port(0);
  local_node_info.set_object_manager_port(0);
  RAY_CHECK_OK(client->client_table().Connect(local_node_info));
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableConnect) {
  test = this;
  TestClientTableConnect(job_id_, client_);
}

void TestClientTableDisconnect(const JobID &job_id,
                               std::shared_ptr<gcs::RedisGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::RedisGcsClient *client, const ClientID &id, const GcsNodeInfo &data) {
        ClientTableNotification(client, id, data, /*is_insertion=*/true);
        // Disconnect from the client table. We should receive a notification
        // for the removal of our own entry.
        RAY_CHECK_OK(client->client_table().Disconnect());
      });
  client->client_table().RegisterClientRemovedCallback(
      [](gcs::RedisGcsClient *client, const ClientID &id, const GcsNodeInfo &data) {
        ClientTableNotification(client, id, data, /*is_insertion=*/false);
        test->Stop();
      });
  // Connect to the client table. We should receive notification for the
  // addition of our own entry.
  GcsNodeInfo local_node_info = client->client_table().GetLocalClient();
  local_node_info.set_node_manager_address("127.0.0.1");
  local_node_info.set_node_manager_port(0);
  local_node_info.set_object_manager_port(0);
  RAY_CHECK_OK(client->client_table().Connect(local_node_info));
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableDisconnect) {
  test = this;
  TestClientTableDisconnect(job_id_, client_);
}

void TestClientTableImmediateDisconnect(const JobID &job_id,
                                        std::shared_ptr<gcs::RedisGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::RedisGcsClient *client, const ClientID &id, const GcsNodeInfo &data) {
        ClientTableNotification(client, id, data, true);
      });
  client->client_table().RegisterClientRemovedCallback(
      [](gcs::RedisGcsClient *client, const ClientID &id, const GcsNodeInfo &data) {
        ClientTableNotification(client, id, data, false);
        test->Stop();
      });
  // Connect to then immediately disconnect from the client table. We should
  // receive notifications for the addition and removal of our own entry.
  GcsNodeInfo local_node_info = client->client_table().GetLocalClient();
  local_node_info.set_node_manager_address("127.0.0.1");
  local_node_info.set_node_manager_port(0);
  local_node_info.set_object_manager_port(0);
  RAY_CHECK_OK(client->client_table().Connect(local_node_info));
  RAY_CHECK_OK(client->client_table().Disconnect());
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableImmediateDisconnect) {
  test = this;
  TestClientTableImmediateDisconnect(job_id_, client_);
}

void TestClientTableMarkDisconnected(const JobID &job_id,
                                     std::shared_ptr<gcs::RedisGcsClient> client) {
  GcsNodeInfo local_node_info = client->client_table().GetLocalClient();
  local_node_info.set_node_manager_address("127.0.0.1");
  local_node_info.set_node_manager_port(0);
  local_node_info.set_object_manager_port(0);
  // Connect to the client table to start receiving notifications.
  RAY_CHECK_OK(client->client_table().Connect(local_node_info));
  // Mark a different client as dead.
  ClientID dead_client_id = ClientID::FromRandom();
  RAY_CHECK_OK(client->client_table().MarkDisconnected(dead_client_id));
  // Make sure we only get a notification for the removal of the client we
  // marked as dead.
  client->client_table().RegisterClientRemovedCallback(
      [dead_client_id](gcs::RedisGcsClient *client, const UniqueID &id,
                       const GcsNodeInfo &data) {
        ASSERT_EQ(ClientID::FromBinary(data.node_id()), dead_client_id);
        test->Stop();
      });
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableMarkDisconnected) {
  test = this;
  TestClientTableMarkDisconnected(job_id_, client_);
}

void TestHashTable(const JobID &job_id, std::shared_ptr<gcs::RedisGcsClient> client) {
  const int expected_count = 14;
  ClientID client_id = ClientID::FromRandom();
  // Prepare the first resource map: data_map1.
  DynamicResourceTable::DataMap data_map1;
  auto cpu_data = std::make_shared<ResourceTableData>();
  cpu_data->set_resource_capacity(100);
  data_map1.emplace("CPU", cpu_data);
  auto gpu_data = std::make_shared<ResourceTableData>();
  gpu_data->set_resource_capacity(2);
  data_map1.emplace("GPU", gpu_data);
  // Prepare the second resource map: data_map2 which decreases CPU,
  // increases GPU and add a new CUSTOM compared to data_map1.
  DynamicResourceTable::DataMap data_map2;
  auto data_cpu = std::make_shared<ResourceTableData>();
  data_cpu->set_resource_capacity(50);
  data_map2.emplace("CPU", data_cpu);
  auto data_gpu = std::make_shared<ResourceTableData>();
  data_gpu->set_resource_capacity(10);
  data_map2.emplace("GPU", data_gpu);
  auto data_custom = std::make_shared<ResourceTableData>();
  data_custom->set_resource_capacity(2);
  data_map2.emplace("CUSTOM", data_custom);
  data_map2["CPU"]->set_resource_capacity(50);
  // This is a common comparison function for the test.
  auto compare_test = [](const DynamicResourceTable::DataMap &data1,
                         const DynamicResourceTable::DataMap &data2) {
    ASSERT_EQ(data1.size(), data2.size());
    for (const auto &data : data1) {
      auto iter = data2.find(data.first);
      ASSERT_TRUE(iter != data2.end());
      ASSERT_EQ(iter->second->resource_capacity(), data.second->resource_capacity());
    }
  };
  auto subscribe_callback = [](RedisGcsClient *client) {
    ASSERT_TRUE(true);
    test->IncrementNumCallbacks();
  };
  auto notification_callback = [data_map1, data_map2, compare_test](
                                   RedisGcsClient *client, const ClientID &id,
                                   const GcsChangeMode change_mode,
                                   const DynamicResourceTable::DataMap &data) {
    if (change_mode == GcsChangeMode::REMOVE) {
      ASSERT_EQ(data.size(), 2);
      ASSERT_TRUE(data.find("GPU") != data.end());
      ASSERT_TRUE(data.find("CUSTOM") != data.end() || data.find("CPU") != data.end());
      // The key "None-Existent" will not appear in the notification.
    } else {
      if (data.size() == 2) {
        compare_test(data_map1, data);
      } else if (data.size() == 3) {
        compare_test(data_map2, data);
      } else {
        ASSERT_TRUE(false);
      }
    }
    test->IncrementNumCallbacks();
    // It is not sure which of the notification or lookup callback will come first.
    if (test->NumCallbacks() == expected_count) {
      test->Stop();
    }
  };
  // Step 0: Subscribe the change of the hash table.
  RAY_CHECK_OK(client->resource_table().Subscribe(
      job_id, ClientID::Nil(), notification_callback, subscribe_callback));
  RAY_CHECK_OK(client->resource_table().RequestNotifications(
      job_id, client_id, client->client_table().GetLocalClientId()));

  // Step 1: Add elements to the hash table.
  auto update_callback1 = [data_map1, compare_test](
                              RedisGcsClient *client, const ClientID &id,
                              const DynamicResourceTable::DataMap &callback_data) {
    compare_test(data_map1, callback_data);
    test->IncrementNumCallbacks();
  };
  RAY_CHECK_OK(
      client->resource_table().Update(job_id, client_id, data_map1, update_callback1));
  auto lookup_callback1 = [data_map1, compare_test](
                              RedisGcsClient *client, const ClientID &id,
                              const DynamicResourceTable::DataMap &callback_data) {
    compare_test(data_map1, callback_data);
    test->IncrementNumCallbacks();
  };
  RAY_CHECK_OK(client->resource_table().Lookup(job_id, client_id, lookup_callback1));

  // Step 2: Decrease one element, increase one and add a new one.
  RAY_CHECK_OK(client->resource_table().Update(job_id, client_id, data_map2, nullptr));
  auto lookup_callback2 = [data_map2, compare_test](
                              RedisGcsClient *client, const ClientID &id,
                              const DynamicResourceTable::DataMap &callback_data) {
    compare_test(data_map2, callback_data);
    test->IncrementNumCallbacks();
  };
  RAY_CHECK_OK(client->resource_table().Lookup(job_id, client_id, lookup_callback2));
  std::vector<std::string> delete_keys({"GPU", "CUSTOM", "None-Existent"});
  auto remove_callback = [delete_keys](RedisGcsClient *client, const ClientID &id,
                                       const std::vector<std::string> &callback_data) {
    for (size_t i = 0; i < callback_data.size(); ++i) {
      // All deleting keys exist in this argument even if the key doesn't exist.
      ASSERT_EQ(callback_data[i], delete_keys[i]);
    }
    test->IncrementNumCallbacks();
  };
  RAY_CHECK_OK(client->resource_table().RemoveEntries(job_id, client_id, delete_keys,
                                                      remove_callback));
  DynamicResourceTable::DataMap data_map3(data_map2);
  data_map3.erase("GPU");
  data_map3.erase("CUSTOM");
  auto lookup_callback3 = [data_map3, compare_test](
                              RedisGcsClient *client, const ClientID &id,
                              const DynamicResourceTable::DataMap &callback_data) {
    compare_test(data_map3, callback_data);
    test->IncrementNumCallbacks();
  };
  RAY_CHECK_OK(client->resource_table().Lookup(job_id, client_id, lookup_callback3));

  // Step 3: Reset the the resources to data_map1.
  RAY_CHECK_OK(
      client->resource_table().Update(job_id, client_id, data_map1, update_callback1));
  auto lookup_callback4 = [data_map1, compare_test](
                              RedisGcsClient *client, const ClientID &id,
                              const DynamicResourceTable::DataMap &callback_data) {
    compare_test(data_map1, callback_data);
    test->IncrementNumCallbacks();
  };
  RAY_CHECK_OK(client->resource_table().Lookup(job_id, client_id, lookup_callback4));

  // Step 4: Removing all elements will remove the home Hash table from GCS.
  RAY_CHECK_OK(client->resource_table().RemoveEntries(
      job_id, client_id, {"GPU", "CPU", "CUSTOM", "None-Existent"}, nullptr));
  auto lookup_callback5 = [](RedisGcsClient *client, const ClientID &id,
                             const DynamicResourceTable::DataMap &callback_data) {
    ASSERT_EQ(callback_data.size(), 0);
    test->IncrementNumCallbacks();
    // It is not sure which of notification or lookup callback will come first.
    if (test->NumCallbacks() == expected_count) {
      test->Stop();
    }
  };
  RAY_CHECK_OK(client->resource_table().Lookup(job_id, client_id, lookup_callback5));
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), expected_count);
}

TEST_F(TestGcsWithAsio, TestHashTable) {
  test = this;
  TestHashTable(job_id_, client_);
}

#undef TEST_MACRO

}  // namespace gcs
}  // namespace ray
