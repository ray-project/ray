#include "gtest/gtest.h"

// TODO(pcm): get rid of this and replace with the type safe plasma event loop
extern "C" {
#include "ray/thirdparty/hiredis/hiredis.h"
}

#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"
#include "ray/ray_config.h"

namespace ray {

namespace gcs {

namespace {
constexpr char kRandomId[] = "abcdefghijklmnopqrst";
}  // namespace

/* Flush redis. */
static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

class TestGcs : public ::testing::Test {
 public:
  TestGcs(CommandType command_type) : num_callbacks_(0), command_type_(command_type) {
    client_ = std::make_shared<gcs::AsyncGcsClient>("127.0.0.1", 6379, command_type_,
                                                    /*is_test_client=*/true);
    driver_id_ = DriverID::from_random();
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
  std::shared_ptr<gcs::AsyncGcsClient> client_;
  DriverID driver_id_;
};

TestGcs *test;

class TestGcsWithAsio : public TestGcs {
 public:
  TestGcsWithAsio(CommandType command_type)
      : TestGcs(command_type), io_service_(), work_(io_service_) {
    RAY_CHECK_OK(client_->Attach(io_service_));
  }

  TestGcsWithAsio() : TestGcsWithAsio(CommandType::kRegular) {}

  ~TestGcsWithAsio() {
    // Destroy the client first since it has a reference to the event loop.
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

void TestTableLookup(const DriverID &driver_id,
                     std::shared_ptr<gcs::AsyncGcsClient> client) {
  TaskID task_id = TaskID::from_random();
  auto data = std::make_shared<protocol::TaskT>();
  data->task_specification = "123";

  // Check that we added the correct task.
  auto add_callback = [task_id, data](gcs::AsyncGcsClient *client, const UniqueID &id,
                                      const protocol::TaskT &d) {
    ASSERT_EQ(id, task_id);
    ASSERT_EQ(data->task_specification, d.task_specification);
  };

  // Check that the lookup returns the added task.
  auto lookup_callback = [task_id, data](gcs::AsyncGcsClient *client, const TaskID &id,
                                         const protocol::TaskT &d) {
    ASSERT_EQ(id, task_id);
    ASSERT_EQ(data->task_specification, d.task_specification);
    test->Stop();
  };

  // Check that the lookup does not return an empty entry.
  auto failure_callback = [](gcs::AsyncGcsClient *client, const UniqueID &id) {
    RAY_CHECK(false);
  };

  // Add the task, then do a lookup.
  RAY_CHECK_OK(client->raylet_task_table().Add(driver_id, task_id, data, add_callback));
  RAY_CHECK_OK(client->raylet_task_table().Lookup(driver_id, task_id, lookup_callback,
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
    TEST(driver_id_, client_);    \
  }

TEST_MACRO(TestGcsWithAsio, TestTableLookup);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAsio, TestTableLookup);
#endif

void TestLogLookup(const DriverID &driver_id,
                   std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Append some entries to the log at an object ID.
  TaskID task_id = TaskID::from_random();
  std::vector<std::string> node_manager_ids = {"abc", "def", "ghi"};
  for (auto &node_manager_id : node_manager_ids) {
    auto data = std::make_shared<TaskReconstructionDataT>();
    data->node_manager_id = node_manager_id;
    // Check that we added the correct object entries.
    auto add_callback = [task_id, data](gcs::AsyncGcsClient *client, const UniqueID &id,
                                        const TaskReconstructionDataT &d) {
      ASSERT_EQ(id, task_id);
      ASSERT_EQ(data->node_manager_id, d.node_manager_id);
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Append(driver_id, task_id, data, add_callback));
  }

  // Check that lookup returns the added object entries.
  auto lookup_callback = [task_id, node_manager_ids](
      gcs::AsyncGcsClient *client, const UniqueID &id,
      const std::vector<TaskReconstructionDataT> &data) {
    ASSERT_EQ(id, task_id);
    for (const auto &entry : data) {
      ASSERT_EQ(entry.node_manager_id, node_manager_ids[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == node_manager_ids.size()) {
      test->Stop();
    }
  };

  // Do a lookup at the object ID.
  RAY_CHECK_OK(
      client->task_reconstruction_log().Lookup(driver_id, task_id, lookup_callback));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), node_manager_ids.size());
}

TEST_F(TestGcsWithAsio, TestLogLookup) {
  test = this;
  TestLogLookup(driver_id_, client_);
}

void TestTableLookupFailure(const DriverID &driver_id,
                            std::shared_ptr<gcs::AsyncGcsClient> client) {
  TaskID task_id = TaskID::from_random();

  // Check that the lookup does not return data.
  auto lookup_callback = [](gcs::AsyncGcsClient *client, const UniqueID &id,
                            const protocol::TaskT &d) { RAY_CHECK(false); };

  // Check that the lookup returns an empty entry.
  auto failure_callback = [task_id](gcs::AsyncGcsClient *client, const UniqueID &id) {
    ASSERT_EQ(id, task_id);
    test->Stop();
  };

  // Lookup the task. We have not done any writes, so the key should be empty.
  RAY_CHECK_OK(client->raylet_task_table().Lookup(driver_id, task_id, lookup_callback,
                                                  failure_callback));
  // Run the event loop. The loop will only stop if the failure callback is
  // called (or an assertion failure).
  test->Start();
}

TEST_MACRO(TestGcsWithAsio, TestTableLookupFailure);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAsio, TestTableLookupFailure);
#endif

void TestLogAppendAt(const DriverID &driver_id,
                     std::shared_ptr<gcs::AsyncGcsClient> client) {
  TaskID task_id = TaskID::from_random();
  std::vector<std::string> node_manager_ids = {"A", "B"};
  std::vector<std::shared_ptr<TaskReconstructionDataT>> data_log;
  for (const auto &node_manager_id : node_manager_ids) {
    auto data = std::make_shared<TaskReconstructionDataT>();
    data->node_manager_id = node_manager_id;
    data_log.push_back(data);
  }

  // Check that we added the correct task.
  auto failure_callback = [task_id](gcs::AsyncGcsClient *client, const UniqueID &id,
                                    const TaskReconstructionDataT &d) {
    ASSERT_EQ(id, task_id);
    test->IncrementNumCallbacks();
  };

  // Will succeed.
  RAY_CHECK_OK(client->task_reconstruction_log().Append(driver_id, task_id,
                                                        data_log.front(),
                                                        /*done callback=*/nullptr));
  // Append at index 0 will fail.
  RAY_CHECK_OK(client->task_reconstruction_log().AppendAt(
      driver_id, task_id, data_log[1],
      /*done callback=*/nullptr, failure_callback, /*log_length=*/0));

  // Append at index 2 will fail.
  RAY_CHECK_OK(client->task_reconstruction_log().AppendAt(
      driver_id, task_id, data_log[1],
      /*done callback=*/nullptr, failure_callback, /*log_length=*/2));

  // Append at index 1 will succeed.
  RAY_CHECK_OK(client->task_reconstruction_log().AppendAt(
      driver_id, task_id, data_log[1],
      /*done callback=*/nullptr, failure_callback, /*log_length=*/1));

  auto lookup_callback = [node_manager_ids](
      gcs::AsyncGcsClient *client, const UniqueID &id,
      const std::vector<TaskReconstructionDataT> &data) {
    std::vector<std::string> appended_managers;
    for (const auto &entry : data) {
      appended_managers.push_back(entry.node_manager_id);
    }
    ASSERT_EQ(appended_managers, node_manager_ids);
    test->Stop();
  };
  RAY_CHECK_OK(
      client->task_reconstruction_log().Lookup(driver_id, task_id, lookup_callback));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), 2);
}

TEST_F(TestGcsWithAsio, TestLogAppendAt) {
  test = this;
  TestLogAppendAt(driver_id_, client_);
}

void TestSet(const DriverID &driver_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add some entries to the set at an object ID.
  ObjectID object_id = ObjectID::from_random();
  std::vector<std::string> managers = {"abc", "def", "ghi"};
  for (auto &manager : managers) {
    auto data = std::make_shared<ObjectTableDataT>();
    data->manager = manager;
    // Check that we added the correct object entries.
    auto add_callback = [object_id, data](gcs::AsyncGcsClient *client, const UniqueID &id,
                                          const ObjectTableDataT &d) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data->manager, d.manager);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Add(driver_id, object_id, data, add_callback));
  }

  // Check that lookup returns the added object entries.
  auto lookup_callback = [object_id, managers](
      gcs::AsyncGcsClient *client, const ObjectID &id,
      const std::vector<ObjectTableDataT> &data) {
    ASSERT_EQ(id, object_id);
    ASSERT_EQ(data.size(), managers.size());
    test->IncrementNumCallbacks();
  };

  // Do a lookup at the object ID.
  RAY_CHECK_OK(client->object_table().Lookup(driver_id, object_id, lookup_callback));

  for (auto &manager : managers) {
    auto data = std::make_shared<ObjectTableDataT>();
    data->manager = manager;
    // Check that we added the correct object entries.
    auto remove_entry_callback = [object_id, data](
        gcs::AsyncGcsClient *client, const UniqueID &id, const ObjectTableDataT &d) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data->manager, d.manager);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->object_table().Remove(driver_id, object_id, data, remove_entry_callback));
  }

  // Check that the entries are removed.
  auto lookup_callback2 = [object_id, managers](
      gcs::AsyncGcsClient *client, const ObjectID &id,
      const std::vector<ObjectTableDataT> &data) {
    ASSERT_EQ(id, object_id);
    ASSERT_EQ(data.size(), 0);
    test->IncrementNumCallbacks();
    test->Stop();
  };

  // Do a lookup at the object ID.
  RAY_CHECK_OK(client->object_table().Lookup(driver_id, object_id, lookup_callback2));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), managers.size() * 2 + 2);
}

TEST_F(TestGcsWithAsio, TestSet) {
  test = this;
  TestSet(driver_id_, client_);
}

void TestDeleteKeysFromLog(
    const DriverID &driver_id, std::shared_ptr<gcs::AsyncGcsClient> client,
    std::vector<std::shared_ptr<TaskReconstructionDataT>> &data_vector) {
  std::vector<TaskID> ids;
  TaskID task_id;
  for (auto &data : data_vector) {
    task_id = TaskID::from_random();
    ids.push_back(task_id);
    // Check that we added the correct object entries.
    auto add_callback = [task_id, data](gcs::AsyncGcsClient *client, const UniqueID &id,
                                        const TaskReconstructionDataT &d) {
      ASSERT_EQ(id, task_id);
      ASSERT_EQ(data->node_manager_id, d.node_manager_id);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Append(driver_id, task_id, data, add_callback));
  }
  for (const auto &task_id : ids) {
    // Check that lookup returns the added object entries.
    auto lookup_callback = [task_id, data_vector](
        gcs::AsyncGcsClient *client, const UniqueID &id,
        const std::vector<TaskReconstructionDataT> &data) {
      ASSERT_EQ(id, task_id);
      ASSERT_EQ(data.size(), 1);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Lookup(driver_id, task_id, lookup_callback));
  }
  if (ids.size() == 1) {
    client->task_reconstruction_log().Delete(driver_id, ids[0]);
  } else {
    client->task_reconstruction_log().Delete(driver_id, ids);
  }
  for (const auto &task_id : ids) {
    auto lookup_callback = [task_id](gcs::AsyncGcsClient *client, const TaskID &id,
                                     const std::vector<TaskReconstructionDataT> &data) {
      ASSERT_EQ(id, task_id);
      ASSERT_TRUE(data.size() == 0);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(
        client->task_reconstruction_log().Lookup(driver_id, task_id, lookup_callback));
  }
}

void TestDeleteKeysFromTable(const DriverID &driver_id,
                             std::shared_ptr<gcs::AsyncGcsClient> client,
                             std::vector<std::shared_ptr<protocol::TaskT>> &data_vector,
                             bool stop_at_end) {
  std::vector<TaskID> ids;
  TaskID task_id;
  for (auto &data : data_vector) {
    task_id = TaskID::from_random();
    ids.push_back(task_id);
    // Check that we added the correct object entries.
    auto add_callback = [task_id, data](gcs::AsyncGcsClient *client, const UniqueID &id,
                                        const protocol::TaskT &d) {
      ASSERT_EQ(id, task_id);
      ASSERT_EQ(data->task_specification, d.task_specification);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->raylet_task_table().Add(driver_id, task_id, data, add_callback));
  }
  for (const auto &task_id : ids) {
    auto task_lookup_callback = [task_id](gcs::AsyncGcsClient *client, const TaskID &id,
                                          const protocol::TaskT &data) {
      ASSERT_EQ(id, task_id);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->raylet_task_table().Lookup(driver_id, task_id,
                                                    task_lookup_callback, nullptr));
  }
  if (ids.size() == 1) {
    client->raylet_task_table().Delete(driver_id, ids[0]);
  } else {
    client->raylet_task_table().Delete(driver_id, ids);
  }
  auto expected_failure_callback = [](AsyncGcsClient *client, const TaskID &id) {
    ASSERT_TRUE(true);
    test->IncrementNumCallbacks();
  };
  auto undesired_callback = [](gcs::AsyncGcsClient *client, const TaskID &id,
                               const protocol::TaskT &data) { ASSERT_TRUE(false); };
  for (size_t i = 0; i < ids.size(); ++i) {
    RAY_CHECK_OK(client->raylet_task_table().Lookup(
        driver_id, task_id, undesired_callback, expected_failure_callback));
  }
  if (stop_at_end) {
    auto stop_callback = [](AsyncGcsClient *client, const TaskID &id) { test->Stop(); };
    RAY_CHECK_OK(
        client->raylet_task_table().Lookup(driver_id, ids[0], nullptr, stop_callback));
  }
}

void TestDeleteKeysFromSet(const DriverID &driver_id,
                           std::shared_ptr<gcs::AsyncGcsClient> client,
                           std::vector<std::shared_ptr<ObjectTableDataT>> &data_vector) {
  std::vector<ObjectID> ids;
  ObjectID object_id;
  for (auto &data : data_vector) {
    object_id = ObjectID::from_random();
    ids.push_back(object_id);
    // Check that we added the correct object entries.
    auto add_callback = [object_id, data](gcs::AsyncGcsClient *client, const UniqueID &id,
                                          const ObjectTableDataT &d) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data->manager, d.manager);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Add(driver_id, object_id, data, add_callback));
  }
  for (const auto &object_id : ids) {
    // Check that lookup returns the added object entries.
    auto lookup_callback = [object_id, data_vector](
        gcs::AsyncGcsClient *client, const ObjectID &id,
        const std::vector<ObjectTableDataT> &data) {
      ASSERT_EQ(id, object_id);
      ASSERT_EQ(data.size(), 1);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Lookup(driver_id, object_id, lookup_callback));
  }
  if (ids.size() == 1) {
    client->object_table().Delete(driver_id, ids[0]);
  } else {
    client->object_table().Delete(driver_id, ids);
  }
  for (const auto &object_id : ids) {
    auto lookup_callback = [object_id](gcs::AsyncGcsClient *client, const ObjectID &id,
                                       const std::vector<ObjectTableDataT> &data) {
      ASSERT_EQ(id, object_id);
      ASSERT_TRUE(data.size() == 0);
      test->IncrementNumCallbacks();
    };
    RAY_CHECK_OK(client->object_table().Lookup(driver_id, object_id, lookup_callback));
  }
}

// Test delete function for keys of Log or Table.
void TestDeleteKeys(const DriverID &driver_id,
                    std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Test delete function for keys of Log.
  std::vector<std::shared_ptr<TaskReconstructionDataT>> task_reconstruction_vector;
  auto AppendTaskReconstructionData = [&task_reconstruction_vector](size_t add_count) {
    for (size_t i = 0; i < add_count; ++i) {
      auto data = std::make_shared<TaskReconstructionDataT>();
      data->node_manager_id = ObjectID::from_random().hex();
      task_reconstruction_vector.push_back(data);
    }
  };
  // Test one element case.
  AppendTaskReconstructionData(1);
  ASSERT_EQ(task_reconstruction_vector.size(), 1);
  TestDeleteKeysFromLog(driver_id, client, task_reconstruction_vector);
  // Test the case for more than one elements and less than
  // maximum_gcs_deletion_batch_size.
  AppendTaskReconstructionData(RayConfig::instance().maximum_gcs_deletion_batch_size() /
                               2);
  ASSERT_GT(task_reconstruction_vector.size(), 1);
  ASSERT_LT(task_reconstruction_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromLog(driver_id, client, task_reconstruction_vector);
  // Test the case for more than maximum_gcs_deletion_batch_size.
  // The Delete function will split the data into two commands.
  AppendTaskReconstructionData(RayConfig::instance().maximum_gcs_deletion_batch_size() /
                               2);
  ASSERT_GT(task_reconstruction_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromLog(driver_id, client, task_reconstruction_vector);

  // Test delete function for keys of Table.
  std::vector<std::shared_ptr<protocol::TaskT>> task_vector;
  auto AppendTaskData = [&task_vector](size_t add_count) {
    for (size_t i = 0; i < add_count; ++i) {
      auto task_data = std::make_shared<protocol::TaskT>();
      task_data->task_specification = ObjectID::from_random().hex();
      task_vector.push_back(task_data);
    }
  };
  AppendTaskData(1);
  ASSERT_EQ(task_vector.size(), 1);
  TestDeleteKeysFromTable(driver_id, client, task_vector, false);

  AppendTaskData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(task_vector.size(), 1);
  ASSERT_LT(task_vector.size(), RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromTable(driver_id, client, task_vector, false);

  AppendTaskData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(task_vector.size(), RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromTable(driver_id, client, task_vector, true);

  test->Start();
  ASSERT_GT(test->NumCallbacks(),
            9 * RayConfig::instance().maximum_gcs_deletion_batch_size());

  // Test delete function for keys of Set.
  std::vector<std::shared_ptr<ObjectTableDataT>> object_vector;
  auto AppendObjectData = [&object_vector](size_t add_count) {
    for (size_t i = 0; i < add_count; ++i) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = ObjectID::from_random().hex();
      object_vector.push_back(data);
    }
  };
  // Test one element case.
  AppendObjectData(1);
  ASSERT_EQ(object_vector.size(), 1);
  TestDeleteKeysFromSet(driver_id, client, object_vector);
  // Test the case for more than one elements and less than
  // maximum_gcs_deletion_batch_size.
  AppendObjectData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(object_vector.size(), 1);
  ASSERT_LT(object_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromSet(driver_id, client, object_vector);
  // Test the case for more than maximum_gcs_deletion_batch_size.
  // The Delete function will split the data into two commands.
  AppendObjectData(RayConfig::instance().maximum_gcs_deletion_batch_size() / 2);
  ASSERT_GT(object_vector.size(),
            RayConfig::instance().maximum_gcs_deletion_batch_size());
  TestDeleteKeysFromSet(driver_id, client, object_vector);
}

TEST_F(TestGcsWithAsio, TestDeleteKey) {
  test = this;
  TestDeleteKeys(driver_id_, client_);
}

// Task table callbacks.
void TaskAdded(gcs::AsyncGcsClient *client, const TaskID &id,
               const TaskTableDataT &data) {
  ASSERT_EQ(data.scheduling_state, SchedulingState::SCHEDULED);
  ASSERT_EQ(data.raylet_id, kRandomId);
}

void TaskLookupHelper(gcs::AsyncGcsClient *client, const TaskID &id,
                      const TaskTableDataT &data, bool do_stop) {
  ASSERT_EQ(data.scheduling_state, SchedulingState::SCHEDULED);
  ASSERT_EQ(data.raylet_id, kRandomId);
  if (do_stop) {
    test->Stop();
  }
}
void TaskLookup(gcs::AsyncGcsClient *client, const TaskID &id,
                const TaskTableDataT &data) {
  TaskLookupHelper(client, id, data, /*do_stop=*/false);
}
void TaskLookupWithStop(gcs::AsyncGcsClient *client, const TaskID &id,
                        const TaskTableDataT &data) {
  TaskLookupHelper(client, id, data, /*do_stop=*/true);
}

void TaskLookupFailure(gcs::AsyncGcsClient *client, const TaskID &id) {
  RAY_CHECK(false);
}

void TaskLookupAfterUpdate(gcs::AsyncGcsClient *client, const TaskID &id,
                           const TaskTableDataT &data) {
  ASSERT_EQ(data.scheduling_state, SchedulingState::LOST);
  test->Stop();
}

void TaskLookupAfterUpdateFailure(gcs::AsyncGcsClient *client, const TaskID &id) {
  RAY_CHECK(false);
  test->Stop();
}

void TestLogSubscribeAll(const DriverID &driver_id,
                         std::shared_ptr<gcs::AsyncGcsClient> client) {
  std::vector<DriverID> driver_ids;
  for (int i = 0; i < 3; i++) {
    driver_ids.emplace_back(DriverID::from_random());
  }
  // Callback for a notification.
  auto notification_callback = [driver_ids](gcs::AsyncGcsClient *client,
                                            const UniqueID &id,
                                            const std::vector<DriverTableDataT> data) {
    ASSERT_EQ(id, driver_ids[test->NumCallbacks()]);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.driver_id, driver_ids[test->NumCallbacks()].binary());
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == driver_ids.size()) {
      test->Stop();
    }
  };

  // Callback for subscription success. We are guaranteed to receive
  // notifications after this is called.
  auto subscribe_callback = [driver_ids](gcs::AsyncGcsClient *client) {
    // We have subscribed. Do the writes to the table.
    for (size_t i = 0; i < driver_ids.size(); i++) {
      RAY_CHECK_OK(client->driver_table().AppendDriverData(driver_ids[i], false));
    }
  };

  // Subscribe to all driver table notifications. Once we have successfully
  // subscribed, we will append to the key several times and check that we get
  // notified for each.
  RAY_CHECK_OK(client->driver_table().Subscribe(
      driver_id, ClientID::nil(), notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one notification callback for each write.
  ASSERT_EQ(test->NumCallbacks(), driver_ids.size());
}

TEST_F(TestGcsWithAsio, TestLogSubscribeAll) {
  test = this;
  TestLogSubscribeAll(driver_id_, client_);
}

void TestSetSubscribeAll(const DriverID &driver_id,
                         std::shared_ptr<gcs::AsyncGcsClient> client) {
  std::vector<ObjectID> object_ids;
  for (int i = 0; i < 3; i++) {
    object_ids.emplace_back(ObjectID::from_random());
  }
  std::vector<std::string> managers = {"abc", "def", "ghi"};

  // Callback for a notification.
  auto notification_callback = [object_ids, managers](
      gcs::AsyncGcsClient *client, const UniqueID &id,
      const GcsTableNotificationMode notification_mode,
      const std::vector<ObjectTableDataT> data) {
    if (test->NumCallbacks() < 3 * 3) {
      ASSERT_EQ(notification_mode, GcsTableNotificationMode::APPEND_OR_ADD);
    } else {
      ASSERT_EQ(notification_mode, GcsTableNotificationMode::REMOVE);
    }
    ASSERT_EQ(id, object_ids[test->NumCallbacks() / 3 % 3]);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.manager, managers[test->NumCallbacks() % 3]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == object_ids.size() * 3 * 2) {
      test->Stop();
    }
  };

  // Callback for subscription success. We are guaranteed to receive
  // notifications after this is called.
  auto subscribe_callback = [driver_id, object_ids,
                             managers](gcs::AsyncGcsClient *client) {
    // We have subscribed. Do the writes to the table.
    for (size_t i = 0; i < object_ids.size(); i++) {
      for (size_t j = 0; j < managers.size(); j++) {
        auto data = std::make_shared<ObjectTableDataT>();
        data->manager = managers[j];
        for (int k = 0; k < 3; k++) {
          // Add the same entry several times.
          // Expect no notification if the entry already exists.
          RAY_CHECK_OK(
              client->object_table().Add(driver_id, object_ids[i], data, nullptr));
        }
      }
    }
    for (size_t i = 0; i < object_ids.size(); i++) {
      for (size_t j = 0; j < managers.size(); j++) {
        auto data = std::make_shared<ObjectTableDataT>();
        data->manager = managers[j];
        for (int k = 0; k < 3; k++) {
          // Remove the same entry several times.
          // Expect no notification if the entry doesn't exist.
          RAY_CHECK_OK(
              client->object_table().Remove(driver_id, object_ids[i], data, nullptr));
        }
      }
    }
  };

  // Subscribe to all driver table notifications. Once we have successfully
  // subscribed, we will append to the key several times and check that we get
  // notified for each.
  RAY_CHECK_OK(client->object_table().Subscribe(
      driver_id, ClientID::nil(), notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one notification callback for each write.
  ASSERT_EQ(test->NumCallbacks(), object_ids.size() * 3 * 2);
}

TEST_F(TestGcsWithAsio, TestSetSubscribeAll) {
  test = this;
  TestSetSubscribeAll(driver_id_, client_);
}

void TestTableSubscribeId(const DriverID &driver_id,
                          std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a table entry.
  TaskID task_id1 = TaskID::from_random();
  std::vector<std::string> task_specs1 = {"abc", "def", "ghi"};

  // Add a table entry at a second key.
  TaskID task_id2 = TaskID::from_random();
  std::vector<std::string> task_specs2 = {"jkl", "mno", "pqr"};

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [task_id2, task_specs2](
      gcs::AsyncGcsClient *client, const TaskID &id, const protocol::TaskT &data) {
    // Check that we only get notifications for the requested key.
    ASSERT_EQ(id, task_id2);
    // Check that we get notifications in the same order as the writes.
    ASSERT_EQ(data.task_specification, task_specs2[test->NumCallbacks()]);
    test->IncrementNumCallbacks();
    if (test->NumCallbacks() == task_specs2.size()) {
      test->Stop();
    }
  };

  // The failure callback should be called once since both keys start as empty.
  bool failure_notification_received = false;
  auto failure_callback = [task_id2, &failure_notification_received](
      gcs::AsyncGcsClient *client, const UniqueID &id) {
    ASSERT_EQ(id, task_id2);
    // The failure notification should be the first notification received.
    ASSERT_EQ(test->NumCallbacks(), 0);
    failure_notification_received = true;
  };

  // The callback for subscription success. Once we've subscribed, request
  // notifications for only one of the keys, then write to both keys.
  auto subscribe_callback = [driver_id, task_id1, task_id2, task_specs1,
                             task_specs2](gcs::AsyncGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        driver_id, task_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    for (const auto &task_spec : task_specs1) {
      auto data = std::make_shared<protocol::TaskT>();
      data->task_specification = task_spec;
      RAY_CHECK_OK(client->raylet_task_table().Add(driver_id, task_id1, data, nullptr));
    }
    for (const auto &task_spec : task_specs2) {
      auto data = std::make_shared<protocol::TaskT>();
      data->task_specification = task_spec;
      RAY_CHECK_OK(client->raylet_task_table().Add(driver_id, task_id2, data, nullptr));
    }
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->raylet_task_table().Subscribe(
      driver_id, client->client_table().GetLocalClientId(), notification_callback,
      failure_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that the failure callback was called since the key was initially
  // empty.
  ASSERT_TRUE(failure_notification_received);
  // Check that we received one notification callback for each write to the
  // requested key.
  ASSERT_EQ(test->NumCallbacks(), task_specs2.size());
}

TEST_MACRO(TestGcsWithAsio, TestTableSubscribeId);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAsio, TestTableSubscribeId);
#endif

void TestLogSubscribeId(const DriverID &driver_id,
                        std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a log entry.
  DriverID driver_id1 = DriverID::from_random();
  std::vector<std::string> driver_ids1 = {"abc", "def", "ghi"};
  auto data1 = std::make_shared<DriverTableDataT>();
  data1->driver_id = driver_ids1[0];
  RAY_CHECK_OK(client->driver_table().Append(driver_id, driver_id1, data1, nullptr));

  // Add a log entry at a second key.
  DriverID driver_id2 = DriverID::from_random();
  std::vector<std::string> driver_ids2 = {"jkl", "mno", "pqr"};
  auto data2 = std::make_shared<DriverTableDataT>();
  data2->driver_id = driver_ids2[0];
  RAY_CHECK_OK(client->driver_table().Append(driver_id, driver_id2, data2, nullptr));

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [driver_id2, driver_ids2](
      gcs::AsyncGcsClient *client, const UniqueID &id,
      const std::vector<DriverTableDataT> &data) {
    // Check that we only get notifications for the requested key.
    ASSERT_EQ(id, driver_id2);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.driver_id, driver_ids2[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == driver_ids2.size()) {
      test->Stop();
    }
  };

  // The callback for subscription success. Once we've subscribed, request
  // notifications for only one of the keys, then write to both keys.
  auto subscribe_callback = [driver_id, driver_id1, driver_id2, driver_ids1,
                             driver_ids2](gcs::AsyncGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->driver_table().RequestNotifications(
        driver_id, driver_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    auto remaining = std::vector<std::string>(++driver_ids1.begin(), driver_ids1.end());
    for (const auto &driver_id_it : remaining) {
      auto data = std::make_shared<DriverTableDataT>();
      data->driver_id = driver_id_it;
      RAY_CHECK_OK(client->driver_table().Append(driver_id, driver_id1, data, nullptr));
    }
    remaining = std::vector<std::string>(++driver_ids2.begin(), driver_ids2.end());
    for (const auto &driver_id_it : remaining) {
      auto data = std::make_shared<DriverTableDataT>();
      data->driver_id = driver_id_it;
      RAY_CHECK_OK(client->driver_table().Append(driver_id, driver_id2, data, nullptr));
    }
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->driver_table().Subscribe(
      driver_id, client->client_table().GetLocalClientId(), notification_callback,
      subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received one notification callback for each write to the
  // requested key.
  ASSERT_EQ(test->NumCallbacks(), driver_ids2.size());
}

TEST_F(TestGcsWithAsio, TestLogSubscribeId) {
  test = this;
  TestLogSubscribeId(driver_id_, client_);
}

void TestSetSubscribeId(const DriverID &driver_id,
                        std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a set entry.
  ObjectID object_id1 = ObjectID::from_random();
  std::vector<std::string> managers1 = {"abc", "def", "ghi"};
  auto data1 = std::make_shared<ObjectTableDataT>();
  data1->manager = managers1[0];
  RAY_CHECK_OK(client->object_table().Add(driver_id, object_id1, data1, nullptr));

  // Add a set entry at a second key.
  ObjectID object_id2 = ObjectID::from_random();
  std::vector<std::string> managers2 = {"jkl", "mno", "pqr"};
  auto data2 = std::make_shared<ObjectTableDataT>();
  data2->manager = managers2[0];
  RAY_CHECK_OK(client->object_table().Add(driver_id, object_id2, data2, nullptr));

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [object_id2, managers2](
      gcs::AsyncGcsClient *client, const ObjectID &id,
      const GcsTableNotificationMode notification_mode,
      const std::vector<ObjectTableDataT> &data) {
    ASSERT_EQ(notification_mode, GcsTableNotificationMode::APPEND_OR_ADD);
    // Check that we only get notifications for the requested key.
    ASSERT_EQ(id, object_id2);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.manager, managers2[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == managers2.size()) {
      test->Stop();
    }
  };

  // The callback for subscription success. Once we've subscribed, request
  // notifications for only one of the keys, then write to both keys.
  auto subscribe_callback = [driver_id, object_id1, object_id2, managers1,
                             managers2](gcs::AsyncGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        driver_id, object_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    auto remaining = std::vector<std::string>(++managers1.begin(), managers1.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = manager;
      RAY_CHECK_OK(client->object_table().Add(driver_id, object_id1, data, nullptr));
    }
    remaining = std::vector<std::string>(++managers2.begin(), managers2.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = manager;
      RAY_CHECK_OK(client->object_table().Add(driver_id, object_id2, data, nullptr));
    }
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->object_table().Subscribe(
      driver_id, client->client_table().GetLocalClientId(), notification_callback,
      subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received one notification callback for each write to the
  // requested key.
  ASSERT_EQ(test->NumCallbacks(), managers2.size());
}

TEST_F(TestGcsWithAsio, TestSetSubscribeId) {
  test = this;
  TestSetSubscribeId(driver_id_, client_);
}

void TestTableSubscribeCancel(const DriverID &driver_id,
                              std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a table entry.
  TaskID task_id = TaskID::from_random();
  std::vector<std::string> task_specs = {"jkl", "mno", "pqr"};
  auto data = std::make_shared<protocol::TaskT>();
  data->task_specification = task_specs[0];
  RAY_CHECK_OK(client->raylet_task_table().Add(driver_id, task_id, data, nullptr));

  // The failure callback should not be called since all keys are non-empty
  // when notifications are requested.
  auto failure_callback = [](gcs::AsyncGcsClient *client, const UniqueID &id) {
    RAY_CHECK(false);
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [task_id, task_specs](
      gcs::AsyncGcsClient *client, const TaskID &id, const protocol::TaskT &data) {
    ASSERT_EQ(id, task_id);
    // Check that we only get notifications for the first and last writes,
    // since notifications are canceled in between.
    if (test->NumCallbacks() == 0) {
      ASSERT_EQ(data.task_specification, task_specs.front());
    } else {
      ASSERT_EQ(data.task_specification, task_specs.back());
    }
    test->IncrementNumCallbacks();
    if (test->NumCallbacks() == 2) {
      test->Stop();
    }
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto subscribe_callback = [driver_id, task_id,
                             task_specs](gcs::AsyncGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        driver_id, task_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->raylet_task_table().CancelNotifications(
        driver_id, task_id, client->client_table().GetLocalClientId()));
    // Write to the key. Since we canceled notifications, we should not receive
    // a notification for these writes.
    auto remaining = std::vector<std::string>(++task_specs.begin(), task_specs.end());
    for (const auto &task_spec : remaining) {
      auto data = std::make_shared<protocol::TaskT>();
      data->task_specification = task_spec;
      RAY_CHECK_OK(client->raylet_task_table().Add(driver_id, task_id, data, nullptr));
    }
    // Request notifications again. We should receive a notification for the
    // current value at the key.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        driver_id, task_id, client->client_table().GetLocalClientId()));
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->raylet_task_table().Subscribe(
      driver_id, client->client_table().GetLocalClientId(), notification_callback,
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

void TestLogSubscribeCancel(const DriverID &driver_id,
                            std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a log entry.
  DriverID random_driver_id = DriverID::from_random();
  std::vector<std::string> driver_ids = {"jkl", "mno", "pqr"};
  auto data = std::make_shared<DriverTableDataT>();
  data->driver_id = driver_ids[0];
  RAY_CHECK_OK(client->driver_table().Append(driver_id, random_driver_id, data, nullptr));

  // The callback for a notification from the object table. This should only be
  // received for the object that we requested notifications for.
  auto notification_callback = [random_driver_id, driver_ids](
      gcs::AsyncGcsClient *client, const UniqueID &id,
      const std::vector<DriverTableDataT> &data) {
    ASSERT_EQ(id, random_driver_id);
    // Check that we get a duplicate notification for the first write. We get a
    // duplicate notification because the log is append-only and notifications
    // are canceled after the first write, then requested again.
    auto driver_ids_copy = driver_ids;
    driver_ids_copy.insert(driver_ids_copy.begin(), driver_ids_copy.front());
    for (const auto &entry : data) {
      ASSERT_EQ(entry.driver_id, driver_ids_copy[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == driver_ids_copy.size()) {
      test->Stop();
    }
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto subscribe_callback = [driver_id, random_driver_id,
                             driver_ids](gcs::AsyncGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->driver_table().RequestNotifications(
        driver_id, random_driver_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->driver_table().CancelNotifications(
        driver_id, random_driver_id, client->client_table().GetLocalClientId()));
    // Append to the key. Since we canceled notifications, we should not
    // receive a notification for these writes.
    auto remaining = std::vector<std::string>(++driver_ids.begin(), driver_ids.end());
    for (const auto &remaining_driver_id : remaining) {
      auto data = std::make_shared<DriverTableDataT>();
      data->driver_id = remaining_driver_id;
      RAY_CHECK_OK(
          client->driver_table().Append(driver_id, random_driver_id, data, nullptr));
    }
    // Request notifications again. We should receive a notification for the
    // current values at the key.
    RAY_CHECK_OK(client->driver_table().RequestNotifications(
        driver_id, random_driver_id, client->client_table().GetLocalClientId()));
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->driver_table().Subscribe(
      driver_id, client->client_table().GetLocalClientId(), notification_callback,
      subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received a notification callback for the first append to the
  // key, then a notification for all of the appends, because we cancel
  // notifications in between.
  ASSERT_EQ(test->NumCallbacks(), driver_ids.size() + 1);
}

TEST_F(TestGcsWithAsio, TestLogSubscribeCancel) {
  test = this;
  TestLogSubscribeCancel(driver_id_, client_);
}

void TestSetSubscribeCancel(const DriverID &driver_id,
                            std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a set entry.
  ObjectID object_id = ObjectID::from_random();
  std::vector<std::string> managers = {"jkl", "mno", "pqr"};
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = managers[0];
  RAY_CHECK_OK(client->object_table().Add(driver_id, object_id, data, nullptr));

  // The callback for a notification from the object table. This should only be
  // received for the object that we requested notifications for.
  auto notification_callback = [object_id, managers](
      gcs::AsyncGcsClient *client, const ObjectID &id,
      const GcsTableNotificationMode notification_mode,
      const std::vector<ObjectTableDataT> &data) {
    ASSERT_EQ(notification_mode, GcsTableNotificationMode::APPEND_OR_ADD);
    ASSERT_EQ(id, object_id);
    // Check that we get a duplicate notification for the first write. We get a
    // duplicate notification because notifications
    // are canceled after the first write, then requested again.
    if (data.size() == 1) {
      // first notification
      ASSERT_EQ(data[0].manager, managers[0]);
      test->IncrementNumCallbacks();
    } else {
      // second notification
      ASSERT_EQ(data.size(), managers.size());
      std::unordered_set<std::string> managers_set(managers.begin(), managers.end());
      std::unordered_set<std::string> data_managers_set;
      for (const auto &entry : data) {
        data_managers_set.insert(entry.manager);
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
  auto subscribe_callback = [driver_id, object_id,
                             managers](gcs::AsyncGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        driver_id, object_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->object_table().CancelNotifications(
        driver_id, object_id, client->client_table().GetLocalClientId()));
    // Add to the key. Since we canceled notifications, we should not
    // receive a notification for these writes.
    auto remaining = std::vector<std::string>(++managers.begin(), managers.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = manager;
      RAY_CHECK_OK(client->object_table().Add(driver_id, object_id, data, nullptr));
    }
    // Request notifications again. We should receive a notification for the
    // current values at the key.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        driver_id, object_id, client->client_table().GetLocalClientId()));
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->object_table().Subscribe(
      driver_id, client->client_table().GetLocalClientId(), notification_callback,
      subscribe_callback));
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
  TestSetSubscribeCancel(driver_id_, client_);
}

void ClientTableNotification(gcs::AsyncGcsClient *client, const ClientID &client_id,
                             const ClientTableDataT &data, bool is_insertion) {
  ClientID added_id = client->client_table().GetLocalClientId();
  ASSERT_EQ(client_id, added_id);
  ASSERT_EQ(ClientID::from_binary(data.client_id), added_id);
  ASSERT_EQ(ClientID::from_binary(data.client_id), added_id);
  ASSERT_EQ(data.is_insertion, is_insertion);

  ClientTableDataT cached_client;
  client->client_table().GetClient(added_id, cached_client);
  ASSERT_EQ(ClientID::from_binary(cached_client.client_id), added_id);
  ASSERT_EQ(cached_client.is_insertion, is_insertion);
}

void TestClientTableConnect(const DriverID &driver_id,
                            std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, true);
        test->Stop();
      });

  // Connect and disconnect to client table. We should receive notifications
  // for the addition and removal of our own entry.
  ClientTableDataT local_client_info = client->client_table().GetLocalClient();
  local_client_info.node_manager_address = "127.0.0.1";
  local_client_info.node_manager_port = 0;
  local_client_info.object_manager_port = 0;
  RAY_CHECK_OK(client->client_table().Connect(local_client_info));
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableConnect) {
  test = this;
  TestClientTableConnect(driver_id_, client_);
}

void TestClientTableDisconnect(const DriverID &driver_id,
                               std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, /*is_insertion=*/true);
        // Disconnect from the client table. We should receive a notification
        // for the removal of our own entry.
        RAY_CHECK_OK(client->client_table().Disconnect());
      });
  client->client_table().RegisterClientRemovedCallback(
      [](gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, /*is_insertion=*/false);
        test->Stop();
      });
  // Connect to the client table. We should receive notification for the
  // addition of our own entry.
  ClientTableDataT local_client_info = client->client_table().GetLocalClient();
  local_client_info.node_manager_address = "127.0.0.1";
  local_client_info.node_manager_port = 0;
  local_client_info.object_manager_port = 0;
  RAY_CHECK_OK(client->client_table().Connect(local_client_info));
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableDisconnect) {
  test = this;
  TestClientTableDisconnect(driver_id_, client_);
}

void TestClientTableImmediateDisconnect(const DriverID &driver_id,
                                        std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, true);
      });
  client->client_table().RegisterClientRemovedCallback(
      [](gcs::AsyncGcsClient *client, const ClientID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, false);
        test->Stop();
      });
  // Connect to then immediately disconnect from the client table. We should
  // receive notifications for the addition and removal of our own entry.
  ClientTableDataT local_client_info = client->client_table().GetLocalClient();
  local_client_info.node_manager_address = "127.0.0.1";
  local_client_info.node_manager_port = 0;
  local_client_info.object_manager_port = 0;
  RAY_CHECK_OK(client->client_table().Connect(local_client_info));
  RAY_CHECK_OK(client->client_table().Disconnect());
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableImmediateDisconnect) {
  test = this;
  TestClientTableImmediateDisconnect(driver_id_, client_);
}

void TestClientTableMarkDisconnected(const DriverID &driver_id,
                                     std::shared_ptr<gcs::AsyncGcsClient> client) {
  ClientTableDataT local_client_info = client->client_table().GetLocalClient();
  local_client_info.node_manager_address = "127.0.0.1";
  local_client_info.node_manager_port = 0;
  local_client_info.object_manager_port = 0;
  // Connect to the client table to start receiving notifications.
  RAY_CHECK_OK(client->client_table().Connect(local_client_info));
  // Mark a different client as dead.
  ClientID dead_client_id = ClientID::from_random();
  RAY_CHECK_OK(client->client_table().MarkDisconnected(dead_client_id));
  // Make sure we only get a notification for the removal of the client we
  // marked as dead.
  client->client_table().RegisterClientRemovedCallback([dead_client_id](
      gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
    ASSERT_EQ(ClientID::from_binary(data.client_id), dead_client_id);
    test->Stop();
  });
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableMarkDisconnected) {
  test = this;
  TestClientTableMarkDisconnected(driver_id_, client_);
}

#undef TEST_MACRO

}  // namespace gcs
}  // namespace ray
