#include "gtest/gtest.h"

// TODO(pcm): get rid of this and replace with the type safe plasma event loop
extern "C" {
#include "hiredis/adapters/ae.h"
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"

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
    client_ = std::make_shared<gcs::AsyncGcsClient>(command_type_);
    RAY_CHECK_OK(client_->Connect("127.0.0.1", 6379));

    job_id_ = JobID::from_random();
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
  JobID job_id_;
};

TestGcs *test;

class TestGcsWithAe : public TestGcs {
 public:
  TestGcsWithAe(CommandType command_type) : TestGcs(command_type) {
    loop_ = aeCreateEventLoop(1024);
    RAY_CHECK_OK(client_->context()->AttachToEventLoop(loop_));
  }

  TestGcsWithAe() : TestGcsWithAe(CommandType::kRegular) {}

  ~TestGcsWithAe() override {
    // Destroy the client first since it has a reference to the event loop.
    client_.reset();
    aeDeleteEventLoop(loop_);
  }
  void Start() override { aeMain(loop_); }
  void Stop() override { aeStop(loop_); }

 private:
  aeEventLoop *loop_;
};

class TestGcsWithChainAe : public TestGcsWithAe {
 public:
  TestGcsWithChainAe() : TestGcsWithAe(gcs::CommandType::kChain){};
};

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

void TestTableLookup(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
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

TEST_MACRO(TestGcsWithAe, TestTableLookup);
TEST_MACRO(TestGcsWithAsio, TestTableLookup);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAe, TestTableLookup);
TEST_MACRO(TestGcsWithChainAsio, TestTableLookup);
#endif

void TestLogLookup(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Append some entries to the log at an object ID.
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
    };
    RAY_CHECK_OK(client->object_table().Append(job_id, object_id, data, add_callback));
  }

  // Check that lookup returns the added object entries.
  auto lookup_callback = [object_id, managers](
      gcs::AsyncGcsClient *client, const ObjectID &id,
      const std::vector<ObjectTableDataT> &data) {
    ASSERT_EQ(id, object_id);
    for (const auto &entry : data) {
      ASSERT_EQ(entry.manager, managers[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == managers.size()) {
      test->Stop();
    }
  };

  // Do a lookup at the object ID.
  RAY_CHECK_OK(client->object_table().Lookup(job_id, object_id, lookup_callback));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), managers.size());
}

TEST_F(TestGcsWithAe, TestLogLookup) {
  test = this;
  TestLogLookup(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestLogLookup) {
  test = this;
  TestLogLookup(job_id_, client_);
}

void TestTableLookupFailure(const JobID &job_id,
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
  RAY_CHECK_OK(client->raylet_task_table().Lookup(job_id, task_id, lookup_callback,
                                                  failure_callback));
  // Run the event loop. The loop will only stop if the failure callback is
  // called (or an assertion failure).
  test->Start();
}

TEST_MACRO(TestGcsWithAe, TestTableLookupFailure);
TEST_MACRO(TestGcsWithAsio, TestTableLookupFailure);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAe, TestTableLookupFailure);
TEST_MACRO(TestGcsWithChainAsio, TestTableLookupFailure);
#endif

void TestLogAppendAt(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  TaskID task_id = TaskID::from_random();
  std::vector<std::string> managers = {"A", "B"};
  std::vector<std::shared_ptr<TaskReconstructionDataT>> data_log;
  for (const auto &manager : managers) {
    auto data = std::make_shared<TaskReconstructionDataT>();
    data->node_manager_id = manager;
    data_log.push_back(data);
  }

  // Check that we added the correct task.
  auto failure_callback = [task_id](gcs::AsyncGcsClient *client, const UniqueID &id,
                                    const TaskReconstructionDataT &d) {
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

  auto lookup_callback = [managers](gcs::AsyncGcsClient *client, const UniqueID &id,
                                    const std::vector<TaskReconstructionDataT> &data) {
    std::vector<std::string> appended_managers;
    for (const auto &entry : data) {
      appended_managers.push_back(entry.node_manager_id);
    }
    ASSERT_EQ(appended_managers, managers);
    test->Stop();
  };
  RAY_CHECK_OK(
      client->task_reconstruction_log().Lookup(job_id, task_id, lookup_callback));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
  ASSERT_EQ(test->NumCallbacks(), 2);
}

TEST_F(TestGcsWithAe, TestLogAppendAt) {
  test = this;
  TestLogAppendAt(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestLogAppendAt) {
  test = this;
  TestLogAppendAt(job_id_, client_);
}

// Task table callbacks.
void TaskAdded(gcs::AsyncGcsClient *client, const TaskID &id,
               const TaskTableDataT &data) {
  ASSERT_EQ(data.scheduling_state, SchedulingState::SCHEDULED);
  ASSERT_EQ(data.scheduler_id, kRandomId);
}

void TaskLookupHelper(gcs::AsyncGcsClient *client, const TaskID &id,
                      const TaskTableDataT &data, bool do_stop) {
  ASSERT_EQ(data.scheduling_state, SchedulingState::SCHEDULED);
  ASSERT_EQ(data.scheduler_id, kRandomId);
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

void TaskUpdateCallback(gcs::AsyncGcsClient *client, const TaskID &task_id,
                        const TaskTableDataT &task, bool updated) {
  RAY_CHECK_OK(client->task_table().Lookup(DriverID::nil(), task_id,
                                           &TaskLookupAfterUpdate, &TaskLookupFailure));
}

void TestTaskTable(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  auto data = std::make_shared<TaskTableDataT>();
  data->scheduling_state = SchedulingState::SCHEDULED;
  ClientID local_scheduler_id = ClientID::from_binary(kRandomId);
  data->scheduler_id = local_scheduler_id.binary();
  TaskID task_id = TaskID::from_random();
  RAY_CHECK_OK(client->task_table().Add(job_id, task_id, data, &TaskAdded));
  RAY_CHECK_OK(
      client->task_table().Lookup(job_id, task_id, &TaskLookup, &TaskLookupFailure));
  auto update = std::make_shared<TaskTableTestAndUpdateT>();
  update->test_scheduler_id = local_scheduler_id.binary();
  update->test_state_bitmask = SchedulingState::SCHEDULED;
  update->update_state = SchedulingState::LOST;
  // After test-and-setting, the callback will lookup the current state of the
  // task.
  RAY_CHECK_OK(
      client->task_table().TestAndUpdate(job_id, task_id, update, &TaskUpdateCallback));
  // Run the event loop. The loop will only stop if the lookup after the
  // test-and-set succeeds (or an assertion failure).
  test->Start();
}

TEST_MACRO(TestGcsWithAe, TestTaskTable);
TEST_MACRO(TestGcsWithAsio, TestTaskTable);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAe, TestTaskTable);
TEST_MACRO(TestGcsWithChainAsio, TestTaskTable);
#endif

void TestTableSubscribeAll(const JobID &job_id,
                           std::shared_ptr<gcs::AsyncGcsClient> client) {
  TaskID task_id = TaskID::from_random();
  std::vector<std::string> task_specs = {"abc", "def", "ghi"};
  // Callback for a notification.
  auto notification_callback = [task_id, task_specs](
      gcs::AsyncGcsClient *client, const UniqueID &id, const protocol::TaskT &data) {
    ASSERT_EQ(id, task_id);
    // Check that we get notifications in the same order as the writes.
    ASSERT_EQ(data.task_specification, task_specs[test->NumCallbacks()]);
    test->IncrementNumCallbacks();
    if (test->NumCallbacks() == task_specs.size()) {
      test->Stop();
    }
  };

  // Callback for subscription success. We are guaranteed to receive
  // notifications after this is called.
  auto subscribe_callback = [job_id, task_id, task_specs](gcs::AsyncGcsClient *client) {
    // We have subscribed. Do the writes to the table.
    for (const auto &task_spec : task_specs) {
      auto data = std::make_shared<protocol::TaskT>();
      data->task_specification = task_spec;
      RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id, data, nullptr));
    }
  };

  // Subscribe to all task table notifications. Once we have successfully
  // subscribed, we will write the key several times and check that we get
  // notified for each.
  RAY_CHECK_OK(client->raylet_task_table().Subscribe(
      job_id, ClientID::nil(), notification_callback, subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one notification callback for each write.
  ASSERT_EQ(test->NumCallbacks(), task_specs.size());
}

TEST_MACRO(TestGcsWithAe, TestTableSubscribeAll);
TEST_MACRO(TestGcsWithAsio, TestTableSubscribeAll);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAe, TestTableSubscribeAll);
TEST_MACRO(TestGcsWithChainAsio, TestTableSubscribeAll);
#endif

void TestLogSubscribeAll(const JobID &job_id,
                         std::shared_ptr<gcs::AsyncGcsClient> client) {
  std::vector<std::string> managers = {"abc", "def", "ghi"};
  std::vector<ObjectID> object_ids;
  for (size_t i = 0; i < managers.size(); i++) {
    object_ids.push_back(ObjectID::from_random());
  }
  // Callback for a notification.
  auto notification_callback = [object_ids, managers](
      gcs::AsyncGcsClient *client, const UniqueID &id,
      const std::vector<ObjectTableDataT> data) {
    ASSERT_EQ(id, object_ids[test->NumCallbacks()]);
    // Check that we get notifications in the same order as the writes.
    for (const auto &entry : data) {
      ASSERT_EQ(entry.manager, managers[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == managers.size()) {
      test->Stop();
    }
  };

  // Callback for subscription success. We are guaranteed to receive
  // notifications after this is called.
  auto subscribe_callback = [job_id, object_ids, managers](gcs::AsyncGcsClient *client) {
    // We have subscribed. Do the writes to the table.
    for (size_t i = 0; i < object_ids.size(); i++) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = managers[i];
      RAY_CHECK_OK(client->object_table().Append(job_id, object_ids[i], data, nullptr));
    }
  };

  // Subscribe to all task table notifications. Once we have successfully
  // subscribed, we will append to the key several times and check that we get
  // notified for each.
  RAY_CHECK_OK(client->object_table().Subscribe(
      job_id, ClientID::nil(), notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one notification callback for each write.
  ASSERT_EQ(test->NumCallbacks(), managers.size());
}

TEST_F(TestGcsWithAe, TestLogSubscribeAll) {
  test = this;
  TestLogSubscribeAll(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestLogSubscribeAll) {
  test = this;
  TestLogSubscribeAll(job_id_, client_);
}

void TestTableSubscribeId(const JobID &job_id,
                          std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a table entry.
  TaskID task_id1 = TaskID::from_random();
  std::vector<std::string> task_specs1 = {"abc", "def", "ghi"};
  auto data1 = std::make_shared<protocol::TaskT>();
  data1->task_specification = task_specs1[0];
  RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id1, data1, nullptr));

  // Add a table entry at a second key.
  TaskID task_id2 = TaskID::from_random();
  std::vector<std::string> task_specs2 = {"jkl", "mno", "pqr"};
  auto data2 = std::make_shared<protocol::TaskT>();
  data2->task_specification = task_specs2[0];
  RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id2, data2, nullptr));

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

  // The callback for subscription success. Once we've subscribed, request
  // notifications for only one of the keys, then write to both keys.
  auto subscribe_callback = [job_id, task_id1, task_id2, task_specs1,
                             task_specs2](gcs::AsyncGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        job_id, task_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    auto remaining = std::vector<std::string>(++task_specs1.begin(), task_specs1.end());
    for (const auto &task_spec : remaining) {
      auto data = std::make_shared<protocol::TaskT>();
      data->task_specification = task_spec;
      RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id1, data, nullptr));
    }
    remaining = std::vector<std::string>(++task_specs2.begin(), task_specs2.end());
    for (const auto &task_spec : remaining) {
      auto data = std::make_shared<protocol::TaskT>();
      data->task_specification = task_spec;
      RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id2, data, nullptr));
    }
  };

  // Subscribe to notifications for this client. This allows us to request and
  // receive notifications for specific keys.
  RAY_CHECK_OK(client->raylet_task_table().Subscribe(
      job_id, client->client_table().GetLocalClientId(), notification_callback,
      subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received one notification callback for each write to the
  // requested key.
  ASSERT_EQ(test->NumCallbacks(), task_specs2.size());
}

TEST_MACRO(TestGcsWithAe, TestTableSubscribeId);
TEST_MACRO(TestGcsWithAsio, TestTableSubscribeId);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAe, TestTableSubscribeId);
TEST_MACRO(TestGcsWithChainAsio, TestTableSubscribeId);
#endif

void TestLogSubscribeId(const JobID &job_id,
                        std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a log entry.
  ObjectID object_id1 = ObjectID::from_random();
  std::vector<std::string> managers1 = {"abc", "def", "ghi"};
  auto data1 = std::make_shared<ObjectTableDataT>();
  data1->manager = managers1[0];
  RAY_CHECK_OK(client->object_table().Append(job_id, object_id1, data1, nullptr));

  // Add a log entry at a second key.
  ObjectID object_id2 = ObjectID::from_random();
  std::vector<std::string> managers2 = {"jkl", "mno", "pqr"};
  auto data2 = std::make_shared<ObjectTableDataT>();
  data2->manager = managers2[0];
  RAY_CHECK_OK(client->object_table().Append(job_id, object_id2, data2, nullptr));

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto notification_callback = [object_id2, managers2](
      gcs::AsyncGcsClient *client, const ObjectID &id,
      const std::vector<ObjectTableDataT> &data) {
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
  auto subscribe_callback = [job_id, object_id1, object_id2, managers1,
                             managers2](gcs::AsyncGcsClient *client) {
    // Request notifications for one of the keys.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id2, client->client_table().GetLocalClientId()));
    // Write both keys. We should only receive notifications for the key that
    // we requested them for.
    auto remaining = std::vector<std::string>(++managers1.begin(), managers1.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = manager;
      RAY_CHECK_OK(client->object_table().Append(job_id, object_id1, data, nullptr));
    }
    remaining = std::vector<std::string>(++managers2.begin(), managers2.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = manager;
      RAY_CHECK_OK(client->object_table().Append(job_id, object_id2, data, nullptr));
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

TEST_F(TestGcsWithAe, TestLogSubscribeId) {
  test = this;
  TestLogSubscribeId(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestLogSubscribeId) {
  test = this;
  TestLogSubscribeId(job_id_, client_);
}

void TestTableSubscribeCancel(const JobID &job_id,
                              std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a table entry.
  TaskID task_id = TaskID::from_random();
  std::vector<std::string> task_specs = {"jkl", "mno", "pqr"};
  auto data = std::make_shared<protocol::TaskT>();
  data->task_specification = task_specs[0];
  RAY_CHECK_OK(client->raylet_task_table().Add(job_id, task_id, data, nullptr));

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
  auto subscribe_callback = [job_id, task_id, task_specs](gcs::AsyncGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->raylet_task_table().RequestNotifications(
        job_id, task_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->raylet_task_table().CancelNotifications(
        job_id, task_id, client->client_table().GetLocalClientId()));
    // Write to the key. Since we canceled notifications, we should not receive
    // a notification for these writes.
    auto remaining = std::vector<std::string>(++task_specs.begin(), task_specs.end());
    for (const auto &task_spec : remaining) {
      auto data = std::make_shared<protocol::TaskT>();
      data->task_specification = task_spec;
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
      subscribe_callback));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for the requested key.
  test->Start();
  // Check that we received a notification callback for the first and least
  // writes to the key, since notifications are canceled in between.
  ASSERT_EQ(test->NumCallbacks(), 2);
}

TEST_MACRO(TestGcsWithAe, TestTableSubscribeCancel);
TEST_MACRO(TestGcsWithAsio, TestTableSubscribeCancel);
#if RAY_USE_NEW_GCS
TEST_MACRO(TestGcsWithChainAe, TestTableSubscribeCancel);
TEST_MACRO(TestGcsWithChainAsio, TestTableSubscribeCancel);
#endif

void TestLogSubscribeCancel(const JobID &job_id,
                            std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add a log entry.
  ObjectID object_id = ObjectID::from_random();
  std::vector<std::string> managers = {"jkl", "mno", "pqr"};
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = managers[0];
  RAY_CHECK_OK(client->object_table().Append(job_id, object_id, data, nullptr));

  // The callback for a notification from the object table. This should only be
  // received for the object that we requested notifications for.
  auto notification_callback = [object_id, managers](
      gcs::AsyncGcsClient *client, const ObjectID &id,
      const std::vector<ObjectTableDataT> &data) {
    ASSERT_EQ(id, object_id);
    // Check that we get a duplicate notification for the first write. We get a
    // duplicate notification because the log is append-only and notifications
    // are canceled after the first write, then requested again.
    auto managers_copy = managers;
    managers_copy.insert(managers_copy.begin(), managers_copy.front());
    for (const auto &entry : data) {
      ASSERT_EQ(entry.manager, managers_copy[test->NumCallbacks()]);
      test->IncrementNumCallbacks();
    }
    if (test->NumCallbacks() == managers_copy.size()) {
      test->Stop();
    }
  };

  // The callback for a notification from the table. This should only be
  // received for keys that we requested notifications for.
  auto subscribe_callback = [job_id, object_id, managers](gcs::AsyncGcsClient *client) {
    // Request notifications, then cancel immediately. We should receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
    RAY_CHECK_OK(client->object_table().CancelNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
    // Append to the key. Since we canceled notifications, we should not
    // receive a notification for these writes.
    auto remaining = std::vector<std::string>(++managers.begin(), managers.end());
    for (const auto &manager : remaining) {
      auto data = std::make_shared<ObjectTableDataT>();
      data->manager = manager;
      RAY_CHECK_OK(client->object_table().Append(job_id, object_id, data, nullptr));
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

TEST_F(TestGcsWithAe, TestLogSubscribeCancel) {
  test = this;
  TestLogSubscribeCancel(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestLogSubscribeCancel) {
  test = this;
  TestLogSubscribeCancel(job_id_, client_);
}

void ClientTableNotification(gcs::AsyncGcsClient *client, const ClientID &client_id,
                             const ClientTableDataT &data, bool is_insertion) {
  ClientID added_id = client->client_table().GetLocalClientId();
  ASSERT_EQ(client_id, added_id);
  ASSERT_EQ(ClientID::from_binary(data.client_id), added_id);
  ASSERT_EQ(ClientID::from_binary(data.client_id), added_id);
  ASSERT_EQ(data.is_insertion, is_insertion);

  auto cached_client = client->client_table().GetClient(added_id);
  ASSERT_EQ(ClientID::from_binary(cached_client.client_id), added_id);
  ASSERT_EQ(cached_client.is_insertion, is_insertion);
}

void TestClientTableConnect(const JobID &job_id,
                            std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
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
  TestClientTableConnect(job_id_, client_);
}

void TestClientTableDisconnect(const JobID &job_id,
                               std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, /*is_insertion=*/true);
        // Disconnect from the client table. We should receive a notification
        // for the removal of our own entry.
        RAY_CHECK_OK(client->client_table().Disconnect());
      });
  client->client_table().RegisterClientRemovedCallback(
      [](gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
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
  TestClientTableDisconnect(job_id_, client_);
}

void TestClientTableImmediateDisconnect(const JobID &job_id,
                                        std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Register callbacks for when a client gets added and removed. The latter
  // event will stop the event loop.
  client->client_table().RegisterClientAddedCallback(
      [](gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, true);
      });
  client->client_table().RegisterClientRemovedCallback(
      [](gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
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
  TestClientTableImmediateDisconnect(job_id_, client_);
}

void TestClientTableMarkDisconnected(const JobID &job_id,
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
  TestClientTableMarkDisconnected(job_id_, client_);
}

#undef TEST_MACRO

}  // namespace gcs
}  // namespace ray
