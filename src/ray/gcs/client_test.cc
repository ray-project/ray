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

/* Flush redis. */
static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

class TestGcs : public ::testing::Test {
 public:
  TestGcs() : num_callbacks_(0) {
    client_ = std::make_shared<gcs::AsyncGcsClient>();
    ClientTableDataT client_info;
    client_info.client_id = ClientID::from_random().binary();
    client_info.node_manager_address = "127.0.0.1";
    client_info.local_scheduler_port = 0;
    client_info.object_manager_port = 0;
    RAY_CHECK_OK(client_->Connect("127.0.0.1", 6379, client_info));

    job_id_ = JobID::from_random();
  }

  virtual ~TestGcs() {
    // Clear all keys in the GCS.
    flushall_redis();
  };

  virtual void Start() = 0;

  virtual void Stop() = 0;

  int64_t NumCallbacks() const { return num_callbacks_; }

  void IncrementNumCallbacks() { num_callbacks_++; }

 protected:
  int64_t num_callbacks_;
  std::shared_ptr<gcs::AsyncGcsClient> client_;
  JobID job_id_;
};

TestGcs *test;

class TestGcsWithAe : public TestGcs {
 public:
  TestGcsWithAe() {
    loop_ = aeCreateEventLoop(1024);
    RAY_CHECK_OK(client_->context()->AttachToEventLoop(loop_));
  }
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

class TestGcsWithAsio : public TestGcs {
 public:
  TestGcsWithAsio() : TestGcs(), io_service_(), work_(io_service_) {
    RAY_CHECK_OK(client_->Attach(io_service_));
  }
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

void ObjectAdded(gcs::AsyncGcsClient *client, const UniqueID &id,
                 const ObjectTableDataT &data) {
  ASSERT_EQ(data.managers, std::vector<std::string>({"A", "B"}));
}

void Lookup(gcs::AsyncGcsClient *client, const UniqueID &id,
            const ObjectTableDataT &data) {
  // Check that the object entry was added.
  ASSERT_EQ(data.managers, std::vector<std::string>({"A", "B"}));
  test->Stop();
}

void LookupFailed(gcs::AsyncGcsClient *client, const UniqueID &id) {
  // Object entry failed.
  RAY_CHECK(false);
  test->Stop();
}

void TestObjectTable(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back("A");
  data->managers.push_back("B");
  ObjectID object_id = ObjectID::from_random();
  RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, &ObjectAdded));
  RAY_CHECK_OK(client->object_table().Lookup(job_id, object_id, &Lookup, &LookupFailed));
  // Run the event loop. The loop will only stop if the Lookup callback is
  // called (or an assertion failure).
  test->Start();
}

TEST_F(TestGcsWithAe, TestObjectTable) {
  test = this;
  TestObjectTable(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestObjectTable) {
  test = this;
  TestObjectTable(job_id_, client_);
}

void TestLookupFailure(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  auto object_id = ObjectID::from_random();
  // Looking up an empty object ID should call the failure callback.
  auto failure_callback = [](gcs::AsyncGcsClient *client, const UniqueID &id) {
    test->Stop();
  };
  RAY_CHECK_OK(
      client->object_table().Lookup(job_id, object_id, nullptr, failure_callback));
  // Run the event loop. The loop will only stop if the failure callback is
  // called.
  test->Start();
}

TEST_F(TestGcsWithAe, TestLookupFailure) {
  test = this;
  TestLookupFailure(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestLookupFailure) {
  test = this;
  TestLookupFailure(job_id_, client_);
}

void TaskAdded(gcs::AsyncGcsClient *client, const TaskID &id,
               const TaskTableDataT &data) {
  ASSERT_EQ(data.scheduling_state, SchedulingState_SCHEDULED);
}

void TaskLookup(gcs::AsyncGcsClient *client, const TaskID &id,
                const TaskTableDataT &data) {
  ASSERT_EQ(data.scheduling_state, SchedulingState_SCHEDULED);
}

void TaskLookupFailure(gcs::AsyncGcsClient *client, const TaskID &id) {
  RAY_CHECK(false);
}

void TaskLookupAfterUpdate(gcs::AsyncGcsClient *client, const TaskID &id,
                           const TaskTableDataT &data) {
  ASSERT_EQ(data.scheduling_state, SchedulingState_LOST);
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
  data->scheduling_state = SchedulingState_SCHEDULED;
  ClientID local_scheduler_id = ClientID::from_binary("abcdefghijklmnopqrst");
  data->scheduler_id = local_scheduler_id.binary();
  TaskID task_id = TaskID::from_random();
  RAY_CHECK_OK(client->task_table().Add(job_id, task_id, data, &TaskAdded));
  RAY_CHECK_OK(
      client->task_table().Lookup(job_id, task_id, &TaskLookup, &TaskLookupFailure));
  auto update = std::make_shared<TaskTableTestAndUpdateT>();
  update->test_scheduler_id = local_scheduler_id.binary();
  update->test_state_bitmask = SchedulingState_SCHEDULED;
  update->update_state = SchedulingState_LOST;
  // After test-and-setting, the callback will lookup the current state of the
  // task.
  RAY_CHECK_OK(
      client->task_table().TestAndUpdate(job_id, task_id, update, &TaskUpdateCallback));
  // Run the event loop. The loop will only stop if the lookup after the
  // test-and-set succeeds (or an assertion failure).
  test->Start();
}

TEST_F(TestGcsWithAe, TestTaskTable) {
  test = this;
  TestTaskTable(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestTaskTable) {
  test = this;
  TestTaskTable(job_id_, client_);
}

void TestSubscribeAll(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  ObjectID object_id = ObjectID::from_random();
  // Callback for a notification.
  auto notification_callback = [object_id](
      gcs::AsyncGcsClient *client, const UniqueID &id, const ObjectTableDataT &data) {
    ASSERT_EQ(id, object_id);
    // Check that the object entry was added.
    ASSERT_EQ(data.managers, std::vector<std::string>({"A", "B"}));
    test->IncrementNumCallbacks();
    test->Stop();
  };

  // Callback for subscription success. This should only be called once.
  auto subscribe_callback = [job_id, object_id](gcs::AsyncGcsClient *client) {
    test->IncrementNumCallbacks();
    // We have subscribed. Add an object table entry.
    auto data = std::make_shared<ObjectTableDataT>();
    data->managers.push_back("A");
    data->managers.push_back("B");
    RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, &ObjectAdded));
  };

  // Subscribe to all object table notifications. Once we have successfully
  // subscribed, we will add an object and check that we get notified of the
  // operation.
  RAY_CHECK_OK(client->object_table().Subscribe(
      job_id, ClientID::nil(), notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one callback for subscription success and one for
  // the Add notification.
  ASSERT_EQ(test->NumCallbacks(), 2);
}

TEST_F(TestGcsWithAe, TestSubscribeAll) {
  test = this;
  TestSubscribeAll(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestSubscribeAll) {
  test = this;
  TestSubscribeAll(job_id_, client_);
}

void TestSubscribeId(const JobID &job_id, std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Add an object table entry.
  ObjectID object_id1 = ObjectID::from_random();
  auto data1 = std::make_shared<ObjectTableDataT>();
  data1->managers.push_back("A");
  data1->managers.push_back("B");
  RAY_CHECK_OK(client->object_table().Add(job_id, object_id1, data1, nullptr));

  // Add a second object table entry.
  ObjectID object_id2 = ObjectID::from_random();
  auto data2 = std::make_shared<ObjectTableDataT>();
  data2->managers.push_back("C");
  RAY_CHECK_OK(client->object_table().Add(job_id, object_id2, data2, nullptr));

  // The callback for subscription success. Once we've subscribed, request
  // notifications for the second object that was added.
  auto subscribe_callback = [job_id, object_id2](gcs::AsyncGcsClient *client) {
    test->IncrementNumCallbacks();
    // Request notifications for the second object. Since we already added the
    // entry to the table, we should receive an initial notification for its
    // current value.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id2, client->client_table().GetLocalClientId()));
    // Overwrite the entry for the object. We should receive a second
    // notification for its new value.
    auto data = std::make_shared<ObjectTableDataT>();
    data->managers.push_back("C");
    data->managers.push_back("D");
    RAY_CHECK_OK(client->object_table().Add(job_id, object_id2, data, nullptr));
  };

  // The callback for a notification from the object table. This should only be
  // received for the object that we requested notifications for.
  auto notification_callback = [data2, object_id2](
      gcs::AsyncGcsClient *client, const UniqueID &id, const ObjectTableDataT &data) {
    ASSERT_EQ(id, object_id2);
    // Check that we got a notification for the correct object.
    ASSERT_EQ(data.managers.front(), "C");
    test->IncrementNumCallbacks();
    // Stop the loop once we've received notifications for both writes to the
    // object key.
    if (test->NumCallbacks() == 3) {
      test->Stop();
    }
  };

  RAY_CHECK_OK(
      client->object_table().Subscribe(job_id, client->client_table().GetLocalClientId(),
                                       notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called for both writes to the object key.
  test->Start();
  // Check that we received one callback for subscription success and two
  // callbacks for the Add notifications.
  ASSERT_EQ(test->NumCallbacks(), 3);
}

TEST_F(TestGcsWithAe, TestSubscribeId) {
  test = this;
  TestSubscribeId(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestSubscribeId) {
  test = this;
  TestSubscribeId(job_id_, client_);
}

void TestSubscribeCancel(const JobID &job_id,
                         std::shared_ptr<gcs::AsyncGcsClient> client) {
  // Write the object table once.
  ObjectID object_id = ObjectID::from_random();
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back("A");
  RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, nullptr));

  // The callback for subscription success. Once we've subscribed, request
  // notifications for the second object that was added.
  auto subscribe_callback = [job_id, object_id](gcs::AsyncGcsClient *client) {
    test->IncrementNumCallbacks();
    // Request notifications for the object. We should receive a notification
    // for the current value at the key.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
    // Cancel notifications.
    RAY_CHECK_OK(client->object_table().CancelNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
    // Write the object table entry twice. Since we canceled notifications, we
    // should not get notifications for either of these writes.
    auto data = std::make_shared<ObjectTableDataT>();
    data->managers.push_back("B");
    RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, nullptr));
    data = std::make_shared<ObjectTableDataT>();
    data->managers.push_back("C");
    RAY_CHECK_OK(client->object_table().Add(job_id, object_id, data, nullptr));
    // Request notifications for the object again. We should only receive a
    // notification for the current value at the key.
    RAY_CHECK_OK(client->object_table().RequestNotifications(
        job_id, object_id, client->client_table().GetLocalClientId()));
  };

  // The callback for a notification from the object table.
  auto notification_callback = [object_id](
      gcs::AsyncGcsClient *client, const UniqueID &id, const ObjectTableDataT &data) {
    ASSERT_EQ(id, object_id);
    // Check that we only receive notifications for the key when we have
    // requested notifications for it. We should not get a notification for the
    // entry that began with "B" since we canceled notifications then.
    if (test->NumCallbacks() == 1) {
      ASSERT_EQ(data.managers.front(), "A");
    } else {
      ASSERT_EQ(data.managers.front(), "C");
    }
    test->IncrementNumCallbacks();
    if (test->NumCallbacks() == 3) {
      test->Stop();
    }
  };

  RAY_CHECK_OK(
      client->object_table().Subscribe(job_id, client->client_table().GetLocalClientId(),
                                       notification_callback, subscribe_callback));

  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
  // Check that we received one callback for subscription success and two
  // callbacks for the Add notifications.
  ASSERT_EQ(test->NumCallbacks(), 3);
}

TEST_F(TestGcsWithAe, TestSubscribeCancel) {
  test = this;
  TestSubscribeCancel(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestSubscribeCancel) {
  test = this;
  TestSubscribeCancel(job_id_, client_);
}

void ClientTableNotification(gcs::AsyncGcsClient *client, const UniqueID &id,
                             const ClientTableDataT &data, bool is_insertion) {
  ClientID added_id = client->client_table().GetLocalClientId();
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
  RAY_CHECK_OK(client->client_table().Connect());
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
        ClientTableNotification(client, id, data, true);
      });
  client->client_table().RegisterClientRemovedCallback(
      [](gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
        ClientTableNotification(client, id, data, false);
        test->Stop();
      });
  // Connect and disconnect to client table. We should receive notifications
  // for the addition and removal of our own entry.
  RAY_CHECK_OK(client->client_table().Connect());
  RAY_CHECK_OK(client->client_table().Disconnect());
  test->Start();
}

TEST_F(TestGcsWithAsio, TestClientTableDisconnect) {
  test = this;
  TestClientTableDisconnect(job_id_, client_);
}

}  // namespace
