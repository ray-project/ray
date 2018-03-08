#include "gtest/gtest.h"

// TODO(pcm): get rid of this and replace with the type safe plasma event loop
extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
}

#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"

namespace ray {

class TestGcs : public ::testing::Test {
 public:
  TestGcs() {
    RAY_CHECK_OK(client_.Connect("127.0.0.1", 6379));
    job_id_ = UniqueID::from_random();
  }

  virtual ~TestGcs(){};

  virtual void Start() = 0;

  virtual void Stop() = 0;

 protected:
  gcs::AsyncGcsClient client_;
  UniqueID job_id_;
};

TestGcs *test;

class TestGcsWithAe : public TestGcs {
 public:
  TestGcsWithAe() {
    loop_ = aeCreateEventLoop(1024);
    RAY_CHECK_OK(client_.context()->AttachToEventLoop(loop_));
  }
  ~TestGcsWithAe() override { aeDeleteEventLoop(loop_); }
  void Start() override { aeMain(loop_); }
  void Stop() override { aeStop(loop_); }

 private:
  aeEventLoop *loop_;
};

class TestGcsWithAsio : public TestGcs {
 public:
  TestGcsWithAsio() { RAY_CHECK_OK(client_.Attach(io_service_)); }
  void Start() override { io_service_.run(); }
  void Stop() override { io_service_.stop(); }

 private:
  boost::asio::io_service io_service_;
};

void ObjectAdded(gcs::AsyncGcsClient *client,
                 const UniqueID &id,
                 std::shared_ptr<ObjectTableDataT> data) {
  ASSERT_EQ(data->managers, std::vector<std::string>({"A", "B"}));
}

void Lookup(gcs::AsyncGcsClient *client,
            const UniqueID &id,
            std::shared_ptr<ObjectTableDataT> data) {
  // Check that the object entry was added.
  ASSERT_EQ(data->managers, std::vector<std::string>({"A", "B"}));
  test->Stop();
}

void TestObjectTable(const UniqueID &job_id, gcs::AsyncGcsClient &client) {
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back("A");
  data->managers.push_back("B");
  ObjectID object_id = ObjectID::from_random();
  RAY_CHECK_OK(client.object_table().Add(job_id, object_id, data, &ObjectAdded));
  RAY_CHECK_OK(client.object_table().Lookup(job_id, object_id, &Lookup));
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

void TaskAdded(gcs::AsyncGcsClient *client,
               const TaskID &id,
               std::shared_ptr<TaskTableDataT> data) {
  ASSERT_EQ(data->scheduling_state, SchedulingState_SCHEDULED);
}

void TaskLookup(gcs::AsyncGcsClient *client,
                const TaskID &id,
                std::shared_ptr<TaskTableDataT> data) {
  ASSERT_EQ(data->scheduling_state, SchedulingState_SCHEDULED);
}

void TaskLookupAfterUpdate(gcs::AsyncGcsClient *client,
                           const TaskID &id,
                           std::shared_ptr<TaskTableDataT> data) {
  ASSERT_EQ(data->scheduling_state, SchedulingState_LOST);
  test->Stop();
}

void TaskUpdateCallback(gcs::AsyncGcsClient *client,
                        const TaskID &task_id,
                        const TaskTableDataT &task,
                        bool updated) {
  RAY_CHECK_OK(client->task_table().Lookup(DriverID::nil(), task_id,
                                           &TaskLookupAfterUpdate));
}

void TestTaskTable(const UniqueID &job_id, gcs::AsyncGcsClient &client) {
  auto data = std::make_shared<TaskTableDataT>();
  data->scheduling_state = SchedulingState_SCHEDULED;
  ClientID local_scheduler_id = ClientID::from_binary("abcdefghijklmnopqrst");
  data->scheduler_id = local_scheduler_id.binary();
  TaskID task_id = TaskID::from_random();
  RAY_CHECK_OK(client.task_table().Add(job_id, task_id, data, &TaskAdded));
  RAY_CHECK_OK(client.task_table().Lookup(job_id, task_id, &TaskLookup));
  auto update = std::make_shared<TaskTableTestAndUpdateT>();
  update->test_scheduler_id = local_scheduler_id.binary();
  update->test_state_bitmask = SchedulingState_SCHEDULED;
  update->update_state = SchedulingState_LOST;
  // After test-and-setting, the callback will lookup the current state of the
  // task.
  RAY_CHECK_OK(
      client.task_table().TestAndUpdate(job_id, task_id, update, &TaskUpdateCallback));
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

void ObjectTableSubscribed(gcs::AsyncGcsClient *client, const UniqueID &id,
                           std::shared_ptr<ObjectTableDataT> data) {
  test->Stop();
}

void TestSubscribeAll(const UniqueID &job_id, gcs::AsyncGcsClient &client) {
  // Subscribe to all object table notifications. The registered callback for
  // notifications will check whether the object below is added.
  RAY_CHECK_OK(client.object_table().Subscribe(job_id, ClientID::nil(), &Lookup,
                                               &ObjectTableSubscribed));
  // Run the event loop. The loop will only stop if the subscription succeeds.
  test->Start();

  // We have subscribed. Add an object table entry.
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back("A");
  data->managers.push_back("B");
  ObjectID object_id = ObjectID::from_random();
  RAY_CHECK_OK(client.object_table().Add(job_id, object_id, data, &ObjectAdded));
  // Run the event loop. The loop will only stop if the registered subscription
  // callback is called (or an assertion failure).
  test->Start();
}

TEST_F(TestGcsWithAe, TestSubscribeAll) {
  test = this;
  TestSubscribeAll(job_id_, client_);
}

TEST_F(TestGcsWithAsio, TestSubscribeAll) {
  test = this;
  TestSubscribeAll(job_id_, client_);
}

}  // namespace
