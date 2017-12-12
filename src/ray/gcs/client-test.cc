#include "gtest/gtest.h"

#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"

// TODO(pcm): get rid of this and replace with the type safe plasma event loop
extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
}

namespace ray {

aeEventLoop* loop;

class TestGCS : public ::testing::Test {
 public:
  TestGCS() {
    RAY_CHECK_OK(client_.Connect("127.0.0.1", 6379));
    job_id_ = UniqueID::from_random();
  }
 protected:
  gcs::AsyncGCSClient client_;
  UniqueID job_id_;
};

void ObjectAdded(gcs::AsyncGCSClient* client, const UniqueID& id, std::shared_ptr<ObjectTableDataT> data) {
  std::cout << "added object" << std::endl;
}

void Lookup(gcs::AsyncGCSClient* client, const UniqueID& id, std::shared_ptr<ObjectTableDataT> data) {
  std::cout << "looked up object" << std::endl;
  std::cout << "manager" << data->managers[0] << std::endl;
  aeStop(loop);
}

TEST_F(TestGCS, TestObjectTable) {
  loop = aeCreateEventLoop(1024);
  RAY_CHECK_OK(client_.context()->AttachToEventLoop(loop));
  auto data = std::make_shared<ObjectTableDataT>();
  data->managers.push_back("A");
  data->managers.push_back("B");
  ObjectID object_id = ObjectID::from_random();
  RAY_CHECK_OK(client_.object_table().Add(job_id_, object_id, data, &ObjectAdded));
  RAY_CHECK_OK(client_.object_table().Lookup(job_id_, object_id, &Lookup, &Lookup));
  aeMain(loop);
  aeDeleteEventLoop(loop);
}

void TaskAdded(gcs::AsyncGCSClient* client, const TaskID& id, std::shared_ptr<TaskTableDataT> data) {
  std::cout << "added task" << std::endl;
}

void TaskLookup(gcs::AsyncGCSClient* client, const TaskID& id, std::shared_ptr<TaskTableDataT> data) {
  std::cout << "scheduling_state = " << data->scheduling_state << std::endl;
  aeStop(loop);
}

TEST_F(TestGCS, TestTaskTable) {
  loop = aeCreateEventLoop(1024);
  RAY_CHECK_OK(client_.context()->AttachToEventLoop(loop));
  auto data = std::make_shared<TaskTableDataT>();
  data->scheduling_state = 3;
  TaskID task_id = TaskID::from_random();
  RAY_CHECK_OK(client_.task_table().Add(job_id_, task_id, data, &TaskAdded));
  RAY_CHECK_OK(client_.task_table().Lookup(job_id_, task_id, &TaskLookup, &TaskLookup));
  aeMain(loop);
  aeDeleteEventLoop(loop);
}

}
