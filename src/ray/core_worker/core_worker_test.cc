#include <thread>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/buffer.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/raylet/raylet_client.h"

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "ray/thirdparty/hiredis/async.h"
#include "ray/thirdparty/hiredis/hiredis.h"

namespace ray {

std::string store_executable;
std::string raylet_executable;
std::string mock_worker_executable;

ray::ObjectID RandomObjectID() { return ObjectID::FromRandom(); }

static void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);
}

class CoreWorkerTest : public ::testing::Test {
 public:
  CoreWorkerTest(int num_nodes) {
    // flush redis first.
    flushall_redis();

    RAY_CHECK(num_nodes >= 0);
    if (num_nodes > 0) {
      raylet_socket_names_.resize(num_nodes);
      raylet_store_socket_names_.resize(num_nodes);
    }

    // start plasma store.
    for (auto &store_socket : raylet_store_socket_names_) {
      store_socket = StartStore();
    }

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node.
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] =
          StartRaylet(raylet_store_socket_names_[i], "127.0.0.1", "127.0.0.1",
                      "\"CPU,4.0,resource" + std::to_string(i) + ",10\"");
    }
  }

  ~CoreWorkerTest() {
    for (const auto &raylet_socket : raylet_socket_names_) {
      StopRaylet(raylet_socket);
    }

    for (const auto &store_socket : raylet_store_socket_names_) {
      StopStore(store_socket);
    }
  }

  std::string StartStore() {
    std::string store_socket_name = "/tmp/store" + RandomObjectID().Hex();
    std::string store_pid = store_socket_name + ".pid";
    std::string plasma_command = store_executable + " -m 10000000 -s " +
                                 store_socket_name +
                                 " 1> /dev/null 2> /dev/null & echo $! > " + store_pid;
    RAY_LOG(DEBUG) << plasma_command;
    RAY_CHECK(system(plasma_command.c_str()) == 0);
    usleep(200 * 1000);
    return store_socket_name;
  }

  void StopStore(std::string store_socket_name) {
    std::string store_pid = store_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + store_pid + "`";
    RAY_LOG(DEBUG) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + store_socket_name).c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + store_socket_name + ".pid").c_str()) == 0);
  }

  std::string StartRaylet(std::string store_socket_name, std::string node_ip_address,
                          std::string redis_address, std::string resource) {
    std::string raylet_socket_name = "/tmp/raylet" + RandomObjectID().Hex();
    std::string ray_start_cmd = raylet_executable;
    ray_start_cmd.append(" --raylet_socket_name=" + raylet_socket_name)
        .append(" --store_socket_name=" + store_socket_name)
        .append(" --object_manager_port=0 --node_manager_port=0")
        .append(" --node_ip_address=" + node_ip_address)
        .append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")
        .append(" --num_initial_workers=1")
        .append(" --maximum_startup_concurrency=10")
        .append(" --static_resource_list=" + resource)
        .append(" --python_worker_command=\"" + mock_worker_executable + " " +
                store_socket_name + " " + raylet_socket_name + "\"")
        .append(" & echo $! > " + raylet_socket_name + ".pid");

    RAY_LOG(DEBUG) << "Ray Start command: " << ray_start_cmd;
    RAY_CHECK(system(ray_start_cmd.c_str()) == 0);
    usleep(200 * 1000);
    return raylet_socket_name;
  }

  void StopRaylet(std::string raylet_socket_name) {
    std::string raylet_pid = raylet_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + raylet_pid + "`";
    RAY_LOG(DEBUG) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name).c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name + ".pid").c_str()) == 0);
  }

  void SetUp() {}

  void TearDown() {}

  void TestNormalTask(const std::unordered_map<std::string, double> &resources) {
    CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                      raylet_socket_names_[0], JobID::FromInt(1), nullptr);

    // Test pass by value.
    {
      uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};

      auto buffer1 = std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1));

      RayFunction func{Language::PYTHON, {}};
      std::vector<TaskArg> args;
      args.emplace_back(TaskArg::PassByValue(buffer1));

      TaskOptions options;

      std::vector<ObjectID> return_ids;
      RAY_CHECK_OK(driver.Tasks().SubmitTask(func, args, options, &return_ids));

      ASSERT_EQ(return_ids.size(), 1);

      std::vector<std::shared_ptr<ray::RayObject>> results;
      RAY_CHECK_OK(driver.Objects().Get(return_ids, -1, &results));

      ASSERT_EQ(results.size(), 1);
      ASSERT_EQ(results[0]->GetData()->Size(), buffer1->Size());
      ASSERT_EQ(memcmp(results[0]->GetData()->Data(), buffer1->Data(), buffer1->Size()),
                0);
    }

    // Test pass by reference.
    {
      uint8_t array1[] = {10, 11, 12, 13, 14, 15};
      auto buffer1 = std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1));

      ObjectID object_id;
      RAY_CHECK_OK(driver.Objects().Put(RayObject(buffer1, nullptr), &object_id));

      std::vector<TaskArg> args;
      args.emplace_back(TaskArg::PassByReference(object_id));

      RayFunction func{Language::PYTHON, {}};
      TaskOptions options;

      std::vector<ObjectID> return_ids;
      RAY_CHECK_OK(driver.Tasks().SubmitTask(func, args, options, &return_ids));

      ASSERT_EQ(return_ids.size(), 1);

      std::vector<std::shared_ptr<ray::RayObject>> results;
      RAY_CHECK_OK(driver.Objects().Get(return_ids, -1, &results));

      ASSERT_EQ(results.size(), 1);
      ASSERT_EQ(results[0]->GetData()->Size(), buffer1->Size());
      ASSERT_EQ(memcmp(results[0]->GetData()->Data(), buffer1->Data(), buffer1->Size()),
                0);
    }
  }

  void TestActorTask(const std::unordered_map<std::string, double> &resources) {
    CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                      raylet_socket_names_[0], JobID::FromInt(1), nullptr);

    std::unique_ptr<ActorHandle> actor_handle;

    // Test creating actor.
    {
      uint8_t array[] = {1, 2, 3};
      auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

      RayFunction func{Language::PYTHON, {}};
      std::vector<TaskArg> args;
      args.emplace_back(TaskArg::PassByValue(buffer));

      ActorCreationOptions actor_options{0, resources};

      // Create an actor.
      RAY_CHECK_OK(driver.Tasks().CreateActor(func, args, actor_options, &actor_handle));
    }

    // Test submitting a task for that actor.
    {
      uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
      uint8_t array2[] = {10, 11, 12, 13, 14, 15};

      auto buffer1 = std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1));
      auto buffer2 = std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2));

      ObjectID object_id;
      RAY_CHECK_OK(driver.Objects().Put(RayObject(buffer1, nullptr), &object_id));

      // Create arguments with PassByRef and PassByValue.
      std::vector<TaskArg> args;
      args.emplace_back(TaskArg::PassByReference(object_id));
      args.emplace_back(TaskArg::PassByValue(buffer2));

      TaskOptions options{1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func{Language::PYTHON, {}};
      RAY_CHECK_OK(driver.Tasks().SubmitActorTask(*actor_handle, func, args, options,
                                                  &return_ids));
      RAY_CHECK(return_ids.size() == 1);

      std::vector<std::shared_ptr<ray::RayObject>> results;
      RAY_CHECK_OK(driver.Objects().Get(return_ids, -1, &results));

      ASSERT_EQ(results.size(), 1);
      ASSERT_EQ(results[0]->GetData()->Size(), buffer1->Size() + buffer2->Size());
      ASSERT_EQ(memcmp(results[0]->GetData()->Data(), buffer1->Data(), buffer1->Size()),
                0);
      ASSERT_EQ(memcmp(results[0]->GetData()->Data() + buffer1->Size(), buffer2->Data(),
                       buffer2->Size()),
                0);
    }
  }

 protected:
  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> raylet_store_socket_names_;
};

class ZeroNodeTest : public CoreWorkerTest {
 public:
  ZeroNodeTest() : CoreWorkerTest(0) {}
};

class SingleNodeTest : public CoreWorkerTest {
 public:
  SingleNodeTest() : CoreWorkerTest(1) {}
};

class TwoNodeTest : public CoreWorkerTest {
 public:
  TwoNodeTest() : CoreWorkerTest(2) {}
};

TEST_F(ZeroNodeTest, TestTaskArg) {
  // Test by-reference argument.
  ObjectID id = ObjectID::FromRandom();
  TaskArg by_ref = TaskArg::PassByReference(id);
  ASSERT_TRUE(by_ref.IsPassedByReference());
  ASSERT_EQ(by_ref.GetReference(), id);
  // Test by-value argument.
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(static_cast<uint8_t *>(0), 0);
  TaskArg by_value = TaskArg::PassByValue(buffer);
  ASSERT_FALSE(by_value.IsPassedByReference());
  auto data = by_value.GetValue();
  ASSERT_TRUE(data != nullptr);
  ASSERT_EQ(*data, *buffer);
}

TEST_F(ZeroNodeTest, TestWorkerContext) {
  auto job_id = JobID::JobID::FromInt(1);

  WorkerContext context(WorkerType::WORKER, job_id);
  ASSERT_TRUE(context.GetCurrentTaskID().IsNil());
  ASSERT_EQ(context.GetNextTaskIndex(), 1);
  ASSERT_EQ(context.GetNextTaskIndex(), 2);
  ASSERT_EQ(context.GetNextPutIndex(), 1);
  ASSERT_EQ(context.GetNextPutIndex(), 2);

  auto thread_func = [&context]() {
    // Verify that task_index, put_index are thread-local.
    ASSERT_TRUE(!context.GetCurrentTaskID().IsNil());
    ASSERT_EQ(context.GetNextTaskIndex(), 1);
    ASSERT_EQ(context.GetNextPutIndex(), 1);
  };

  std::thread async_thread(thread_func);
  async_thread.join();

  // Verify that these fields are thread-local.
  ASSERT_EQ(context.GetNextTaskIndex(), 3);
  ASSERT_EQ(context.GetNextPutIndex(), 3);
}

TEST_F(ZeroNodeTest, TestActorHandle) {
  ActorHandle handle1(ActorID::FromRandom(), ActorHandleID::FromRandom(), Language::JAVA,
                      {"org.ray.exampleClass", "exampleMethod", "exampleSignature"});

  auto forkedHandle1 = handle1.Fork();
  ASSERT_EQ(1, handle1.NumForks());
  ASSERT_EQ(handle1.ActorID(), forkedHandle1.ActorID());
  ASSERT_NE(handle1.ActorHandleID(), forkedHandle1.ActorHandleID());
  ASSERT_EQ(handle1.ActorLanguage(), forkedHandle1.ActorLanguage());
  ASSERT_EQ(handle1.ActorCreationTaskFunctionDescriptor(),
            forkedHandle1.ActorCreationTaskFunctionDescriptor());
  ASSERT_EQ(handle1.ActorCursor(), forkedHandle1.ActorCursor());
  ASSERT_EQ(0, forkedHandle1.TaskCounter());
  ASSERT_EQ(0, forkedHandle1.NumForks());
  auto forkedHandle2 = handle1.Fork();
  ASSERT_EQ(2, handle1.NumForks());
  ASSERT_EQ(0, forkedHandle2.TaskCounter());
  ASSERT_EQ(0, forkedHandle2.NumForks());

  std::string buffer;
  handle1.Serialize(&buffer);
  auto handle2 = ActorHandle::Deserialize(buffer);
  ASSERT_EQ(handle1.ActorID(), handle2.ActorID());
  ASSERT_EQ(handle1.ActorHandleID(), handle2.ActorHandleID());
  ASSERT_EQ(handle1.ActorLanguage(), handle2.ActorLanguage());
  ASSERT_EQ(handle1.ActorCreationTaskFunctionDescriptor(),
            handle2.ActorCreationTaskFunctionDescriptor());
  ASSERT_EQ(handle1.ActorCursor(), handle2.ActorCursor());
  ASSERT_EQ(handle1.TaskCounter(), handle2.TaskCounter());
  ASSERT_EQ(handle1.NumForks(), handle2.NumForks());
}

TEST_F(SingleNodeTest, TestObjectInterface) {
  CoreWorker core_worker(WorkerType::DRIVER, Language::PYTHON,
                         raylet_store_socket_names_[0], raylet_socket_names_[0],
                         JobID::FromInt(1), nullptr);

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<RayObject> buffers;
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1)),
                       std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1) / 2));
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2)),
                       std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2) / 2));

  std::vector<ObjectID> ids(buffers.size());
  for (size_t i = 0; i < ids.size(); i++) {
    RAY_CHECK_OK(core_worker.Objects().Put(buffers[i], &ids[i]));
  }

  // Test Get().
  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(core_worker.Objects().Get(ids, -1, &results));

  ASSERT_EQ(results.size(), 2);
  for (size_t i = 0; i < ids.size(); i++) {
    ASSERT_EQ(results[i]->GetData()->Size(), buffers[i].GetData()->Size());
    ASSERT_EQ(memcmp(results[i]->GetData()->Data(), buffers[i].GetData()->Data(),
                     buffers[i].GetData()->Size()),
              0);
    ASSERT_EQ(results[i]->GetMetadata()->Size(), buffers[i].GetMetadata()->Size());
    ASSERT_EQ(memcmp(results[i]->GetMetadata()->Data(), buffers[i].GetMetadata()->Data(),
                     buffers[i].GetMetadata()->Size()),
              0);
  }

  // Test Wait().
  ObjectID non_existent_id = ObjectID::FromRandom();
  std::vector<ObjectID> all_ids(ids);
  all_ids.push_back(non_existent_id);

  std::vector<bool> wait_results;
  RAY_CHECK_OK(core_worker.Objects().Wait(all_ids, 2, -1, &wait_results));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  RAY_CHECK_OK(core_worker.Objects().Wait(all_ids, 3, 100, &wait_results));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  // Test Delete().
  // clear the reference held by PlasmaBuffer.
  results.clear();
  RAY_CHECK_OK(core_worker.Objects().Delete(ids, true, false));

  // Note that Delete() calls RayletClient::FreeObjects and would not
  // wait for objects being deleted, so wait a while for plasma store
  // to process the command.
  usleep(200 * 1000);
  RAY_CHECK_OK(core_worker.Objects().Get(ids, 0, &results));
  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(!results[0]);
  ASSERT_TRUE(!results[1]);
}

TEST_F(TwoNodeTest, TestObjectInterfaceCrossNodes) {
  CoreWorker worker1(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                     raylet_socket_names_[0], JobID::FromInt(1), nullptr);

  CoreWorker worker2(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[1],
                     raylet_socket_names_[1], JobID::FromInt(1), nullptr);

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<LocalMemoryBuffer> buffers;
  buffers.emplace_back(array1, sizeof(array1));
  buffers.emplace_back(array2, sizeof(array2));

  std::vector<ObjectID> ids(buffers.size());
  for (size_t i = 0; i < ids.size(); i++) {
    RAY_CHECK_OK(worker1.Objects().Put(
        RayObject(std::make_shared<LocalMemoryBuffer>(buffers[i]), nullptr), &ids[i]));
  }

  // Test Get() from remote node.
  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(worker2.Objects().Get(ids, -1, &results));

  ASSERT_EQ(results.size(), 2);
  for (size_t i = 0; i < ids.size(); i++) {
    ASSERT_EQ(results[i]->GetData()->Size(), buffers[i].Size());
    ASSERT_EQ(memcmp(results[i]->GetData()->Data(), buffers[i].Data(), buffers[i].Size()),
              0);
  }

  // Test Wait() from remote node.
  ObjectID non_existent_id = ObjectID::FromRandom();
  std::vector<ObjectID> all_ids(ids);
  all_ids.push_back(non_existent_id);

  std::vector<bool> wait_results;
  RAY_CHECK_OK(worker2.Objects().Wait(all_ids, 2, -1, &wait_results));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  RAY_CHECK_OK(worker2.Objects().Wait(all_ids, 3, 100, &wait_results));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  // Test Delete() from all machines.
  // clear the reference held by PlasmaBuffer.
  results.clear();
  RAY_CHECK_OK(worker2.Objects().Delete(ids, false, false));

  // Note that Delete() calls RayletClient::FreeObjects and would not
  // wait for objects being deleted, so wait a while for plasma store
  // to process the command.
  usleep(1000 * 1000);
  // Verify objects are deleted from both machines.
  RAY_CHECK_OK(worker2.Objects().Get(ids, 0, &results));
  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(!results[0]);
  ASSERT_TRUE(!results[1]);

  RAY_CHECK_OK(worker1.Objects().Get(ids, 0, &results));
  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(!results[0]);
  ASSERT_TRUE(!results[1]);
}

TEST_F(SingleNodeTest, TestNormalTaskLocal) {
  std::unordered_map<std::string, double> resources;
  TestNormalTask(resources);
}

TEST_F(TwoNodeTest, TestNormalTaskCrossNodes) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestNormalTask(resources);
}

TEST_F(SingleNodeTest, TestActorTaskLocal) {
  std::unordered_map<std::string, double> resources;
  TestActorTask(resources);
}

TEST_F(TwoNodeTest, TestActorTaskCrossNodes) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorTask(resources);
}

TEST_F(SingleNodeTest, TestCoreWorkerConstructorFailure) {
  try {
    CoreWorker core_worker(WorkerType::DRIVER, Language::PYTHON, "",
                           raylet_socket_names_[0], JobID::FromInt(1), nullptr);
  } catch (const std::exception &e) {
    std::cout << "Caught exception when constructing core worker: " << e.what();
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::store_executable = std::string(argv[1]);
  ray::raylet_executable = std::string(argv[2]);
  ray::mock_worker_executable = std::string(argv[3]);
  return RUN_ALL_TESTS();
}
