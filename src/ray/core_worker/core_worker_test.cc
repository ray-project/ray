#include <thread>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "context.h"
#include "core_worker.h"
#include "ray/common/buffer.h"
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
    RAY_CHECK(num_nodes >= 0);
    if (num_nodes > 0) {
      raylet_socket_names_.resize(num_nodes);
      raylet_store_socket_names_.resize(num_nodes);
    }

    // start plasma store.
    for (auto &store_socket : raylet_store_socket_names_) {
      store_socket = StartStore();
    }

    // start raylet on each node
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] = StartRaylet(raylet_store_socket_names_[i], "127.0.0.1",
                                            "127.0.0.1", "\"CPU,4.0\"");
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
    RAY_LOG(INFO) << plasma_command;
    RAY_CHECK(system(plasma_command.c_str()) == 0);
    usleep(200 * 1000);
    return store_socket_name;
  }

  void StopStore(std::string store_socket_name) {
    std::string store_pid = store_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + store_pid + "`";
    RAY_LOG(INFO) << kill_9;
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
        .append(" --python_worker_command=\"" + mock_worker_executable
            + " " + store_socket_name + " " + raylet_socket_name + "\"")
        .append(" & echo $! > " + raylet_socket_name + ".pid");

    RAY_LOG(INFO) << "Ray Start command: " << ray_start_cmd;
    RAY_CHECK(system(ray_start_cmd.c_str()) == 0);
    usleep(200 * 1000);
    return raylet_socket_name;
  }

  void StopRaylet(std::string raylet_socket_name) {
    std::string raylet_pid = raylet_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + raylet_pid + "`";
    RAY_LOG(INFO) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name).c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name + ".pid").c_str()) == 0);
  }

  void SetUp() { flushall_redis(); }

  void TearDown() {}

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

TEST_F(ZeroNodeTest, TestAttributeGetters) {
  CoreWorker core_worker(WorkerType::DRIVER, Language::PYTHON, "", "",
                         DriverID::FromRandom());
  ASSERT_EQ(core_worker.WorkerType(), WorkerType::DRIVER);
  ASSERT_EQ(core_worker.Language(), Language::PYTHON);
}

TEST_F(ZeroNodeTest, TestWorkerContext) {
  auto driver_id = DriverID::FromRandom();

  WorkerContext context(WorkerType::WORKER, driver_id);
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

TEST_F(SingleNodeTest, TestObjectInterface) {
  CoreWorker core_worker(WorkerType::DRIVER, Language::PYTHON,
                         raylet_store_socket_names_[0], raylet_socket_names_[0],
                         DriverID::FromRandom());
  RAY_CHECK_OK(core_worker.Connect());

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<LocalMemoryBuffer> buffers;
  buffers.emplace_back(array1, sizeof(array1));
  buffers.emplace_back(array2, sizeof(array2));

  std::vector<ObjectID> ids(buffers.size());
  for (int i = 0; i < ids.size(); i++) {
    core_worker.Objects().Put(buffers[i], &ids[i]);
  }

  // Test Get().
  std::vector<std::shared_ptr<Buffer>> results;
  core_worker.Objects().Get(ids, -1, &results);

  ASSERT_EQ(results.size(), 2);
  for (int i = 0; i < ids.size(); i++) {
    ASSERT_EQ(results[i]->Size(), buffers[i].Size());
    ASSERT_EQ(memcmp(results[i]->Data(), buffers[i].Data(), buffers[i].Size()), 0);
  }

  // Test Wait().
  ObjectID non_existent_id = ObjectID::FromRandom();
  std::vector<ObjectID> all_ids(ids);
  all_ids.push_back(non_existent_id);

  std::vector<bool> wait_results;
  core_worker.Objects().Wait(all_ids, 2, -1, &wait_results);
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  core_worker.Objects().Wait(all_ids, 3, 100, &wait_results);
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  // Test Delete().
  // clear the reference held by PlasmaBuffer.
  results.clear();
  core_worker.Objects().Delete(ids, true, false);

  // Note that Delete() calls RayletClient::FreeObjects and would not
  // wait for objects being deleted, so wait a while for plasma store
  // to process the command.
  usleep(200 * 1000);
  core_worker.Objects().Get(ids, 0, &results);
  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(!results[0]);
  ASSERT_TRUE(!results[1]);
}

TEST_F(TwoNodeTest, TestObjectInterfaceCrossMachine) {
  CoreWorker worker1(WorkerType::DRIVER, Language::PYTHON,
                     raylet_store_socket_names_[0], raylet_socket_names_[0],
                     DriverID::FromRandom());
  RAY_CHECK_OK(worker1.Connect());

  CoreWorker worker2(WorkerType::DRIVER, Language::PYTHON,
                     raylet_store_socket_names_[1], raylet_socket_names_[1],
                     DriverID::FromRandom());
  RAY_CHECK_OK(worker2.Connect());

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<LocalMemoryBuffer> buffers;
  buffers.emplace_back(array1, sizeof(array1));
  buffers.emplace_back(array2, sizeof(array2));

  std::vector<ObjectID> ids(buffers.size());
  for (int i = 0; i < ids.size(); i++) {
    worker1.Objects().Put(buffers[i], &ids[i]);
  }

  // Test Get() from remote node.
  std::vector<std::shared_ptr<Buffer>> results;
  worker2.Objects().Get(ids, -1, &results);

  ASSERT_EQ(results.size(), 2);
  for (int i = 0; i < ids.size(); i++) {
    ASSERT_EQ(results[i]->Size(), buffers[i].Size());
    ASSERT_EQ(memcmp(results[i]->Data(), buffers[i].Data(), buffers[i].Size()), 0);
  }

  // Test Wait() from remote node.
  ObjectID non_existent_id = ObjectID::FromRandom();
  std::vector<ObjectID> all_ids(ids);
  all_ids.push_back(non_existent_id);

  std::vector<bool> wait_results;
  worker2.Objects().Wait(all_ids, 2, -1, &wait_results);
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  worker2.Objects().Wait(all_ids, 3, 100, &wait_results);
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  // Test Delete() from all machines.
  // clear the reference held by PlasmaBuffer.
  results.clear();
  worker2.Objects().Delete(ids, false, false);

  // Note that Delete() calls RayletClient::FreeObjects and would not
  // wait for objects being deleted, so wait a while for plasma store
  // to process the command.
  usleep(1000 * 1000);
  // Verify objects are deleted from both machines.
  worker2.Objects().Get(ids, 0, &results);
  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(!results[0]);
  ASSERT_TRUE(!results[1]);

  worker1.Objects().Get(ids, 0, &results);
  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(!results[0]);
  ASSERT_TRUE(!results[1]);  
}


TEST_F(SingleNodeTest, TestNormalTask) {
  CoreWorker driver(WorkerType::DRIVER, Language::PYTHON,
                     raylet_store_socket_names_[0], raylet_socket_names_[0],
                     DriverID::FromRandom());
  RAY_CHECK_OK(driver.Connect());

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  auto buffer1 = std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1));

  RayFunction func{ ray::Language::PYTHON, {}};
  std::vector<TaskArg> args;
  args.emplace_back(TaskArg::PassByValue(buffer1));

  std::unordered_map<std::string, double> resources;
  TaskOptions options;

  std::vector<ObjectID> return_ids;
  //driver.Tasks().SubmitTask(func, args, options, &return_ids);

  ASSERT_EQ(return_ids.size(), 1);

  std::vector<std::shared_ptr<ray::Buffer>> results;
  driver.Objects().Get(return_ids, -1, &results);

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0]->Size(), buffer1->Size());
  ASSERT_EQ(memcmp(results[0]->Data(), buffer1->Data(), buffer1->Size()), 0);

  std::vector<LocalMemoryBuffer> buffers;
  buffers.emplace_back(array1, sizeof(array1));
  buffers.emplace_back(array2, sizeof(array2));
}


TEST_F(SingleNodeTest, TestActorTask) {
  CoreWorker driver(WorkerType::DRIVER, Language::PYTHON,
                     raylet_store_socket_names_[0], raylet_socket_names_[0],
                     DriverID::FromRandom());
  RAY_CHECK_OK(driver.Connect());

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  auto buffer1 = std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1));
  auto buffer2 = std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2));

  RayFunction func{ ray::Language::PYTHON, {}};
  std::vector<TaskArg> args;
  args.emplace_back(TaskArg::PassByValue(buffer1));

  std::unordered_map<std::string, double> resources;
  ActorCreationOptions actor_options;

  std::vector<ObjectID> return_ids;

  std::unique_ptr<ActorHandle> actor_handle;
  driver.Tasks().CreateActor(func, args, actor_options, &actor_handle);
  // TODO: check returned actor handle id.

  args.emplace_back(TaskArg::PassByValue(buffer2));

  TaskOptions options;
  driver.Tasks().SubmitActorTask(*actor_handle, func, args,
      options, &return_ids);

  std::vector<std::shared_ptr<ray::Buffer>> results;
  driver.Objects().Get(return_ids, -1, &results);

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0]->Size(), buffer1->Size() + buffer2->Size());
  ASSERT_EQ(memcmp(results[0]->Data(), buffer1->Data(), buffer1->Size()), 0);
  ASSERT_EQ(memcmp(results[0]->Data() +buffer1->Size(),
      buffer2->Data(), buffer2->Size()), 0);
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
