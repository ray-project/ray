#include <thread>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

#include "ray/core_worker/store_provider/memory_store_provider.h"

#include "ray/raylet/raylet_client.h"
#include "src/ray/protobuf/direct_actor.grpc.pb.h"
#include "src/ray/protobuf/direct_actor.pb.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/util/test_util.h"

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>

#include "ray/thirdparty/hiredis/async.h"
#include "ray/thirdparty/hiredis/hiredis.h"
#include "ray/util/test_util.h"

namespace ray {

std::string store_executable;
std::string raylet_executable;
std::string mock_worker_executable;

static void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);
}

std::shared_ptr<Buffer> GenerateRandomBuffer() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> dis(1, 10);
  std::uniform_int_distribution<> value_dis(1, 255);

  std::vector<uint8_t> arg1(dis(gen), value_dis(gen));
  return std::make_shared<LocalMemoryBuffer>(arg1.data(), arg1.size(), true);
}

std::unique_ptr<ActorHandle> CreateActorHelper(
    CoreWorker &worker, const std::unordered_map<std::string, double> &resources,
    bool is_direct_call, uint64_t max_reconstructions) {
  std::unique_ptr<ActorHandle> actor_handle;

  // Test creating actor.
  uint8_t array[] = {1, 2, 3};
  auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

  RayFunction func{ray::Language::PYTHON, {"actor creation task"}};
  std::vector<TaskArg> args;
  args.emplace_back(TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr)));

  ActorCreationOptions actor_options{max_reconstructions, is_direct_call, resources, {}};

  // Create an actor.
  RAY_CHECK_OK(worker.Tasks().CreateActor(func, args, actor_options, &actor_handle));
  return actor_handle;
}

class CoreWorkerTest : public ::testing::Test {
 public:
  CoreWorkerTest(int num_nodes) : gcs_options_("127.0.0.1", 6379, "") {
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

  JobID NextJobId() const {
    static uint32_t job_counter = 1;
    return JobID::FromInt(job_counter++);
  }

  std::string StartStore() {
    std::string store_socket_name = "/tmp/store" + ObjectID::FromRandom().Hex();
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
    ASSERT_EQ(system(kill_9.c_str()), 0);
    ASSERT_EQ(system(("rm -rf " + store_socket_name).c_str()), 0);
    ASSERT_EQ(system(("rm -rf " + store_socket_name + ".pid").c_str()), 0);
  }

  std::string StartRaylet(std::string store_socket_name, std::string node_ip_address,
                          std::string redis_address, std::string resource) {
    std::string raylet_socket_name = "/tmp/raylet" + ObjectID::FromRandom().Hex();
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
        .append(" --config_list=initial_reconstruction_timeout_milliseconds,2000")
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

  // Test tore provider.
  void TestStoreProvider(StoreProviderType type);

  // Test normal tasks.
  void TestNormalTask(const std::unordered_map<std::string, double> &resources);

  // Test actor tasks.
  void TestActorTask(const std::unordered_map<std::string, double> &resources,
                     bool is_direct_call);

  // Test actor failure case, verify that the tasks would either succeed or
  // fail with exceptions, in that case the return objects fetched from `Get`
  // contain errors.
  void TestActorFailure(const std::unordered_map<std::string, double> &resources,
                        bool is_direct_call);

  // Test actor failover case. Verify that actor can be reconstructed successfully,
  // and as long as we wait for actor reconstruction before submitting new tasks,
  // it is guaranteed that all tasks are successfully completed.
  void TestActorReconstruction(const std::unordered_map<std::string, double> &resources,
                               bool is_direct_call);

 protected:
  bool WaitForDirectCallActorState(CoreWorker &worker, const ActorID &actor_id,
                                   bool wait_alive, int timeout_ms);

  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> raylet_store_socket_names_;
  gcs::GcsClientOptions gcs_options_;
};

bool CoreWorkerTest::WaitForDirectCallActorState(CoreWorker &worker,
                                                 const ActorID &actor_id, bool wait_alive,
                                                 int timeout_ms) {
  auto condition_func = [&worker, actor_id, wait_alive]() -> bool {
    auto &task_submitters = worker.Tasks().task_submitters_;
    RAY_CHECK(task_submitters.count(TaskTransportType::DIRECT_ACTOR) > 0);
    auto submitter =
        worker.Tasks().task_submitters_[TaskTransportType::DIRECT_ACTOR].get();
    auto direct_actor_submitter =
        dynamic_cast<CoreWorkerDirectActorTaskSubmitter *>(submitter);
    RAY_CHECK(direct_actor_submitter != nullptr);
    bool actor_alive = direct_actor_submitter->IsActorAlive(actor_id);
    return wait_alive ? actor_alive : !actor_alive;
  };

  return WaitForCondition(condition_func, timeout_ms);
}

void CoreWorkerTest::TestNormalTask(
    const std::unordered_map<std::string, double> &resources) {
  CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                    raylet_socket_names_[0], NextJobId(), gcs_options_, "", nullptr);

  // Test for tasks with by-value and by-ref args.
  {
    const int num_tasks = 100;
    for (int i = 0; i < num_tasks; i++) {
      auto buffer1 = GenerateRandomBuffer();
      auto buffer2 = GenerateRandomBuffer();

      ObjectID object_id;
      RAY_CHECK_OK(driver.Objects().Put(RayObject(buffer2, nullptr), &object_id));

      std::vector<TaskArg> args;
      args.emplace_back(
          TaskArg::PassByValue(std::make_shared<RayObject>(buffer1, nullptr)));
      args.emplace_back(TaskArg::PassByReference(object_id));

      RayFunction func{ray::Language::PYTHON, {}};
      TaskOptions options;

      std::vector<ObjectID> return_ids;
      RAY_CHECK_OK(driver.Tasks().SubmitTask(func, args, options, &return_ids));

      ASSERT_EQ(return_ids.size(), 1);

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
}

void CoreWorkerTest::TestActorTask(
    const std::unordered_map<std::string, double> &resources, bool is_direct_call) {
  CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                    raylet_socket_names_[0], NextJobId(), gcs_options_, "", nullptr);

  auto actor_handle = CreateActorHelper(driver, resources, is_direct_call, 1000);

  // Test submitting some tasks with by-value args for that actor.
  {
    const int num_tasks = 100;
    for (int i = 0; i < num_tasks; i++) {
      auto buffer1 = GenerateRandomBuffer();
      auto buffer2 = GenerateRandomBuffer();

      // Create arguments with PassByRef and PassByValue.
      std::vector<TaskArg> args;
      args.emplace_back(
          TaskArg::PassByValue(std::make_shared<RayObject>(buffer1, nullptr)));
      args.emplace_back(
          TaskArg::PassByValue(std::make_shared<RayObject>(buffer2, nullptr)));

      TaskOptions options{1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func{ray::Language::PYTHON, {}};

      RAY_CHECK_OK(driver.Tasks().SubmitActorTask(*actor_handle, func, args, options,
                                                  &return_ids));
      ASSERT_EQ(return_ids.size(), 1);
      ASSERT_TRUE(return_ids[0].IsReturnObject());
      ASSERT_EQ(
          static_cast<TaskTransportType>(return_ids[0].GetTransportType()),
          is_direct_call ? TaskTransportType::DIRECT_ACTOR : TaskTransportType::RAYLET);

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

  // Test submitting a task with both by-value and by-ref args for that actor.
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
    args.emplace_back(
        TaskArg::PassByValue(std::make_shared<RayObject>(buffer2, nullptr)));

    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON, {}};
    auto status =
        driver.Tasks().SubmitActorTask(*actor_handle, func, args, options, &return_ids);
    if (is_direct_call) {
      // For direct actor call, submitting a task with by-reference arguments
      // would fail.
      ASSERT_TRUE(!status.ok());
      return;
    }

    ASSERT_EQ(return_ids.size(), 1);

    std::vector<std::shared_ptr<ray::RayObject>> results;
    RAY_CHECK_OK(driver.Objects().Get(return_ids, -1, &results));

    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0]->GetData()->Size(), buffer1->Size() + buffer2->Size());
    ASSERT_EQ(memcmp(results[0]->GetData()->Data(), buffer1->Data(), buffer1->Size()), 0);
    ASSERT_EQ(memcmp(results[0]->GetData()->Data() + buffer1->Size(), buffer2->Data(),
                     buffer2->Size()),
              0);
  }
}

void CoreWorkerTest::TestActorReconstruction(
    const std::unordered_map<std::string, double> &resources, bool is_direct_call) {
  CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                    raylet_socket_names_[0], NextJobId(), gcs_options_, "", nullptr);

  // creating actor.
  auto actor_handle = CreateActorHelper(driver, resources, is_direct_call, 1000);

  // Wait for actor alive event.
  ASSERT_TRUE(WaitForDirectCallActorState(driver, actor_handle->ActorID(), true,
                                          30 * 1000 /* 30s */));
  RAY_LOG(INFO) << "actor has been created";

  // Test submitting some tasks with by-value args for that actor.
  {
    const int num_tasks = 100;
    const int task_index_to_kill_worker = (num_tasks + 1) / 2;
    std::vector<std::pair<ObjectID, std::vector<uint8_t>>> all_results;
    for (int i = 0; i < num_tasks; i++) {
      if (i == task_index_to_kill_worker) {
        RAY_LOG(INFO) << "killing worker";
        ASSERT_EQ(system("pkill mock_worker"), 0);

        // Wait for actor restruction event, and then for alive event.
        ASSERT_TRUE(WaitForDirectCallActorState(driver, actor_handle->ActorID(), false,
                                                30 * 1000 /* 30s */));
        ASSERT_TRUE(WaitForDirectCallActorState(driver, actor_handle->ActorID(), true,
                                                30 * 1000 /* 30s */));

        RAY_LOG(INFO) << "actor has been reconstructed";
      }

      // wait for actor being reconstructed.
      auto buffer1 = GenerateRandomBuffer();

      // Create arguments with PassByValue.
      std::vector<TaskArg> args;
      args.emplace_back(
          TaskArg::PassByValue(std::make_shared<RayObject>(buffer1, nullptr)));

      TaskOptions options{1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func{ray::Language::PYTHON, {}};

      auto status =
          driver.Tasks().SubmitActorTask(*actor_handle, func, args, options, &return_ids);
      RAY_CHECK_OK(status);
      ASSERT_EQ(return_ids.size(), 1);
      // Verify if it's expected data.
      std::vector<std::shared_ptr<RayObject>> results;

      RAY_CHECK_OK(driver.Objects().Get(return_ids, -1, &results));
      ASSERT_EQ(results[0]->GetData()->Size(), buffer1->Size());
      ASSERT_EQ(*results[0]->GetData(), *buffer1);
    }
  }
}

void CoreWorkerTest::TestActorFailure(
    const std::unordered_map<std::string, double> &resources, bool is_direct_call) {
  CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                    raylet_socket_names_[0], NextJobId(), gcs_options_, "", nullptr);

  // creating actor.
  auto actor_handle =
      CreateActorHelper(driver, resources, is_direct_call, 0 /* not reconstructable */);

  // Test submitting some tasks with by-value args for that actor.
  {
    const int num_tasks = 3000;
    const int task_index_to_kill_worker = (num_tasks + 1) / 2;
    std::vector<std::pair<ObjectID, std::shared_ptr<Buffer>>> all_results;
    for (int i = 0; i < num_tasks; i++) {
      if (i == task_index_to_kill_worker) {
        RAY_LOG(INFO) << "killing worker";
        ASSERT_EQ(system("pkill mock_worker"), 0);
      }

      // wait for actor being reconstructed.
      auto buffer1 = GenerateRandomBuffer();

      // Create arguments with PassByRef and PassByValue.
      std::vector<TaskArg> args;
      args.emplace_back(
          TaskArg::PassByValue(std::make_shared<RayObject>(buffer1, nullptr)));

      TaskOptions options{1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func{ray::Language::PYTHON, {}};

      auto status =
          driver.Tasks().SubmitActorTask(*actor_handle, func, args, options, &return_ids);
      if (i < task_index_to_kill_worker) {
        RAY_CHECK_OK(status);
      }

      ASSERT_EQ(return_ids.size(), 1);
      all_results.emplace_back(std::make_pair(return_ids[0], buffer1));
    }

    for (int i = 0; i < num_tasks; i++) {
      const auto &entry = all_results[i];
      std::vector<ObjectID> return_ids;
      return_ids.push_back(entry.first);
      std::vector<std::shared_ptr<RayObject>> results;
      RAY_CHECK_OK(driver.Objects().Get(return_ids, -1, &results));
      ASSERT_EQ(results.size(), 1);

      if (results[0]->HasMetadata()) {
        // Verify if this is the desired error.
        std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::ACTOR_DIED));
        ASSERT_TRUE(memcmp(results[0]->GetMetadata()->Data(), meta.data(), meta.size()) ==
                    0);
      } else {
        // Verify if it's expected data.
        ASSERT_EQ(*results[0]->GetData(), *entry.second);
      }
    }
  }
}

void CoreWorkerTest::TestStoreProvider(StoreProviderType type) {
  std::unique_ptr<CoreWorkerStoreProvider> provider_ptr;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store;

  switch (type) {
  case StoreProviderType::MEMORY:
    memory_store = std::make_shared<CoreWorkerMemoryStore>();
    provider_ptr = std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerMemoryStoreProvider(memory_store));
    break;
  default:
    RAY_LOG(FATAL) << "unspported store provider type " << static_cast<int>(type);
    break;
  }

  auto &provider = *provider_ptr;

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<RayObject> buffers;
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1)),
                       std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1) / 2));
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2)),
                       std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2) / 2));

  std::vector<ObjectID> ids(buffers.size());
  for (size_t i = 0; i < ids.size(); i++) {
    ids[i] = ObjectID::FromRandom();
    RAY_CHECK_OK(provider.Put(buffers[i], ids[i]));
  }

  std::unordered_set<ObjectID> wait_ids(ids.begin(), ids.end());
  std::unordered_set<ObjectID> wait_results;

  ObjectID nonexistent_id = ObjectID::FromRandom();
  wait_ids.insert(nonexistent_id);
  RAY_CHECK_OK(
      provider.Wait(wait_ids, ids.size() + 1, 100, RandomTaskId(), &wait_results));
  ASSERT_EQ(wait_results.size(), ids.size());
  ASSERT_TRUE(wait_results.count(nonexistent_id) == 0);

  // Test Wait() where the required `num_objects` is less than size of `wait_ids`.
  wait_results.clear();
  RAY_CHECK_OK(provider.Wait(wait_ids, ids.size(), -1, RandomTaskId(), &wait_results));
  ASSERT_EQ(wait_results.size(), ids.size());
  ASSERT_TRUE(wait_results.count(nonexistent_id) == 0);

  // Test Get().
  bool got_exception = false;
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> results;
  std::unordered_set<ObjectID> ids_set(ids.begin(), ids.end());
  RAY_CHECK_OK(provider.Get(ids_set, -1, RandomTaskId(), &results, &got_exception));

  ASSERT_TRUE(!got_exception);
  ASSERT_EQ(results.size(), ids.size());
  for (size_t i = 0; i < ids.size(); i++) {
    const auto &expected = buffers[i];
    ASSERT_EQ(results[ids[i]]->GetData()->Size(), expected.GetData()->Size());
    ASSERT_EQ(memcmp(results[ids[i]]->GetData()->Data(), expected.GetData()->Data(),
                     expected.GetData()->Size()),
              0);
    ASSERT_EQ(results[ids[i]]->GetMetadata()->Size(), expected.GetMetadata()->Size());
    ASSERT_EQ(memcmp(results[ids[i]]->GetMetadata()->Data(),
                     expected.GetMetadata()->Data(), expected.GetMetadata()->Size()),
              0);
  }

  // Test Delete().
  // clear the reference held.
  results.clear();

  RAY_CHECK_OK(provider.Delete(ids, true, false));

  usleep(200 * 1000);
  RAY_CHECK_OK(provider.Get(ids_set, 0, RandomTaskId(), &results, &got_exception));
  ASSERT_TRUE(!got_exception);
  ASSERT_EQ(results.size(), 0);

  // Test Wait() with objects which will become ready later.
  std::vector<ObjectID> ready_ids(buffers.size());
  std::vector<ObjectID> unready_ids(buffers.size());
  for (size_t i = 0; i < unready_ids.size(); i++) {
    ready_ids[i] = ObjectID::FromRandom();
    RAY_CHECK_OK(provider.Put(buffers[i], ready_ids[i]));
    unready_ids[i] = ObjectID::FromRandom();
  }

  auto thread_func = [&unready_ids, &provider, &buffers]() {
    sleep(1);

    for (size_t i = 0; i < unready_ids.size(); i++) {
      RAY_CHECK_OK(provider.Put(buffers[i], unready_ids[i]));
    }
  };

  std::thread async_thread(thread_func);

  wait_ids.clear();
  wait_ids.insert(ready_ids.begin(), ready_ids.end());
  wait_ids.insert(unready_ids.begin(), unready_ids.end());
  wait_results.clear();

  // Check that only the ready ids are returned when timeout ends before thread runs.
  RAY_CHECK_OK(
      provider.Wait(wait_ids, ready_ids.size() + 1, 100, RandomTaskId(), &wait_results));
  ASSERT_EQ(ready_ids.size(), wait_results.size());
  for (const auto &ready_id : ready_ids) {
    ASSERT_TRUE(wait_results.find(ready_id) != wait_results.end());
  }
  for (const auto &unready_id : unready_ids) {
    ASSERT_TRUE(wait_results.find(unready_id) == wait_results.end());
  }

  wait_results.clear();
  // Check that enough objects are returned after the thread inserts at least one object.
  RAY_CHECK_OK(
      provider.Wait(wait_ids, ready_ids.size() + 1, 5000, RandomTaskId(), &wait_results));
  ASSERT_TRUE(wait_results.size() >= ready_ids.size() + 1);
  for (const auto &ready_id : ready_ids) {
    ASSERT_TRUE(wait_results.find(ready_id) != wait_results.end());
  }

  wait_results.clear();
  // Check that all objects are returned after the thread completes.
  async_thread.join();
  RAY_CHECK_OK(
      provider.Wait(wait_ids, wait_ids.size(), -1, RandomTaskId(), &wait_results));
  ASSERT_EQ(wait_results.size(), ready_ids.size() + unready_ids.size());
  for (const auto &ready_id : ready_ids) {
    ASSERT_TRUE(wait_results.find(ready_id) != wait_results.end());
  }
  for (const auto &unready_id : unready_ids) {
    ASSERT_TRUE(wait_results.find(unready_id) != wait_results.end());
  }
}

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
  auto buffer = GenerateRandomBuffer();
  TaskArg by_value = TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr));
  ASSERT_FALSE(by_value.IsPassedByReference());
  auto data = by_value.GetValue().GetData();
  ASSERT_TRUE(data != nullptr);
  ASSERT_EQ(*data, *buffer);
}

// Performance batchmark for `PushTaskRequest` creation.
TEST_F(ZeroNodeTest, TestTaskSpecPerf) {
  // Create a dummy actor handle, and then create a number of `TaskSpec`
  // to benchmark performance.
  uint8_t array[] = {1, 2, 3};
  auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));
  RayFunction function{ray::Language::PYTHON, {}};
  std::vector<TaskArg> args;
  args.emplace_back(TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr)));

  std::unordered_map<std::string, double> resources;
  ActorCreationOptions actor_options{0, /*is_direct_call*/ true, resources, {}};
  const auto job_id = NextJobId();
  ActorHandle actor_handle(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1),
                           ActorHandleID::Nil(), function.language, true,
                           function.function_descriptor);

  // Manually create `num_tasks` task specs, and for each of them create a
  // `PushTaskRequest`, this is to batch performance of TaskSpec
  // creation/copy/destruction.
  int64_t start_ms = current_time_ms();
  const auto num_tasks = 10000 * 10;
  RAY_LOG(INFO) << "start creating " << num_tasks << " PushTaskRequests";
  for (int i = 0; i < num_tasks; i++) {
    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    auto num_returns = options.num_returns;

    TaskSpecBuilder builder;
    builder.SetCommonTaskSpec(RandomTaskId(), function.language,
                              function.function_descriptor, job_id, RandomTaskId(), 0,
                              num_returns, resources, resources);
    // Set task arguments.
    for (const auto &arg : args) {
      if (arg.IsPassedByReference()) {
        builder.AddByRefArg(arg.GetReference());
      } else {
        builder.AddByValueArg(arg.GetValue());
      }
    }

    const auto actor_creation_dummy_object_id =
        ObjectID::ForTaskReturn(TaskID::ForActorCreationTask(actor_handle.ActorID()),
                                /*index=*/1, /*transport_type=*/0);
    builder.SetActorTaskSpec(
        actor_handle.ActorID(), actor_handle.ActorHandleID(),
        actor_creation_dummy_object_id,
        /*previous_actor_task_dummy_object_id=*/actor_handle.ActorCursor(), 0, {});

    const auto &task_spec = builder.Build();

    ASSERT_TRUE(task_spec.IsActorTask());
    auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
    request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());
  }
  RAY_LOG(INFO) << "Finish creating " << num_tasks << " PushTaskRequests"
                << ", which takes " << current_time_ms() - start_ms << " ms";
}

TEST_F(SingleNodeTest, TestDirectActorTaskSubmissionPerf) {
  CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                    raylet_socket_names_[0], JobID::FromInt(1), gcs_options_, "",
                    nullptr);
  std::unique_ptr<ActorHandle> actor_handle;

  // Test creating actor.
  uint8_t array[] = {1, 2, 3};
  auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));
  RayFunction func{ray::Language::PYTHON, {}};
  std::vector<TaskArg> args;
  args.emplace_back(TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr)));

  std::unordered_map<std::string, double> resources;
  ActorCreationOptions actor_options{0, /*is_direct_call*/ true, resources, {}};
  // Create an actor.
  RAY_CHECK_OK(driver.Tasks().CreateActor(func, args, actor_options, &actor_handle));
  // wait for actor creation finish.
  ASSERT_TRUE(WaitForDirectCallActorState(driver, actor_handle->ActorID(), true,
                                          30 * 1000 /* 30s */));
  // Test submitting some tasks with by-value args for that actor.
  int64_t start_ms = current_time_ms();
  const int num_tasks = 10000;
  RAY_LOG(INFO) << "start submitting " << num_tasks << " tasks";
  for (int i = 0; i < num_tasks; i++) {
    // Create arguments with PassByValue.
    std::vector<TaskArg> args;
    args.emplace_back(TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr)));

    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON, {}};

    RAY_CHECK_OK(
        driver.Tasks().SubmitActorTask(*actor_handle, func, args, options, &return_ids));
    ASSERT_EQ(return_ids.size(), 1);
  }
  RAY_LOG(INFO) << "finish submitting " << num_tasks << " tasks"
                << ", which takes " << current_time_ms() - start_ms << " ms";
}

TEST_F(ZeroNodeTest, TestWorkerContext) {
  auto job_id = NextJobId();

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
  const auto job_id = NextJobId();
  ActorHandle handle1(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1),
                      ActorHandleID::FromRandom(), Language::JAVA, false,
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

TEST_F(SingleNodeTest, TestMemoryStoreProvider) {
  TestStoreProvider(StoreProviderType::MEMORY);
}

TEST_F(SingleNodeTest, TestObjectInterface) {
  CoreWorker core_worker(WorkerType::DRIVER, Language::PYTHON,
                         raylet_store_socket_names_[0], raylet_socket_names_[0],
                         JobID::FromInt(1), gcs_options_, "", nullptr);

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
  ASSERT_EQ(results.size(), ids.size());
  for (size_t i = 0; i < ids.size(); i++) {
    ASSERT_EQ(*results[i]->GetData(), *buffers[i].GetData());
    ASSERT_EQ(*results[i]->GetMetadata(), *buffers[i].GetMetadata());
  }

  // Test Get() returns early when it encounters an error.
  std::vector<ObjectID> ids_with_exception(ids.begin(), ids.end());
  ids_with_exception.push_back(ObjectID::FromRandom());
  std::vector<RayObject> buffers_with_exception(buffers.begin(), buffers.end());
  std::string error_string = std::to_string(ray::rpc::TASK_EXECUTION_EXCEPTION);
  char error_buffer[error_string.size()];
  size_t len = error_string.copy(error_buffer, error_string.size(), 0);
  buffers_with_exception.emplace_back(
      nullptr, std::make_shared<LocalMemoryBuffer>(
                   reinterpret_cast<uint8_t *>(error_buffer), len));

  RAY_CHECK_OK(core_worker.Objects().Put(buffers_with_exception.back(),
                                         ids_with_exception.back()));
  RAY_CHECK_OK(core_worker.Objects().Get(ids_with_exception, -1, &results));

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
                     raylet_socket_names_[0], NextJobId(), gcs_options_, "", nullptr);

  CoreWorker worker2(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[1],
                     raylet_socket_names_[1], NextJobId(), gcs_options_, "", nullptr);

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers;
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1)));
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2)));

  std::vector<ObjectID> ids(buffers.size());
  for (size_t i = 0; i < ids.size(); i++) {
    RAY_CHECK_OK(worker1.Objects().Put(RayObject(buffers[i], nullptr), &ids[i]));
  }

  // Test Get() from remote node.
  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(worker2.Objects().Get(ids, -1, &results));

  ASSERT_EQ(results.size(), 2);
  for (size_t i = 0; i < ids.size(); i++) {
    ASSERT_EQ(results[i]->GetData()->Size(), buffers[i]->Size());
    ASSERT_EQ(*(results[i]->GetData()), *buffers[i]);
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
  TestActorTask(resources, false);
}

TEST_F(TwoNodeTest, TestActorTaskCrossNodes) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorTask(resources, false);
}

TEST_F(SingleNodeTest, TestDirectActorTaskLocal) {
  std::unordered_map<std::string, double> resources;
  TestActorTask(resources, true);
}

TEST_F(TwoNodeTest, TestDirectActorTaskCrossNodes) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorTask(resources, true);
}

TEST_F(SingleNodeTest, TestDirectActorTaskLocalReconstruction) {
  std::unordered_map<std::string, double> resources;
  TestActorReconstruction(resources, true);
}

TEST_F(TwoNodeTest, TestDirectActorTaskCrossNodesReconstruction) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorReconstruction(resources, true);
}

TEST_F(SingleNodeTest, TestDirectActorTaskLocalFailure) {
  std::unordered_map<std::string, double> resources;
  TestActorFailure(resources, true);
}

TEST_F(TwoNodeTest, TestDirectActorTaskCrossNodesFailure) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorFailure(resources, true);
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
