// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/core_worker.h"

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>
#include <thread>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/filesystem.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace {

std::string store_executable;
std::string raylet_executable;
int node_manager_port = 0;
std::string raylet_monitor_executable;
std::string mock_worker_executable;
std::string gcs_server_executable;

}  // namespace

namespace ray {

static void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);
}

ActorID CreateActorHelper(std::unordered_map<std::string, double> &resources,
                          uint64_t max_reconstructions) {
  std::unique_ptr<ActorHandle> actor_handle;

  uint8_t array[] = {1, 2, 3};
  auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

  RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                              "actor creation task", "", "", ""));
  std::vector<TaskArg> args;
  args.emplace_back(TaskArg::PassByValue(
      std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>())));

  ActorCreationOptions actor_options{max_reconstructions,
                                     /*max_concurrency*/ 1, resources, resources, {},
                                     /*is_detached*/ false,
                                     /*is_asyncio*/ false};

  // Create an actor.
  ActorID actor_id;
  RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().CreateActor(
      func, args, actor_options, /*extension_data*/ "", &actor_id));
  return actor_id;
}

std::string MetadataToString(std::shared_ptr<RayObject> obj) {
  auto metadata = obj->GetMetadata();
  return std::string(reinterpret_cast<const char *>(metadata->Data()), metadata->Size());
}

class CoreWorkerTest : public ::testing::Test {
 public:
  CoreWorkerTest(int num_nodes)
      : num_nodes_(num_nodes), gcs_options_("127.0.0.1", 6379, "") {
#ifdef _WIN32
    RAY_CHECK(false) << "port system() calls to Windows before running this test";
#endif
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

    // start gcs server
    if (RayConfig::instance().gcs_service_enabled()) {
      gcs_server_pid_ = StartGcsServer("127.0.0.1");
    } else {
      // core worker test relies on node resources. It's important that one raylet can
      // receive the heartbeat from another. So starting raylet monitor is required here.
      raylet_monitor_pid_ = StartRayletMonitor("127.0.0.1");
    }

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node.
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] =
          StartRaylet(raylet_store_socket_names_[i], "127.0.0.1", node_manager_port + i,
                      "127.0.0.1", "\"CPU,4.0,resource" + std::to_string(i) + ",10\"");
    }
  }

  ~CoreWorkerTest() {
    for (const auto &raylet_socket : raylet_socket_names_) {
      StopRaylet(raylet_socket);
    }

    for (const auto &store_socket : raylet_store_socket_names_) {
      StopStore(store_socket);
    }

    if (!raylet_monitor_pid_.empty()) {
      StopRayletMonitor(raylet_monitor_pid_);
    }

    if (!gcs_server_pid_.empty()) {
      StopGcsServer(gcs_server_pid_);
    }
  }

  JobID NextJobId() const {
    static uint32_t job_counter = 1;
    return JobID::FromInt(job_counter++);
  }

  std::string StartStore() {
    std::string store_socket_name =
        ray::JoinPaths(ray::GetUserTempDir(), "store" + ObjectID::FromRandom().Hex());
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
                          int port, std::string redis_address, std::string resource) {
    std::string raylet_socket_name =
        ray::JoinPaths(ray::GetUserTempDir(), "raylet" + ObjectID::FromRandom().Hex());
    std::string ray_start_cmd = raylet_executable;
    ray_start_cmd.append(" --raylet_socket_name=" + raylet_socket_name)
        .append(" --store_socket_name=" + store_socket_name)
        .append(" --object_manager_port=0 --node_manager_port=" + std::to_string(port))
        .append(" --node_ip_address=" + node_ip_address)
        .append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")
        .append(" --num_initial_workers=1")
        .append(" --maximum_startup_concurrency=10")
        .append(" --static_resource_list=" + resource)
        .append(" --python_worker_command=\"" + mock_worker_executable + " " +
                store_socket_name + " " + raylet_socket_name + " " +
                std::to_string(port) + "\"")
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

  std::string StartRayletMonitor(std::string redis_address) {
    std::string raylet_monitor_pid = ray::JoinPaths(
        ray::GetUserTempDir(), "raylet_monitor" + ObjectID::FromRandom().Hex() + ".pid");
    std::string raylet_monitor_start_cmd = raylet_monitor_executable;
    raylet_monitor_start_cmd.append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")
        .append(" & echo $! > " + raylet_monitor_pid);

    RAY_LOG(DEBUG) << "Raylet monitor Start command: " << raylet_monitor_start_cmd;
    RAY_CHECK(system(raylet_monitor_start_cmd.c_str()) == 0);
    usleep(200 * 1000);
    return raylet_monitor_pid;
  }

  void StopRayletMonitor(std::string raylet_monitor_pid) {
    std::string kill_9 = "kill -9 `cat " + raylet_monitor_pid + "`";
    RAY_LOG(DEBUG) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -f " + raylet_monitor_pid).c_str()) == 0);
  }

  std::string StartGcsServer(std::string redis_address) {
    std::string gcs_server_pid = ray::JoinPaths(
        ray::GetUserTempDir(), "gcs_server" + ObjectID::FromRandom().Hex() + ".pid");
    std::string gcs_server_start_cmd = gcs_server_executable;
    gcs_server_start_cmd.append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")
        .append(" --config_list=initial_reconstruction_timeout_milliseconds,2000")
        .append(" & echo $! > " + gcs_server_pid);

    RAY_LOG(DEBUG) << "Starting GCS server, command: " << gcs_server_start_cmd;
    RAY_CHECK(system(gcs_server_start_cmd.c_str()) == 0);
    usleep(200 * 1000);
    RAY_LOG(INFO) << "GCS server started.";
    return gcs_server_pid;
  }

  void StopGcsServer(std::string gcs_server_pid) {
    std::string kill_9 = "kill -9 `cat " + gcs_server_pid + "`";
    RAY_LOG(DEBUG) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -f " + gcs_server_pid).c_str()) == 0);
  }

  void SetUp() {
    if (num_nodes_ > 0) {
      CoreWorkerOptions options = {
          WorkerType::DRIVER,             // worker_type
          Language::PYTHON,               // langauge
          raylet_store_socket_names_[0],  // store_socket
          raylet_socket_names_[0],        // raylet_socket
          NextJobId(),                    // job_id
          gcs_options_,                   // gcs_options
          "",                             // log_dir
          true,                           // install_failure_signal_handler
          "127.0.0.1",                    // node_ip_address
          node_manager_port,              // node_manager_port
          "127.0.0.1",                    // raylet_ip_address
          "core_worker_test",             // driver_name
          "",                             // stdout_file
          "",                             // stderr_file
          nullptr,                        // task_execution_callback
          nullptr,                        // check_signals
          nullptr,                        // gc_collect
          nullptr,                        // get_lang_stack
          nullptr,                        // kill_main
          true,                           // ref_counting_enabled
          false,                          // is_local_mode
          1,                              // num_workers
      };
      CoreWorkerProcess::Initialize(options);
    }
  }

  void TearDown() {
    if (num_nodes_ > 0) {
      CoreWorkerProcess::Shutdown();
    }
  }

  // Test normal tasks.
  void TestNormalTask(std::unordered_map<std::string, double> &resources);

  // Test actor tasks.
  void TestActorTask(std::unordered_map<std::string, double> &resources);

  // Test actor failure case, verify that the tasks would either succeed or
  // fail with exceptions, in that case the return objects fetched from `Get`
  // contain errors.
  void TestActorFailure(std::unordered_map<std::string, double> &resources);

  // Test actor failover case. Verify that actor can be reconstructed successfully,
  // and as long as we wait for actor reconstruction before submitting new tasks,
  // it is guaranteed that all tasks are successfully completed.
  void TestActorReconstruction(std::unordered_map<std::string, double> &resources);

 protected:
  bool WaitForDirectCallActorState(const ActorID &actor_id, bool wait_alive,
                                   int timeout_ms);

  // Get the pid for the worker process that runs the actor.
  int GetActorPid(const ActorID &actor_id,
                  std::unordered_map<std::string, double> &resources);

  int num_nodes_;
  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> raylet_store_socket_names_;
  std::string raylet_monitor_pid_;
  gcs::GcsClientOptions gcs_options_;
  std::string gcs_server_pid_;
};

bool CoreWorkerTest::WaitForDirectCallActorState(const ActorID &actor_id, bool wait_alive,
                                                 int timeout_ms) {
  auto condition_func = [actor_id, wait_alive]() -> bool {
    bool actor_alive =
        CoreWorkerProcess::GetCoreWorker().direct_actor_submitter_->IsActorAlive(
            actor_id);
    return wait_alive ? actor_alive : !actor_alive;
  };

  return WaitForCondition(condition_func, timeout_ms);
}

int CoreWorkerTest::GetActorPid(const ActorID &actor_id,
                                std::unordered_map<std::string, double> &resources) {
  std::vector<TaskArg> args;
  TaskOptions options{1, resources};
  std::vector<ObjectID> return_ids;
  RayFunction func{Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                         "GetWorkerPid", "", "", "")};

  RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SubmitActorTask(actor_id, func, args,
                                                                  options, &return_ids));

  std::vector<std::shared_ptr<ray::RayObject>> results;
  RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().Get(return_ids, -1, &results));

  if (nullptr == results[0]->GetData()) {
    // If failed to get actor process pid, return -1
    return -1;
  }

  auto data = reinterpret_cast<char *>(results[0]->GetData()->Data());
  std::string pid_string(data, results[0]->GetData()->Size());
  return std::stoi(pid_string);
}

void CoreWorkerTest::TestNormalTask(std::unordered_map<std::string, double> &resources) {
  auto &driver = CoreWorkerProcess::GetCoreWorker();

  // Test for tasks with by-value and by-ref args.
  {
    const int num_tasks = 100;
    for (int i = 0; i < num_tasks; i++) {
      auto buffer1 = GenerateRandomBuffer();
      auto buffer2 = GenerateRandomBuffer();

      ObjectID object_id;
      RAY_CHECK_OK(driver.Put(RayObject(buffer2, nullptr, std::vector<ObjectID>()), {},
                              &object_id));

      std::vector<TaskArg> args;
      args.emplace_back(TaskArg::PassByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));
      args.emplace_back(TaskArg::PassByReference(object_id));

      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));
      TaskOptions options;
      std::vector<ObjectID> return_ids;
      driver.SubmitTask(func, args, options, &return_ids, /*max_retries=*/0);

      ASSERT_EQ(return_ids.size(), 1);

      std::vector<std::shared_ptr<ray::RayObject>> results;
      RAY_CHECK_OK(driver.Get(return_ids, -1, &results));

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

void CoreWorkerTest::TestActorTask(std::unordered_map<std::string, double> &resources) {
  auto &driver = CoreWorkerProcess::GetCoreWorker();

  auto actor_id = CreateActorHelper(resources, 1000);

  // Test submitting some tasks with by-value args for that actor.
  {
    const int num_tasks = 100;
    for (int i = 0; i < num_tasks; i++) {
      auto buffer1 = GenerateRandomBuffer();
      auto buffer2 = GenerateRandomBuffer();

      // Create arguments with PassByRef and PassByValue.
      std::vector<TaskArg> args;
      args.emplace_back(TaskArg::PassByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));
      args.emplace_back(TaskArg::PassByValue(
          std::make_shared<RayObject>(buffer2, nullptr, std::vector<ObjectID>())));

      TaskOptions options{1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));

      RAY_CHECK_OK(driver.SubmitActorTask(actor_id, func, args, options, &return_ids));
      ASSERT_EQ(return_ids.size(), 1);
      ASSERT_TRUE(return_ids[0].IsReturnObject());
      ASSERT_EQ(static_cast<TaskTransportType>(return_ids[0].GetTransportType()),
                TaskTransportType::DIRECT);

      std::vector<std::shared_ptr<ray::RayObject>> results;
      RAY_CHECK_OK(driver.Get(return_ids, -1, &results));

      ASSERT_EQ(results.size(), 1);
      ASSERT_TRUE(!results[0]->HasMetadata())
          << "metadata: " << MetadataToString(results[0])
          << ", object ID: " << return_ids[0];
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
    RAY_CHECK_OK(
        driver.Put(RayObject(buffer1, nullptr, std::vector<ObjectID>()), {}, &object_id));

    // Create arguments with PassByRef and PassByValue.
    std::vector<TaskArg> args;
    args.emplace_back(TaskArg::PassByReference(object_id));
    args.emplace_back(TaskArg::PassByValue(
        std::make_shared<RayObject>(buffer2, nullptr, std::vector<ObjectID>())));

    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "MergeInputArgsAsOutput", "", "", ""));
    auto status = driver.SubmitActorTask(actor_id, func, args, options, &return_ids);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(return_ids.size(), 1);

    std::vector<std::shared_ptr<ray::RayObject>> results;
    RAY_CHECK_OK(driver.Get(return_ids, -1, &results));

    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0]->GetData()->Size(), buffer1->Size() + buffer2->Size());
    ASSERT_EQ(memcmp(results[0]->GetData()->Data(), buffer1->Data(), buffer1->Size()), 0);
    ASSERT_EQ(memcmp(results[0]->GetData()->Data() + buffer1->Size(), buffer2->Data(),
                     buffer2->Size()),
              0);
  }
}

void CoreWorkerTest::TestActorReconstruction(
    std::unordered_map<std::string, double> &resources) {
  auto &driver = CoreWorkerProcess::GetCoreWorker();

  // creating actor.
  auto actor_id = CreateActorHelper(resources, 1000);

  // Wait for actor alive event.
  ASSERT_TRUE(WaitForDirectCallActorState(actor_id, true, 30 * 1000 /* 30s */));
  RAY_LOG(INFO) << "actor has been created";

  auto pid = GetActorPid(actor_id, resources);
  RAY_CHECK(pid != -1);

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
        auto check_actor_restart_func = [this, pid, &actor_id, &resources]() -> bool {
          auto new_pid = GetActorPid(actor_id, resources);
          return new_pid != -1 && new_pid != pid;
        };
        ASSERT_TRUE(WaitForCondition(check_actor_restart_func, 30 * 1000 /* 30s */));

        RAY_LOG(INFO) << "actor has been reconstructed";
      }

      // wait for actor being reconstructed.
      auto buffer1 = GenerateRandomBuffer();

      // Create arguments with PassByValue.
      std::vector<TaskArg> args;
      args.emplace_back(TaskArg::PassByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));

      TaskOptions options{1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));

      RAY_CHECK_OK(driver.SubmitActorTask(actor_id, func, args, options, &return_ids));
      ASSERT_EQ(return_ids.size(), 1);
      // Verify if it's expected data.
      std::vector<std::shared_ptr<RayObject>> results;

      RAY_CHECK_OK(driver.Get(return_ids, -1, &results));
      ASSERT_EQ(results[0]->GetData()->Size(), buffer1->Size());
      ASSERT_EQ(*results[0]->GetData(), *buffer1);
    }
  }
}

void CoreWorkerTest::TestActorFailure(
    std::unordered_map<std::string, double> &resources) {
  auto &driver = CoreWorkerProcess::GetCoreWorker();

  // creating actor.
  auto actor_id = CreateActorHelper(resources, 0 /* not reconstructable */);

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
      args.emplace_back(TaskArg::PassByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));

      TaskOptions options{1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));

      RAY_CHECK_OK(driver.SubmitActorTask(actor_id, func, args, options, &return_ids));

      ASSERT_EQ(return_ids.size(), 1);
      all_results.emplace_back(std::make_pair(return_ids[0], buffer1));
    }

    for (int i = 0; i < num_tasks; i++) {
      const auto &entry = all_results[i];
      std::vector<ObjectID> return_ids;
      return_ids.push_back(entry.first);
      std::vector<std::shared_ptr<RayObject>> results;
      RAY_CHECK_OK(driver.Get(return_ids, -1, &results));
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
  TaskArg by_value = TaskArg::PassByValue(
      std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>()));
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
  RayFunction function(ray::Language::PYTHON,
                       ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""));
  std::vector<TaskArg> args;
  args.emplace_back(TaskArg::PassByValue(
      std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>())));

  std::unordered_map<std::string, double> resources;
  ActorCreationOptions actor_options{0,
                                     1,
                                     resources,
                                     resources,
                                     {},
                                     /*is_detached*/ false,
                                     /*is_asyncio*/ false};
  const auto job_id = NextJobId();
  ActorHandle actor_handle(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1),
                           TaskID::Nil(), rpc::Address(), job_id, ObjectID::FromRandom(),
                           function.GetLanguage(), function.GetFunctionDescriptor(), "");

  // Manually create `num_tasks` task specs, and for each of them create a
  // `PushTaskRequest`, this is to batch performance of TaskSpec
  // creation/copy/destruction.
  int64_t start_ms = current_time_ms();
  const auto num_tasks = 10000 * 10;
  RAY_LOG(INFO) << "start creating " << num_tasks << " PushTaskRequests";
  rpc::Address address;
  for (int i = 0; i < num_tasks; i++) {
    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    auto num_returns = options.num_returns;

    TaskSpecBuilder builder;
    builder.SetCommonTaskSpec(RandomTaskId(), function.GetLanguage(),
                              function.GetFunctionDescriptor(), job_id, RandomTaskId(), 0,
                              RandomTaskId(), address, num_returns, resources, resources);
    // Set task arguments.
    for (const auto &arg : args) {
      if (arg.IsPassedByReference()) {
        builder.AddByRefArg(arg.GetReference());
      } else {
        builder.AddByValueArg(arg.GetValue());
      }
    }

    actor_handle.SetActorTaskSpec(builder, ObjectID::FromRandom());

    auto task_spec = builder.Build();

    ASSERT_TRUE(task_spec.IsActorTask());
    auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
    request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());
  }
  RAY_LOG(INFO) << "Finish creating " << num_tasks << " PushTaskRequests"
                << ", which takes " << current_time_ms() - start_ms << " ms";
}

TEST_F(SingleNodeTest, TestDirectActorTaskSubmissionPerf) {
  auto &driver = CoreWorkerProcess::GetCoreWorker();
  std::vector<ObjectID> object_ids;
  // Create an actor.
  std::unordered_map<std::string, double> resources;
  auto actor_id = CreateActorHelper(resources,
                                    /*max_reconstructions=*/0);
  // wait for actor creation finish.
  ASSERT_TRUE(WaitForDirectCallActorState(actor_id, true, 30 * 1000 /* 30s */));
  // Test submitting some tasks with by-value args for that actor.
  int64_t start_ms = current_time_ms();
  const int num_tasks = 100000;
  RAY_LOG(INFO) << "start submitting " << num_tasks << " tasks";
  for (int i = 0; i < num_tasks; i++) {
    // Create arguments with PassByValue.
    std::vector<TaskArg> args;
    int64_t array[] = {SHOULD_CHECK_MESSAGE_ORDER, i};
    auto buffer = std::make_shared<LocalMemoryBuffer>(reinterpret_cast<uint8_t *>(array),
                                                      sizeof(array));
    args.emplace_back(TaskArg::PassByValue(
        std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>())));

    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "MergeInputArgsAsOutput", "", "", ""));

    RAY_CHECK_OK(driver.SubmitActorTask(actor_id, func, args, options, &return_ids));
    ASSERT_EQ(return_ids.size(), 1);
    object_ids.emplace_back(return_ids[0]);
  }
  RAY_LOG(INFO) << "finish submitting " << num_tasks << " tasks"
                << ", which takes " << current_time_ms() - start_ms << " ms";

  for (const auto &object_id : object_ids) {
    std::vector<std::shared_ptr<RayObject>> results;
    RAY_CHECK_OK(driver.Get({object_id}, -1, &results));
    ASSERT_EQ(results.size(), 1);
  }
  RAY_LOG(INFO) << "finish executing " << num_tasks << " tasks"
                << ", which takes " << current_time_ms() - start_ms << " ms";
}

TEST_F(ZeroNodeTest, TestWorkerContext) {
  auto job_id = NextJobId();

  WorkerContext context(WorkerType::WORKER, WorkerID::FromRandom(), job_id);
  ASSERT_TRUE(context.GetCurrentTaskID().IsNil());
  ASSERT_EQ(context.GetNextTaskIndex(), 1);
  ASSERT_EQ(context.GetNextTaskIndex(), 2);
  ASSERT_EQ(context.GetNextPutIndex(), 1);
  ASSERT_EQ(context.GetNextPutIndex(), 2);

  auto thread_func = [&context]() {
    // Verify that task_index, put_index are thread-local.
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
  // Test actor handle serialization and deserialization round trip.
  JobID job_id = NextJobId();
  ActorHandle original(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 0),
                       TaskID::Nil(), rpc::Address(), job_id, ObjectID::FromRandom(),
                       Language::PYTHON,
                       ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""), "");
  std::string output;
  original.Serialize(&output);
  ActorHandle deserialized(output);
  ASSERT_EQ(deserialized.GetActorID(), original.GetActorID());
  ASSERT_EQ(deserialized.ActorLanguage(), original.ActorLanguage());
  ASSERT_EQ(deserialized.ActorCreationTaskFunctionDescriptor(),
            original.ActorCreationTaskFunctionDescriptor());

  // TODO: Test submission from different handles.
}

TEST_F(SingleNodeTest, TestMemoryStoreProvider) {
  std::shared_ptr<CoreWorkerMemoryStore> provider_ptr =
      std::make_shared<CoreWorkerMemoryStore>();

  auto &provider = *provider_ptr;

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<RayObject> buffers;
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1)),
                       std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1) / 2),
                       std::vector<ObjectID>());
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2)),
                       std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2) / 2),
                       std::vector<ObjectID>());

  std::vector<ObjectID> ids(buffers.size());
  for (size_t i = 0; i < ids.size(); i++) {
    ids[i] = ObjectID::FromRandom().WithDirectTransportType();
    RAY_CHECK(provider.Put(buffers[i], ids[i]));
  }

  absl::flat_hash_set<ObjectID> wait_ids(ids.begin(), ids.end());
  absl::flat_hash_set<ObjectID> wait_results;

  ObjectID nonexistent_id = ObjectID::FromRandom().WithDirectTransportType();
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::Nil());
  wait_ids.insert(nonexistent_id);
  RAY_CHECK_OK(provider.Wait(wait_ids, ids.size() + 1, 100, ctx, &wait_results));
  ASSERT_EQ(wait_results.size(), ids.size());
  ASSERT_TRUE(wait_results.count(nonexistent_id) == 0);

  // Test Wait() where the required `num_objects` is less than size of `wait_ids`.
  wait_results.clear();
  RAY_CHECK_OK(provider.Wait(wait_ids, ids.size(), -1, ctx, &wait_results));
  ASSERT_EQ(wait_results.size(), ids.size());
  ASSERT_TRUE(wait_results.count(nonexistent_id) == 0);

  // Test Get().
  bool got_exception = false;
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> results;
  absl::flat_hash_set<ObjectID> ids_set(ids.begin(), ids.end());
  RAY_CHECK_OK(provider.Get(ids_set, -1, ctx, &results, &got_exception));

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

  absl::flat_hash_set<ObjectID> plasma_object_ids;
  provider.Delete(ids_set, &plasma_object_ids);
  ASSERT_TRUE(plasma_object_ids.empty());

  usleep(200 * 1000);
  ASSERT_TRUE(provider.Get(ids_set, 0, ctx, &results, &got_exception).IsTimedOut());
  ASSERT_TRUE(!got_exception);
  ASSERT_EQ(results.size(), 0);

  // Test Wait() with objects which will become ready later.
  std::vector<ObjectID> ready_ids(buffers.size());
  std::vector<ObjectID> unready_ids(buffers.size());
  for (size_t i = 0; i < unready_ids.size(); i++) {
    ready_ids[i] = ObjectID::FromRandom().WithDirectTransportType();
    RAY_CHECK(provider.Put(buffers[i], ready_ids[i]));
    unready_ids[i] = ObjectID::FromRandom().WithDirectTransportType();
  }

  auto thread_func = [&unready_ids, &provider, &buffers]() {
    sleep(1);

    for (size_t i = 0; i < unready_ids.size(); i++) {
      RAY_CHECK(provider.Put(buffers[i], unready_ids[i]));
    }
  };

  std::thread async_thread(thread_func);

  wait_ids.clear();
  wait_ids.insert(ready_ids.begin(), ready_ids.end());
  wait_ids.insert(unready_ids.begin(), unready_ids.end());
  wait_results.clear();

  // Check that only the ready ids are returned when timeout ends before thread runs.
  RAY_CHECK_OK(provider.Wait(wait_ids, ready_ids.size() + 1, 100, ctx, &wait_results));
  ASSERT_EQ(ready_ids.size(), wait_results.size());
  for (const auto &ready_id : ready_ids) {
    ASSERT_TRUE(wait_results.find(ready_id) != wait_results.end());
  }
  for (const auto &unready_id : unready_ids) {
    ASSERT_TRUE(wait_results.find(unready_id) == wait_results.end());
  }

  wait_results.clear();
  // Check that enough objects are returned after the thread inserts at least one object.
  RAY_CHECK_OK(provider.Wait(wait_ids, ready_ids.size() + 1, 5000, ctx, &wait_results));
  ASSERT_TRUE(wait_results.size() >= ready_ids.size() + 1);
  for (const auto &ready_id : ready_ids) {
    ASSERT_TRUE(wait_results.find(ready_id) != wait_results.end());
  }

  wait_results.clear();
  // Check that all objects are returned after the thread completes.
  async_thread.join();
  RAY_CHECK_OK(provider.Wait(wait_ids, wait_ids.size(), -1, ctx, &wait_results));
  ASSERT_EQ(wait_results.size(), ready_ids.size() + unready_ids.size());
  for (const auto &ready_id : ready_ids) {
    ASSERT_TRUE(wait_results.find(ready_id) != wait_results.end());
  }
  for (const auto &unready_id : unready_ids) {
    ASSERT_TRUE(wait_results.find(unready_id) != wait_results.end());
  }
}

TEST_F(SingleNodeTest, TestObjectInterface) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();

  uint8_t array1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint8_t array2[] = {10, 11, 12, 13, 14, 15};

  std::vector<RayObject> buffers;
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1)),
                       std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1) / 2),
                       std::vector<ObjectID>());
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2)),
                       std::make_shared<LocalMemoryBuffer>(array2, sizeof(array2) / 2),
                       std::vector<ObjectID>());

  std::vector<ObjectID> ids(buffers.size());
  for (size_t i = 0; i < ids.size(); i++) {
    RAY_CHECK_OK(core_worker.Put(buffers[i], {}, &ids[i]));
  }

  // Test Get().
  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(core_worker.Get(ids, -1, &results));
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
      nullptr,
      std::make_shared<LocalMemoryBuffer>(reinterpret_cast<uint8_t *>(error_buffer), len),
      std::vector<ObjectID>());

  RAY_CHECK_OK(
      core_worker.Put(buffers_with_exception.back(), {}, ids_with_exception.back()));
  RAY_CHECK_OK(core_worker.Get(ids_with_exception, -1, &results));

  // Test Wait().
  ObjectID non_existent_id = ObjectID::FromRandom();
  std::vector<ObjectID> all_ids(ids);
  all_ids.push_back(non_existent_id);

  std::vector<bool> wait_results;
  RAY_CHECK_OK(core_worker.Wait(all_ids, 2, -1, &wait_results));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  RAY_CHECK_OK(core_worker.Wait(all_ids, 3, 100, &wait_results));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  // Test Delete().
  // clear the reference held by PlasmaBuffer.
  results.clear();
  RAY_CHECK_OK(core_worker.Delete(ids, true, false));

  // Note that Delete() calls RayletClient::FreeObjects and would not
  // wait for objects being deleted, so wait a while for plasma store
  // to process the command.
  usleep(200 * 1000);
  ASSERT_TRUE(core_worker.Get(ids, 0, &results).IsTimedOut());
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

TEST_F(SingleNodeTest, TestActorTaskLocalReconstruction) {
  std::unordered_map<std::string, double> resources;
  TestActorReconstruction(resources);
}

TEST_F(TwoNodeTest, TestActorTaskCrossNodesReconstruction) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorReconstruction(resources);
}

TEST_F(SingleNodeTest, TestActorTaskLocalFailure) {
  std::unordered_map<std::string, double> resources;
  TestActorFailure(resources);
}

TEST_F(TwoNodeTest, TestActorTaskCrossNodesFailure) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorFailure(resources);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 7);
  store_executable = std::string(argv[1]);
  raylet_executable = std::string(argv[2]);
  node_manager_port = std::stoi(std::string(argv[3]));
  raylet_monitor_executable = std::string(argv[4]);
  mock_worker_executable = std::string(argv[5]);
  gcs_server_executable = std::string(argv[6]);
  return RUN_ALL_TESTS();
}
