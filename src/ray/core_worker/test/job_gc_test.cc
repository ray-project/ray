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

namespace ray {

static void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", REDIS_SERVER_PORT);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  std::string cmd = "LPUSH RedisShards 127.0.0.1:" + std::to_string(REDIS_SHARD_SERVER_PORT);
  freeReplyObject(redisCommand(context, cmd.c_str()));
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

class JobGcTest : public RedisShardServiceManagerForTest {
 public:
  JobGcTest(int num_nodes)
      : num_nodes_(num_nodes), gcs_options_("127.0.0.1", REDIS_SERVER_PORT, "") {
    RAY_LOG(INFO) << "hellp.........gcs_options_ port = " << gcs_options_.server_port_;
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
    gcs_server_pid_ = StartGcsServer("127.0.0.1");

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node.
    node_manager_port_ = REDIS_SHARD_SERVER_PORT + 1;
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] =
          StartRaylet(raylet_store_socket_names_[i], "127.0.0.1", node_manager_port_ + i,
                      "127.0.0.1", "\"CPU,4.0,resource" + std::to_string(i) + ",10\"");
    }
  }

  ~JobGcTest() {
    for (const auto &raylet_socket : raylet_socket_names_) {
      StopRaylet(raylet_socket);
    }

    for (const auto &store_socket : raylet_store_socket_names_) {
      StopStore(store_socket);
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
    std::string plasma_command = STORE_EXEC_PATH + " -m 100000000 -s " +
                                 store_socket_name +
                                 " 1> /dev/null 2> /dev/null & echo $! > " + store_pid;
    RAY_LOG(INFO) << plasma_command;
    RAY_CHECK(system(plasma_command.c_str()) == 0);
    usleep(200 * 1000);
    return store_socket_name;
  }

  void StopStore(const std::string &store_socket_name) {
    std::string store_pid = store_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + store_pid + "`";
    RAY_LOG(INFO) << kill_9;
    ASSERT_EQ(system(kill_9.c_str()), 0);
    ASSERT_EQ(system(("rm -rf " + store_socket_name).c_str()), 0);
    ASSERT_EQ(system(("rm -rf " + store_socket_name + ".pid").c_str()), 0);
  }

  std::string StartRaylet(const std::string &store_socket_name,
                          const std::string &node_ip_address, int port,
                          const std::string &redis_address, const std::string &resource) {
    std::string raylet_socket_name =
        ray::JoinPaths(ray::GetUserTempDir(), "raylet" + ObjectID::FromRandom().Hex());
    std::stringstream ray_start_cmd;
    ray_start_cmd << RAYLET_EXEC_PATH << " --raylet_socket_name=" << raylet_socket_name
                  << " --store_socket_name=" << store_socket_name
                  << " --object_manager_port=0"
                  << " --node_manager_port=" << port
                  << " --node_ip_address=" << node_ip_address
                  << " --redis_address=" << redis_address
                  << " --redis_port=" << REDIS_SERVER_PORT << " --num_initial_workers=1"
                  << " --maximum_startup_concurrency=10"
                  << " --static_resource_list=" << resource
                  << " --python_worker_command=\"" << MOCK_WORKER_EXEC_PATH << " "
                  << store_socket_name << " " << raylet_socket_name << " " << port << "\""
                  << " --config_list=initial_reconstruction_timeout_milliseconds,2000"
                  << " & echo $! > " << raylet_socket_name << ".pid";

    RAY_LOG(INFO) << "Ray Start command: " << ray_start_cmd.str();
    RAY_CHECK(system(ray_start_cmd.str().c_str()) == 0);
    usleep(200 * 1000);
    return raylet_socket_name;
  }

  void StopRaylet(const std::string &raylet_socket_name) {
    std::string raylet_pid = raylet_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + raylet_pid + "`";
    RAY_LOG(INFO) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name).c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name + ".pid").c_str()) == 0);
  }

  std::string StartGcsServer(const std::string &redis_address) {
    std::string gcs_server_pid = ray::JoinPaths(
        ray::GetUserTempDir(), "gcs_server" + ObjectID::FromRandom().Hex() + ".pid");
    std::stringstream gcs_server_start_cmd;
    gcs_server_start_cmd
        << GCS_SERVER_EXEC_PATH << " --redis_address=" << redis_address
        << " --redis_port=" << REDIS_SERVER_PORT
        << " --config_list=initial_reconstruction_timeout_milliseconds,2000"
        << " & echo $! > " << gcs_server_pid;

    RAY_LOG(INFO) << "Starting GCS server, command: " << gcs_server_start_cmd.str();
    RAY_CHECK(system(gcs_server_start_cmd.str().c_str()) == 0);
    usleep(2000 * 1000);
    RAY_LOG(INFO) << "GCS server started.";
    return gcs_server_pid;
  }

  void StopGcsServer(const std::string &gcs_server_pid) {
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
          node_manager_port_,              // node_manager_port
          "core_worker_test",             // driver_name
          "",                             // stdout_file
          "",                             // stderr_file
          nullptr,                        // task_execution_callback
          nullptr,                        // check_signals
          nullptr,                        // gc_collect
          nullptr,                        // get_lang_stack
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

  int num_nodes_;
  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> raylet_store_socket_names_;
  gcs::GcsClientOptions gcs_options_;
  std::string gcs_server_pid_;
  int node_manager_port_;
};

class JobGcSingleNodeTest : public JobGcTest {
 public:
  JobGcSingleNodeTest() : JobGcTest(1) {}
};

TEST_F(JobGcSingleNodeTest, TestApi) {
  RAY_LOG(INFO) << "Hello world!!!!!----begining";

  std::unordered_map<std::string, double> resources;
  auto &driver = CoreWorkerProcess::GetCoreWorker();
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
  RAY_CHECK_OK(
      driver.SubmitTask(func, args, options, &return_ids, /*max_retries=*/0));

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
  RAY_LOG(INFO) << "Hello world!!!!!----ending";
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  RAY_CHECK(argc == 8);
  for (int index = 0; index < argc; ++index) {
    RAY_LOG(INFO) << "Argc " << index << " = " << argv[index];
  }

  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  ray::RAYLET_EXEC_PATH = argv[4];
  ray::GCS_SERVER_EXEC_PATH = argv[5];
  ray::STORE_EXEC_PATH = argv[6];
  ray::MOCK_WORKER_EXEC_PATH = argv[7];

  return RUN_ALL_TESTS();
}
