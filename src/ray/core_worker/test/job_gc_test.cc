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

class JobGcTest : public ::testing::Test {
 public:
  JobGcTest() {}

  JobGcTest(int num_nodes) : num_nodes_(num_nodes), gcs_options_("127.0.0.1", 6379, "") {
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

  ~JobGcTest() {
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
  std::string raylet_monitor_pid_;
  gcs::GcsClientOptions gcs_options_;
  std::string gcs_server_pid_;
};

TEST_F(JobGcTest, TestApi) {
  RAY_LOG(INFO) << "Hello world!!!!!";
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
//  RAY_CHECK(argc == 7);
//  store_executable = std::string(argv[1]);
//  raylet_executable = std::string(argv[2]);
//  node_manager_port = std::stoi(std::string(argv[3]));
//  raylet_monitor_executable = std::string(argv[4]);
//  mock_worker_executable = std::string(argv[5]);
//  gcs_server_executable = std::string(argv[6]);

  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
