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
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/util/filesystem.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace {

int node_manager_port = 0;

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
                          int64_t max_restarts) {
  std::unique_ptr<ActorHandle> actor_handle;

  uint8_t array[] = {1, 2, 3};
  auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

  RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                              "actor creation task", "", "", ""));
  std::vector<std::unique_ptr<TaskArg>> args;
  args.emplace_back(new TaskArgByValue(
      std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>())));

  std::string name = "";
  ActorCreationOptions actor_options{
      max_restarts,
      /*max_task_retries=*/0,
      /*max_concurrency*/ 1,  resources, resources,           {},
      /*is_detached=*/false,  name,      /*is_asyncio=*/false};

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
    TestSetupUtil::StartUpRedisServers(std::vector<int>{6379, 6380});

    // flush redis first.
    flushall_redis();

    RAY_CHECK(num_nodes >= 0);
    if (num_nodes > 0) {
      raylet_socket_names_.resize(num_nodes);
      raylet_store_socket_names_.resize(num_nodes);
    }

    // start plasma store.
    for (auto &store_socket : raylet_store_socket_names_) {
      store_socket = TestSetupUtil::StartObjectStore();
    }

    // start gcs server
    gcs_server_socket_name_ = TestSetupUtil::StartGcsServer("127.0.0.1");

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node.
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] = TestSetupUtil::StartRaylet(
          raylet_store_socket_names_[i], "127.0.0.1", node_manager_port + i, "127.0.0.1",
          "\"CPU,4.0,resource" + std::to_string(i) + ",10\"");
    }
  }

  ~CoreWorkerTest() {
    for (const auto &raylet_socket_name : raylet_socket_names_) {
      TestSetupUtil::StopRaylet(raylet_socket_name);
    }

    for (const auto &store_socket_name : raylet_store_socket_names_) {
      TestSetupUtil::StopObjectStore(store_socket_name);
    }

    if (!gcs_server_socket_name_.empty()) {
      TestSetupUtil::StopGcsServer(gcs_server_socket_name_);
    }

    TestSetupUtil::ShutDownRedisServers();
  }

  JobID NextJobId() const {
    static uint32_t job_counter = 1;
    return JobID::FromInt(job_counter++);
  }

  void SetUp() {
    if (num_nodes_ > 0) {
      CoreWorkerOptions options;
      options.worker_type = WorkerType::DRIVER;
      options.language = Language::PYTHON;
      options.store_socket = raylet_store_socket_names_[0];
      options.raylet_socket = raylet_socket_names_[0];
      options.job_id = NextJobId();
      options.gcs_options = gcs_options_;
      options.enable_logging = true;
      options.install_failure_signal_handler = true;
      options.node_ip_address = "127.0.0.1";
      options.node_manager_port = node_manager_port;
      options.raylet_ip_address = "127.0.0.1";
      options.driver_name = "core_worker_test";
      options.ref_counting_enabled = true;
      options.num_workers = 1;
      options.metrics_agent_port = -1;
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
  void TestActorRestart(std::unordered_map<std::string, double> &resources);

 protected:
  bool WaitForDirectCallActorState(const ActorID &actor_id, bool wait_alive,
                                   int timeout_ms);

  // Get the pid for the worker process that runs the actor.
  int GetActorPid(const ActorID &actor_id,
                  std::unordered_map<std::string, double> &resources);

  int num_nodes_;
  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> raylet_store_socket_names_;
  gcs::GcsClientOptions gcs_options_;
  std::string gcs_server_socket_name_;
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
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options{"", 1, resources};
  std::vector<ObjectID> return_ids;
  RayFunction func{Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                         "GetWorkerPid", "", "", "")};

  CoreWorkerProcess::GetCoreWorker().SubmitActorTask(actor_id, func, args, options,
                                                     &return_ids);

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

      std::vector<std::unique_ptr<TaskArg>> args;
      args.emplace_back(new TaskArgByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));
      args.emplace_back(new TaskArgByReference(object_id, driver.GetRpcAddress()));

      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));
      TaskOptions options;
      std::vector<ObjectID> return_ids;
      driver.SubmitTask(func, args, options, &return_ids, /*max_retries=*/0,
                        std::make_pair(PlacementGroupID::Nil(), -1), true,
                        /*debugger_breakpoint=*/"");

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
      std::vector<std::unique_ptr<TaskArg>> args;
      args.emplace_back(new TaskArgByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));
      args.emplace_back(new TaskArgByValue(
          std::make_shared<RayObject>(buffer2, nullptr, std::vector<ObjectID>())));

      TaskOptions options{"", 1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));

      driver.SubmitActorTask(actor_id, func, args, options, &return_ids);
      ASSERT_EQ(return_ids.size(), 1);

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
    std::vector<std::unique_ptr<TaskArg>> args;
    args.emplace_back(new TaskArgByReference(object_id, driver.GetRpcAddress()));
    args.emplace_back(new TaskArgByValue(
        std::make_shared<RayObject>(buffer2, nullptr, std::vector<ObjectID>())));

    TaskOptions options{"", 1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "MergeInputArgsAsOutput", "", "", ""));
    driver.SubmitActorTask(actor_id, func, args, options, &return_ids);

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

void CoreWorkerTest::TestActorRestart(
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
        ASSERT_EQ(KillAllExecutable(GetFileName(TEST_MOCK_WORKER_EXEC_PATH)), 0);

        // Wait for actor restruction event, and then for alive event.
        auto check_actor_restart_func = [this, pid, &actor_id, &resources]() -> bool {
          auto new_pid = GetActorPid(actor_id, resources);
          return new_pid != -1 && new_pid != pid;
        };
        ASSERT_TRUE(WaitForCondition(check_actor_restart_func, 30 * 1000 /* 30s */));

        RAY_LOG(INFO) << "actor has been restarted";
      }

      // wait for actor being restarted.
      auto buffer1 = GenerateRandomBuffer();

      // Create arguments with PassByValue.
      std::vector<std::unique_ptr<TaskArg>> args;
      args.emplace_back(new TaskArgByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));

      TaskOptions options{"", 1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));

      driver.SubmitActorTask(actor_id, func, args, options, &return_ids);
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
        ASSERT_EQ(KillAllExecutable(GetFileName(TEST_MOCK_WORKER_EXEC_PATH)), 0);
      }

      // wait for actor being restarted.
      auto buffer1 = GenerateRandomBuffer();

      // Create arguments with PassByRef and PassByValue.
      std::vector<std::unique_ptr<TaskArg>> args;
      args.emplace_back(new TaskArgByValue(
          std::make_shared<RayObject>(buffer1, nullptr, std::vector<ObjectID>())));

      TaskOptions options{"", 1, resources};
      std::vector<ObjectID> return_ids;
      RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                  "MergeInputArgsAsOutput", "", "", ""));

      driver.SubmitActorTask(actor_id, func, args, options, &return_ids);

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

// Performance batchmark for `PushTaskRequest` creation.
TEST_F(ZeroNodeTest, TestTaskSpecPerf) {
  // Create a dummy actor handle, and then create a number of `TaskSpec`
  // to benchmark performance.
  uint8_t array[] = {1, 2, 3};
  auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));
  RayFunction function(ray::Language::PYTHON,
                       ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""));
  std::vector<std::unique_ptr<TaskArg>> args;
  args.emplace_back(new TaskArgByValue(
      std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>())));

  std::unordered_map<std::string, double> resources;
  std::string name = "";
  ActorCreationOptions actor_options{0,
                                     0,
                                     1,
                                     resources,
                                     resources,
                                     {},
                                     /*is_detached=*/false,
                                     name,
                                     /*is_asyncio=*/false};
  const auto job_id = NextJobId();
  ActorHandle actor_handle(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1),
                           TaskID::Nil(), rpc::Address(), job_id, ObjectID::FromRandom(),
                           function.GetLanguage(), function.GetFunctionDescriptor(), "",
                           0);

  // Manually create `num_tasks` task specs, and for each of them create a
  // `PushTaskRequest`, this is to batch performance of TaskSpec
  // creation/copy/destruction.
  int64_t start_ms = current_time_ms();
  const auto num_tasks = 10000 * 10;
  RAY_LOG(INFO) << "start creating " << num_tasks << " PushTaskRequests";
  rpc::Address address;
  for (int i = 0; i < num_tasks; i++) {
    TaskOptions options{"", 1, resources};
    std::vector<ObjectID> return_ids;
    auto num_returns = options.num_returns;

    TaskSpecBuilder builder;
    builder.SetCommonTaskSpec(RandomTaskId(), options.name, function.GetLanguage(),
                              function.GetFunctionDescriptor(), job_id, RandomTaskId(), 0,
                              RandomTaskId(), address, num_returns, resources, resources,
                              std::make_pair(PlacementGroupID::Nil(), -1), true, "");
    // Set task arguments.
    for (const auto &arg : args) {
      builder.AddArg(*arg);
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
                                    /*max_restarts=*/0);
  // wait for actor creation finish.
  ASSERT_TRUE(WaitForDirectCallActorState(actor_id, true, 30 * 1000 /* 30s */));
  // Test submitting some tasks with by-value args for that actor.
  int64_t start_ms = current_time_ms();
  const int num_tasks = 100000;
  RAY_LOG(INFO) << "start submitting " << num_tasks << " tasks";
  for (int i = 0; i < num_tasks; i++) {
    // Create arguments with PassByValue.
    std::vector<std::unique_ptr<TaskArg>> args;
    int64_t array[] = {SHOULD_CHECK_MESSAGE_ORDER, i};
    auto buffer = std::make_shared<LocalMemoryBuffer>(reinterpret_cast<uint8_t *>(array),
                                                      sizeof(array));
    args.emplace_back(new TaskArgByValue(
        std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>())));

    TaskOptions options{"", 1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "MergeInputArgsAsOutput", "", "", ""));

    driver.SubmitActorTask(actor_id, func, args, options, &return_ids);
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

  TaskSpecification task_spec;
  size_t num_returns = 3;
  task_spec.GetMutableMessage().set_job_id(job_id.Binary());
  task_spec.GetMutableMessage().set_num_returns(num_returns);
  context.ResetCurrentTask();
  context.SetCurrentTask(task_spec);
  ASSERT_EQ(context.GetCurrentTaskID(), task_spec.TaskId());

  // Verify that put index doesn't confict with the return object range.
  ASSERT_EQ(context.GetNextPutIndex(), num_returns + 1);
  ASSERT_EQ(context.GetNextPutIndex(), num_returns + 2);
}

TEST_F(ZeroNodeTest, TestActorHandle) {
  // Test actor handle serialization and deserialization round trip.
  JobID job_id = NextJobId();
  ActorHandle original(
      ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 0), TaskID::Nil(),
      rpc::Address(), job_id, ObjectID::FromRandom(), Language::PYTHON,
      ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""), "", 0);
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
    ids[i] = ObjectID::FromRandom();
    RAY_CHECK(provider.Put(buffers[i], ids[i]));
  }

  absl::flat_hash_set<ObjectID> wait_ids(ids.begin(), ids.end());
  absl::flat_hash_set<ObjectID> wait_results;

  ObjectID nonexistent_id = ObjectID::FromRandom();
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

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_TRUE(provider.Get(ids_set, 0, ctx, &results, &got_exception).IsTimedOut());
  ASSERT_TRUE(!got_exception);
  ASSERT_EQ(results.size(), 0);

  // Test Wait() with objects which will become ready later.
  std::vector<ObjectID> ready_ids(buffers.size());
  std::vector<ObjectID> unready_ids(buffers.size());
  for (size_t i = 0; i < unready_ids.size(); i++) {
    ready_ids[i] = ObjectID::FromRandom();
    RAY_CHECK(provider.Put(buffers[i], ready_ids[i]));
    unready_ids[i] = ObjectID::FromRandom();
  }

  auto thread_func = [&unready_ids, &provider, &buffers]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

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
  const size_t array2_size = 200 * 1024;
  uint8_t *array2 = new uint8_t[array2_size];

  std::vector<RayObject> buffers;
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1)),
                       std::make_shared<LocalMemoryBuffer>(array1, sizeof(array1) / 2),
                       std::vector<ObjectID>());
  buffers.emplace_back(std::make_shared<LocalMemoryBuffer>(array2, array2_size),
                       std::make_shared<LocalMemoryBuffer>(array2, array2_size / 2),
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

  // Test Wait().
  ObjectID non_existent_id = ObjectID::FromRandom();
  std::vector<ObjectID> all_ids(ids);
  all_ids.push_back(non_existent_id);

  std::vector<bool> wait_results;
  RAY_CHECK_OK(core_worker.Wait(all_ids, 2, -1, &wait_results, true));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  RAY_CHECK_OK(core_worker.Wait(all_ids, 3, 100, &wait_results, true));
  ASSERT_EQ(wait_results.size(), 3);
  ASSERT_EQ(wait_results, std::vector<bool>({true, true, false}));

  // Test Delete().
  // clear the reference held by TrackedBuffer.
  results.clear();
  RAY_CHECK_OK(core_worker.Delete(ids, true));

  // Note that Delete() calls RayletClient::FreeObjects and would not
  // wait for objects being deleted, so wait a while for plasma store
  // to process the command.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_TRUE(core_worker.Get(ids, 0, &results).ok());
  // Since array2 has been deleted from the plasma store, the Get should
  // return UnreconstructableError for all results.
  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(results[0]->IsException());
  ASSERT_TRUE(results[1]->IsException());
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
  TestActorRestart(resources);
}

TEST_F(TwoNodeTest, TestActorTaskCrossNodesReconstruction) {
  std::unordered_map<std::string, double> resources;
  resources.emplace("resource1", 1);
  TestActorRestart(resources);
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
  RAY_CHECK(argc == 8);
  ray::TEST_STORE_EXEC_PATH = std::string(argv[1]);
  ray::TEST_RAYLET_EXEC_PATH = std::string(argv[2]);

  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> random_gen{2000, 2009};
  // Use random port to avoid port conflicts between UTs.
  node_manager_port = random_gen(gen);
  ray::TEST_MOCK_WORKER_EXEC_PATH = std::string(argv[3]);
  ray::TEST_GCS_SERVER_EXEC_PATH = std::string(argv[4]);

  ray::TEST_REDIS_CLIENT_EXEC_PATH = std::string(argv[5]);
  ray::TEST_REDIS_SERVER_EXEC_PATH = std::string(argv[6]);
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = std::string(argv[7]);
  return RUN_ALL_TESTS();
}
