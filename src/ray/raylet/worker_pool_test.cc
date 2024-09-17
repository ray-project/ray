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

#include "ray/raylet/worker_pool.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "nlohmann/json.hpp"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/constants.h"
#include "ray/raylet/node_manager.h"
#include "ray/raylet/runtime_env_agent_client.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/runtime_env_agent.pb.h"

using json = nlohmann::json;
namespace ray {

namespace raylet {

int MAXIMUM_STARTUP_CONCURRENCY = 15;
int PYTHON_PRESTART_WORKERS = 15;
int MAX_IO_WORKER_SIZE = 2;
int POOL_SIZE_SOFT_LIMIT = 3;
int WORKER_REGISTER_TIMEOUT_SECONDS = 3;
JobID JOB_ID = JobID::FromInt(1);
JobID JOB_ID2 = JobID::FromInt(2);
std::string BAD_RUNTIME_ENV = "bad runtime env";
const std::string BAD_RUNTIME_ENV_ERROR_MSG = "bad runtime env";

std::vector<Language> LANGUAGES = {Language::PYTHON, Language::JAVA};

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  MockWorkerClient() {}

  void Exit(const rpc::ExitRequest &request,
            const rpc::ClientCallback<rpc::ExitReply> &callback) {
    exit_count++;
    last_exit_forced = request.force_exit();
    callbacks_.push_back(callback);
  }

  bool ExitReplySucceed() {
    if (callbacks_.size() == 0) {
      return false;
    }
    const auto &callback = callbacks_.front();
    rpc::ExitReply exit_reply;
    exit_reply.set_success(true);
    callback(Status::OK(), std::move(exit_reply));
    callbacks_.pop_front();
    return true;
  }

  bool ExitReplyFailed() {
    if (callbacks_.size() == 0) {
      return false;
    }
    const auto &callback = callbacks_.front();
    rpc::ExitReply exit_reply;
    exit_reply.set_success(false);
    callback(Status::OK(), std::move(exit_reply));
    callbacks_.pop_front();
    return true;
  }

  bool last_exit_forced = false;
  int64_t exit_count = 0;
  std::list<rpc::ClientCallback<rpc::ExitReply>> callbacks_;
};

static std::unordered_map<std::string, int> runtime_env_reference;

static int GetReferenceCount(const std::string serialized_runtime_env) {
  auto it = runtime_env_reference.find(serialized_runtime_env);
  return it == runtime_env_reference.end() ? 0 : it->second;
}

class MockRuntimeEnvAgentClient : public RuntimeEnvAgentClient {
 public:
  void GetOrCreateRuntimeEnv(const JobID &job_id,
                             const std::string &serialized_runtime_env,
                             const rpc::RuntimeEnvConfig &runtime_env_config,
                             GetOrCreateRuntimeEnvCallback callback) override {
    if (serialized_runtime_env == BAD_RUNTIME_ENV) {
      callback(false, "", BAD_RUNTIME_ENV_ERROR_MSG);
    } else {
      rpc::GetOrCreateRuntimeEnvReply reply;
      auto it = runtime_env_reference.find(serialized_runtime_env);
      if (it == runtime_env_reference.end()) {
        runtime_env_reference[serialized_runtime_env] = 1;
      } else {
        runtime_env_reference[serialized_runtime_env] += 1;
      }
      callback(true, "{\"dummy\":\"dummy\"}", "");
    }
  };

  void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env,
                                  DeleteRuntimeEnvIfPossibleCallback callback) override {
    auto it = runtime_env_reference.find(serialized_runtime_env);
    RAY_CHECK(it != runtime_env_reference.end());
    runtime_env_reference[serialized_runtime_env] -= 1;
    RAY_CHECK(runtime_env_reference[serialized_runtime_env] >= 0);
    callback(true);
  };
};

class WorkerPoolMock : public WorkerPool {
 public:
  explicit WorkerPoolMock(instrumented_io_context &io_service,
                          const WorkerCommandMap &worker_commands,
                          absl::flat_hash_map<WorkerID, std::shared_ptr<MockWorkerClient>>
                              &mock_worker_rpc_clients)
      : WorkerPool(
            io_service,
            NodeID::FromRandom(),
            "",
            [this]() { return num_available_cpus_; },
            PYTHON_PRESTART_WORKERS,
            MAXIMUM_STARTUP_CONCURRENCY,
            0,
            0,
            {},
            nullptr,
            worker_commands,
            "",
            []() {},
            0,
            [this]() { return current_time_ms_; }),
        last_worker_process_(),
        instrumented_io_service_(io_service),
        error_message_type_(1),
        client_call_manager_(instrumented_io_service_),
        mock_worker_rpc_clients_(mock_worker_rpc_clients) {
    SetNodeManagerPort(1);
  }

  ~WorkerPoolMock() {
    // Avoid killing real processes
    states_by_lang_.clear();
  }

  using WorkerPool::StartWorkerProcess;  // we need this to be public for testing

  using WorkerPool::PopWorkerCallbackInternal;

  // Mock `PopWorkerCallbackAsync` to synchronized function.
  void PopWorkerCallbackAsync(const TaskSpecification &task_spec,
                              const PopWorkerCallback &callback,
                              std::shared_ptr<WorkerInterface> worker,
                              PopWorkerStatus status = PopWorkerStatus::OK) override {
    PopWorkerCallbackInternal(task_spec, callback, worker, status);
  }

  Process StartProcess(const std::vector<std::string> &worker_command_args,
                       const ProcessEnvironment &env) override {
    // Use a bogus process ID that won't conflict with those in the system
    pid_t pid = static_cast<pid_t>(PID_MAX_LIMIT + 1 + worker_commands_by_proc_.size());
    last_worker_process_ = Process::FromPid(pid);
    worker_commands_by_proc_[last_worker_process_] = worker_command_args;
    startup_tokens_by_proc_[last_worker_process_] =
        WorkerPool::worker_startup_token_counter_;
    return last_worker_process_;
  }

  void WarnAboutSize() override {}

  Process LastStartedWorkerProcess() const { return last_worker_process_; }

  const std::vector<std::string> &GetWorkerCommand(Process proc) {
    return worker_commands_by_proc_[proc];
  }

  int NumWorkersStarting() const {
    int total = 0;
    for (auto &state_entry : states_by_lang_) {
      for (auto &process_entry : state_entry.second.worker_processes) {
        total += process_entry.second.is_pending_registration ? 1 : 0;
      }
    }
    return total;
  }

  int NumPendingPopWorkerRequests() const {
    int total = 0;
    for (auto &entry : states_by_lang_) {
      total += entry.second.pending_pop_worker_requests.size();
    }
    return total;
  }

  int NumSpillWorkerStarting() const {
    auto state = states_by_lang_.find(Language::PYTHON)->second;
    return state.spill_io_worker_state.num_starting_io_workers;
  }

  int NumSpillWorkerStarted() const {
    auto state = states_by_lang_.find(Language::PYTHON)->second;
    return state.spill_io_worker_state.started_io_workers.size();
  }

  int NumRestoreWorkerStarting() const {
    auto state = states_by_lang_.find(Language::PYTHON)->second;
    return state.restore_io_worker_state.num_starting_io_workers;
  }

  StartupToken GetStartupToken(const Process &proc) {
    return startup_tokens_by_proc_[proc];
  }

  int GetProcessSize() const { return worker_commands_by_proc_.size(); }

  const absl::flat_hash_map<Process, std::vector<std::string>> &GetProcesses() {
    return worker_commands_by_proc_;
  }

  void ClearProcesses() { worker_commands_by_proc_.clear(); }

  void SetCurrentTimeMs(double current_time) { current_time_ms_ = current_time; }

  size_t GetIdleWorkerSize() { return idle_of_all_languages_.size(); }

  std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>> &GetIdleWorkers() {
    return idle_of_all_languages_;
  }

  std::shared_ptr<WorkerInterface> CreateWorker(
      const Process &proc,
      const Language &language = Language::PYTHON,
      const JobID &job_id = JOB_ID,
      const rpc::WorkerType worker_type = rpc::WorkerType::WORKER,
      int runtime_env_hash = 0,
      StartupToken worker_startup_token = 0,
      bool set_process = true) {
    std::function<void(ClientConnection &)> client_handler =
        [this](ClientConnection &client) { HandleNewClient(client); };
    std::function<void(
        std::shared_ptr<ClientConnection>, int64_t, const std::vector<uint8_t> &)>
        message_handler = [this](std::shared_ptr<ClientConnection> client,
                                 int64_t message_type,
                                 const std::vector<uint8_t> &message) {
          HandleMessage(client, message_type, message);
        };
    local_stream_socket socket(instrumented_io_service_);
    auto client = ClientConnection::Create(client_handler,
                                           message_handler,
                                           std::move(socket),
                                           "worker",
                                           {},
                                           error_message_type_);
    std::shared_ptr<Worker> worker_ = std::make_shared<Worker>(job_id,
                                                               runtime_env_hash,
                                                               WorkerID::FromRandom(),
                                                               language,
                                                               worker_type,
                                                               "127.0.0.1",
                                                               client,
                                                               client_call_manager_,
                                                               worker_startup_token);
    std::shared_ptr<WorkerInterface> worker =
        std::dynamic_pointer_cast<WorkerInterface>(worker_);
    auto rpc_client = std::make_shared<MockWorkerClient>();
    worker->Connect(rpc_client);
    mock_worker_rpc_clients_.emplace(worker->WorkerId(), rpc_client);
    if (set_process && !proc.IsNull()) {
      worker->SetProcess(proc);
    }
    return worker;
  }

  void PushAvailableWorker(const std::shared_ptr<WorkerInterface> &worker) {
    if (worker->GetWorkerType() == rpc::WorkerType::SPILL_WORKER) {
      PushSpillWorker(worker);
      return;
    }
    if (worker->GetWorkerType() == rpc::WorkerType::RESTORE_WORKER) {
      PushRestoreWorker(worker);
      return;
    }
    PushWorker(worker);
  }

  // Create workers for processes and push them to worker pool.
  // \param[in] timeout_worker_number Don't register some workers to simulate worker
  // registration timeout.
  void PushWorkers(int timeout_worker_number = 0) {
    auto processes = GetProcesses();
    for (auto it = processes.begin(); it != processes.end(); ++it) {
      auto pushed_it = pushedProcesses_.find(it->first);
      if (pushed_it == pushedProcesses_.end()) {
        int runtime_env_hash = 0;
        bool is_java = false;
        // Parses runtime env hash to make sure the pushed workers can be popped out.
        for (const std::string &command_args : it->second) {
          std::string runtime_env_key = "--runtime-env-hash=";
          auto pos = command_args.find(runtime_env_key);
          if (pos != std::string::npos) {
            runtime_env_hash =
                std::stoi(command_args.substr(pos + runtime_env_key.size()));
          }
          pos = command_args.find("java");
          if (pos != std::string::npos) {
            is_java = true;
          }
        }
        // TODO(SongGuyang): support C++ language workers.
        int num_workers = 1;
        RAY_CHECK(timeout_worker_number <= num_workers)
            << "The timeout worker number cannot exceed the total number of workers";
        auto register_workers = num_workers - timeout_worker_number;
        for (int i = 0; i < register_workers; i++) {
          auto worker = CreateWorker(
              it->first,
              is_java ? Language::JAVA : Language::PYTHON,
              JOB_ID,
              rpc::WorkerType::WORKER,
              runtime_env_hash,
              startup_tokens_by_proc_[it->first],
              // Don't set process to ensure the `RegisterWorker` succeeds below.
              false);
          RAY_CHECK_OK(RegisterWorker(worker,
                                      it->first.GetId(),
                                      startup_tokens_by_proc_[it->first],
                                      [](Status, int) {}));
          OnWorkerStarted(worker);
          PushAvailableWorker(worker);
        }
        pushedProcesses_[it->first] = it->second;
      }
    }
  }

  // We have mocked worker starting and runtime env creation to make the execution of pop
  // worker synchronously.
  // \param[in] push_workers If true, tries to push the workers from the started
  // processes.
  std::shared_ptr<WorkerInterface> PopWorkerSync(
      const TaskSpecification &task_spec,
      bool push_workers = true,
      PopWorkerStatus *worker_status = nullptr,
      int timeout_worker_number = 0,
      std::string *runtime_env_error_msg = nullptr) {
    std::shared_ptr<WorkerInterface> popped_worker = nullptr;
    std::promise<bool> promise;
    this->PopWorker(task_spec,
                    [&popped_worker, worker_status, &promise, runtime_env_error_msg](
                        const std::shared_ptr<WorkerInterface> worker,
                        PopWorkerStatus status,
                        const std::string &runtime_env_setup_error_message) -> bool {
                      popped_worker = worker;
                      if (worker_status != nullptr) {
                        *worker_status = status;
                      }
                      if (runtime_env_error_msg) {
                        *runtime_env_error_msg = runtime_env_setup_error_message;
                      }
                      promise.set_value(true);
                      return true;
                    });
    if (push_workers) {
      PushWorkers(timeout_worker_number);
    }
    promise.get_future().get();
    return popped_worker;
  }

  int num_available_cpus_ = POOL_SIZE_SOFT_LIMIT;

 private:
  Process last_worker_process_;
  // The worker commands by process.
  absl::flat_hash_map<Process, std::vector<std::string>> worker_commands_by_proc_;
  absl::flat_hash_map<Process, StartupToken> startup_tokens_by_proc_;
  double current_time_ms_ = 0;
  absl::flat_hash_map<Process, std::vector<std::string>> pushedProcesses_;
  instrumented_io_context &instrumented_io_service_;
  int64_t error_message_type_;
  rpc::ClientCallManager client_call_manager_;
  absl::flat_hash_map<WorkerID, std::shared_ptr<MockWorkerClient>>
      &mock_worker_rpc_clients_;
  void HandleNewClient(ClientConnection &){};
  void HandleMessage(std::shared_ptr<ClientConnection>,
                     int64_t,
                     const std::vector<uint8_t> &){};
};

class WorkerPoolTest : public ::testing::Test {
 public:
  void SetUp() override {
    RayConfig::instance().initialize(
        R"({"worker_register_timeout_seconds": )" +
        std::to_string(WORKER_REGISTER_TIMEOUT_SECONDS) +
        R"(, "object_spilling_config": "dummy", "max_io_workers": )" +
        std::to_string(MAX_IO_WORKER_SIZE) + R"(, "kill_idle_workers_interval_ms": 0)" +
        R"(, "enable_worker_prestart": true)" + "}");
    SetWorkerCommands({{Language::PYTHON, {"dummy_py_worker_command"}},
                       {Language::JAVA,
                        {"java", "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER", "MainClass"}}});
    std::promise<bool> promise;
    thread_io_service_.reset(new std::thread([this, &promise] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      promise.set_value(true);
      io_service_.run();
    }));
    promise.get_future().get();
    worker_pool_->SetRuntimeEnvAgentClient(std::make_shared<MockRuntimeEnvAgentClient>());
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
    AssertNoLeaks();
    runtime_env_reference.clear();
    worker_pool_->all_jobs_.clear();
  }

  void AssertNoLeaks() { ASSERT_EQ(worker_pool_->pending_exit_idle_workers_.size(), 0); }

  std::shared_ptr<WorkerInterface> CreateSpillWorker(Process proc) {
    return worker_pool_->CreateWorker(
        proc, Language::PYTHON, JobID::Nil(), rpc::WorkerType::SPILL_WORKER);
  }

  std::shared_ptr<WorkerInterface> CreateRestoreWorker(Process proc) {
    return worker_pool_->CreateWorker(
        proc, Language::PYTHON, JobID::Nil(), rpc::WorkerType::RESTORE_WORKER);
  }

  std::shared_ptr<WorkerInterface> RegisterDriver(
      const Language &language = Language::PYTHON,
      const JobID &job_id = JOB_ID,
      const rpc::JobConfig &job_config = rpc::JobConfig()) {
    auto driver =
        worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, job_id);
    driver->AssignTaskId(TaskID::ForDriverTask(job_id));
    RAY_CHECK_OK(worker_pool_->RegisterDriver(driver, job_config, [](Status, int) {}));
    return driver;
  }

  void SetWorkerCommands(const WorkerCommandMap &worker_commands) {
    worker_pool_ = std::make_unique<WorkerPoolMock>(
        io_service_, worker_commands, mock_worker_rpc_clients_);
  }

  void TestStartupWorkerProcessCount(Language language, int num_workers_per_process) {
    int desired_initial_worker_process_count = 100;
    int expected_worker_process_count = static_cast<int>(std::ceil(
        static_cast<double>(MAXIMUM_STARTUP_CONCURRENCY) / num_workers_per_process));
    ASSERT_TRUE(expected_worker_process_count <
                static_cast<int>(desired_initial_worker_process_count));
    Process last_started_worker_process;
    for (int i = 0; i < desired_initial_worker_process_count; i++) {
      PopWorkerStatus status;
      worker_pool_->StartWorkerProcess(
          language, rpc::WorkerType::WORKER, JOB_ID, &status);
      ASSERT_TRUE(worker_pool_->NumWorkersStarting() <= expected_worker_process_count);
      Process prev = worker_pool_->LastStartedWorkerProcess();
      if (std::equal_to<Process>()(last_started_worker_process, prev)) {
        ASSERT_EQ(worker_pool_->NumWorkersStarting(), expected_worker_process_count);
        ASSERT_TRUE(i >= expected_worker_process_count);
      }
    }
    // Check number of starting workers
    ASSERT_EQ(worker_pool_->NumWorkersStarting(), expected_worker_process_count);
  }

  absl::flat_hash_map<WorkerID, std::shared_ptr<MockWorkerClient>>
      mock_worker_rpc_clients_;

 protected:
  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<WorkerPoolMock> worker_pool_;
};

class WorkerPoolDriverRegisteredTest : public WorkerPoolTest {
 public:
  void SetUp() override {
    WorkerPoolTest::SetUp();
    rpc::JobConfig job_config;
    RegisterDriver(Language::PYTHON, JOB_ID, job_config);
  }
};

static inline rpc::RuntimeEnvInfo ExampleRuntimeEnvInfo(
    const std::vector<std::string> uris, bool eager_install = false) {
  json runtime_env;
  runtime_env["py_modules"] = uris;
  rpc::RuntimeEnvInfo runtime_env_info;
  runtime_env_info.set_serialized_runtime_env(runtime_env.dump());
  for (auto &uri : uris) {
    runtime_env_info.mutable_uris()->add_py_modules_uris(std::string(uri));
  }
  runtime_env_info.mutable_runtime_env_config()->set_eager_install(eager_install);
  return runtime_env_info;
}

static inline rpc::RuntimeEnvInfo ExampleRuntimeEnvInfoFromString(
    std::string serialized_runtime_env) {
  rpc::RuntimeEnvInfo runtime_env_info;
  runtime_env_info.set_serialized_runtime_env(serialized_runtime_env);
  return runtime_env_info;
}

static inline TaskSpecification ExampleTaskSpec(
    const ActorID actor_id = ActorID::Nil(),
    const Language &language = Language::PYTHON,
    const JobID &job_id = JOB_ID,
    const ActorID actor_creation_id = ActorID::Nil(),
    const std::vector<std::string> &dynamic_worker_options = {},
    const TaskID &task_id = TaskID::FromRandom(JobID::Nil()),
    const rpc::RuntimeEnvInfo runtime_env_info = rpc::RuntimeEnvInfo(),
    std::unordered_map<std::string, double> resources = {{"CPU", 1}}) {
  rpc::TaskSpec message;
  message.set_job_id(job_id.Binary());
  message.set_language(language);
  // Make sure no reduplicative task id.
  RAY_CHECK(!task_id.IsNil());
  message.set_task_id(task_id.Binary());
  if (!actor_id.IsNil()) {
    message.set_type(TaskType::ACTOR_TASK);
    message.mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  } else if (!actor_creation_id.IsNil()) {
    message.set_type(TaskType::ACTOR_CREATION_TASK);
    message.mutable_actor_creation_task_spec()->set_actor_id(actor_creation_id.Binary());
    for (const auto &option : dynamic_worker_options) {
      message.mutable_actor_creation_task_spec()->add_dynamic_worker_options(option);
    }
  } else {
    message.set_type(TaskType::NORMAL_TASK);
  }
  message.mutable_required_resources()->insert(resources.begin(), resources.end());

  message.mutable_runtime_env_info()->CopyFrom(runtime_env_info);
  return TaskSpecification(std::move(message));
}

TEST_F(WorkerPoolDriverRegisteredTest, CompareWorkerProcessObjects) {
  typedef Process T;
  T a(T::CreateNewDummy()), b(T::CreateNewDummy()), empty = T();
  ASSERT_TRUE(empty.IsNull());
  ASSERT_TRUE(!empty.IsValid());
  ASSERT_TRUE(!a.IsNull());
  ASSERT_TRUE(!a.IsValid());  // a dummy process is not a valid process!
  ASSERT_TRUE(std::equal_to<T>()(a, a));
  ASSERT_TRUE(!std::equal_to<T>()(a, b));
  ASSERT_TRUE(!std::equal_to<T>()(b, a));
  ASSERT_TRUE(!std::equal_to<T>()(empty, a));
  ASSERT_TRUE(!std::equal_to<T>()(a, empty));
}

TEST_F(WorkerPoolDriverRegisteredTest, HandleWorkerRegistration) {
  PopWorkerStatus status;
  auto [proc, token] = worker_pool_->StartWorkerProcess(
      Language::JAVA, rpc::WorkerType::WORKER, JOB_ID, &status);
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  workers.push_back(worker_pool_->CreateWorker(Process(), Language::JAVA));
  for (const auto &worker : workers) {
    // Check that there's still a starting worker process
    // before all workers have been registered
    ASSERT_EQ(worker_pool_->NumWorkersStarting(), 1);
    // Check that we cannot lookup the worker before it's registered.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), nullptr);
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    // Check that we can lookup the worker after it's registered.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
  }
  // Check that there's no starting worker process
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 0);
  for (const auto &worker : workers) {
    worker_pool_->DisconnectWorker(
        worker, /*disconnect_type=*/rpc::WorkerExitType::INTENDED_USER_EXIT);
    // Check that we cannot lookup the worker after it's disconnected.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), nullptr);
  }

  {
    // Test the case where DisconnectClient happens after RegisterClientRequest but before
    // AnnounceWorkerPort.
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::WORKER, JOB_ID, &status);
    auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON);
    ASSERT_EQ(worker_pool_->NumWorkersStarting(), 1);
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
    worker->SetStartupToken(worker_pool_->GetStartupToken(proc));
    worker_pool_->DisconnectWorker(
        worker, /*disconnect_type=*/rpc::WorkerExitType::INTENDED_USER_EXIT);
    ASSERT_EQ(worker_pool_->NumWorkersStarting(), 0);
  }
}

TEST_F(WorkerPoolDriverRegisteredTest, HandleUnknownWorkerRegistration) {
  auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON);
  auto status = worker_pool_->RegisterWorker(
      worker, 1234, -1, [](const Status & /*unused*/, int /*unused*/) {});
  ASSERT_FALSE(status.ok());
}

TEST_F(WorkerPoolDriverRegisteredTest, StartupPythonWorkerProcessCount) {
  TestStartupWorkerProcessCount(Language::PYTHON, 1);
}

TEST_F(WorkerPoolDriverRegisteredTest, StartupJavaWorkerProcessCount) {
  TestStartupWorkerProcessCount(Language::JAVA, 1);
}

TEST_F(WorkerPoolDriverRegisteredTest, InitialWorkerProcessCount) {
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 0);
}

TEST_F(WorkerPoolDriverRegisteredTest, TestPrestartingWorkers) {
  const auto task_spec = ExampleTaskSpec();
  // Prestarts 2 workers.
  worker_pool_->PrestartWorkers(task_spec, 2);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 2);
  // Prestarts 1 more worker.
  worker_pool_->PrestartWorkers(task_spec, 3);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 3);
  // No more needed.
  worker_pool_->PrestartWorkers(task_spec, 1);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 3);
  // Capped by soft limit.
  worker_pool_->PrestartWorkers(task_spec, 20);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), POOL_SIZE_SOFT_LIMIT);
}

TEST_F(WorkerPoolDriverRegisteredTest, HandleWorkerPushPop) {
  std::shared_ptr<WorkerInterface> popped_worker;
  const auto task_spec = ExampleTaskSpec();
  // Create some workers.
  std::unordered_set<std::shared_ptr<WorkerInterface>> workers;
  workers.insert(worker_pool_->CreateWorker(Process::CreateNewDummy()));
  workers.insert(worker_pool_->CreateWorker(Process::CreateNewDummy()));
  // Add the workers to the pool.
  for (auto &worker : workers) {
    worker_pool_->PushWorker(worker);
  }
  // Pop two workers and make sure they're one of the workers we created.
  popped_worker = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  // Pop a worker from the empty pool and make sure it isn't one of the workers we
  // created.
  popped_worker = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) == 0);
}

TEST_F(WorkerPoolDriverRegisteredTest, PopWorkerSyncsOfMultipleLanguages) {
  // Create a Python Worker, and add it to the pool
  auto py_worker =
      worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON);
  worker_pool_->PushWorker(py_worker);
  // Check that the Python worker will not be popped if the given task is a Java task
  const auto java_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::JAVA);
  ASSERT_NE(worker_pool_->PopWorkerSync(java_task_spec), py_worker);
  // Check that the Python worker can be popped if the given task is a Python task
  const auto py_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON);
  ASSERT_EQ(worker_pool_->PopWorkerSync(py_task_spec), py_worker);

  // Create a Java Worker, and add it to the pool
  auto java_worker =
      worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::JAVA);
  worker_pool_->PushWorker(java_worker);
  // Check that the Java worker will be popped now for Java task
  ASSERT_EQ(worker_pool_->PopWorkerSync(java_task_spec), java_worker);
}

TEST_F(WorkerPoolDriverRegisteredTest, StartWorkerWithNodeIdArg) {
  auto task_id = TaskID::FromRandom(JOB_ID);
  TaskSpecification task_spec = ExampleTaskSpec(
      ActorID::Nil(), Language::PYTHON, JOB_ID, ActorID::Nil(), {}, task_id);
  ASSERT_NE(worker_pool_->PopWorkerSync(task_spec), nullptr);
  const auto real_command =
      worker_pool_->GetWorkerCommand(worker_pool_->LastStartedWorkerProcess());

  std::ostringstream stringStream;
  stringStream << "--node-id=" << worker_pool_->GetNodeID();
  std::string expected_node_id_arg = stringStream.str();

  bool node_id_arg_found = false;
  for (const auto &arg : real_command) {
    if (arg.find(expected_node_id_arg) != std::string::npos) {
      node_id_arg_found = true;
      break;
    }
  }
  ASSERT_TRUE(node_id_arg_found);
}

TEST_F(WorkerPoolDriverRegisteredTest, StartWorkerWithDynamicOptionsCommand) {
  std::vector<std::string> actor_jvm_options;
  actor_jvm_options.insert(
      actor_jvm_options.end(),
      {"-Dmy-actor.hello=foo", "-Dmy-actor.world=bar", "-Xmx2g", "-Xms1g"});
  JobID job_id = JobID::FromInt(12345);
  auto task_id = TaskID::ForDriverTask(job_id);
  auto actor_id = ActorID::Of(job_id, task_id, 1);
  TaskSpecification task_spec = ExampleTaskSpec(
      ActorID::Nil(), Language::JAVA, job_id, actor_id, actor_jvm_options, task_id);

  rpc::JobConfig job_config = rpc::JobConfig();
  job_config.add_code_search_path("/test/code_search_path");
  job_config.add_jvm_options("-Xmx1g");
  job_config.add_jvm_options("-Xms500m");
  job_config.add_jvm_options("-Dmy-job.hello=world");
  job_config.add_jvm_options("-Dmy-job.foo=bar");
  worker_pool_->HandleJobStarted(job_id, job_config);

  ASSERT_NE(worker_pool_->PopWorkerSync(task_spec), nullptr);
  const auto real_command =
      worker_pool_->GetWorkerCommand(worker_pool_->LastStartedWorkerProcess());

  // NOTE: When adding a new parameter to Java worker command, think carefully about the
  // position of this new parameter. Do not modify the order of existing parameters.
  std::vector<std::string> expected_command;
  expected_command.push_back("java");
  // Ray-defined per-job options
  expected_command.insert(expected_command.end(),
                          {"-Dray.job.code-search-path=/test/code_search_path"});
  // User-defined per-job options
  expected_command.insert(
      expected_command.end(),
      {"-Xmx1g", "-Xms500m", "-Dmy-job.hello=world", "-Dmy-job.foo=bar"});
  // Ray-defined per-process options
  expected_command.push_back("-Dray.raylet.startup-token=0");
  expected_command.push_back("-Dray.internal.runtime-env-hash=0");
  // User-defined per-process options
  expected_command.insert(
      expected_command.end(), actor_jvm_options.begin(), actor_jvm_options.end());
  // Entry point
  expected_command.push_back("MainClass");
  expected_command.push_back("--language=JAVA");
  ASSERT_EQ(real_command, expected_command);
  worker_pool_->HandleJobFinished(job_id);
}

TEST_F(WorkerPoolDriverRegisteredTest, PopWorkerMultiTenancy) {
  auto job_id1 = JOB_ID;
  auto job_id2 = JobID::FromInt(2);
  ASSERT_NE(job_id1, job_id2);
  JobID job_ids[] = {job_id1, job_id2};

  // The driver of job 1 is already registered. Here we register the driver for job 2.
  RegisterDriver(Language::PYTHON, job_id2);

  // Register 2 workers for each job.
  for (auto job_id : job_ids) {
    for (int i = 0; i < 2; i++) {
      int runtime_env_hash = 0;
      // Make the first worker an actor worker.
      if (i == 0) {
        auto actor_creation_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1);
        auto task_spec = ExampleTaskSpec(
            /*actor_id=*/ActorID::Nil(), Language::PYTHON, job_id, actor_creation_id);
        runtime_env_hash = task_spec.GetRuntimeEnvHash();
      }
      auto worker = worker_pool_->CreateWorker(Process::CreateNewDummy(),
                                               Language::PYTHON,
                                               job_id,
                                               rpc::WorkerType::WORKER,
                                               runtime_env_hash);
      worker_pool_->PushWorker(worker);
    }
  }
  std::unordered_set<WorkerID> worker_ids;
  for (int round = 0; round < 2; round++) {
    std::vector<std::shared_ptr<WorkerInterface>> workers;

    // Pop workers for actor.
    for (auto job_id : job_ids) {
      auto actor_creation_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1);
      // Pop workers for actor creation tasks.
      auto task_spec = ExampleTaskSpec(
          /*actor_id=*/ActorID::Nil(), Language::PYTHON, job_id, actor_creation_id);
      auto worker = worker_pool_->PopWorkerSync(task_spec);
      ASSERT_TRUE(worker);
      ASSERT_EQ(worker->GetAssignedJobId(), job_id);
      workers.push_back(worker);
    }

    // Pop workers for normal tasks.
    for (auto job_id : job_ids) {
      auto task_spec = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, job_id);
      auto worker = worker_pool_->PopWorkerSync(task_spec);
      ASSERT_TRUE(worker);
      ASSERT_EQ(worker->GetAssignedJobId(), job_id);
      workers.push_back(worker);
    }

    // Return all workers.
    for (auto worker : workers) {
      worker_pool_->PushWorker(worker);
      if (round == 0) {
        // For the first round, all workers are new.
        ASSERT_TRUE(worker_ids.insert(worker->WorkerId()).second);
      } else {
        // For the second round, all workers are existing ones.
        ASSERT_TRUE(worker_ids.count(worker->WorkerId()) > 0);
      }
    }
  }
}

TEST_F(WorkerPoolDriverRegisteredTest, MaximumStartupConcurrency) {
  auto task_spec = ExampleTaskSpec();
  std::vector<Process> started_processes;

  // Try to pop some workers. Some worker processes will be started.
  for (int i = 0; i < MAXIMUM_STARTUP_CONCURRENCY; i++) {
    worker_pool_->PopWorker(
        task_spec,
        [](const std::shared_ptr<WorkerInterface> worker,
           PopWorkerStatus status,
           const std::string &runtime_env_setup_error_message) -> bool { return true; });
    auto last_process = worker_pool_->LastStartedWorkerProcess();
    RAY_CHECK(last_process.IsValid());
    started_processes.push_back(last_process);
  }
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  ASSERT_EQ(0, worker_pool_->NumPendingPopWorkerRequests());

  // Can't start a new worker process at this point.
  worker_pool_->PopWorker(
      task_spec,
      [](const std::shared_ptr<WorkerInterface> worker,
         PopWorkerStatus status,
         const std::string &runtime_env_setup_error_message) -> bool { return true; });
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  ASSERT_EQ(1, worker_pool_->NumPendingPopWorkerRequests());

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  // Call `RegisterWorker` to emulate worker registration.
  for (const auto &process : started_processes) {
    auto worker = worker_pool_->CreateWorker(Process());
    worker->SetStartupToken(worker_pool_->GetStartupToken(process));
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, process.GetId(), worker_pool_->GetStartupToken(process), [](Status, int) {
        }));
    // Calling `RegisterWorker` won't affect the counter of starting worker processes.
    ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
    ASSERT_EQ(1, worker_pool_->NumPendingPopWorkerRequests());
    workers.push_back(worker);
  }

  // Can't start a new worker process at this point.
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  worker_pool_->PopWorker(
      task_spec,
      [](const std::shared_ptr<WorkerInterface> worker,
         PopWorkerStatus status,
         const std::string &runtime_env_setup_error_message) -> bool { return true; });
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  ASSERT_EQ(2, worker_pool_->NumPendingPopWorkerRequests());

  // Call `OnWorkerStarted` to emulate worker port announcement.
  worker_pool_->OnWorkerStarted(workers[0]);
  worker_pool_->PushWorker(workers[0]);
  // Calling `OnWorkerStarted` will affect the counter of starting worker processes.
  // One pending pop worker request now can be fulfilled.
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY + 1, worker_pool_->GetProcessSize());
  ASSERT_EQ(1, worker_pool_->NumPendingPopWorkerRequests());

  // Can't start a new worker process at this point.
  worker_pool_->PopWorker(
      task_spec,
      [](const std::shared_ptr<WorkerInterface> worker,
         PopWorkerStatus status,
         const std::string &runtime_env_setup_error_message) -> bool { return true; });
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY + 1, worker_pool_->GetProcessSize());
  ASSERT_EQ(2, worker_pool_->NumPendingPopWorkerRequests());

  // Return a worker.
  worker_pool_->PushWorker(workers[0]);
  // One more pending pop worker request can be fulfilled.
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY + 1, worker_pool_->GetProcessSize());
  ASSERT_EQ(1, worker_pool_->NumPendingPopWorkerRequests());
  ASSERT_EQ(0, worker_pool_->GetIdleWorkerSize());

  // Disconnect a worker.
  worker_pool_->DisconnectWorker(workers[1], rpc::WorkerExitType::SYSTEM_ERROR);
  // One more pending pop worker request can be fulfilled.
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkersStarting());
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY + 2, worker_pool_->GetProcessSize());
  ASSERT_EQ(0, worker_pool_->NumPendingPopWorkerRequests());
  ASSERT_EQ(0, worker_pool_->GetIdleWorkerSize());

  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolDriverRegisteredTest, HandleIOWorkersPushPop) {
  std::unordered_set<std::shared_ptr<WorkerInterface>> spill_pushed_worker;
  std::unordered_set<std::shared_ptr<WorkerInterface>> restore_pushed_worker;
  auto spill_worker_callback =
      [&spill_pushed_worker](std::shared_ptr<WorkerInterface> worker) {
        spill_pushed_worker.emplace(worker);
      };
  auto restore_worker_callback =
      [&restore_pushed_worker](std::shared_ptr<WorkerInterface> worker) {
        restore_pushed_worker.emplace(worker);
      };

  // Popping spill worker shouldn't invoke callback because there's no workers pushed yet.
  worker_pool_->PopSpillWorker(spill_worker_callback);
  worker_pool_->PopSpillWorker(spill_worker_callback);
  worker_pool_->PopRestoreWorker(restore_worker_callback);
  ASSERT_EQ(spill_pushed_worker.size(), 0);
  ASSERT_EQ(restore_pushed_worker.size(), 0);

  // Create some workers.
  std::unordered_set<std::shared_ptr<WorkerInterface>> spill_workers;
  spill_workers.insert(CreateSpillWorker(Process()));
  spill_workers.insert(CreateSpillWorker(Process()));
  // Add the workers to the pool.
  // 2 pending tasks / 2 new idle workers.
  for (const auto &worker : spill_workers) {
    auto status = PopWorkerStatus::OK;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        rpc::Language::PYTHON, rpc::WorkerType::SPILL_WORKER, JobID::Nil(), &status);
    ASSERT_EQ(status, PopWorkerStatus::OK);
    RAY_CHECK_OK(
        worker_pool_->RegisterWorker(worker, proc.GetId(), token, [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushSpillWorker(worker);
  }
  ASSERT_EQ(spill_pushed_worker.size(), 2);
  // Restore workers haven't pushed yet.
  ASSERT_EQ(restore_pushed_worker.size(), 0);

  // Create a new idle worker.
  {
    auto worker = CreateSpillWorker(Process());
    spill_workers.insert(worker);
    auto status = PopWorkerStatus::OK;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        rpc::Language::PYTHON, rpc::WorkerType::SPILL_WORKER, JobID::Nil(), &status);
    ASSERT_EQ(status, PopWorkerStatus::OK);
    RAY_CHECK_OK(
        worker_pool_->RegisterWorker(worker, proc.GetId(), token, [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
  }
  // Now push back to used workers
  // 0 pending task, 3 idle workers.
  for (const auto &worker : spill_workers) {
    worker_pool_->PushSpillWorker(worker);
  }
  for (size_t i = 0; i < spill_workers.size(); i++) {
    worker_pool_->PopSpillWorker(spill_worker_callback);
  }
  ASSERT_EQ(spill_pushed_worker.size(), 3);

  // At the same time push an idle worker to the restore worker pool.
  std::unordered_set<std::shared_ptr<WorkerInterface>> restore_workers;
  restore_workers.insert(CreateRestoreWorker(Process()));
  for (const auto &worker : restore_workers) {
    auto status = PopWorkerStatus::OK;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        rpc::Language::PYTHON, rpc::WorkerType::RESTORE_WORKER, JobID::Nil(), &status);
    ASSERT_EQ(status, PopWorkerStatus::OK);
    RAY_CHECK_OK(
        worker_pool_->RegisterWorker(worker, proc.GetId(), token, [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushRestoreWorker(worker);
  }
  ASSERT_EQ(restore_pushed_worker.size(), 1);
}

TEST_F(WorkerPoolDriverRegisteredTest, MaxIOWorkerSimpleTest) {
  // Make sure max number of spill workers are respected.
  auto callback = [](std::shared_ptr<WorkerInterface> worker) {};
  std::vector<Process> started_processes;
  Process last_process;
  for (int i = 0; i < 10; i++) {
    worker_pool_->PopSpillWorker(callback);
    if (last_process.GetId() != worker_pool_->LastStartedWorkerProcess().GetId()) {
      last_process = worker_pool_->LastStartedWorkerProcess();
      started_processes.push_back(last_process);
    }
  }
  // Make sure process size is not exceeding max io worker size + worker prestarted.
  ASSERT_EQ(worker_pool_->GetProcessSize(), MAX_IO_WORKER_SIZE);
  ASSERT_EQ(started_processes.size(), MAX_IO_WORKER_SIZE);
  ASSERT_EQ(worker_pool_->NumSpillWorkerStarting(), MAX_IO_WORKER_SIZE);
  ASSERT_EQ(worker_pool_->NumRestoreWorkerStarting(), 0);

  // Make sure process size doesn't exceed the max size when some of workers are
  // registered.
  std::unordered_set<std::shared_ptr<WorkerInterface>> spill_workers;
  for (auto &process : started_processes) {
    auto worker = CreateSpillWorker(process);
    spill_workers.insert(worker);
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushSpillWorker(worker);
  }
  ASSERT_EQ(worker_pool_->NumSpillWorkerStarting(), 0);
}

TEST_F(WorkerPoolDriverRegisteredTest, MaxIOWorkerComplicateTest) {
  // Make sure max number of restore workers are respected.
  // This test will test a little more complicated scneario.
  // For example, it tests scenarios where there are
  // mix of starting / registered workers.
  auto callback = [](std::shared_ptr<WorkerInterface> worker) {};
  std::vector<Process> started_processes;
  Process last_process;
  worker_pool_->PopSpillWorker(callback);
  if (last_process.GetId() != worker_pool_->LastStartedWorkerProcess().GetId()) {
    last_process = worker_pool_->LastStartedWorkerProcess();
    started_processes.push_back(last_process);
  }
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  ASSERT_EQ(started_processes.size(), 1);
  ASSERT_EQ(worker_pool_->NumSpillWorkerStarting(), 1);

  // Worker is started and registered.
  std::unordered_set<std::shared_ptr<WorkerInterface>> spill_workers;
  for (auto &process : started_processes) {
    auto worker = CreateSpillWorker(process);
    spill_workers.insert(worker);
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushSpillWorker(worker);
    started_processes.pop_back();
  }

  // Try pop multiple workers and make sure it doesn't exceed max_io_workers.
  for (int i = 0; i < 10; i++) {
    worker_pool_->PopSpillWorker(callback);
    if (last_process.GetId() != worker_pool_->LastStartedWorkerProcess().GetId()) {
      last_process = worker_pool_->LastStartedWorkerProcess();
      started_processes.push_back(last_process);
    }
  }
  ASSERT_EQ(worker_pool_->GetProcessSize(), MAX_IO_WORKER_SIZE);
  ASSERT_EQ(started_processes.size(), 1);
  ASSERT_EQ(worker_pool_->NumSpillWorkerStarting(), 1);

  // Register the worker.
  for (auto &process : started_processes) {
    auto worker = CreateSpillWorker(process);
    spill_workers.insert(worker);
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushSpillWorker(worker);
    started_processes.pop_back();
  }
  ASSERT_EQ(worker_pool_->GetProcessSize(), MAX_IO_WORKER_SIZE);
  ASSERT_EQ(started_processes.size(), 0);
  ASSERT_EQ(worker_pool_->NumSpillWorkerStarting(), 0);
}

TEST_F(WorkerPoolDriverRegisteredTest, MaxSpillRestoreWorkersIntegrationTest) {
  auto callback = [](std::shared_ptr<WorkerInterface> worker) {};
  // Run many pop spill/restore workers and make sure the max worker size doesn't exceed.
  std::vector<Process> started_restore_processes;
  Process last_restore_process;
  std::vector<Process> started_spill_processes;
  Process last_spill_process;
  // NOTE: Should be a multiplication of MAX_IO_WORKER_SIZE.
  int max_time = 30;
  for (int i = 0; i <= max_time; i++) {
    // Pop spill worker
    worker_pool_->PopSpillWorker(callback);
    if (last_spill_process.GetId() != worker_pool_->LastStartedWorkerProcess().GetId()) {
      last_spill_process = worker_pool_->LastStartedWorkerProcess();
      started_spill_processes.push_back(last_spill_process);
    }
    // Pop Restore Worker
    worker_pool_->PopRestoreWorker(callback);
    if (last_restore_process.GetId() !=
        worker_pool_->LastStartedWorkerProcess().GetId()) {
      last_restore_process = worker_pool_->LastStartedWorkerProcess();
      started_restore_processes.push_back(last_restore_process);
    }
    // Register workers with 10% probability at each time.
    if (rand() % 100 < 10) {
      // Push spill worker if there's a process.
      if (started_spill_processes.size() > 0) {
        auto spill_worker = CreateSpillWorker(
            started_spill_processes[started_spill_processes.size() - 1]);
        worker_pool_->OnWorkerStarted(spill_worker);
        worker_pool_->PushSpillWorker(spill_worker);
        started_spill_processes.pop_back();
      }
      // Push restore worker if there's a process.
      if (started_restore_processes.size() > 0) {
        auto restore_worker = CreateRestoreWorker(
            started_restore_processes[started_restore_processes.size() - 1]);
        worker_pool_->OnWorkerStarted(restore_worker);
        worker_pool_->PushRestoreWorker(restore_worker);
        started_restore_processes.pop_back();
      }
    }
  }

  ASSERT_EQ(worker_pool_->GetProcessSize(), 2 * MAX_IO_WORKER_SIZE);
}

TEST_F(WorkerPoolDriverRegisteredTest, DeleteWorkerPushPop) {
  /// Make sure delete workers always pop an I/O worker that has more idle worker in their
  /// pools.
  // 2 spill worker and 1 restore worker.
  std::unordered_set<std::shared_ptr<WorkerInterface>> spill_workers;
  spill_workers.insert(CreateSpillWorker(Process::CreateNewDummy()));
  spill_workers.insert(CreateSpillWorker(Process::CreateNewDummy()));

  std::unordered_set<std::shared_ptr<WorkerInterface>> restore_workers;
  restore_workers.insert(CreateRestoreWorker(Process::CreateNewDummy()));

  for (const auto &worker : spill_workers) {
    worker_pool_->PushSpillWorker(worker);
  }
  for (const auto &worker : restore_workers) {
    worker_pool_->PushRestoreWorker(worker);
  }

  // PopDeleteWorker should pop a spill worker in this case.
  worker_pool_->PopDeleteWorker([this](std::shared_ptr<WorkerInterface> worker) {
    ASSERT_EQ(worker->GetWorkerType(), rpc::WorkerType::SPILL_WORKER);
    worker_pool_->PushDeleteWorker(worker);
  });

  // Add 2 more restore workers. Now we have 2 spill workers and 3 restore workers.
  for (int i = 0; i < 2; i++) {
    auto restore_worker = CreateRestoreWorker(Process::CreateNewDummy());
    restore_workers.insert(restore_worker);
    worker_pool_->PushRestoreWorker(restore_worker);
  }

  // PopDeleteWorker should pop a spill worker in this case.
  worker_pool_->PopDeleteWorker([this](std::shared_ptr<WorkerInterface> worker) {
    ASSERT_EQ(worker->GetWorkerType(), rpc::WorkerType::RESTORE_WORKER);
    worker_pool_->PushDeleteWorker(worker);
  });
}

TEST_F(WorkerPoolDriverRegisteredTest, TestWorkerCapping) {
  auto job_id = JOB_ID;

  // The driver of job 1 is already registered. Here we register the driver for job 2.
  RegisterDriver(Language::PYTHON, job_id);

  ///
  /// Register 4 workers (2 more than soft limit).
  ///
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  int num_workers = POOL_SIZE_SOFT_LIMIT + 2;
  for (int i = 0; i < num_workers; i++) {
    PopWorkerStatus status;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::WORKER, job_id, &status);
    auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON, job_id);
    worker->SetStartupToken(worker_pool_->GetStartupToken(proc));
    workers.push_back(worker);
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
    worker_pool_->PushWorker(worker);
  }
  ///
  /// Pop all workers to reset their order.
  ///
  std::vector<std::shared_ptr<WorkerInterface>> popped_workers;
  for (int i = 0; i < num_workers; i++) {
    // Pop workers for actor creation tasks.
    auto task_spec =
        ExampleTaskSpec(/*actor_id=*/ActorID::Nil(), Language::PYTHON, job_id);
    auto worker = worker_pool_->PopWorkerSync(task_spec, false);
    popped_workers.push_back(worker);
    ASSERT_TRUE(worker);
    ASSERT_EQ(worker->GetAssignedJobId(), job_id);
  }
  // After scheduling an actor and task, there's no more idle worker.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);

  ///
  /// Return workers and test KillingIdleWorkers
  ///
  // Return all workers.
  for (const auto &worker : popped_workers) {
    worker_pool_->PushWorker(worker);
  }
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers);
  // It is supposed to be no-op here.
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers);

  // 2000 ms has passed, so idle workers should be killed.
  worker_pool_->SetCurrentTimeMs(2000);
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT);

  // The first core worker exits, so one of idle workers should've been killed.
  // Since the idle workers are killed in FIFO, we can assume the first entry in the idle
  // workers will be killed.
  auto mock_rpc_client_it = mock_worker_rpc_clients_.find(popped_workers[0]->WorkerId());
  ASSERT_EQ(mock_rpc_client_it->second->exit_count, 1);
  ASSERT_EQ(mock_rpc_client_it->second->last_exit_forced, false);
  mock_rpc_client_it->second->ExitReplySucceed();
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT);

  // The second core worker doesn't exit, meaning idle worker shouldn't have been killed.
  mock_rpc_client_it = mock_worker_rpc_clients_.find(popped_workers[1]->WorkerId());
  ASSERT_EQ(mock_rpc_client_it->second->exit_count, 1);
  ASSERT_EQ(mock_rpc_client_it->second->last_exit_forced, false);
  mock_rpc_client_it->second->ExitReplyFailed();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT + 1);
  // Try killing the idle workers again.
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT);

  // We retry the exit request at the next worker in the queue.
  // This tests that if a worker can't be killed (e.g., because it owns
  // objects), we will still try to cap the workers by killing other workers
  // that may have been idle for less time.
  mock_rpc_client_it = mock_worker_rpc_clients_.find(popped_workers[2]->WorkerId());
  mock_rpc_client_it->second->ExitReplySucceed();

  // Now that we have the number of workers == soft limit, it shouldn't kill any idle
  // worker.
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT);
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT);

  // Try decreasing and increasing the soft limit.
  worker_pool_->num_available_cpus_ = 2;
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), worker_pool_->num_available_cpus_);
  mock_rpc_client_it = mock_worker_rpc_clients_.find(popped_workers[3]->WorkerId());
  mock_rpc_client_it->second->ExitReplyFailed();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT);
  worker_pool_->num_available_cpus_ = POOL_SIZE_SOFT_LIMIT;

  // Start two IO workers. These don't count towards the limit.
  {
    PopWorkerStatus status;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::SPILL_WORKER, job_id, &status);
    auto worker = CreateSpillWorker(Process());
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
    worker_pool_->PushSpillWorker(worker);
  }
  {
    PopWorkerStatus status;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::RESTORE_WORKER, job_id, &status);
    auto worker = CreateRestoreWorker(Process());
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
    worker_pool_->PushRestoreWorker(worker);
  }
  // All workers still alive.
  worker_pool_->SetCurrentTimeMs(10000);
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), POOL_SIZE_SOFT_LIMIT);
  for (auto &worker : worker_pool_->GetIdleWorkers()) {
    mock_rpc_client_it = mock_worker_rpc_clients_.find(worker.first->WorkerId());
    ASSERT_EQ(mock_rpc_client_it->second->last_exit_forced, false);
    ASSERT_FALSE(mock_rpc_client_it->second->ExitReplySucceed());
  }
  int num_callbacks = 0;
  auto callback = [&](std::shared_ptr<WorkerInterface> worker) { num_callbacks++; };
  worker_pool_->PopSpillWorker(callback);
  worker_pool_->PopRestoreWorker(callback);
  ASSERT_EQ(num_callbacks, 2);
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolDriverRegisteredTest, TestWorkerCappingWithExitDelay) {
  ///
  /// When there are multiple workers in a worker process, and the worker process's Exit
  /// reply is delayed, We shouldn't send more Exit requests to workers in this process
  /// until we received all Exit replies form this process.
  ///

  ///
  /// Register some idle Python and Java (w/ multi-worker enabled) workers
  ///
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::vector<Language> languages({Language::PYTHON, Language::JAVA});
  for (int i = 0; i < POOL_SIZE_SOFT_LIMIT * 2; i++) {
    for (const auto &language : languages) {
      PopWorkerStatus status;
      auto [proc, token] = worker_pool_->StartWorkerProcess(
          language, rpc::WorkerType::WORKER, JOB_ID, &status);
      int workers_to_start = 1;
      for (int j = 0; j < workers_to_start; j++) {
        auto worker = worker_pool_->CreateWorker(Process(), language);
        worker->SetStartupToken(worker_pool_->GetStartupToken(proc));
        workers.push_back(worker);
        RAY_CHECK_OK(worker_pool_->RegisterWorker(
            worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {
            }));
        worker_pool_->OnWorkerStarted(worker);
        ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
        worker_pool_->PushWorker(worker);
      }
    }
  }
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), workers.size());

  // 1000 ms has passed, so idle workers should be killed.
  worker_pool_->SetCurrentTimeMs(1000);
  worker_pool_->TryKillingIdleWorkers();

  // Let's assume that all workers own objects, so they won't be killed.

  // Due to the heavy load on this machine, some workers may reply Exit with a delay, so
  // only a part of workers replied before the next round of killing.
  std::vector<std::shared_ptr<WorkerInterface>> delayed_workers;
  bool delay = false;
  for (auto worker : workers) {
    auto mock_rpc_client_it = mock_worker_rpc_clients_.find(worker->WorkerId());
    if (mock_rpc_client_it->second->callbacks_.size() == 0) {
      // This worker is not being killed. Skip it.
      continue;
    }
    if (!delay) {
      ASSERT_TRUE(mock_rpc_client_it->second->ExitReplyFailed());
    } else {
      delayed_workers.push_back(worker);
    }
    delay = !delay;
  }
  // No workers are killed because they own objects.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), workers.size());

  // The second round of killing starts.
  worker_pool_->SetCurrentTimeMs(2000);
  worker_pool_->TryKillingIdleWorkers();

  // Delayed workers reply first, then all workers reply the second time.
  for (auto worker : delayed_workers) {
    auto mock_rpc_client_it = mock_worker_rpc_clients_.find(worker->WorkerId());
    ASSERT_TRUE(mock_rpc_client_it->second->ExitReplyFailed());
  }

  for (auto worker : workers) {
    auto mock_rpc_client_it = mock_worker_rpc_clients_.find(worker->WorkerId());
    if (mock_rpc_client_it->second->callbacks_.size() == 0) {
      // This worker is not being killed. Skip it.
      continue;
    }
    ASSERT_TRUE(mock_rpc_client_it->second->ExitReplyFailed());
  }

  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), workers.size());
}

TEST_F(WorkerPoolDriverRegisteredTest, TestJobFinishedForPopWorker) {
  // Test to make sure that if job finishes,
  // PopWorker should fail with PopWorkerStatus::JobFinished

  auto job_id = JOB_ID;

  /// Add worker to the pool.
  PopWorkerStatus status;
  auto [proc, token] = worker_pool_->StartWorkerProcess(
      Language::PYTHON, rpc::WorkerType::WORKER, job_id, &status);
  auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON, job_id);
  worker->SetStartupToken(worker_pool_->GetStartupToken(proc));
  RAY_CHECK_OK(worker_pool_->RegisterWorker(
      worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
  worker_pool_->OnWorkerStarted(worker);
  worker_pool_->PushWorker(worker);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);

  auto mock_rpc_client_it = mock_worker_rpc_clients_.find(worker->WorkerId());
  auto mock_rpc_client = mock_rpc_client_it->second;

  // Finish the job.
  worker_pool_->HandleJobFinished(job_id);

  auto task_spec = ExampleTaskSpec(/*actor_id=*/ActorID::Nil(), Language::PYTHON, job_id);
  PopWorkerStatus pop_worker_status;
  // This PopWorker should fail since the job finished.
  worker = worker_pool_->PopWorkerSync(task_spec, false, &pop_worker_status);
  ASSERT_EQ(pop_worker_status, PopWorkerStatus::JobFinished);
  ASSERT_FALSE(worker);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);

  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(mock_rpc_client->exit_count, 1);
  ASSERT_EQ(mock_rpc_client->last_exit_forced, true);
  mock_rpc_client->ExitReplySucceed();

  job_id = JOB_ID2;
  rpc::JobConfig job_config;
  RegisterDriver(Language::PYTHON, job_id, job_config);
  task_spec = ExampleTaskSpec(/*actor_id=*/ActorID::Nil(), Language::PYTHON, job_id);
  pop_worker_status = PopWorkerStatus::OK;
  // This will start a new worker.
  worker_pool_->PopWorker(
      task_spec,
      [&](const std::shared_ptr<WorkerInterface> worker,
          PopWorkerStatus status,
          const std::string &runtime_env_setup_error_message) -> bool {
        pop_worker_status = status;
        return false;
      });
  auto process = worker_pool_->LastStartedWorkerProcess();
  RAY_CHECK(process.IsValid());
  ASSERT_EQ(1, worker_pool_->NumWorkersStarting());

  worker = worker_pool_->CreateWorker(Process());
  worker->SetStartupToken(worker_pool_->GetStartupToken(process));
  RAY_CHECK_OK(worker_pool_->RegisterWorker(
      worker, process.GetId(), worker_pool_->GetStartupToken(process), [](Status, int) {
      }));
  // Call `OnWorkerStarted` to emulate worker port announcement.
  worker_pool_->OnWorkerStarted(worker);

  mock_rpc_client_it = mock_worker_rpc_clients_.find(worker->WorkerId());
  mock_rpc_client = mock_rpc_client_it->second;

  // Finish the job.
  worker_pool_->HandleJobFinished(job_id);

  // This will trigger the PopWorker callback.
  worker_pool_->PushWorker(worker);
  ASSERT_EQ(pop_worker_status, PopWorkerStatus::JobFinished);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);

  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(mock_rpc_client->exit_count, 1);
  ASSERT_EQ(mock_rpc_client->last_exit_forced, true);
  mock_rpc_client->ExitReplySucceed();
}

TEST_F(WorkerPoolDriverRegisteredTest, TestJobFinishedForceKillIdleWorker) {
  auto job_id = JOB_ID;

  /// Add worker to the pool.
  PopWorkerStatus status;
  auto [proc, token] = worker_pool_->StartWorkerProcess(
      Language::PYTHON, rpc::WorkerType::WORKER, job_id, &status);
  auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON, job_id);
  worker->SetStartupToken(worker_pool_->GetStartupToken(proc));
  RAY_CHECK_OK(worker_pool_->RegisterWorker(
      worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
  worker_pool_->OnWorkerStarted(worker);
  worker_pool_->PushWorker(worker);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);

  /// Execute some task with the worker.
  auto task_spec = ExampleTaskSpec(/*actor_id=*/ActorID::Nil(), Language::PYTHON, job_id);
  worker = worker_pool_->PopWorkerSync(task_spec, false);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);

  /// Return the worker.
  worker_pool_->PushWorker(worker);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);

  auto mock_rpc_client_it = mock_worker_rpc_clients_.find(worker->WorkerId());
  auto mock_rpc_client = mock_rpc_client_it->second;

  worker_pool_->SetCurrentTimeMs(2000);

  // Won't kill the worker since job hasn't finished and we are under
  // the soft limit (5).
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(mock_rpc_client->exit_count, 0);

  // Finish the job.
  worker_pool_->HandleJobFinished(job_id);

  // The pool should try to force kill the worker.
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(mock_rpc_client->exit_count, 1);
  ASSERT_EQ(mock_rpc_client->last_exit_forced, true);

  mock_rpc_client->ExitReplySucceed();
}

TEST_F(WorkerPoolDriverRegisteredTest,
       WorkerFromAliveJobDoesNotBlockWorkerFromDeadJobFromGettingKilled) {
  rpc::JobConfig job_config;

  /// Add worker to the pool whose job will stay alive.
  auto job_id_alive = JobID::FromInt(11111);
  RegisterDriver(Language::PYTHON, job_id_alive, job_config);
  {
    PopWorkerStatus status;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::WORKER, job_id_alive, &status);
    auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON, job_id_alive);
    worker->SetStartupToken(worker_pool_->GetStartupToken(proc));
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushWorker(worker);
  }
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);

  /// Add worker to the pool whose job will be killed.
  auto job_id_dead = JobID::FromInt(22222);
  RegisterDriver(Language::PYTHON, job_id_dead, job_config);
  std::shared_ptr<WorkerInterface> worker_to_kill;
  {
    PopWorkerStatus status;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::WORKER, job_id_dead, &status);
    auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON, job_id_dead);
    worker->SetStartupToken(worker_pool_->GetStartupToken(proc));
    RAY_CHECK_OK(worker_pool_->RegisterWorker(
        worker, proc.GetId(), worker_pool_->GetStartupToken(proc), [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushWorker(worker);

    worker_to_kill = worker;
  }
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 2);

  auto mock_rpc_client_it = mock_worker_rpc_clients_.find(worker_to_kill->WorkerId());
  auto mock_rpc_client = mock_rpc_client_it->second;

  worker_pool_->SetCurrentTimeMs(2000);

  // Won't kill the workers since neither job has finished.
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(mock_rpc_client->exit_count, 0);

  // Finish the job of the second worker.
  worker_pool_->HandleJobFinished(job_id_dead);

  // The pool should try to force kill the second worker whose job is dead,
  // and keep the first worker whose job is alive.
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(mock_rpc_client->exit_count, 1);
  ASSERT_EQ(mock_rpc_client->last_exit_forced, true);

  mock_rpc_client->ExitReplySucceed();
}

TEST_F(WorkerPoolDriverRegisteredTest, PopWorkerWithRuntimeEnv) {
  ASSERT_EQ(worker_pool_->GetProcessSize(), 0);
  auto actor_creation_id = ActorID::Of(JOB_ID, TaskID::ForDriverTask(JOB_ID), 1);
  const auto actor_creation_task_spec = ExampleTaskSpec(ActorID::Nil(),
                                                        Language::PYTHON,
                                                        JOB_ID,
                                                        actor_creation_id,
                                                        {"XXX=YYY"},
                                                        TaskID::FromRandom(JobID::Nil()),
                                                        ExampleRuntimeEnvInfo({"XXX"}));
  const auto normal_task_spec = ExampleTaskSpec(ActorID::Nil(),
                                                Language::PYTHON,
                                                JOB_ID,
                                                ActorID::Nil(),
                                                {"XXX=YYY"},
                                                TaskID::FromRandom(JobID::Nil()),
                                                ExampleRuntimeEnvInfo({"XXX"}));
  const auto normal_task_spec_without_runtime_env =
      ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, JOB_ID, ActorID::Nil(), {});
  // Pop worker for actor creation task again.
  auto popped_worker = worker_pool_->PopWorkerSync(actor_creation_task_spec);
  // Got a worker with correct runtime env hash.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(popped_worker->GetRuntimeEnvHash(),
            actor_creation_task_spec.GetRuntimeEnvHash());
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  // Pop worker for normal task.
  popped_worker = worker_pool_->PopWorkerSync(normal_task_spec);
  // Got a worker with correct runtime env hash.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(popped_worker->GetRuntimeEnvHash(), normal_task_spec.GetRuntimeEnvHash());
  ASSERT_EQ(worker_pool_->GetProcessSize(), 2);
  // Pop worker for normal task without runtime env.
  popped_worker = worker_pool_->PopWorkerSync(normal_task_spec_without_runtime_env);
  // Got a worker with correct runtime env hash.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(popped_worker->GetRuntimeEnvHash(),
            normal_task_spec_without_runtime_env.GetRuntimeEnvHash());
  ASSERT_EQ(worker_pool_->GetProcessSize(), 3);
}

TEST_F(WorkerPoolDriverRegisteredTest, RuntimeEnvUriReferenceJobLevel) {
  // First part, test start job with eager installed runtime env.
  {
    auto job_id = JobID::FromInt(12345);
    std::string uri = "s3://123";
    auto runtime_env_info = ExampleRuntimeEnvInfo({uri}, true);
    rpc::JobConfig job_config;
    job_config.mutable_runtime_env_info()->CopyFrom(runtime_env_info);
    // Start job.
    worker_pool_->HandleJobStarted(job_id, job_config);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 1);
    // Finish the job.
    worker_pool_->HandleJobFinished(job_id);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 0);
  }

  // Second part, test start job without eager installed runtime env.
  {
    auto job_id = JobID::FromInt(67890);
    std::string uri = "s3://678";
    auto runtime_env_info = ExampleRuntimeEnvInfo({uri}, false);
    rpc::JobConfig job_config;
    job_config.mutable_runtime_env_info()->CopyFrom(runtime_env_info);
    // Start job.
    worker_pool_->HandleJobStarted(job_id, job_config);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 0);
    // Finish the job.
    worker_pool_->HandleJobFinished(job_id);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 0);
  }
}

TEST_F(WorkerPoolDriverRegisteredTest, RuntimeEnvUriReferenceWorkerLevel) {
  // First part, test URI reference with eager install.
  {
    auto job_id = JobID::FromInt(12345);
    std::string uri = "s3://123";
    auto runtime_env_info = ExampleRuntimeEnvInfo({uri}, true);
    rpc::JobConfig job_config;
    job_config.mutable_runtime_env_info()->CopyFrom(runtime_env_info);
    // Start job with eager installed runtime env.
    worker_pool_->HandleJobStarted(job_id, job_config);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 1);
    // Start actor with runtime env.
    auto actor_creation_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1);
    const auto actor_creation_task_spec =
        ExampleTaskSpec(ActorID::Nil(),
                        Language::PYTHON,
                        job_id,
                        actor_creation_id,
                        {"XXX=YYY"},
                        TaskID::FromRandom(JobID::Nil()),
                        runtime_env_info);
    auto popped_actor_worker = worker_pool_->PopWorkerSync(actor_creation_task_spec);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 2);
    // Start task with runtime env.
    const auto normal_task_spec = ExampleTaskSpec(ActorID::Nil(),
                                                  Language::PYTHON,
                                                  job_id,
                                                  ActorID::Nil(),
                                                  {"XXX=YYY"},
                                                  TaskID::FromRandom(JobID::Nil()),
                                                  runtime_env_info);
    auto popped_normal_worker = worker_pool_->PopWorkerSync(actor_creation_task_spec);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 3);
    // Disconnect actor worker.
    worker_pool_->DisconnectWorker(popped_actor_worker,
                                   rpc::WorkerExitType::INTENDED_USER_EXIT);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 2);
    // Disconnect task worker.
    worker_pool_->DisconnectWorker(popped_normal_worker,
                                   rpc::WorkerExitType::INTENDED_USER_EXIT);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 1);
    // Finish the job.
    worker_pool_->HandleJobFinished(job_id);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 0);
  }

  // Second part, test URI reference without eager install.
  {
    auto job_id = JobID::FromInt(67890);
    std::string uri = "s3://678";
    auto runtime_env_info = ExampleRuntimeEnvInfo({uri}, true);
    auto runtime_env_info_without_eager_install = ExampleRuntimeEnvInfo({uri}, false);
    rpc::JobConfig job_config;
    job_config.mutable_runtime_env_info()->CopyFrom(
        runtime_env_info_without_eager_install);
    // Start job without eager installed runtime env.
    worker_pool_->HandleJobStarted(job_id, job_config);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 0);
    // Start actor with runtime env.
    auto actor_creation_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 2);
    const auto actor_creation_task_spec =
        ExampleTaskSpec(ActorID::Nil(),
                        Language::PYTHON,
                        job_id,
                        actor_creation_id,
                        {"XXX=YYY"},
                        TaskID::FromRandom(JobID::Nil()),
                        runtime_env_info);
    auto popped_actor_worker = worker_pool_->PopWorkerSync(actor_creation_task_spec);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 1);
    // Start task with runtime env.
    auto popped_normal_worker = worker_pool_->PopWorkerSync(actor_creation_task_spec);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 2);
    // Disconnect actor worker.
    worker_pool_->DisconnectWorker(popped_actor_worker,
                                   rpc::WorkerExitType::INTENDED_USER_EXIT);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 1);
    // Disconnect task worker.
    worker_pool_->DisconnectWorker(popped_normal_worker,
                                   rpc::WorkerExitType::INTENDED_USER_EXIT);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 0);
    // Finish the job.
    worker_pool_->HandleJobFinished(job_id);
    ASSERT_EQ(GetReferenceCount(runtime_env_info.serialized_runtime_env()), 0);
  }
}

TEST_F(WorkerPoolDriverRegisteredTest, CacheWorkersByRuntimeEnvHash) {
  ///
  /// Check that a worker can be popped only if there is a
  /// worker available whose runtime env matches the runtime env
  /// in the task spec.
  ///
  ASSERT_EQ(worker_pool_->GetProcessSize(), 0);
  auto actor_creation_id = ActorID::Of(JOB_ID, TaskID::ForDriverTask(JOB_ID), 1);
  const auto actor_creation_task_spec_1 =
      ExampleTaskSpec(ActorID::Nil(),
                      Language::PYTHON,
                      JOB_ID,
                      actor_creation_id,
                      /*dynamic_worker_options=*/{},
                      TaskID::FromRandom(JobID::Nil()),
                      ExampleRuntimeEnvInfoFromString("mock_runtime_env_1"));
  const auto task_spec_1 =
      ExampleTaskSpec(ActorID::Nil(),
                      Language::PYTHON,
                      JOB_ID,
                      ActorID::Nil(),
                      /*dynamic_worker_options=*/{},
                      TaskID::FromRandom(JobID::Nil()),
                      ExampleRuntimeEnvInfoFromString("mock_runtime_env_1"));
  const auto task_spec_2 =
      ExampleTaskSpec(ActorID::Nil(),
                      Language::PYTHON,
                      JOB_ID,
                      ActorID::Nil(),
                      /*dynamic_worker_options=*/{},
                      TaskID::FromRandom(JobID::Nil()),
                      ExampleRuntimeEnvInfoFromString("mock_runtime_env_2"));

  const int runtime_env_hash_1 = actor_creation_task_spec_1.GetRuntimeEnvHash();

  // Push worker with runtime env 1.
  auto worker = worker_pool_->CreateWorker(Process::CreateNewDummy(),
                                           Language::PYTHON,
                                           JOB_ID,
                                           rpc::WorkerType::WORKER,
                                           runtime_env_hash_1);
  worker_pool_->PushWorker(worker);

  // Try to pop worker for task with runtime env 2.
  auto popped_worker = worker_pool_->PopWorkerSync(task_spec_2);
  // Check that popped worker isn't the one we pushed.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_NE(popped_worker, worker);

  // Try to pop the worker for task with runtime env 1.
  popped_worker = worker_pool_->PopWorkerSync(task_spec_1);
  ASSERT_EQ(popped_worker, worker);

  // Push another worker with runtime env 1.
  worker = worker_pool_->CreateWorker(Process::CreateNewDummy(),
                                      Language::PYTHON,
                                      JOB_ID,
                                      rpc::WorkerType::WORKER,
                                      runtime_env_hash_1);
  worker_pool_->PushWorker(worker);

  // Try to pop the worker for an actor with runtime env 1.
  popped_worker = worker_pool_->PopWorkerSync(actor_creation_task_spec_1);
  // Check that we got the pushed worker.
  ASSERT_EQ(popped_worker, worker);
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolDriverRegisteredTest, WorkerNoLeaks) {
  std::shared_ptr<WorkerInterface> popped_worker;
  const auto task_spec = ExampleTaskSpec();

  // Pop a worker and don't dispatch.
  worker_pool_->PopWorker(task_spec,
                          [](const std::shared_ptr<WorkerInterface> worker,
                             PopWorkerStatus status,
                             const std::string &runtime_env_setup_error_message) -> bool {
                            // Don't dispatch this worker.
                            return false;
                          });
  // One worker process has been started.
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  // No idle workers because no workers pushed.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);
  // push workers.
  worker_pool_->PushWorkers();
  // The worker has been pushed but not dispatched.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);
  // Pop a worker and don't dispatch.
  worker_pool_->PopWorker(task_spec,
                          [](const std::shared_ptr<WorkerInterface> worker,
                             PopWorkerStatus status,
                             const std::string &runtime_env_setup_error_message) -> bool {
                            // Don't dispatch this worker.
                            return false;
                          });
  // The worker is popped but not dispatched.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  // Pop a worker and dispatch.
  worker_pool_->PopWorker(task_spec,
                          [](const std::shared_ptr<WorkerInterface> worker,
                             PopWorkerStatus status,
                             const std::string &runtime_env_setup_error_message) -> bool {
                            // Dispatch this worker.
                            return true;
                          });
  // The worker is popped and dispatched.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolDriverRegisteredTest, PopWorkerStatus) {
  std::shared_ptr<WorkerInterface> popped_worker;
  PopWorkerStatus status;

  /* Test PopWorkerStatus JobConfigMissing */
  // Create a task by unregistered job id.
  auto job_id = JobID::FromInt(123);
  auto task_spec = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, job_id);
  popped_worker = worker_pool_->PopWorkerSync(task_spec, true, &status);
  // PopWorker failed and the status is `JobConfigMissing`.
  ASSERT_EQ(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::JobConfigMissing);

  // Register driver fot the job.
  RegisterDriver(Language::PYTHON, job_id);
  popped_worker = worker_pool_->PopWorkerSync(task_spec, true, &status);
  // PopWorker success.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::OK);

  /* Test PopWorkerStatus RuntimeEnvCreationFailed */
  // Create a task with bad runtime env.
  const auto task_spec_with_bad_runtime_env =
      ExampleTaskSpec(ActorID::Nil(),
                      Language::PYTHON,
                      job_id,
                      ActorID::Nil(),
                      {"XXX=YYY"},
                      TaskID::FromRandom(JobID::Nil()),
                      ExampleRuntimeEnvInfoFromString(BAD_RUNTIME_ENV));
  std::string error_msg;
  popped_worker = worker_pool_->PopWorkerSync(
      task_spec_with_bad_runtime_env, true, &status, 0, &error_msg);
  // PopWorker failed and the status is `RuntimeEnvCreationFailed`.
  ASSERT_EQ(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::RuntimeEnvCreationFailed);
  ASSERT_EQ(error_msg, BAD_RUNTIME_ENV_ERROR_MSG);

  // Create a task with available runtime env.
  const auto task_spec_with_runtime_env =
      ExampleTaskSpec(ActorID::Nil(),
                      Language::PYTHON,
                      job_id,
                      ActorID::Nil(),
                      {"XXX=YYY"},
                      TaskID::FromRandom(JobID::Nil()),
                      ExampleRuntimeEnvInfo({"XXX"}));
  popped_worker = worker_pool_->PopWorkerSync(task_spec_with_runtime_env, true, &status);
  // PopWorker success.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::OK);

  /* Test PopWorkerStatus WorkerPendingRegistration */
  // Create a task without push worker.
  popped_worker = worker_pool_->PopWorkerSync(task_spec, false, &status);
  ASSERT_EQ(popped_worker, nullptr);
  // PopWorker failed while the timer was triggered and the status is
  // `WorkerPendingRegistration`.
  ASSERT_EQ(status, PopWorkerStatus::WorkerPendingRegistration);
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolDriverRegisteredTest, TestIOWorkerFailureAndSpawn) {
  std::unordered_set<std::shared_ptr<WorkerInterface>> spill_worker_set;
  auto spill_worker_callback =
      [&spill_worker_set](std::shared_ptr<WorkerInterface> worker) {
        spill_worker_set.emplace(worker);
      };

  // Initialize the worker pool with MAX_IO_WORKER_SIZE idle spill workers.

  std::vector<std::tuple<Process, StartupToken>> processes;
  for (int i = 0; i < MAX_IO_WORKER_SIZE; i++) {
    auto status = PopWorkerStatus::OK;
    auto process = worker_pool_->StartWorkerProcess(
        rpc::Language::PYTHON, rpc::WorkerType::SPILL_WORKER, JobID::Nil(), &status);
    ASSERT_EQ(status, PopWorkerStatus::OK);
    processes.push_back(process);
  }
  for (const auto &process : processes) {
    auto [proc, token] = process;
    auto worker = CreateSpillWorker(Process());
    RAY_CHECK_OK(
        worker_pool_->RegisterWorker(worker, proc.GetId(), token, [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    worker_pool_->PushSpillWorker(worker);
  }

  {
    // Test the case where DisconnectClient happens after RegisterClientRequest but before
    // AnnounceWorkerPort.
    auto status = PopWorkerStatus::OK;
    auto [proc, token] = worker_pool_->StartWorkerProcess(
        rpc::Language::PYTHON, rpc::WorkerType::SPILL_WORKER, JobID::Nil(), &status);
    ASSERT_EQ(status, PopWorkerStatus::OK);
    auto worker = CreateSpillWorker(Process());
    RAY_CHECK_OK(
        worker_pool_->RegisterWorker(worker, proc.GetId(), token, [](Status, int) {}));
    // The worker failed before announcing the worker port (i.e. OnworkerStarted)
    worker_pool_->DisconnectWorker(worker,
                                   /*disconnect_type=*/rpc::WorkerExitType::SYSTEM_ERROR);
  }

  ASSERT_EQ(worker_pool_->NumSpillWorkerStarting(), 0);
  ASSERT_EQ(worker_pool_->NumSpillWorkerStarted(), MAX_IO_WORKER_SIZE);

  // Pop spill workers should work.

  for (int i = 0; i < MAX_IO_WORKER_SIZE; i++) {
    // Pop a spill worker.
    worker_pool_->PopSpillWorker(spill_worker_callback);
  }
  ASSERT_EQ(spill_worker_set.size(), MAX_IO_WORKER_SIZE);
  std::unordered_set<WorkerID> worker_ids;
  for (const auto &worker : spill_worker_set) {
    worker_ids.emplace(worker->WorkerId());
  }

  // Push them back and mock worker failure.

  for (const auto &worker : spill_worker_set) {
    worker_pool_->PushSpillWorker(worker);
    worker_pool_->DisconnectWorker(worker,
                                   /*disconnect_type=*/rpc::WorkerExitType::SYSTEM_ERROR);
  }
  spill_worker_set.clear();

  // Pop a spill worker.

  worker_pool_->PopSpillWorker(spill_worker_callback);
  // Unable to pop a spill worker from the idle pool, but a new one is being started.
  ASSERT_EQ(spill_worker_set.size(), 0);
  auto worker2 = CreateSpillWorker(Process());
  auto status = PopWorkerStatus::OK;
  auto [proc2, token2] = worker_pool_->StartWorkerProcess(
      rpc::Language::PYTHON, rpc::WorkerType::SPILL_WORKER, JobID::Nil(), &status);
  ASSERT_EQ(status, PopWorkerStatus::OK);
  RAY_CHECK_OK(
      worker_pool_->RegisterWorker(worker2, proc2.GetId(), token2, [](Status, int) {}));
  worker_pool_->OnWorkerStarted(worker2);
  worker_pool_->PushSpillWorker(worker2);
  ASSERT_EQ(spill_worker_set.size(), 1);
  ASSERT_EQ(worker2, *spill_worker_set.begin());
  // The popped spill worker should be newly created.
  ASSERT_FALSE(worker_ids.count(worker2->WorkerId()));
  worker_ids.emplace(worker2->WorkerId());

  // This time, we mock worker failure before it's returning to worker pool.

  worker_pool_->DisconnectWorker(worker2,
                                 /*disconnect_type=*/rpc::WorkerExitType::SYSTEM_ERROR);
  worker_pool_->PushSpillWorker(worker2);
  spill_worker_set.clear();

  // Pop a spill worker.

  worker_pool_->PopSpillWorker(spill_worker_callback);
  // Unable to pop a spill worker from the idle pool, but a new one is being started.
  ASSERT_EQ(spill_worker_set.size(), 0);
  auto worker3 = CreateSpillWorker(Process());
  auto [proc3, token3] = worker_pool_->StartWorkerProcess(
      rpc::Language::PYTHON, rpc::WorkerType::SPILL_WORKER, JobID::Nil(), &status);
  ASSERT_EQ(status, PopWorkerStatus::OK);
  RAY_CHECK_OK(
      worker_pool_->RegisterWorker(worker3, proc3.GetId(), token3, [](Status, int) {}));
  worker_pool_->OnWorkerStarted(worker3);
  worker_pool_->PushSpillWorker(worker3);
  ASSERT_EQ(spill_worker_set.size(), 1);
  ASSERT_EQ(worker3, *spill_worker_set.begin());
  // The popped spill worker should be newly created.
  ASSERT_FALSE(worker_ids.count(worker3->WorkerId()));
}

TEST_F(WorkerPoolDriverRegisteredTest, WorkerReuseForPrestartedWorker) {
  const auto task_spec = ExampleTaskSpec();

  worker_pool_->PrestartDefaultCpuWorkers(ray::Language::PYTHON, 1);
  worker_pool_->PushWorkers();
  // One worker process has been prestarted.
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);
  // Pop a worker and don't dispatch.
  auto popped_worker = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  // no new worker started since we can reuse the cached worker.
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  // The worker is popped but not dispatched so the worker is still idle.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);
}

TEST_F(WorkerPoolDriverRegisteredTest, WorkerReuseForSameJobId) {
  const auto task_spec = ExampleTaskSpec();

  // start one worker
  auto popped_worker = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);
  worker_pool_->PushWorker(popped_worker);

  // start a new worker withe same job_id resuse the same worker.
  auto popped_worker1 = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(popped_worker1, nullptr);
  ASSERT_EQ(popped_worker1, popped_worker);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);
}

TEST_F(WorkerPoolDriverRegisteredTest, WorkerReuseFailureForDifferentJobId) {
  const auto task_spec = ExampleTaskSpec();
  const auto task_spec1 = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, JOB_ID2);

  // start one worker
  auto popped_worker = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);
  worker_pool_->PushWorker(popped_worker);

  RegisterDriver(Language::PYTHON, JOB_ID2);

  // start a new worker with different job_id requires a new worker.
  auto popped_worker1 = worker_pool_->PopWorkerSync(task_spec1);
  ASSERT_NE(popped_worker1, nullptr);
  ASSERT_NE(popped_worker1, popped_worker);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 2);
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);
}

TEST_F(WorkerPoolTest, RegisterFirstPythonDriverWaitForWorkerStart) {
  auto driver =
      worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, JOB_ID);
  driver->AssignTaskId(TaskID::ForDriverTask(JOB_ID));
  bool callback_called = false;
  auto callback = [callback_called_ptr = &callback_called](Status, int) mutable {
    *callback_called_ptr = true;
  };
  RAY_CHECK_OK(worker_pool_->RegisterDriver(driver, rpc::JobConfig(), callback));
  ASSERT_FALSE(callback_called);
}

TEST_F(WorkerPoolTest, RegisterSecondPythonDriverCallbackImmediately) {
  auto driver =
      worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, JOB_ID);
  driver->AssignTaskId(TaskID::ForDriverTask(JOB_ID));
  RAY_CHECK_OK(
      worker_pool_->RegisterDriver(driver, rpc::JobConfig(), [](Status, int) {}));

  bool callback_called = false;
  auto callback = [callback_called_ptr = &callback_called](Status, int) mutable {
    *callback_called_ptr = true;
  };
  auto second_driver =
      worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, JOB_ID);
  second_driver->AssignTaskId(TaskID::ForDriverTask(JOB_ID));
  RAY_CHECK_OK(worker_pool_->RegisterDriver(second_driver, rpc::JobConfig(), callback));
  ASSERT_TRUE(callback_called);
}

TEST_F(WorkerPoolTest, RegisterFirstJavaDriverCallbackImmediately) {
  auto driver =
      worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::JAVA, JOB_ID);

  driver->AssignTaskId(TaskID::ForDriverTask(JOB_ID));
  bool callback_called = false;
  auto callback = [callback_called_ptr = &callback_called](Status, int) mutable {
    *callback_called_ptr = true;
  };
  RAY_CHECK_OK(worker_pool_->RegisterDriver(driver, rpc::JobConfig(), callback));
  ASSERT_TRUE(callback_called);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(
      ray::RayLog::StartRayLog,
      []() { ray::RayLog::ShutDownRayLog(); },
      argv[0],
      ray::RayLogLevel::INFO,
      /*log_dir=*/"");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
