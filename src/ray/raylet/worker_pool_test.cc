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
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/constants.h"
#include "ray/raylet/node_manager.h"
#include "ray/util/process.h"

namespace ray {

namespace raylet {

int NUM_WORKERS_PER_PROCESS_JAVA = 3;
int MAXIMUM_STARTUP_CONCURRENCY = 5;
int MAX_IO_WORKER_SIZE = 2;
int POOL_SIZE_SOFT_LIMIT = 5;
JobID JOB_ID = JobID::FromInt(1);
std::string BAD_RUNTIME_ENV = "bad runtime env";

std::vector<Language> LANGUAGES = {Language::PYTHON, Language::JAVA};

static inline std::string GetNumJavaWorkersPerProcessSystemProperty(int num) {
  return std::string("-Dray.job.num-java-workers-per-process=") + std::to_string(num);
}

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  MockWorkerClient(instrumented_io_context &io_service) : io_service_(io_service) {}

  void Exit(const rpc::ExitRequest &request,
            const rpc::ClientCallback<rpc::ExitReply> &callback) {
    callbacks_.push_back(callback);
  }

  bool ExitReplySucceed() {
    if (callbacks_.size() == 0) {
      return false;
    }
    const auto &callback = callbacks_.front();
    rpc::ExitReply exit_reply;
    exit_reply.set_success(true);
    callback(Status::OK(), exit_reply);
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
    callback(Status::OK(), exit_reply);
    callbacks_.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::ExitReply>> callbacks_;
  instrumented_io_context &io_service_;
};

class MockRuntimeEnvAgentClient : public rpc::RuntimeEnvAgentClientInterface {
 public:
  void CreateRuntimeEnv(const rpc::CreateRuntimeEnvRequest &request,
                        const rpc::ClientCallback<rpc::CreateRuntimeEnvReply> &callback) {
    rpc::CreateRuntimeEnvReply reply;
    if (request.serialized_runtime_env() == BAD_RUNTIME_ENV) {
      reply.set_status(rpc::AGENT_RPC_STATUS_FAILED);
    } else {
      reply.set_status(rpc::AGENT_RPC_STATUS_OK);
    }
    callback(Status::OK(), reply);
  };

  void DeleteRuntimeEnv(const rpc::DeleteRuntimeEnvRequest &request,
                        const rpc::ClientCallback<rpc::DeleteRuntimeEnvReply> &callback) {
    rpc::DeleteRuntimeEnvReply reply;
    reply.set_status(rpc::AGENT_RPC_STATUS_OK);
    callback(Status::OK(), reply);
  };
};

class WorkerPoolMock : public WorkerPool {
 public:
  explicit WorkerPoolMock(instrumented_io_context &io_service,
                          const WorkerCommandMap &worker_commands,
                          absl::flat_hash_map<WorkerID, std::shared_ptr<MockWorkerClient>>
                              &mock_worker_rpc_clients)
      : WorkerPool(io_service, NodeID::FromRandom(), "", POOL_SIZE_SOFT_LIMIT, 0,
                   MAXIMUM_STARTUP_CONCURRENCY, 0, 0, {}, nullptr, worker_commands,
                   []() {}, 0, [this]() { return current_time_ms_; }),
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
  void PopWorkerCallbackAsync(const PopWorkerCallback &callback,
                              std::shared_ptr<WorkerInterface> worker,
                              PopWorkerStatus status = PopWorkerStatus::OK) override {
    PopWorkerCallbackInternal(callback, worker, status);
  }

  Process StartProcess(const std::vector<std::string> &worker_command_args,
                       const ProcessEnvironment &env) override {
    // Use a bogus process ID that won't conflict with those in the system
    pid_t pid = static_cast<pid_t>(PID_MAX_LIMIT + 1 + worker_commands_by_proc_.size());
    last_worker_process_ = Process::FromPid(pid);
    worker_commands_by_proc_[last_worker_process_] = worker_command_args;
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
      for (auto &process_entry : state_entry.second.starting_worker_processes) {
        total += process_entry.second.num_starting_workers;
      }
    }
    return total;
  }

  int NumWorkerProcessesStarting() const {
    int total = 0;
    for (auto &entry : states_by_lang_) {
      total += entry.second.starting_worker_processes.size();
    }
    return total;
  }

  int NumSpillWorkerStarting() const {
    auto state = states_by_lang_.find(Language::PYTHON)->second;
    return state.spill_io_worker_state.num_starting_io_workers;
  }

  int NumRestoreWorkerStarting() const {
    auto state = states_by_lang_.find(Language::PYTHON)->second;
    return state.restore_io_worker_state.num_starting_io_workers;
  }

  int GetProcessSize() const { return worker_commands_by_proc_.size(); }

  const std::unordered_map<Process, std::vector<std::string>> &GetProcesses() {
    return worker_commands_by_proc_;
  }

  void ClearProcesses() { worker_commands_by_proc_.clear(); }

  void SetCurrentTimeMs(double current_time) { current_time_ms_ = current_time; }

  size_t GetIdleWorkerSize() { return idle_of_all_languages_.size(); }

  std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>> &GetIdleWorkers() {
    return idle_of_all_languages_;
  }

  std::shared_ptr<WorkerInterface> CreateWorker(
      Process proc, const Language &language = Language::PYTHON,
      const JobID &job_id = JOB_ID,
      const rpc::WorkerType worker_type = rpc::WorkerType::WORKER,
      int runtime_env_hash = 0) {
    std::function<void(ClientConnection &)> client_handler =
        [this](ClientConnection &client) { HandleNewClient(client); };
    std::function<void(std::shared_ptr<ClientConnection>, int64_t,
                       const std::vector<uint8_t> &)>
        message_handler = [this](std::shared_ptr<ClientConnection> client,
                                 int64_t message_type,
                                 const std::vector<uint8_t> &message) {
          HandleMessage(client, message_type, message);
        };
    local_stream_socket socket(instrumented_io_service_);
    auto client =
        ClientConnection::Create(client_handler, message_handler, std::move(socket),
                                 "worker", {}, error_message_type_);
    std::shared_ptr<Worker> worker_ = std::make_shared<Worker>(
        job_id, runtime_env_hash, WorkerID::FromRandom(), language, worker_type,
        "127.0.0.1", client, client_call_manager_);
    std::shared_ptr<WorkerInterface> worker =
        std::dynamic_pointer_cast<WorkerInterface>(worker_);
    auto rpc_client = std::make_shared<MockWorkerClient>(instrumented_io_service_);
    worker->Connect(rpc_client);
    mock_worker_rpc_clients_.emplace(worker->WorkerId(), rpc_client);
    if (!proc.IsNull()) {
      worker->SetProcess(proc);
      worker->SetShimProcess(proc);
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
    if (worker->GetWorkerType() == rpc::WorkerType::UTIL_WORKER) {
      PushUtilWorker(worker);
      return;
    }
    PushWorker(worker);
  }

  // Create workers for processes and push them to worker pool.
  void PushWorkers() {
    auto processes = GetProcesses();
    for (auto it = processes.begin(); it != processes.end(); ++it) {
      auto pushed_it = pushedProcesses_.find(it->first);
      if (pushed_it == pushedProcesses_.end()) {
        int runtime_env_hash = 0;
        bool is_java = false;
        // Parses runtime env hash to make sure the pushed workers can be popped out.
        for (auto command_args : it->second) {
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
        // TODO(guyang.sgy): support C++ language workers.
        int num_workers = is_java ? NUM_WORKERS_PER_PROCESS_JAVA : 1;
        for (int i = 0; i < num_workers; i++) {
          auto worker =
              CreateWorker(it->first, is_java ? Language::JAVA : Language::PYTHON, JOB_ID,
                           rpc::WorkerType::WORKER, runtime_env_hash);
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
      const TaskSpecification &task_spec, bool push_workers = true,
      PopWorkerStatus *worker_status = nullptr) {
    std::shared_ptr<WorkerInterface> popped_worker = nullptr;
    std::promise<bool> promise;
    this->PopWorker(task_spec,
                    [&popped_worker, worker_status, &promise](
                        const std::shared_ptr<WorkerInterface> worker,
                        PopWorkerStatus status) -> bool {
                      popped_worker = worker;
                      if (worker_status != nullptr) {
                        *worker_status = status;
                      }
                      promise.set_value(true);
                      return true;
                    });
    if (push_workers) {
      PushWorkers();
    }
    promise.get_future().get();
    return popped_worker;
  }

 private:
  Process last_worker_process_;
  // The worker commands by process.
  std::unordered_map<Process, std::vector<std::string>> worker_commands_by_proc_;
  double current_time_ms_ = 0;
  std::unordered_map<Process, std::vector<std::string>> pushedProcesses_;
  instrumented_io_context &instrumented_io_service_;
  int64_t error_message_type_;
  rpc::ClientCallManager client_call_manager_;
  absl::flat_hash_map<WorkerID, std::shared_ptr<MockWorkerClient>>
      &mock_worker_rpc_clients_;
  void HandleNewClient(ClientConnection &){};
  void HandleMessage(std::shared_ptr<ClientConnection>, int64_t,
                     const std::vector<uint8_t> &){};
};

class WorkerPoolTest : public ::testing::Test {
 public:
  WorkerPoolTest() {
    RayConfig::instance().initialize(
        R"({"worker_register_timeout_seconds": 3, "object_spilling_config": "dummy", "max_io_workers": )" +
        std::to_string(MAX_IO_WORKER_SIZE) + "}");
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
    StartMockAgent();
  }

  ~WorkerPoolTest() {
    io_service_.stop();
    thread_io_service_->join();
  }

  std::shared_ptr<WorkerInterface> CreateSpillWorker(Process proc) {
    return worker_pool_->CreateWorker(proc, Language::PYTHON, JobID::Nil(),
                                      rpc::WorkerType::SPILL_WORKER);
  }

  std::shared_ptr<WorkerInterface> CreateRestoreWorker(Process proc) {
    return worker_pool_->CreateWorker(proc, Language::PYTHON, JobID::Nil(),
                                      rpc::WorkerType::RESTORE_WORKER);
  }

  std::shared_ptr<WorkerInterface> RegisterDriver(
      const Language &language = Language::PYTHON, const JobID &job_id = JOB_ID,
      const rpc::JobConfig &job_config = rpc::JobConfig()) {
    auto driver =
        worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, job_id);
    driver->AssignTaskId(TaskID::ForDriverTask(job_id));
    RAY_CHECK_OK(worker_pool_->RegisterDriver(driver, job_config, [](Status, int) {}));
    return driver;
  }

  void SetWorkerCommands(const WorkerCommandMap &worker_commands) {
    worker_pool_ = std::make_unique<WorkerPoolMock>(io_service_, worker_commands,
                                                    mock_worker_rpc_clients_);
    rpc::JobConfig job_config;
    job_config.set_num_java_workers_per_process(NUM_WORKERS_PER_PROCESS_JAVA);
    RegisterDriver(Language::PYTHON, JOB_ID, job_config);
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
      worker_pool_->StartWorkerProcess(language, rpc::WorkerType::WORKER, JOB_ID,
                                       &status);
      ASSERT_TRUE(worker_pool_->NumWorkerProcessesStarting() <=
                  expected_worker_process_count);
      Process prev = worker_pool_->LastStartedWorkerProcess();
      if (!std::equal_to<Process>()(last_started_worker_process, prev)) {
        last_started_worker_process = prev;
        const auto &real_command =
            worker_pool_->GetWorkerCommand(last_started_worker_process);
        if (language == Language::JAVA) {
          auto it = std::find(
              real_command.begin(), real_command.end(),
              GetNumJavaWorkersPerProcessSystemProperty(num_workers_per_process));
          ASSERT_NE(it, real_command.end());
        }
      } else {
        ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(),
                  expected_worker_process_count);
        ASSERT_TRUE(i >= expected_worker_process_count);
      }
    }
    // Check number of starting workers
    ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), expected_worker_process_count);
  }

  void StartMockAgent() {
    std::vector<std::string> agent_commands = {};
    const NodeID node_id = NodeID::FromRandom();
    auto options = AgentManager::Options({node_id, agent_commands});
    auto agent_manager = std::make_shared<AgentManager>(
        std::move(options),
        /*delay_executor=*/
        [this](std::function<void()> task, uint32_t delay_ms) {
          return execute_after(io_service_, task, delay_ms);
        },
        /*runtime_env_agent_factory=*/
        [](const std::string &ip_address, int port) {
          return std::shared_ptr<rpc::RuntimeEnvAgentClientInterface>(
              new MockRuntimeEnvAgentClient());
        },
        false);
    const rpc::RegisterAgentRequest request;
    rpc::RegisterAgentReply reply;
    auto send_reply_callback = [](ray::Status status, std::function<void()> f1,
                                  std::function<void()> f2) {};
    agent_manager->HandleRegisterAgent(request, &reply, send_reply_callback);
    worker_pool_->SetAgentManager(agent_manager);
  }

  absl::flat_hash_map<WorkerID, std::shared_ptr<MockWorkerClient>>
      mock_worker_rpc_clients_;

 protected:
  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<WorkerPoolMock> worker_pool_;
};

static inline TaskSpecification ExampleTaskSpec(
    const ActorID actor_id = ActorID::Nil(), const Language &language = Language::PYTHON,
    const JobID &job_id = JOB_ID, const ActorID actor_creation_id = ActorID::Nil(),
    const std::vector<std::string> &dynamic_worker_options = {},
    const TaskID &task_id = TaskID::ForFakeTask(),
    const std::string serialized_runtime_env = "") {
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
  message.set_serialized_runtime_env(serialized_runtime_env);
  return TaskSpecification(std::move(message));
}

TEST_F(WorkerPoolTest, CompareWorkerProcessObjects) {
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

TEST_F(WorkerPoolTest, HandleWorkerRegistration) {
  PopWorkerStatus status;
  Process proc = worker_pool_->StartWorkerProcess(Language::JAVA, rpc::WorkerType::WORKER,
                                                  JOB_ID, &status);
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  for (int i = 0; i < NUM_WORKERS_PER_PROCESS_JAVA; i++) {
    workers.push_back(worker_pool_->CreateWorker(Process(), Language::JAVA));
  }
  for (const auto &worker : workers) {
    // Check that there's still a starting worker process
    // before all workers have been registered
    ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 1);
    // Check that we cannot lookup the worker before it's registered.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), nullptr);
    RAY_CHECK_OK(worker_pool_->RegisterWorker(worker, proc.GetId(), proc.GetId(),
                                              [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    // Check that we can lookup the worker after it's registered.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
  }
  // Check that there's no starting worker process
  ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 0);
  for (const auto &worker : workers) {
    worker_pool_->DisconnectWorker(
        worker, /*disconnect_type=*/rpc::WorkerExitType::INTENDED_EXIT);
    // Check that we cannot lookup the worker after it's disconnected.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), nullptr);
  }
}

TEST_F(WorkerPoolTest, HandleUnknownWorkerRegistration) {
  auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON);
  auto status = worker_pool_->RegisterWorker(worker, 1234, 1234, [](Status, int) {});
  ASSERT_FALSE(status.ok());
}

TEST_F(WorkerPoolTest, StartupPythonWorkerProcessCount) {
  TestStartupWorkerProcessCount(Language::PYTHON, 1);
}

TEST_F(WorkerPoolTest, StartupJavaWorkerProcessCount) {
  TestStartupWorkerProcessCount(Language::JAVA, NUM_WORKERS_PER_PROCESS_JAVA);
}

TEST_F(WorkerPoolTest, InitialWorkerProcessCount) {
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 0);
  ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 0);
}

TEST_F(WorkerPoolTest, TestPrestartingWorkers) {
  const auto task_spec = ExampleTaskSpec();
  // Prestarts 2 workers.
  worker_pool_->PrestartWorkers(task_spec, 2, /*num_available_cpus=*/5);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 2);
  ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 2);
  // Prestarts 1 more worker.
  worker_pool_->PrestartWorkers(task_spec, 3, /*num_available_cpus=*/5);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 3);
  ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 3);
  // No more needed.
  worker_pool_->PrestartWorkers(task_spec, 1, /*num_available_cpus=*/5);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 3);
  ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 3);
  // Capped by soft limit of 5.
  worker_pool_->PrestartWorkers(task_spec, 20, /*num_available_cpus=*/5);
  ASSERT_EQ(worker_pool_->NumWorkersStarting(), 5);
  ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 5);
}

TEST_F(WorkerPoolTest, HandleWorkerPushPop) {
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

TEST_F(WorkerPoolTest, PopWorkerSyncsOfMultipleLanguages) {
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

TEST_F(WorkerPoolTest, StartWorkerWithDynamicOptionsCommand) {
  std::vector<std::string> actor_jvm_options;
  actor_jvm_options.insert(
      actor_jvm_options.end(),
      {"-Dmy-actor.hello=foo", "-Dmy-actor.world=bar", "-Xmx2g", "-Xms1g"});
  auto task_id = TaskID::ForDriverTask(JOB_ID);
  auto actor_id = ActorID::Of(JOB_ID, task_id, 1);
  TaskSpecification task_spec = ExampleTaskSpec(ActorID::Nil(), Language::JAVA, JOB_ID,
                                                actor_id, actor_jvm_options, task_id);

  rpc::JobConfig job_config = rpc::JobConfig();
  job_config.add_code_search_path("/test/code_search_path");
  job_config.set_num_java_workers_per_process(1);
  job_config.add_jvm_options("-Xmx1g");
  job_config.add_jvm_options("-Xms500m");
  job_config.add_jvm_options("-Dmy-job.hello=world");
  job_config.add_jvm_options("-Dmy-job.foo=bar");
  worker_pool_->HandleJobStarted(JOB_ID, job_config);

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
  expected_command.push_back(GetNumJavaWorkersPerProcessSystemProperty(1));
  // User-defined per-process options
  expected_command.insert(expected_command.end(), actor_jvm_options.begin(),
                          actor_jvm_options.end());
  // Entry point
  expected_command.push_back("MainClass");
  ASSERT_EQ(real_command, expected_command);
  worker_pool_->HandleJobFinished(JOB_ID);
}

TEST_F(WorkerPoolTest, PopWorkerMultiTenancy) {
  auto job_id1 = JOB_ID;
  auto job_id2 = JobID::FromInt(2);
  ASSERT_NE(job_id1, job_id2);
  JobID job_ids[] = {job_id1, job_id2};

  // The driver of job 1 is already registered. Here we register the driver for job 2.
  RegisterDriver(Language::PYTHON, job_id2);

  // Register 2 workers for each job.
  for (auto job_id : job_ids) {
    for (int i = 0; i < 2; i++) {
      auto worker =
          worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, job_id);
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
      auto task_spec = ExampleTaskSpec(/*actor_id=*/ActorID::Nil(), Language::PYTHON,
                                       job_id, actor_creation_id);
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

TEST_F(WorkerPoolTest, MaximumStartupConcurrency) {
  auto task_spec = ExampleTaskSpec();
  std::vector<Process> started_processes;

  // Try to pop some workers. Some worker processes will be started.
  for (int i = 0; i < MAXIMUM_STARTUP_CONCURRENCY; i++) {
    worker_pool_->PopWorker(task_spec,
                            [](const std::shared_ptr<WorkerInterface> worker,
                               PopWorkerStatus status) -> bool { return true; });
    auto last_process = worker_pool_->LastStartedWorkerProcess();
    RAY_CHECK(last_process.IsValid());
    started_processes.push_back(last_process);
  }

  // Can't start a new worker process at this point.
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkerProcessesStarting());
  worker_pool_->PopWorker(task_spec,
                          [](const std::shared_ptr<WorkerInterface> worker,
                             PopWorkerStatus status) -> bool { return true; });
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkerProcessesStarting());

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  // Call `RegisterWorker` to emulate worker registration.
  for (const auto &process : started_processes) {
    auto worker = worker_pool_->CreateWorker(Process());
    RAY_CHECK_OK(worker_pool_->RegisterWorker(worker, process.GetId(), process.GetId(),
                                              [](Status, int) {}));
    // Calling `RegisterWorker` won't affect the counter of starting worker processes.
    ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkerProcessesStarting());
    workers.push_back(worker);
  }

  // Can't start a new worker process at this point.
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkerProcessesStarting());
  worker_pool_->PopWorker(task_spec,
                          [](const std::shared_ptr<WorkerInterface> worker,
                             PopWorkerStatus status) -> bool { return true; });
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkerProcessesStarting());

  // Call `OnWorkerStarted` to emulate worker port announcement.
  for (size_t i = 0; i < workers.size(); i++) {
    worker_pool_->OnWorkerStarted(workers[i]);
    // Calling `OnWorkerStarted` will affect the counter of starting worker processes.
    ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY - i - 1,
              worker_pool_->NumWorkerProcessesStarting());
  }

  ASSERT_EQ(0, worker_pool_->NumWorkerProcessesStarting());
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolTest, HandleIOWorkersPushPop) {
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
  spill_workers.insert(CreateSpillWorker(Process::CreateNewDummy()));
  spill_workers.insert(CreateSpillWorker(Process::CreateNewDummy()));
  // Add the workers to the pool.
  // 2 pending tasks / 2 new idle workers.
  for (const auto &worker : spill_workers) {
    worker_pool_->PushSpillWorker(worker);
  }
  ASSERT_EQ(spill_pushed_worker.size(), 2);
  // Restore workers haven't pushed yet.
  ASSERT_EQ(restore_pushed_worker.size(), 0);

  // Create a new idle worker.
  spill_workers.insert(CreateSpillWorker(Process::CreateNewDummy()));
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
  restore_workers.insert(CreateRestoreWorker(Process::CreateNewDummy()));
  for (const auto &worker : restore_workers) {
    worker_pool_->PushRestoreWorker(worker);
  }
  ASSERT_EQ(restore_pushed_worker.size(), 1);
}

TEST_F(WorkerPoolTest, MaxIOWorkerSimpleTest) {
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
  // Make sure process size is not exceeding max io worker size.
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

TEST_F(WorkerPoolTest, MaxIOWorkerComplicateTest) {
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

TEST_F(WorkerPoolTest, MaxSpillRestoreWorkersIntegrationTest) {
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

TEST_F(WorkerPoolTest, DeleteWorkerPushPop) {
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

TEST_F(WorkerPoolTest, NoPopOnCrashedWorkerProcess) {
  // Start a Java worker process.
  PopWorkerStatus status;
  Process proc = worker_pool_->StartWorkerProcess(Language::JAVA, rpc::WorkerType::WORKER,
                                                  JOB_ID, &status);
  auto worker1 = worker_pool_->CreateWorker(Process(), Language::JAVA);
  auto worker2 = worker_pool_->CreateWorker(Process(), Language::JAVA);

  // We now imitate worker process crashing while core worker initializing.

  // 1. we register both workers.
  RAY_CHECK_OK(worker_pool_->RegisterWorker(worker1, proc.GetId(), proc.GetId(),
                                            [](Status, int) {}));
  RAY_CHECK_OK(worker_pool_->RegisterWorker(worker2, proc.GetId(), proc.GetId(),
                                            [](Status, int) {}));

  // 2. announce worker port for worker 1. When interacting with worker pool, it's
  // PushWorker.
  worker_pool_->PushWorker(worker1);

  // 3. kill the worker process. Now let's assume that Raylet found that the connection
  // with worker 1 disconnected first.
  worker_pool_->DisconnectWorker(
      worker1, /*disconnect_type=*/rpc::WorkerExitType::SYSTEM_ERROR_EXIT);

  // 4. but the RPC for announcing worker port for worker 2 is already in Raylet input
  // buffer. So now Raylet needs to handle worker 2.
  worker_pool_->PushWorker(worker2);

  // 5. Let's try to pop a worker to execute a task. Worker 2 shouldn't be popped because
  // the process has crashed.
  const auto task_spec = ExampleTaskSpec();
  ASSERT_NE(worker_pool_->PopWorkerSync(task_spec), worker1);
  ASSERT_NE(worker_pool_->PopWorkerSync(task_spec), worker2);

  // 6. Now Raylet disconnects with worker 2.
  worker_pool_->DisconnectWorker(
      worker2, /*disconnect_type=*/rpc::WorkerExitType::SYSTEM_ERROR_EXIT);
}

TEST_F(WorkerPoolTest, TestWorkerCapping) {
  auto job_id = JOB_ID;

  // The driver of job 1 is already registered. Here we register the driver for job 2.
  RegisterDriver(Language::PYTHON, job_id);

  ///
  /// Register 7 workers (2 more than soft limit).
  ///
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  int num_workers = POOL_SIZE_SOFT_LIMIT + 2;
  for (int i = 0; i < num_workers; i++) {
    PopWorkerStatus status;
    Process proc = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::WORKER, job_id, &status);
    auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON, job_id);
    workers.push_back(worker);
    RAY_CHECK_OK(worker_pool_->RegisterWorker(worker, proc.GetId(), proc.GetId(),
                                              [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
    worker_pool_->PushWorker(worker);
  }
  ///
  /// Pop 2 workers for a task and actor.
  ///
  // Pop workers for actor.
  std::vector<std::shared_ptr<WorkerInterface>> popped_workers;
  for (int i = 0; i < 2; i++) {
    auto actor_creation_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), i + 1);
    // Pop workers for actor creation tasks.
    auto task_spec = ExampleTaskSpec(/*actor_id=*/ActorID::Nil(), Language::PYTHON,
                                     job_id, actor_creation_id);
    auto worker = worker_pool_->PopWorkerSync(task_spec, false);
    popped_workers.push_back(worker);
    ASSERT_TRUE(worker);
    ASSERT_EQ(worker->GetAssignedJobId(), job_id);
  }
  // After scheduling an actor and task, there's no more idle worker.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers - 2);

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
  // Idle workers haven't been killed because the workers haven't replied yet.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers);

  // The first core worker exits, so one of idle workers should've been killed.
  // Since the idle workers are killed in FIFO, we can assume the first entry in the idle
  // workers will be killed.
  auto mock_rpc_client_it = mock_worker_rpc_clients_.find(
      worker_pool_->GetIdleWorkers().front().first->WorkerId());
  mock_rpc_client_it->second->ExitReplySucceed();
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers - 1);

  // The second core worker doesn't exit, meaning idle worker shouldn't have been killed.
  mock_rpc_client_it = mock_worker_rpc_clients_.find(
      worker_pool_->GetIdleWorkers().front().first->WorkerId());
  mock_rpc_client_it->second->ExitReplyFailed();
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers - 1);

  // Another 1000ms has passed, and we kill the idle worker again.
  worker_pool_->SetCurrentTimeMs(3000);
  worker_pool_->TryKillingIdleWorkers();

  // Make sure 1000ms has passed again, and it won't kill new worker because there's still
  // a pending exiting worker.
  worker_pool_->SetCurrentTimeMs(4000);
  worker_pool_->TryKillingIdleWorkers();
  mock_rpc_client_it = mock_worker_rpc_clients_.find(
      worker_pool_->GetIdleWorkers().back().first->WorkerId());
  ASSERT_FALSE(mock_rpc_client_it->second->ExitReplySucceed());

  // Now let's make sure the pending exiting workers exitted properly.
  mock_rpc_client_it = mock_worker_rpc_clients_.find(
      worker_pool_->GetIdleWorkers().front().first->WorkerId());
  mock_rpc_client_it->second->ExitReplySucceed();
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers - 2);

  // Now that we have the number of workers == soft limit, it shouldn't kill any idle
  // worker.
  worker_pool_->SetCurrentTimeMs(5000);
  worker_pool_->TryKillingIdleWorkers();
  mock_rpc_client_it = mock_worker_rpc_clients_.find(
      worker_pool_->GetIdleWorkers().front().first->WorkerId());
  ASSERT_FALSE(mock_rpc_client_it->second->ExitReplySucceed());

  // Start two IO workers. These don't count towards the limit.
  {
    PopWorkerStatus status;
    Process proc = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::SPILL_WORKER, job_id, &status);
    auto worker = CreateSpillWorker(Process());
    RAY_CHECK_OK(worker_pool_->RegisterWorker(worker, proc.GetId(), proc.GetId(),
                                              [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
    worker_pool_->PushSpillWorker(worker);
  }
  {
    PopWorkerStatus status;
    Process proc = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::RESTORE_WORKER, job_id, &status);
    auto worker = CreateRestoreWorker(Process());
    RAY_CHECK_OK(worker_pool_->RegisterWorker(worker, proc.GetId(), proc.GetId(),
                                              [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
    worker_pool_->PushRestoreWorker(worker);
  }
  // All workers still alive.
  worker_pool_->SetCurrentTimeMs(10000);
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers - 2);
  for (auto &worker : worker_pool_->GetIdleWorkers()) {
    mock_rpc_client_it = mock_worker_rpc_clients_.find(worker.first->WorkerId());
    ASSERT_FALSE(mock_rpc_client_it->second->ExitReplySucceed());
  }
  int num_callbacks = 0;
  auto callback = [&](std::shared_ptr<WorkerInterface> worker) { num_callbacks++; };
  worker_pool_->PopSpillWorker(callback);
  worker_pool_->PopRestoreWorker(callback);
  ASSERT_EQ(num_callbacks, 2);
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolTest, TestWorkerCappingLaterNWorkersNotOwningObjects) {
  ///
  /// When there are 2 * N idle workers where the first N workers own objects,
  /// make sure the later N workers are properly killed.
  ///
  auto job_id = JOB_ID;

  // The driver of job 1 is already registered. Here we register the driver for job 2.
  RegisterDriver(Language::PYTHON, job_id);

  ///
  /// Register 10 workers
  ///
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  int num_workers = POOL_SIZE_SOFT_LIMIT * 2;
  for (int i = 0; i < num_workers; i++) {
    PopWorkerStatus status;
    Process proc = worker_pool_->StartWorkerProcess(
        Language::PYTHON, rpc::WorkerType::WORKER, job_id, &status);
    auto worker = worker_pool_->CreateWorker(Process(), Language::PYTHON, job_id);
    workers.push_back(worker);
    RAY_CHECK_OK(worker_pool_->RegisterWorker(worker, proc.GetId(), proc.GetId(),
                                              [](Status, int) {}));
    worker_pool_->OnWorkerStarted(worker);
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
    worker_pool_->PushWorker(worker);
  }
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers);

  ///
  /// The first N workers will always failed to be killed because they own objects.
  ///
  // 2000 ms has passed, so idle workers should be killed.
  worker_pool_->SetCurrentTimeMs(1000);
  worker_pool_->TryKillingIdleWorkers();

  for (int i = 0; i < num_workers / 2; i++) {
    auto mock_rpc_client_it = mock_worker_rpc_clients_.find(workers[i]->WorkerId());
    ASSERT_TRUE(mock_rpc_client_it->second->ExitReplyFailed());
  }
  worker_pool_->TryKillingIdleWorkers();
  // None of first N workers are killed because they own objects.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers);

  ///
  /// After 1000ms, when it kills idle workers, it should kill the rest of them.
  ///
  worker_pool_->SetCurrentTimeMs(2000);
  worker_pool_->TryKillingIdleWorkers();
  for (int i = 0; i < num_workers / 2; i++) {
    auto mock_rpc_client_it = mock_worker_rpc_clients_.find(workers[i]->WorkerId());
    // These workers shouldn't get any Exit request.
    ASSERT_FALSE(mock_rpc_client_it->second->ExitReplyFailed());
  }
  for (int i = num_workers / 2; i < num_workers; i++) {
    auto mock_rpc_client_it = mock_worker_rpc_clients_.find(workers[i]->WorkerId());
    // These workers shouldn't get any Exit request.
    ASSERT_TRUE(mock_rpc_client_it->second->ExitReplySucceed());
  }
  worker_pool_->TryKillingIdleWorkers();
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), num_workers / 2);
}

TEST_F(WorkerPoolTest, PopWorkerWithRuntimeEnv) {
  ASSERT_EQ(worker_pool_->GetProcessSize(), 0);
  auto actor_creation_id = ActorID::Of(JOB_ID, TaskID::ForDriverTask(JOB_ID), 1);
  const auto actor_creation_task_spec =
      ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, JOB_ID, actor_creation_id,
                      {"XXX=YYY"}, TaskID::ForFakeTask(), "{\"uris\": \"XXX\"}");
  const auto normal_task_spec =
      ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, JOB_ID, ActorID::Nil(),
                      {"XXX=YYY"}, TaskID::ForFakeTask(), "{\"uris\": \"XXX\"}");
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

TEST_F(WorkerPoolTest, CacheWorkersByRuntimeEnvHash) {
  ///
  /// Check that a worker can be popped only if there is a
  /// worker available whose runtime env matches the runtime env
  /// in the task spec.
  ///
  ASSERT_EQ(worker_pool_->GetProcessSize(), 0);
  auto actor_creation_id = ActorID::Of(JOB_ID, TaskID::ForDriverTask(JOB_ID), 1);
  const auto actor_creation_task_spec_1 = ExampleTaskSpec(
      ActorID::Nil(), Language::PYTHON, JOB_ID, actor_creation_id,
      /*dynamic_options=*/{}, TaskID::ForFakeTask(), "mock_runtime_env_1");
  const auto task_spec_1 = ExampleTaskSpec(
      ActorID::Nil(), Language::PYTHON, JOB_ID, ActorID::Nil(),
      /*dynamic_options=*/{}, TaskID::ForFakeTask(), "mock_runtime_env_1");
  const auto task_spec_2 = ExampleTaskSpec(
      ActorID::Nil(), Language::PYTHON, JOB_ID, ActorID::Nil(),
      /*dynamic_options=*/{}, TaskID::ForFakeTask(), "mock_runtime_env_2");

  const WorkerCacheKey env1 = {
      /*override_environment_variables=*/{}, "mock_runtime_env_1", {}};
  const int runtime_env_hash_1 = env1.IntHash();

  // Push worker with runtime env 1.
  auto worker =
      worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, JOB_ID,
                                 rpc::WorkerType::WORKER, runtime_env_hash_1);
  worker_pool_->PushWorker(worker);

  // Try to pop worker for task with runtime env 2.
  auto popped_worker = worker_pool_->PopWorkerSync(task_spec_2);
  // Check that popped worker isn't the one we pushed.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_NE(popped_worker, worker);

  // Try to pop the worker for task with runtime env 1.
  popped_worker = worker_pool_->PopWorkerSync(task_spec_1);
  // Check that we got the pushed worker.
  ASSERT_EQ(popped_worker, worker);

  // Push another worker with runtime env 1.
  worker = worker_pool_->CreateWorker(Process::CreateNewDummy(), Language::PYTHON, JOB_ID,
                                      rpc::WorkerType::WORKER, runtime_env_hash_1);
  worker_pool_->PushWorker(worker);

  // Try to pop the worker for an actor with runtime env 1.
  popped_worker = worker_pool_->PopWorkerSync(actor_creation_task_spec_1);
  // Check that we got the pushed worker.
  ASSERT_EQ(popped_worker, worker);
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolTest, StartWorkWithDifferentShimPid) {
  auto task_spec = ExampleTaskSpec();
  auto worker = worker_pool_->PopWorkerSync(task_spec);
  ASSERT_NE(worker, nullptr);
  auto last_process = worker_pool_->LastStartedWorkerProcess();
  pid_t shim_pid = last_process.GetId();
  ASSERT_EQ(shim_pid, worker->GetShimProcess().GetId());

  // test dedicated worker
  std::vector<std::string> actor_jvm_options;
  actor_jvm_options.insert(
      actor_jvm_options.end(),
      {"-Dmy-actor.hello=foo", "-Dmy-actor.world=bar", "-Xmx2g", "-Xms1g"});
  auto task_id = TaskID::ForDriverTask(JOB_ID);
  auto actor_id = ActorID::Of(JOB_ID, task_id, 1);
  TaskSpecification java_task_spec = ExampleTaskSpec(
      ActorID::Nil(), Language::JAVA, JOB_ID, actor_id, actor_jvm_options, task_id);
  worker = worker_pool_->PopWorkerSync(java_task_spec);
  ASSERT_NE(worker, nullptr);
  last_process = worker_pool_->LastStartedWorkerProcess();
  shim_pid = last_process.GetId();
  ASSERT_EQ(shim_pid, worker->GetShimProcess().GetId());
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolTest, WorkerNoLeaks) {
  std::shared_ptr<WorkerInterface> popped_worker;
  const auto task_spec = ExampleTaskSpec();

  // Pop a worker and don't dispatch.
  worker_pool_->PopWorker(
      task_spec,
      [](const std::shared_ptr<WorkerInterface> worker, PopWorkerStatus status) -> bool {
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
  worker_pool_->PopWorker(
      task_spec,
      [](const std::shared_ptr<WorkerInterface> worker, PopWorkerStatus status) -> bool {
        // Don't dispatch this worker.
        return false;
      });
  // The worker is popped but not dispatched.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 1);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  // Pop a worker and dispatch.
  worker_pool_->PopWorker(
      task_spec,
      [](const std::shared_ptr<WorkerInterface> worker, PopWorkerStatus status) -> bool {
        // Dispatch this worker.
        return true;
      });
  // The worker is popped and dispatched.
  ASSERT_EQ(worker_pool_->GetIdleWorkerSize(), 0);
  ASSERT_EQ(worker_pool_->GetProcessSize(), 1);
  worker_pool_->ClearProcesses();
}

TEST_F(WorkerPoolTest, PopWorkerStatus) {
  std::shared_ptr<WorkerInterface> popped_worker;
  PopWorkerStatus status;

  /* Test PopWorkerStatus TooManyStartingWorkerProcesses */
  // Startup worker processes to maximum.
  for (int i = 0; i < MAXIMUM_STARTUP_CONCURRENCY; i++) {
    auto task_spec = ExampleTaskSpec();
    worker_pool_->PopWorker(task_spec,
                            [](const std::shared_ptr<WorkerInterface> worker,
                               PopWorkerStatus status) -> bool { return true; });
  }
  ASSERT_EQ(MAXIMUM_STARTUP_CONCURRENCY, worker_pool_->NumWorkerProcessesStarting());

  // PopWorker failed and the status is `TooManyStartingWorkerProcesses`.
  auto task_spec = ExampleTaskSpec();
  popped_worker = worker_pool_->PopWorkerSync(task_spec, false, &status);
  ASSERT_EQ(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::TooManyStartingWorkerProcesses);

  // PopWorker success after push workers and reduce the starting processes.
  worker_pool_->PushWorkers();
  ASSERT_EQ(0, worker_pool_->NumWorkerProcessesStarting());
  popped_worker = worker_pool_->PopWorkerSync(task_spec, true, &status);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::OK);

  /* Test PopWorkerStatus JobConfigMissing */
  // Create a task by unregistered job id.
  auto job_id = JobID::FromInt(123);
  task_spec = ExampleTaskSpec(ActorID::Nil(), Language::JAVA, job_id);
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
      ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, job_id, ActorID::Nil(),
                      {"XXX=YYY"}, TaskID::ForFakeTask(), BAD_RUNTIME_ENV);
  popped_worker =
      worker_pool_->PopWorkerSync(task_spec_with_bad_runtime_env, true, &status);
  // PopWorker failed and the status is `RuntimeEnvCreationFailed`.
  ASSERT_EQ(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::RuntimeEnvCreationFailed);

  // Create a task with available runtime env.
  const auto task_spec_with_runtime_env =
      ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, job_id, ActorID::Nil(),
                      {"XXX=YYY"}, TaskID::ForFakeTask(), "{\"uris\": \"XXX\"}");
  popped_worker = worker_pool_->PopWorkerSync(task_spec_with_runtime_env, true, &status);
  // PopWorker success.
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_EQ(status, PopWorkerStatus::OK);

  /* Test PopWorkerStatus RuntimeEnvCreationFailed */
  // Create a task without push worker.
  popped_worker = worker_pool_->PopWorkerSync(task_spec, false, &status);
  ASSERT_EQ(popped_worker, nullptr);
  // PopWorker failed while the timer was triggered and the status is
  // `RuntimeEnvCreationFailed`.
  ASSERT_EQ(status, PopWorkerStatus::WorkerPendingRegistration);
  worker_pool_->ClearProcesses();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
