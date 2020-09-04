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
#include "ray/common/constants.h"
#include "ray/raylet/node_manager.h"
#include "ray/util/process.h"

namespace ray {

namespace raylet {

int NUM_WORKERS_PER_PROCESS_JAVA = 3;
int MAXIMUM_STARTUP_CONCURRENCY = 5;
JobID JOB_ID = JobID::FromInt(1);

std::vector<Language> LANGUAGES = {Language::PYTHON, Language::JAVA};

class WorkerPoolMock : public WorkerPool {
 public:
  explicit WorkerPoolMock(boost::asio::io_service &io_service,
                          const WorkerCommandMap &worker_commands)
      : WorkerPool(io_service, 0, 0, 0, MAXIMUM_STARTUP_CONCURRENCY, 0, 0, nullptr,
                   worker_commands, {}, []() {}),
        last_worker_process_() {
    states_by_lang_[ray::Language::JAVA].num_workers_per_process =
        NUM_WORKERS_PER_PROCESS_JAVA;
  }

  ~WorkerPoolMock() {
    // Avoid killing real processes
    states_by_lang_.clear();
  }

  using WorkerPool::StartWorkerProcess;  // we need this to be public for testing

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
        total += process_entry.second;
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

 private:
  Process last_worker_process_;
  // The worker commands by process.
  std::unordered_map<Process, std::vector<std::string>> worker_commands_by_proc_;
};

class WorkerPoolTest : public ::testing::TestWithParam<bool> {
 public:
  WorkerPoolTest() : error_message_type_(1), client_call_manager_(io_service_) {
    bool enable_multi_tenancy = GetParam();
    RayConfig::instance().initialize(
        {{"enable_multi_tenancy", std::to_string(enable_multi_tenancy)},
         {"num_workers_per_process_java", std::to_string(NUM_WORKERS_PER_PROCESS_JAVA)}});
    SetWorkerCommands(
        {{Language::PYTHON, {"dummy_py_worker_command"}},
         {Language::JAVA,
          {"dummy_java_worker_command", "RAY_WORKER_RAYLET_CONFIG_PLACEHOLDER"}}});
  }

  std::shared_ptr<WorkerInterface> CreateWorker(
      Process proc, const Language &language = Language::PYTHON,
      const JobID &job_id = JOB_ID) {
    std::function<void(ClientConnection &)> client_handler =
        [this](ClientConnection &client) { HandleNewClient(client); };
    std::function<void(std::shared_ptr<ClientConnection>, int64_t,
                       const std::vector<uint8_t> &)>
        message_handler = [this](std::shared_ptr<ClientConnection> client,
                                 int64_t message_type,
                                 const std::vector<uint8_t> &message) {
          HandleMessage(client, message_type, message);
        };
    local_stream_socket socket(io_service_);
    auto client =
        ClientConnection::Create(client_handler, message_handler, std::move(socket),
                                 "worker", {}, error_message_type_);
    std::shared_ptr<Worker> worker_ = std::make_shared<Worker>(
        WorkerID::FromRandom(), language, rpc::WorkerType::WORKER, "127.0.0.1", client,
        client_call_manager_);
    std::shared_ptr<WorkerInterface> worker =
        std::dynamic_pointer_cast<WorkerInterface>(worker_);
    worker->AssignJobId(job_id);
    if (!proc.IsNull()) {
      worker->SetProcess(proc);
    }
    return worker;
  }

  std::shared_ptr<WorkerInterface> RegisterDriver(
      const Language &language = Language::PYTHON, const JobID &job_id = JOB_ID,
      const rpc::JobConfig &job_config = rpc::JobConfig()) {
    auto driver = CreateWorker(Process::CreateNewDummy(), Language::PYTHON, job_id);
    driver->AssignTaskId(TaskID::ForDriverTask(job_id));
    RAY_CHECK_OK(worker_pool_->RegisterDriver(driver, job_id, job_config, nullptr));
    return driver;
  }

  void SetWorkerCommands(const WorkerCommandMap &worker_commands) {
    worker_pool_ =
        std::unique_ptr<WorkerPoolMock>(new WorkerPoolMock(io_service_, worker_commands));
    rpc::JobConfig job_config;
    job_config.set_num_java_workers_per_process(NUM_WORKERS_PER_PROCESS_JAVA);
    RegisterDriver(Language::PYTHON, JOB_ID, job_config);
  }

  void TestStartupWorkerProcessCount(Language language, int num_workers_per_process,
                                     std::vector<std::string> expected_worker_command) {
    int desired_initial_worker_process_count = 100;
    int expected_worker_process_count = static_cast<int>(std::ceil(
        static_cast<double>(MAXIMUM_STARTUP_CONCURRENCY) / num_workers_per_process));
    ASSERT_TRUE(expected_worker_process_count <
                static_cast<int>(desired_initial_worker_process_count));
    Process last_started_worker_process;
    for (int i = 0; i < desired_initial_worker_process_count; i++) {
      worker_pool_->StartWorkerProcess(language, rpc::WorkerType::WORKER, JOB_ID);
      ASSERT_TRUE(worker_pool_->NumWorkerProcessesStarting() <=
                  expected_worker_process_count);
      Process prev = worker_pool_->LastStartedWorkerProcess();
      if (!std::equal_to<Process>()(last_started_worker_process, prev)) {
        last_started_worker_process = prev;
        const auto &real_command =
            worker_pool_->GetWorkerCommand(last_started_worker_process);
        ASSERT_EQ(real_command, expected_worker_command);
      } else {
        ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(),
                  expected_worker_process_count);
        ASSERT_TRUE(i >= expected_worker_process_count);
      }
    }
    // Check number of starting workers
    ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), expected_worker_process_count);
  }

 protected:
  boost::asio::io_service io_service_;
  std::unique_ptr<WorkerPoolMock> worker_pool_;
  int64_t error_message_type_;
  rpc::ClientCallManager client_call_manager_;

 private:
  void HandleNewClient(ClientConnection &){};
  void HandleMessage(std::shared_ptr<ClientConnection>, int64_t,
                     const std::vector<uint8_t> &){};
};

static inline TaskSpecification ExampleTaskSpec(
    const ActorID actor_id = ActorID::Nil(), const Language &language = Language::PYTHON,
    const JobID &job_id = JOB_ID, const ActorID actor_creation_id = ActorID::Nil(),
    const std::vector<std::string> &dynamic_worker_options = {}) {
  rpc::TaskSpec message;
  message.set_job_id(job_id.Binary());
  message.set_language(language);
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
  return TaskSpecification(std::move(message));
}

static inline std::string GetNumJavaWorkersPerProcessSystemProperty(int num) {
  std::string key;
  if (RayConfig::instance().enable_multi_tenancy()) {
    key = "ray.job.num-java-workers-per-process";
  } else {
    key = "ray.raylet.config.num_workers_per_process_java";
  }
  return std::string("-D") + key + "=" + std::to_string(num);
}

TEST_P(WorkerPoolTest, CompareWorkerProcessObjects) {
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

TEST_P(WorkerPoolTest, HandleWorkerRegistration) {
  Process proc =
      worker_pool_->StartWorkerProcess(Language::JAVA, rpc::WorkerType::WORKER, JOB_ID);
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  for (int i = 0; i < NUM_WORKERS_PER_PROCESS_JAVA; i++) {
    workers.push_back(CreateWorker(Process(), Language::JAVA));
  }
  for (const auto &worker : workers) {
    // Check that there's still a starting worker process
    // before all workers have been registered
    ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 1);
    // Check that we cannot lookup the worker before it's registered.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), nullptr);
    RAY_CHECK_OK(worker_pool_->RegisterWorker(worker, proc.GetId(), nullptr));
    // Check that we can lookup the worker after it's registered.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), worker);
  }
  // Check that there's no starting worker process
  ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 0);
  for (const auto &worker : workers) {
    worker_pool_->DisconnectWorker(worker);
    // Check that we cannot lookup the worker after it's disconnected.
    ASSERT_EQ(worker_pool_->GetRegisteredWorker(worker->Connection()), nullptr);
  }
}

TEST_P(WorkerPoolTest, StartupPythonWorkerProcessCount) {
  TestStartupWorkerProcessCount(Language::PYTHON, 1, {"dummy_py_worker_command"});
}

TEST_P(WorkerPoolTest, StartupJavaWorkerProcessCount) {
  TestStartupWorkerProcessCount(
      Language::JAVA, NUM_WORKERS_PER_PROCESS_JAVA,
      {"dummy_java_worker_command",
       GetNumJavaWorkersPerProcessSystemProperty(NUM_WORKERS_PER_PROCESS_JAVA)});
}

TEST_P(WorkerPoolTest, InitialWorkerProcessCount) {
  if (!RayConfig::instance().enable_multi_tenancy()) {
    worker_pool_->Start(1);
    // Here we try to start only 1 worker for each worker language. But since each Java
    // worker process contains exactly NUM_WORKERS_PER_PROCESS_JAVA (3) workers here,
    // it's expected to see 3 workers for Java and 1 worker for Python, instead of 1 for
    // each worker language.
    ASSERT_NE(worker_pool_->NumWorkersStarting(), 1 * LANGUAGES.size());
    ASSERT_EQ(worker_pool_->NumWorkersStarting(), 1 + NUM_WORKERS_PER_PROCESS_JAVA);
    ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), LANGUAGES.size());
  } else {
    ASSERT_EQ(worker_pool_->NumWorkersStarting(), 0);
    ASSERT_EQ(worker_pool_->NumWorkerProcessesStarting(), 0);
  }
}

TEST_P(WorkerPoolTest, HandleWorkerPushPop) {
  // Try to pop a worker from the empty pool and make sure we don't get one.
  std::shared_ptr<WorkerInterface> popped_worker;
  const auto task_spec = ExampleTaskSpec();
  popped_worker = worker_pool_->PopWorker(task_spec);
  ASSERT_EQ(popped_worker, nullptr);

  // Create some workers.
  std::unordered_set<std::shared_ptr<WorkerInterface>> workers;
  workers.insert(CreateWorker(Process::CreateNewDummy()));
  workers.insert(CreateWorker(Process::CreateNewDummy()));
  // Add the workers to the pool.
  for (auto &worker : workers) {
    worker_pool_->PushWorker(worker);
  }

  // Pop two workers and make sure they're one of the workers we created.
  popped_worker = worker_pool_->PopWorker(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_->PopWorker(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_->PopWorker(task_spec);
  ASSERT_EQ(popped_worker, nullptr);
}

TEST_P(WorkerPoolTest, PopActorWorker) {
  // Create a worker.
  auto worker = CreateWorker(Process::CreateNewDummy());
  // Add the worker to the pool.
  worker_pool_->PushWorker(worker);

  // Assign an actor ID to the worker.
  const auto task_spec = ExampleTaskSpec();
  auto actor = worker_pool_->PopWorker(task_spec);
  auto actor_id = ActorID::Of(JOB_ID, TaskID::ForDriverTask(JOB_ID), 1);
  actor->AssignActorId(actor_id);
  worker_pool_->PushWorker(actor);

  // Check that there are no more non-actor workers.
  ASSERT_EQ(worker_pool_->PopWorker(task_spec), nullptr);
  // Check that we can pop the actor worker.
  const auto actor_task_spec = ExampleTaskSpec(actor_id);
  actor = worker_pool_->PopWorker(actor_task_spec);
  ASSERT_EQ(actor, worker);
  ASSERT_EQ(actor->GetActorId(), actor_id);
}

TEST_P(WorkerPoolTest, PopWorkersOfMultipleLanguages) {
  // Create a Python Worker, and add it to the pool
  auto py_worker = CreateWorker(Process::CreateNewDummy(), Language::PYTHON);
  worker_pool_->PushWorker(py_worker);
  // Check that no worker will be popped if the given task is a Java task
  const auto java_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::JAVA);
  ASSERT_EQ(worker_pool_->PopWorker(java_task_spec), nullptr);
  // Check that the worker can be popped if the given task is a Python task
  const auto py_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON);
  ASSERT_NE(worker_pool_->PopWorker(py_task_spec), nullptr);

  // Create a Java Worker, and add it to the pool
  auto java_worker = CreateWorker(Process::CreateNewDummy(), Language::JAVA);
  worker_pool_->PushWorker(java_worker);
  // Check that the worker will be popped now for Java task
  ASSERT_NE(worker_pool_->PopWorker(java_task_spec), nullptr);
}

TEST_P(WorkerPoolTest, StartWorkerWithDynamicOptionsCommand) {
  const std::vector<std::string> java_worker_command = {
      "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER", "dummy_java_worker_command",
      "RAY_WORKER_RAYLET_CONFIG_PLACEHOLDER"};
  SetWorkerCommands({{Language::PYTHON, {"dummy_py_worker_command"}},
                     {Language::JAVA, java_worker_command}});

  TaskSpecification task_spec = ExampleTaskSpec(
      ActorID::Nil(), Language::JAVA, JOB_ID,
      ActorID::Of(JOB_ID, TaskID::ForDriverTask(JOB_ID), 1), {"test_op_0", "test_op_1"});
  worker_pool_->StartWorkerProcess(Language::JAVA, rpc::WorkerType::WORKER, JOB_ID,
                                   task_spec.DynamicWorkerOptions());
  const auto real_command =
      worker_pool_->GetWorkerCommand(worker_pool_->LastStartedWorkerProcess());
  ASSERT_EQ(real_command, std::vector<std::string>(
                              {"test_op_0", "test_op_1", "dummy_java_worker_command",
                               GetNumJavaWorkersPerProcessSystemProperty(1)}));
}

TEST_P(WorkerPoolTest, PopWorkerMultiTenancy) {
  if (!RayConfig::instance().enable_multi_tenancy()) {
    return;
  }

  auto job_id1 = JOB_ID;
  auto job_id2 = JobID::FromInt(2);
  ASSERT_NE(job_id1, job_id2);
  JobID job_ids[] = {job_id1, job_id2};

  // The driver of job 1 is already registered. Here we register the driver for job 2.
  RegisterDriver(Language::PYTHON, job_id2);

  // Register 2 workers for each job.
  for (auto job_id : job_ids) {
    for (int i = 0; i < 2; i++) {
      auto worker = CreateWorker(Process::CreateNewDummy(), Language::PYTHON, job_id);
      worker_pool_->PushWorker(worker);
    }
  }

  std::unordered_set<WorkerID> worker_ids;
  for (int round = 0; round < 2; round++) {
    std::vector<std::shared_ptr<WorkerInterface>> workers;

    // Pop workers for actor (creation) tasks.
    for (auto job_id : job_ids) {
      auto actor_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1);
      // For the first round, we pop for actor creation tasks.
      // For the second round, we pop for actor tasks.
      auto task_spec =
          ExampleTaskSpec(round == 0 ? ActorID::Nil() : actor_id, Language::PYTHON,
                          job_id, round == 0 ? actor_id : ActorID::Nil());
      auto worker = worker_pool_->PopWorker(task_spec);
      ASSERT_TRUE(worker);
      ASSERT_EQ(worker->GetAssignedJobId(), job_id);
      if (round == 0) {
        worker->AssignActorId(actor_id);
      }
      workers.push_back(worker);
    }

    // Pop workers for normal tasks.
    for (auto job_id : job_ids) {
      auto task_spec = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON, job_id);
      auto worker = worker_pool_->PopWorker(task_spec);
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

INSTANTIATE_TEST_CASE_P(WorkerPoolMultiTenancyTest, WorkerPoolTest,
                        ::testing::Values(true, false));

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
