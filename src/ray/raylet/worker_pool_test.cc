#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/constants.h"
#include "ray/raylet/node_manager.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

int NUM_WORKERS_PER_PROCESS = 3;
int MAXIMUM_STARTUP_CONCURRENCY = 5;

class WorkerPoolMock : public WorkerPool {
 public:
  WorkerPoolMock()
      : WorkerPoolMock({{Language::PYTHON, {"dummy_py_worker_command"}},
                        {Language::JAVA, {"dummy_java_worker_command"}}}) {}

  explicit WorkerPoolMock(const WorkerCommandMap &worker_commands)
      : WorkerPool(0, NUM_WORKERS_PER_PROCESS, MAXIMUM_STARTUP_CONCURRENCY, nullptr,
                   worker_commands),
        last_worker_pid_(0) {}

  ~WorkerPoolMock() {
    // Avoid killing real processes
    states_by_lang_.clear();
  }

  void StartWorkerProcess(const Language &language,
                          const std::vector<std::string> &dynamic_options = {}) {
    WorkerPool::StartWorkerProcess(language, dynamic_options);
  }

  pid_t StartProcess(const std::vector<std::string> &worker_command_args) override {
    last_worker_pid_ += 1;
    worker_commands_by_pid[last_worker_pid_] = worker_command_args;
    return last_worker_pid_;
  }

  void WarnAboutSize() override {}

  pid_t LastStartedWorkerProcess() const { return last_worker_pid_; }

  const std::vector<std::string> &GetWorkerCommand(int pid) {
    return worker_commands_by_pid[pid];
  }

  int NumWorkerProcessesStarting() const {
    int total = 0;
    for (auto &entry : states_by_lang_) {
      total += entry.second.starting_worker_processes.size();
    }
    return total;
  }

 private:
  int last_worker_pid_;
  // The worker commands by pid.
  std::unordered_map<int, std::vector<std::string>> worker_commands_by_pid;
};

class WorkerPoolTest : public ::testing::Test {
 public:
  WorkerPoolTest()
      : worker_pool_(),
        io_service_(),
        error_message_type_(1),
        client_call_manager_(io_service_) {}

  std::shared_ptr<Worker> CreateWorker(pid_t pid,
                                       const Language &language = Language::PYTHON) {
    std::function<void(LocalClientConnection &)> client_handler =
        [this](LocalClientConnection &client) { HandleNewClient(client); };
    std::function<void(std::shared_ptr<LocalClientConnection>, int64_t, const uint8_t *)>
        message_handler = [this](std::shared_ptr<LocalClientConnection> client,
                                 int64_t message_type, const uint8_t *message) {
          HandleMessage(client, message_type, message);
        };
    boost::asio::local::stream_protocol::socket socket(io_service_);
    auto client =
        LocalClientConnection::Create(client_handler, message_handler, std::move(socket),
                                      "worker", {}, error_message_type_);
    return std::shared_ptr<Worker>(new Worker(WorkerID::FromRandom(), pid, language, -1,
                                              client, client_call_manager_));
  }

  void SetWorkerCommands(const WorkerCommandMap &worker_commands) {
    WorkerPoolMock worker_pool(worker_commands);
    this->worker_pool_ = std::move(worker_pool);
  }

 protected:
  WorkerPoolMock worker_pool_;
  boost::asio::io_service io_service_;
  int64_t error_message_type_;
  rpc::ClientCallManager client_call_manager_;

 private:
  void HandleNewClient(LocalClientConnection &){};
  void HandleMessage(std::shared_ptr<LocalClientConnection>, int64_t, const uint8_t *){};
};

static inline TaskSpecification ExampleTaskSpec(
    const ActorID actor_id = ActorID::Nil(), const Language &language = Language::PYTHON,
    const ActorID actor_creation_id = ActorID::Nil(),
    const std::vector<std::string> &dynamic_worker_options = {}) {
  rpc::TaskSpec message;
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

TEST_F(WorkerPoolTest, HandleWorkerRegistration) {
  worker_pool_.StartWorkerProcess(Language::PYTHON);
  pid_t pid = worker_pool_.LastStartedWorkerProcess();
  std::vector<std::shared_ptr<Worker>> workers;
  for (int i = 0; i < NUM_WORKERS_PER_PROCESS; i++) {
    workers.push_back(CreateWorker(pid));
  }
  for (const auto &worker : workers) {
    // Check that there's still a starting worker process
    // before all workers have been registered
    ASSERT_EQ(worker_pool_.NumWorkerProcessesStarting(), 1);
    // Check that we cannot lookup the worker before it's registered.
    ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), nullptr);
    worker_pool_.RegisterWorker(worker);
    // Check that we can lookup the worker after it's registered.
    ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), worker);
  }
  // Check that there's no starting worker process
  ASSERT_EQ(worker_pool_.NumWorkerProcessesStarting(), 0);
  for (const auto &worker : workers) {
    worker_pool_.DisconnectWorker(worker);
    // Check that we cannot lookup the worker after it's disconnected.
    ASSERT_EQ(worker_pool_.GetRegisteredWorker(worker->Connection()), nullptr);
  }
}

TEST_F(WorkerPoolTest, StartupWorkerCount) {
  int desired_initial_worker_count_per_language = 20;
  for (int i = 0; i < desired_initial_worker_count_per_language; i++) {
    worker_pool_.StartWorkerProcess(Language::PYTHON);
    worker_pool_.StartWorkerProcess(Language::JAVA);
  }
  // Check that number of starting worker processes equals to
  // maximum_startup_concurrency_ * 2. (because we started both python and java workers)
  ASSERT_EQ(
      worker_pool_.NumWorkerProcessesStarting(),
      /* Provided in constructor of WorkerPoolMock */ MAXIMUM_STARTUP_CONCURRENCY * 2);
}

TEST_F(WorkerPoolTest, HandleWorkerPushPop) {
  // Try to pop a worker from the empty pool and make sure we don't get one.
  std::shared_ptr<Worker> popped_worker;
  const auto task_spec = ExampleTaskSpec();
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_EQ(popped_worker, nullptr);

  // Create some workers.
  std::unordered_set<std::shared_ptr<Worker>> workers;
  workers.insert(CreateWorker(1234));
  workers.insert(CreateWorker(5678));
  // Add the workers to the pool.
  for (auto &worker : workers) {
    worker_pool_.PushWorker(worker);
  }

  // Pop two workers and make sure they're one of the workers we created.
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_NE(popped_worker, nullptr);
  ASSERT_TRUE(workers.count(popped_worker) > 0);
  popped_worker = worker_pool_.PopWorker(task_spec);
  ASSERT_EQ(popped_worker, nullptr);
}

TEST_F(WorkerPoolTest, PopActorWorker) {
  // Create a worker.
  auto worker = CreateWorker(1234);
  // Add the worker to the pool.
  worker_pool_.PushWorker(worker);

  // Assign an actor ID to the worker.
  const auto task_spec = ExampleTaskSpec();
  auto actor = worker_pool_.PopWorker(task_spec);
  const auto job_id = JobID::FromInt(1);
  auto actor_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1);
  actor->AssignActorId(actor_id);
  worker_pool_.PushWorker(actor);

  // Check that there are no more non-actor workers.
  ASSERT_EQ(worker_pool_.PopWorker(task_spec), nullptr);
  // Check that we can pop the actor worker.
  const auto actor_task_spec = ExampleTaskSpec(actor_id);
  actor = worker_pool_.PopWorker(actor_task_spec);
  ASSERT_EQ(actor, worker);
  ASSERT_EQ(actor->GetActorId(), actor_id);
}

TEST_F(WorkerPoolTest, PopWorkersOfMultipleLanguages) {
  // Create a Python Worker, and add it to the pool
  auto py_worker = CreateWorker(1234, Language::PYTHON);
  worker_pool_.PushWorker(py_worker);
  // Check that no worker will be popped if the given task is a Java task
  const auto java_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::JAVA);
  ASSERT_EQ(worker_pool_.PopWorker(java_task_spec), nullptr);
  // Check that the worker can be popped if the given task is a Python task
  const auto py_task_spec = ExampleTaskSpec(ActorID::Nil(), Language::PYTHON);
  ASSERT_NE(worker_pool_.PopWorker(py_task_spec), nullptr);

  // Create a Java Worker, and add it to the pool
  auto java_worker = CreateWorker(1234, Language::JAVA);
  worker_pool_.PushWorker(java_worker);
  // Check that the worker will be popped now for Java task
  ASSERT_NE(worker_pool_.PopWorker(java_task_spec), nullptr);
}

TEST_F(WorkerPoolTest, StartWorkerWithDynamicOptionsCommand) {
  const std::vector<std::string> java_worker_command = {
      "RAY_WORKER_OPTION_0", "dummy_java_worker_command", "RAY_WORKER_OPTION_1"};
  SetWorkerCommands({{Language::PYTHON, {"dummy_py_worker_command"}},
                     {Language::JAVA, java_worker_command}});

  const auto job_id = JobID::FromInt(1);
  TaskSpecification task_spec = ExampleTaskSpec(
      ActorID::Nil(), Language::JAVA,
      ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1), {"test_op_0", "test_op_1"});
  worker_pool_.StartWorkerProcess(Language::JAVA, task_spec.DynamicWorkerOptions());
  const auto real_command =
      worker_pool_.GetWorkerCommand(worker_pool_.LastStartedWorkerProcess());
  ASSERT_EQ(real_command, std::vector<std::string>(
                              {"test_op_0", "dummy_java_worker_command", "test_op_1"}));
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
