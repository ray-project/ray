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

#include <algorithm>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "ray/common/constants.h"
#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace {

// A helper function to get a worker from a list.
std::shared_ptr<ray::raylet::WorkerInterface> GetWorker(
    const std::unordered_set<std::shared_ptr<ray::raylet::WorkerInterface>> &worker_pool,
    const std::shared_ptr<ray::ClientConnection> &connection) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if ((*it)->Connection() == connection) {
      return (*it);
    }
  }
  return nullptr;
}

// A helper function to remove a worker from a list. Returns true if the worker
// was found and removed.
bool RemoveWorker(
    std::unordered_set<std::shared_ptr<ray::raylet::WorkerInterface>> &worker_pool,
    const std::shared_ptr<ray::raylet::WorkerInterface> &worker) {
  return worker_pool.erase(worker) > 0;
}

}  // namespace

namespace ray {

namespace raylet {

WorkerPool::WorkerPool(boost::asio::io_service &io_service, int num_workers_soft_limit,
                       int num_initial_python_workers_for_first_job,
                       int maximum_startup_concurrency, int min_worker_port,
                       int max_worker_port, const std::vector<int> &worker_ports,
                       std::shared_ptr<gcs::GcsClient> gcs_client,
                       const WorkerCommandMap &worker_commands,
                       const std::unordered_map<std::string, std::string> &raylet_config,
                       std::function<void()> starting_worker_timeout_callback)
    : io_service_(&io_service),
      num_workers_soft_limit_(num_workers_soft_limit),
      maximum_startup_concurrency_(maximum_startup_concurrency),
      gcs_client_(std::move(gcs_client)),
      raylet_config_(raylet_config),
      starting_worker_timeout_callback_(starting_worker_timeout_callback),
      first_job_registered_python_worker_count_(0),
      first_job_driver_wait_num_python_workers_(std::min(
          num_initial_python_workers_for_first_job, maximum_startup_concurrency)),
      num_initial_python_workers_for_first_job_(num_initial_python_workers_for_first_job),
      kill_idle_workers_timer_(io_service) {
  RAY_CHECK(maximum_startup_concurrency > 0);
#ifndef _WIN32
  // Ignore SIGCHLD signals. If we don't do this, then worker processes will
  // become zombies instead of dying gracefully.
  signal(SIGCHLD, SIG_IGN);
#endif
  for (const auto &entry : worker_commands) {
    // Initialize the pool state for this language.
    auto &state = states_by_lang_[entry.first];
    state.multiple_for_warning = maximum_startup_concurrency;
    // Set worker command for this language.
    state.worker_command = entry.second;
    RAY_CHECK(!state.worker_command.empty()) << "Worker command must not be empty.";
  }
  // Initialize free ports list with all ports in the specified range.
  if (!worker_ports.empty()) {
    free_ports_ = std::unique_ptr<std::queue<int>>(new std::queue<int>());
    for (int port : worker_ports) {
      free_ports_->push(port);
    }
  } else if (min_worker_port != 0) {
    if (max_worker_port == 0) {
      max_worker_port = 65535;  // Maximum valid port number.
    }
    RAY_CHECK(min_worker_port > 0 && min_worker_port <= 65535);
    RAY_CHECK(max_worker_port >= min_worker_port && max_worker_port <= 65535);
    free_ports_ = std::unique_ptr<std::queue<int>>(new std::queue<int>());
    for (int port = min_worker_port; port <= max_worker_port; port++) {
      free_ports_->push(port);
    }
  }
  ScheduleIdleWorkerKilling();
}

WorkerPool::~WorkerPool() {
  std::unordered_set<Process> procs_to_kill;
  for (const auto &entry : states_by_lang_) {
    // Kill all registered workers. NOTE(swang): This assumes that the registered
    // workers were started by the pool.
    for (const auto &worker : entry.second.registered_workers) {
      procs_to_kill.insert(worker->GetProcess());
    }
    // Kill all the workers that have been started but not registered.
    for (const auto &starting_worker : entry.second.starting_worker_processes) {
      procs_to_kill.insert(starting_worker.first);
    }
  }
  for (Process proc : procs_to_kill) {
    proc.Kill();
    // NOTE: Avoid calling Wait() here. It fails with ECHILD, as SIGCHLD is disabled.
  }
}

Process WorkerPool::StartWorkerProcess(
    const Language &language, const rpc::WorkerType worker_type, const JobID &job_id,
    std::vector<std::string> dynamic_options,
    std::unordered_map<std::string, std::string> override_environment_variables) {
  rpc::JobConfig *job_config = nullptr;
  if (!IsIOWorkerType(worker_type)) {
    RAY_CHECK(!job_id.IsNil());
    auto it = all_jobs_.find(job_id);
    if (it == all_jobs_.end()) {
      RAY_LOG(DEBUG) << "Job config of job " << job_id << " are not local yet.";
      // Will reschedule ready tasks in `NodeManager::HandleJobStarted`.
      return Process();
    }
    job_config = &it->second;
  }

  auto &state = GetStateForLanguage(language);
  // If we are already starting up too many workers, then return without starting
  // more.
  int starting_workers = 0;
  for (auto &entry : state.starting_worker_processes) {
    starting_workers += entry.second;
  }

  // Here we consider both task workers and I/O workers.
  if (starting_workers >= maximum_startup_concurrency_) {
    // Workers have been started, but not registered. Force start disabled -- returning.
    RAY_LOG(DEBUG) << "Worker not started, " << starting_workers
                   << " workers of language type " << static_cast<int>(language)
                   << " pending registration";
    return Process();
  }
  // Either there are no workers pending registration or the worker start is being forced.
  RAY_LOG(DEBUG) << "Starting new worker process, current pool has "
                 << state.idle_actor.size() << " actor workers, and " << state.idle.size()
                 << " non-actor workers";

  int workers_to_start = 1;
  if (dynamic_options.empty()) {
    if (language == Language::JAVA) {
      workers_to_start = job_config->num_java_workers_per_process();
    }
  }

  if (job_config) {
    // Note that we push the item to the front of the vector to make
    // sure this is the freshest option than others.
    if (!job_config->jvm_options().empty()) {
      dynamic_options.insert(dynamic_options.begin(), job_config->jvm_options().begin(),
                             job_config->jvm_options().end());
    }

    std::string code_search_path_str;
    for (int i = 0; i < job_config->code_search_path_size(); i++) {
      auto path = job_config->code_search_path(i);
      if (i != 0) {
        code_search_path_str += ":";
      }
      code_search_path_str += path;
    }
    if (!code_search_path_str.empty()) {
      switch (language) {
      case Language::PYTHON: {
        code_search_path_str = "--code-search-path=" + code_search_path_str;
        break;
      }
      case Language::JAVA: {
        code_search_path_str = "-Dray.job.code-search-path=" + code_search_path_str;
        break;
      }
      default:
        RAY_LOG(FATAL) << "code_search_path is not supported for worker language "
                       << language;
      }
      dynamic_options.push_back(code_search_path_str);
    }
  }

  // Extract pointers from the worker command to pass into execvp.
  std::vector<std::string> worker_command_args;
  bool worker_raylet_config_placeholder_found = false;
  for (auto const &token : state.worker_command) {
    if (token == kWorkerDynamicOptionPlaceholder) {
      for (const auto &dynamic_option : dynamic_options) {
        auto options = ParseCommandLine(dynamic_option);
        worker_command_args.insert(worker_command_args.end(), options.begin(),
                                   options.end());
      }
      continue;
    }

    if (token == kWorkerRayletConfigPlaceholder) {
      worker_raylet_config_placeholder_found = true;
      switch (language) {
      case Language::JAVA:
        for (auto &entry : raylet_config_) {
          std::string arg;
          arg.append("-Dray.raylet.config.");
          arg.append(entry.first);
          arg.append("=");
          arg.append(entry.second);
          worker_command_args.push_back(arg);
        }
        worker_command_args.push_back("-Dray.job.num-java-workers-per-process=" +
                                      std::to_string(workers_to_start));
        break;
      default:
        RAY_LOG(FATAL)
            << "Raylet config placeholder is not supported for worker language "
            << language;
      }
      continue;
    }

    worker_command_args.push_back(token);
  }

  // Currently only Java worker process supports multi-worker.
  if (language == Language::JAVA) {
    RAY_CHECK(worker_raylet_config_placeholder_found)
        << "The " << kWorkerRayletConfigPlaceholder
        << " placeholder is not found in worker command.";
  } else if (language == Language::PYTHON) {
    RAY_CHECK(worker_type == rpc::WorkerType::WORKER || IsIOWorkerType(worker_type));
    if (IsIOWorkerType(worker_type)) {
      // Without "--worker-type", by default the worker type is rpc::WorkerType::WORKER.
      worker_command_args.push_back("--worker-type=" + rpc::WorkerType_Name(worker_type));
    }
  }

  if (IsIOWorkerType(worker_type)) {
    RAY_CHECK(!RayConfig::instance().object_spilling_config().empty());
    RAY_LOG(DEBUG) << "Adding object spill config "
                   << RayConfig::instance().object_spilling_config();
    worker_command_args.push_back("--object-spilling-config=" +
                                  RayConfig::instance().object_spilling_config());
  }

  ProcessEnvironment env;
  if (!IsIOWorkerType(worker_type)) {
    // We pass the job ID to worker processes via an environment variable, so we don't
    // need to add a new CLI parameter for both Python and Java workers.
    env.emplace(kEnvVarKeyJobId, job_id.Hex());
  }
  if (job_config) {
    env.insert(job_config->worker_env().begin(), job_config->worker_env().end());
  }

  for (const auto &pair : override_environment_variables) {
    env[pair.first] = pair.second;
  }
  // Start a process and measure the startup time.
  auto start = std::chrono::high_resolution_clock::now();
  Process proc = StartProcess(worker_command_args, env);
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  stats::ProcessStartupTimeMs.Record(duration.count());

  RAY_LOG(DEBUG) << "Started worker process of " << workers_to_start
                 << " worker(s) with pid " << proc.GetId();
  MonitorStartingWorkerProcess(proc, language, worker_type);
  state.starting_worker_processes.emplace(proc, workers_to_start);
  if (IsIOWorkerType(worker_type)) {
    auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
    io_worker_state.num_starting_io_workers++;
  }
  return proc;
}

void WorkerPool::MonitorStartingWorkerProcess(const Process &proc,
                                              const Language &language,
                                              const rpc::WorkerType worker_type) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      *io_service_, boost::posix_time::seconds(
                        RayConfig::instance().worker_register_timeout_seconds()));
  // Capture timer in lambda to copy it once, so that it can avoid destructing timer.
  timer->async_wait(
      [timer, language, proc, worker_type, this](const boost::system::error_code e) {
        // check the error code.
        auto &state = this->GetStateForLanguage(language);
        // Since this process times out to start, remove it from starting_worker_processes
        // to avoid the zombie worker.
        auto it = state.starting_worker_processes.find(proc);
        if (it != state.starting_worker_processes.end()) {
          RAY_LOG(INFO) << "Some workers of the worker process(" << proc.GetId()
                        << ") have not registered to raylet within timeout.";
          state.starting_worker_processes.erase(it);
          if (IsIOWorkerType(worker_type)) {
            // Mark the I/O worker as failed.
            auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
            io_worker_state.num_starting_io_workers--;
          }
          // We may have places to start more workers now.
          TryStartIOWorkers(language);
          starting_worker_timeout_callback_();
        }
      });
}

Process WorkerPool::StartProcess(const std::vector<std::string> &worker_command_args,
                                 const ProcessEnvironment &env) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    std::stringstream stream;
    stream << "Starting worker process with command:";
    for (const auto &arg : worker_command_args) {
      stream << " " << arg;
    }
    RAY_LOG(DEBUG) << stream.str();
  }

  // Launch the process to create the worker.
  std::error_code ec;
  std::vector<const char *> argv;
  for (const std::string &arg : worker_command_args) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(NULL);
  Process child(argv.data(), io_service_, ec, /*decouple=*/false, env);
  if (!child.IsValid() || ec) {
    // errorcode 24: Too many files. This is caused by ulimit.
    if (ec.value() == 24) {
      RAY_LOG(FATAL) << "Too many workers, failed to create a file. Try setting "
                     << "`ulimit -n <num_files>` then restart Ray.";
    } else {
      // The worker failed to start. This is a fatal error.
      RAY_LOG(FATAL) << "Failed to start worker with return value " << ec << ": "
                     << ec.message();
    }
  }
  return child;
}

Status WorkerPool::GetNextFreePort(int *port) {
  if (!free_ports_) {
    *port = 0;
    return Status::OK();
  }

  // Try up to the current number of ports.
  int current_size = free_ports_->size();
  for (int i = 0; i < current_size; i++) {
    *port = free_ports_->front();
    free_ports_->pop();
    if (CheckFree(*port)) {
      return Status::OK();
    }
    // Return to pool to check later.
    free_ports_->push(*port);
  }
  return Status::Invalid(
      "No available ports. Please specify a wider port range using --min-worker-port and "
      "--max-worker-port.");
}

void WorkerPool::MarkPortAsFree(int port) {
  if (free_ports_) {
    RAY_CHECK(port != 0) << "";
    free_ports_->push(port);
  }
}

void WorkerPool::HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config) {
  all_jobs_[job_id] = job_config;
}

void WorkerPool::HandleJobFinished(const JobID &job_id) {
  // Currently we don't erase the job from `all_jobs_` , as a workaround for
  // https://github.com/ray-project/ray/issues/11437.
  // unfinished_jobs_.erase(job_id);
}

Status WorkerPool::RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                                  pid_t pid,
                                  std::function<void(Status, int)> send_reply_callback) {
  RAY_CHECK(worker);

  auto &state = GetStateForLanguage(worker->GetLanguage());
  auto process = Process::FromPid(pid);

  if (state.starting_worker_processes.count(process) == 0) {
    RAY_LOG(WARNING) << "Received a register request from an unknown worker "
                     << process.GetId();
    Status status = Status::Invalid("Unknown worker");
    send_reply_callback(status, /*port=*/0);
    return status;
  }
  worker->SetProcess(process);

  // The port that this worker's gRPC server should listen on. 0 if the worker
  // should bind on a random port.
  int port = 0;
  Status status = GetNextFreePort(&port);
  if (!status.ok()) {
    send_reply_callback(status, /*port=*/0);
    return status;
  }
  RAY_LOG(DEBUG) << "Registering worker with pid " << pid << ", port: " << port
                 << ", worker_type: " << rpc::WorkerType_Name(worker->GetWorkerType());
  worker->SetAssignedPort(port);

  state.registered_workers.insert(worker);

  // Send the reply immediately for worker registrations.
  send_reply_callback(Status::OK(), port);
  return Status::OK();
}

void WorkerPool::OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) {
  auto &state = GetStateForLanguage(worker->GetLanguage());
  const auto &process = worker->GetProcess();
  RAY_CHECK(process.IsValid());

  auto it = state.starting_worker_processes.find(process);
  if (it != state.starting_worker_processes.end()) {
    it->second--;
    if (it->second == 0) {
      state.starting_worker_processes.erase(it);
      // We may have slots to start more workers now.
      TryStartIOWorkers(worker->GetLanguage());
    }
  }
  const auto &worker_type = worker->GetWorkerType();
  if (IsIOWorkerType(worker_type)) {
    auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
    io_worker_state.registered_io_workers.insert(worker);
    io_worker_state.num_starting_io_workers--;
  }

  // This is a workaround to finish driver registration after all initial workers are
  // registered to Raylet if and only if Raylet is started by a Python driver and the
  // job config is not set in `ray.init(...)`.
  if (first_job_ == worker->GetAssignedJobId() &&
      worker->GetLanguage() == Language::PYTHON) {
    if (++first_job_registered_python_worker_count_ ==
        first_job_driver_wait_num_python_workers_) {
      if (first_job_send_register_client_reply_to_driver_) {
        first_job_send_register_client_reply_to_driver_();
        first_job_send_register_client_reply_to_driver_ = nullptr;
      }
    }
  }
}

Status WorkerPool::RegisterDriver(const std::shared_ptr<WorkerInterface> &driver,
                                  const rpc::JobConfig &job_config,
                                  std::function<void(Status, int)> send_reply_callback) {
  int port;
  RAY_CHECK(!driver->GetAssignedTaskId().IsNil());
  Status status = GetNextFreePort(&port);
  if (!status.ok()) {
    send_reply_callback(status, /*port=*/0);
    return status;
  }
  driver->SetAssignedPort(port);
  auto &state = GetStateForLanguage(driver->GetLanguage());
  state.registered_drivers.insert(std::move(driver));
  const auto job_id = driver->GetAssignedJobId();
  all_jobs_[job_id] = job_config;

  // This is a workaround to start initial workers on this node if and only if Raylet is
  // started by a Python driver and the job config is not set in `ray.init(...)`.
  // Invoke the `send_reply_callback` later to only finish driver
  // registration after all initial workers are registered to Raylet.
  bool delay_callback = false;
  // If this is the first job.
  if (first_job_.IsNil()) {
    first_job_ = job_id;
    // If the number of Python workers we need to wait is positive.
    if (num_initial_python_workers_for_first_job_ > 0) {
      delay_callback = true;
      // Start initial Python workers for the first job.
      for (int i = 0; i < num_initial_python_workers_for_first_job_; i++) {
        StartWorkerProcess(Language::PYTHON, rpc::WorkerType::WORKER, job_id);
      }
    }
  }

  if (delay_callback) {
    RAY_CHECK(!first_job_send_register_client_reply_to_driver_);
    first_job_send_register_client_reply_to_driver_ = [send_reply_callback, port]() {
      send_reply_callback(Status::OK(), port);
    };
  } else {
    send_reply_callback(Status::OK(), port);
  }

  return Status::OK();
}

std::shared_ptr<WorkerInterface> WorkerPool::GetRegisteredWorker(
    const std::shared_ptr<ClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto worker = GetWorker(entry.second.registered_workers, connection);
    if (worker != nullptr) {
      return worker;
    }
  }
  return nullptr;
}

std::shared_ptr<WorkerInterface> WorkerPool::GetRegisteredDriver(
    const std::shared_ptr<ClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto driver = GetWorker(entry.second.registered_drivers, connection);
    if (driver != nullptr) {
      return driver;
    }
  }
  return nullptr;
}

void WorkerPool::PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) {
  PushIOWorkerInternal(worker, rpc::WorkerType::SPILL_WORKER);
}

void WorkerPool::PopSpillWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  PopIOWorkerInternal(rpc::WorkerType::SPILL_WORKER, callback);
}

void WorkerPool::PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) {
  PushIOWorkerInternal(worker, rpc::WorkerType::RESTORE_WORKER);
}

void WorkerPool::PopRestoreWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  PopIOWorkerInternal(rpc::WorkerType::RESTORE_WORKER, callback);
}

void WorkerPool::PushIOWorkerInternal(const std::shared_ptr<WorkerInterface> &worker,
                                      const rpc::WorkerType &worker_type) {
  RAY_CHECK(IsIOWorkerType(worker->GetWorkerType()));
  auto &state = GetStateForLanguage(Language::PYTHON);
  auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);

  RAY_LOG(DEBUG) << "Pushing an IO worker to the worker pool.";
  if (io_worker_state.pending_io_tasks.empty()) {
    io_worker_state.idle_io_workers.push(worker);
  } else {
    auto callback = io_worker_state.pending_io_tasks.front();
    io_worker_state.pending_io_tasks.pop();
    callback(worker);
  }
}

void WorkerPool::PopIOWorkerInternal(
    const rpc::WorkerType &worker_type,
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  auto &state = GetStateForLanguage(Language::PYTHON);
  auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);

  if (io_worker_state.idle_io_workers.empty()) {
    // We must fill the pending task first, because 'TryStartIOWorkers' will
    // start I/O workers according to the number of pending tasks.
    io_worker_state.pending_io_tasks.push(callback);
    TryStartIOWorkers(Language::PYTHON, worker_type);
  } else {
    auto io_worker = io_worker_state.idle_io_workers.front();
    io_worker_state.idle_io_workers.pop();
    callback(io_worker);
  }
}

void WorkerPool::PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) {
  RAY_CHECK(IsIOWorkerType(worker->GetWorkerType()));
  if (worker->GetWorkerType() == rpc::WorkerType::RESTORE_WORKER) {
    PushRestoreWorker(worker);
  } else {
    PushSpillWorker(worker);
  }
}

void WorkerPool::PopDeleteWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  auto &state = GetStateForLanguage(Language::PYTHON);
  // Choose an I/O worker with more idle workers.
  size_t num_spill_idle_workers = state.spill_io_worker_state.idle_io_workers.size();
  size_t num_restore_idle_workers = state.restore_io_worker_state.idle_io_workers.size();

  if (num_restore_idle_workers < num_spill_idle_workers) {
    PopSpillWorker(callback);
  } else {
    PopRestoreWorker(callback);
  }
}

void WorkerPool::PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
  // Since the worker is now idle, unset its assigned task ID.
  RAY_CHECK(worker->GetAssignedTaskId().IsNil())
      << "Idle workers cannot have an assigned task ID";
  auto &state = GetStateForLanguage(worker->GetLanguage());
  auto it = state.dedicated_workers_to_tasks.find(worker->GetProcess());
  if (it != state.dedicated_workers_to_tasks.end()) {
    // The worker is used for the actor creation task with dynamic options.
    // Put it into idle dedicated worker pool.
    const auto task_id = it->second;
    state.idle_dedicated_workers[task_id] = worker;
  } else {
    // The worker is not used for the actor creation task with dynamic options.
    // Put the worker to the corresponding idle pool.
    if (worker->GetActorId().IsNil()) {
      state.idle.insert(worker);
      int64_t now = current_time_ms();
      idle_of_all_languages_.emplace_back(worker, now);
      idle_of_all_languages_map_[worker] = now;
    } else {
      state.idle_actor[worker->GetActorId()] = worker;
    }
  }
}

void WorkerPool::ScheduleIdleWorkerKilling() {
  if (RayConfig::instance().kill_idle_workers_interval_ms() > 0) {
    kill_idle_workers_timer_.expires_from_now(boost::posix_time::milliseconds(
        RayConfig::instance().kill_idle_workers_interval_ms()));
    kill_idle_workers_timer_.async_wait([this](const boost::system::error_code &error) {
      if (error == boost::asio::error::operation_aborted) {
        return;
      }
      TryKillingIdleWorkers();
      ScheduleIdleWorkerKilling();
    });
  }
}

void WorkerPool::TryKillingIdleWorkers() {
  RAY_CHECK(idle_of_all_languages_.size() == idle_of_all_languages_map_.size());

  int64_t now = current_time_ms();
  size_t running_size = 0;
  for (const auto &worker : GetAllRegisteredWorkers()) {
    if (!worker->IsDead()) {
      running_size++;
    }
  }

  // Kill idle workers in FIFO order.
  for (const auto &idle_pair : idle_of_all_languages_) {
    if (running_size <= static_cast<size_t>(num_workers_soft_limit_)) {
      break;
    }
    if (now - idle_pair.second <
        RayConfig::instance().idle_worker_killing_time_threshold_ms()) {
      break;
    }

    const auto &idle_worker = idle_pair.first;
    if (idle_worker->IsDead()) {
      // This worker has already been killed.
      // This is possible because a Java worker process may hold multiple workers.
      continue;
    }
    auto process = idle_worker->GetProcess();

    auto &worker_state = GetStateForLanguage(idle_worker->GetLanguage());

    if (worker_state.starting_worker_processes.count(process) > 0) {
      // A Java worker process may hold multiple workers.
      // Some workers of this process are pending registration. Skip killing this worker.
      continue;
    }

    // Make sure all workers in this worker process are idle.
    // This block of code is needed by Java workers.
    auto workers_in_the_same_process = GetWorkersByProcess(process);
    bool can_be_killed = true;
    for (const auto &worker : workers_in_the_same_process) {
      if (worker_state.idle.count(worker) == 0 ||
          now - idle_of_all_languages_map_[worker] <
              RayConfig::instance().idle_worker_killing_time_threshold_ms()) {
        // Another worker in this process isn't idle, or hasn't been idle for a while, so
        // this process can't be killed.
        can_be_killed = false;
        break;
      }
    }
    if (!can_be_killed) {
      continue;
    }

    if (running_size - workers_in_the_same_process.size() <
        static_cast<size_t>(num_workers_soft_limit_)) {
      // A Java worker process may contain multiple workers. Killing more workers than we
      // expect may slow the job.
      return;
    }

    for (const auto &worker : workers_in_the_same_process) {
      RAY_LOG(DEBUG) << "The worker pool has " << running_size
                     << " registered workers which exceeds the soft limit of "
                     << num_workers_soft_limit_ << ", and worker " << worker->WorkerId()
                     << " with pid " << process.GetId()
                     << " has been idle for a a while. Kill it.";
      // To avoid object lost issue caused by forcibly killing, send an RPC request to the
      // worker to allow it to do cleanup before exiting.
      auto rpc_client = worker->rpc_client();
      RAY_CHECK(rpc_client);
      rpc::ExitRequest request;
      rpc_client->Exit(request, [](const ray::Status &status, const rpc::ExitReply &r) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to send exit request: " << status.ToString();
        }
      });
      // Remove the worker from the idle pool so it can't be popped anymore.
      RemoveWorker(worker_state.idle, worker);
      if (!worker->IsDead()) {
        worker->MarkDead();
        running_size--;
      }
    }
  }

  std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
      new_idle_of_all_languages;
  idle_of_all_languages_map_.clear();
  for (const auto &idle_pair : idle_of_all_languages_) {
    if (!idle_pair.first->IsDead()) {
      new_idle_of_all_languages.push_back(idle_pair);
      idle_of_all_languages_map_.emplace(idle_pair);
    }
  }

  idle_of_all_languages_ = std::move(new_idle_of_all_languages);
  RAY_CHECK(idle_of_all_languages_.size() == idle_of_all_languages_map_.size());
}

std::shared_ptr<WorkerInterface> WorkerPool::PopWorker(
    const TaskSpecification &task_spec) {
  auto &state = GetStateForLanguage(task_spec.GetLanguage());

  std::shared_ptr<WorkerInterface> worker = nullptr;
  Process proc;
  if ((task_spec.IsActorCreationTask() && !task_spec.DynamicWorkerOptions().empty()) ||
      task_spec.OverrideEnvironmentVariables().size() > 0) {
    // Code path of task that needs a dedicated worker: an actor creation task with
    // dynamic worker options, or any task with environment variable overrides.
    // Try to pop it from idle dedicated pool.
    auto it = state.idle_dedicated_workers.find(task_spec.TaskId());
    if (it != state.idle_dedicated_workers.end()) {
      // There is an idle dedicated worker for this task.
      worker = std::move(it->second);
      state.idle_dedicated_workers.erase(it);
      // Because we found a worker that can perform this task,
      // we can remove it from dedicated_workers_to_tasks.
      state.dedicated_workers_to_tasks.erase(worker->GetProcess());
      state.tasks_to_dedicated_workers.erase(task_spec.TaskId());
    } else if (!HasPendingWorkerForTask(task_spec.GetLanguage(), task_spec.TaskId())) {
      // We are not pending a registration from a worker for this task,
      // so start a new worker process for this task.
      std::vector<std::string> dynamic_options = {};
      if (task_spec.IsActorCreationTask()) {
        dynamic_options = task_spec.DynamicWorkerOptions();
      }
      proc = StartWorkerProcess(task_spec.GetLanguage(), rpc::WorkerType::WORKER,
                                task_spec.JobId(), dynamic_options,
                                task_spec.OverrideEnvironmentVariables());
      if (proc.IsValid()) {
        state.dedicated_workers_to_tasks[proc] = task_spec.TaskId();
        state.tasks_to_dedicated_workers[task_spec.TaskId()] = proc;
      }
    }
  } else if (!task_spec.IsActorTask()) {
    // Code path of normal task or actor creation task without dynamic worker options.
    // Find an available worker which is already assigned to this job.
    // Try to pop the most recently pushed worker.
    for (auto it = idle_of_all_languages_.rbegin(); it != idle_of_all_languages_.rend();
         it++) {
      if (task_spec.GetLanguage() != it->first->GetLanguage() ||
          it->first->GetAssignedJobId() != task_spec.JobId()) {
        continue;
      }
      state.idle.erase(it->first);
      // We can't erase a reverse_iterator.
      auto lit = it.base();
      lit--;
      worker = std::move(lit->first);
      idle_of_all_languages_.erase(lit);
      idle_of_all_languages_map_.erase(worker);
      break;
    }
    if (worker == nullptr) {
      // There are no more non-actor workers available to execute this task.
      // Start a new worker process.
      proc = StartWorkerProcess(task_spec.GetLanguage(), rpc::WorkerType::WORKER,
                                task_spec.JobId());
    }
  } else {
    // Code path of actor task.
    const auto &actor_id = task_spec.ActorId();
    auto actor_entry = state.idle_actor.find(actor_id);
    if (actor_entry != state.idle_actor.end()) {
      worker = std::move(actor_entry->second);
      state.idle_actor.erase(actor_entry);
    }
  }

  if (worker == nullptr && proc.IsValid()) {
    WarnAboutSize();
  }

  if (worker) {
    RAY_CHECK(worker->GetAssignedJobId() == task_spec.JobId());
  }
  return worker;
}

void WorkerPool::PrestartWorkers(const TaskSpecification &task_spec,
                                 int64_t backlog_size) {
  // Code path of task that needs a dedicated worker: an actor creation task with
  // dynamic worker options, or any task with environment variable overrides.
  if ((task_spec.IsActorCreationTask() && !task_spec.DynamicWorkerOptions().empty()) ||
      task_spec.OverrideEnvironmentVariables().size() > 0) {
    return;  // Not handled.
  }

  auto &state = GetStateForLanguage(task_spec.GetLanguage());
  // The number of available workers that can be used for this task spec.
  int num_usable_workers = state.idle.size();
  for (auto &entry : state.starting_worker_processes) {
    num_usable_workers += entry.second;
  }
  // The number of workers total regardless of suitability for this task.
  int num_workers_total = 0;
  for (const auto &worker : GetAllRegisteredWorkers()) {
    if (!worker->IsDead()) {
      num_workers_total++;
    }
  }
  auto desired_usable_workers =
      std::min<int64_t>(num_workers_soft_limit_ - num_workers_total, backlog_size);
  if (num_usable_workers < desired_usable_workers) {
    int64_t num_needed = desired_usable_workers - num_usable_workers;
    RAY_LOG(DEBUG) << "Prestarting " << num_needed << " workers given task backlog size "
                   << backlog_size << " and soft limit " << num_workers_soft_limit_;
    for (int i = 0; i < num_needed; i++) {
      StartWorkerProcess(task_spec.GetLanguage(), rpc::WorkerType::WORKER,
                         task_spec.JobId());
    }
  }
}

bool WorkerPool::DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker) {
  auto &state = GetStateForLanguage(worker->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_workers, worker));
  for (auto it = idle_of_all_languages_.begin(); it != idle_of_all_languages_.end();
       it++) {
    if (it->first == worker) {
      idle_of_all_languages_.erase(it);
      idle_of_all_languages_map_.erase(worker);
      break;
    }
  }

  MarkPortAsFree(worker->AssignedPort());
  return RemoveWorker(state.idle, worker);
}

void WorkerPool::DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) {
  auto &state = GetStateForLanguage(driver->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_drivers, driver));
  MarkPortAsFree(driver->AssignedPort());
}

inline WorkerPool::State &WorkerPool::GetStateForLanguage(const Language &language) {
  auto state = states_by_lang_.find(language);
  RAY_CHECK(state != states_by_lang_.end()) << "Required Language isn't supported.";
  return state->second;
}

inline bool WorkerPool::IsIOWorkerType(const rpc::WorkerType &worker_type) {
  return worker_type == rpc::WorkerType::SPILL_WORKER ||
         worker_type == rpc::WorkerType::RESTORE_WORKER;
}

std::vector<std::shared_ptr<WorkerInterface>> WorkerPool::GetWorkersRunningTasksForJob(
    const JobID &job_id) const {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (worker->GetAssignedJobId() == job_id) {
        workers.push_back(worker);
      }
    }
  }

  return workers;
}

const std::vector<std::shared_ptr<WorkerInterface>> WorkerPool::GetAllRegisteredWorkers(
    bool filter_dead_workers) const {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (!worker->IsRegistered()) {
        continue;
      }

      if (filter_dead_workers && worker->IsDead()) {
        continue;
      }
      workers.push_back(worker);
    }
  }

  return workers;
}

const std::vector<std::shared_ptr<WorkerInterface>> WorkerPool::GetAllRegisteredDrivers(
    bool filter_dead_drivers) const {
  std::vector<std::shared_ptr<WorkerInterface>> drivers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &driver : entry.second.registered_drivers) {
      if (!driver->IsRegistered()) {
        continue;
      }

      if (filter_dead_drivers && driver->IsDead()) {
        continue;
      }
      drivers.push_back(driver);
    }
  }

  return drivers;
}

void WorkerPool::WarnAboutSize() {
  for (auto &entry : states_by_lang_) {
    auto &state = entry.second;
    int64_t num_workers_started_or_registered = 0;
    num_workers_started_or_registered +=
        static_cast<int64_t>(state.registered_workers.size());
    for (const auto &starting_process : state.starting_worker_processes) {
      num_workers_started_or_registered += starting_process.second;
    }
    int64_t multiple = num_workers_started_or_registered / state.multiple_for_warning;
    std::stringstream warning_message;
    if (multiple >= 3 && multiple > state.last_warning_multiple) {
      // Push an error message to the user if the worker pool tells us that it is
      // getting too big.
      state.last_warning_multiple = multiple;
      warning_message << "WARNING: " << num_workers_started_or_registered << " "
                      << Language_Name(entry.first)
                      << " workers have been started. This could be a result of using "
                      << "a large number of actors, or it could be a consequence of "
                      << "using nested tasks "
                      << "(see https://github.com/ray-project/ray/issues/3644) for "
                      << "some a discussion of workarounds.";
      auto error_data_ptr = gcs::CreateErrorTableData(
          "worker_pool_large", warning_message.str(), current_time_ms());
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
}

bool WorkerPool::HasPendingWorkerForTask(const Language &language,
                                         const TaskID &task_id) {
  auto &state = GetStateForLanguage(language);
  auto it = state.tasks_to_dedicated_workers.find(task_id);
  return it != state.tasks_to_dedicated_workers.end();
}

void WorkerPool::TryStartIOWorkers(const Language &language) {
  TryStartIOWorkers(language, rpc::WorkerType::RESTORE_WORKER);
  TryStartIOWorkers(language, rpc::WorkerType::SPILL_WORKER);
}

void WorkerPool::TryStartIOWorkers(const Language &language,
                                   const rpc::WorkerType &worker_type) {
  if (language != Language::PYTHON) {
    return;
  }
  auto &state = GetStateForLanguage(language);
  auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);

  int available_io_workers_num = io_worker_state.num_starting_io_workers +
                                 io_worker_state.registered_io_workers.size();
  int max_workers_to_start =
      RayConfig::instance().max_io_workers() - available_io_workers_num;
  // Compare first to prevent unsigned underflow.
  if (io_worker_state.pending_io_tasks.size() > io_worker_state.idle_io_workers.size()) {
    int expected_workers_num =
        io_worker_state.pending_io_tasks.size() - io_worker_state.idle_io_workers.size();
    if (expected_workers_num > max_workers_to_start) {
      expected_workers_num = max_workers_to_start;
    }
    for (; expected_workers_num > 0; expected_workers_num--) {
      Process proc = StartWorkerProcess(ray::Language::PYTHON, worker_type, JobID::Nil());
      if (!proc.IsValid()) {
        // We may hit the maximum worker start up concurrency limit. Stop.
        return;
      }
    }
  }
}

std::unordered_set<std::shared_ptr<WorkerInterface>> WorkerPool::GetWorkersByProcess(
    const Process &process) {
  std::unordered_set<std::shared_ptr<WorkerInterface>> workers_of_process;
  for (auto &entry : states_by_lang_) {
    auto &worker_state = entry.second;
    for (const auto &worker : worker_state.registered_workers) {
      if (worker->GetProcess().GetId() == process.GetId()) {
        workers_of_process.insert(worker);
      }
    }
  }
  return workers_of_process;
}

std::string WorkerPool::DebugString() const {
  std::stringstream result;
  result << "WorkerPool:";
  for (const auto &entry : states_by_lang_) {
    result << "\n- num " << Language_Name(entry.first)
           << " workers: " << entry.second.registered_workers.size();
    result << "\n- num " << Language_Name(entry.first)
           << " drivers: " << entry.second.registered_drivers.size();
  }
  result << "\n- num idle workers: " << idle_of_all_languages_.size();
  return result.str();
}

WorkerPool::IOWorkerState &WorkerPool::GetIOWorkerStateFromWorkerType(
    const rpc::WorkerType &worker_type, WorkerPool::State &state) const {
  RAY_CHECK(worker_type != rpc::WorkerType::WORKER)
      << worker_type << " type cannot be used to retrieve io_worker_state";
  if (worker_type == rpc::WorkerType::SPILL_WORKER) {
    return state.spill_io_worker_state;
  } else {
    return state.restore_io_worker_state;
  }
}

}  // namespace raylet

}  // namespace ray
