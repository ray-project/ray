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
#include <boost/filesystem.hpp>

#include "ray/common/constants.h"
#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/runtime_env_common.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

DEFINE_stats(worker_register_time_ms,
             "end to end latency of register a worker process.",
             (),
             ({1, 10, 100, 1000, 10000}, ),
             ray::stats::HISTOGRAM);

namespace {

// Add this prefix because the worker setup token is just a counter which is easy to
// duplicate with other ids.
static const std::string kWorkerSetupTokenPrefix = "worker_startup_token:";

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

WorkerPool::WorkerPool(instrumented_io_context &io_service,
                       const NodeID node_id,
                       const std::string node_address,
                       int num_workers_soft_limit,
                       int num_initial_python_workers_for_first_job,
                       int maximum_startup_concurrency,
                       int min_worker_port,
                       int max_worker_port,
                       const std::vector<int> &worker_ports,
                       std::shared_ptr<gcs::GcsClient> gcs_client,
                       const WorkerCommandMap &worker_commands,
                       const std::string &native_library_path,
                       std::function<void()> starting_worker_timeout_callback,
                       int ray_debugger_external,
                       const std::function<double()> get_time)
    : worker_startup_token_counter_(0),
      io_service_(&io_service),
      node_id_(node_id),
      node_address_(node_address),
      num_workers_soft_limit_(num_workers_soft_limit),
      maximum_startup_concurrency_(maximum_startup_concurrency),
      gcs_client_(std::move(gcs_client)),
      native_library_path_(native_library_path),
      starting_worker_timeout_callback_(starting_worker_timeout_callback),
      ray_debugger_external(ray_debugger_external),
      first_job_registered_python_worker_count_(0),
      first_job_driver_wait_num_python_workers_(std::min(
          num_initial_python_workers_for_first_job, maximum_startup_concurrency)),
      num_initial_python_workers_for_first_job_(num_initial_python_workers_for_first_job),
      periodical_runner_(io_service),
      get_time_(get_time) {
  RAY_CHECK(maximum_startup_concurrency > 0);
  // We need to record so that the metric exists. This way, we report that 0
  // processes have started before a task runs on the node (as opposed to the
  // metric not existing at all).
  stats::NumWorkersStarted.Record(0);
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
    free_ports_ = std::make_unique<std::queue<int>>();
    for (int port : worker_ports) {
      free_ports_->push(port);
    }
  } else if (min_worker_port != 0) {
    if (max_worker_port == 0) {
      max_worker_port = 65535;  // Maximum valid port number.
    }
    RAY_CHECK(min_worker_port > 0 && min_worker_port <= 65535);
    RAY_CHECK(max_worker_port >= min_worker_port && max_worker_port <= 65535);
    free_ports_ = std::make_unique<std::queue<int>>();
    for (int port = min_worker_port; port <= max_worker_port; port++) {
      free_ports_->push(port);
    }
  }
  if (RayConfig::instance().kill_idle_workers_interval_ms() > 0) {
    periodical_runner_.RunFnPeriodically(
        [this] { TryKillingIdleWorkers(); },
        RayConfig::instance().kill_idle_workers_interval_ms(),
        "RayletWorkerPool.deadline_timer.kill_idle_workers");
  }
}

WorkerPool::~WorkerPool() {
  std::unordered_set<Process> procs_to_kill;
  for (const auto &entry : states_by_lang_) {
    // Kill all the worker processes.
    for (auto &worker_process : entry.second.worker_processes) {
      procs_to_kill.insert(worker_process.second.proc);
    }
  }
  for (Process proc : procs_to_kill) {
    proc.Kill();
    // NOTE: Avoid calling Wait() here. It fails with ECHILD, as SIGCHLD is disabled.
  }
}

// NOTE(kfstorm): The node manager cannot be passed via WorkerPool constructor because the
// grpc server is started after the WorkerPool instance is constructed.
void WorkerPool::SetNodeManagerPort(int node_manager_port) {
  node_manager_port_ = node_manager_port;
}

void WorkerPool::SetAgentManager(std::shared_ptr<AgentManager> agent_manager) {
  agent_manager_ = agent_manager;
}

void WorkerPool::PopWorkerCallbackAsync(const PopWorkerCallback &callback,
                                        std::shared_ptr<WorkerInterface> worker,
                                        PopWorkerStatus status) {
  // This method shouldn't be invoked when runtime env creation has failed because
  // when runtime env is failed to be created, they are all
  // invoking the callback immediately.
  RAY_CHECK(status != PopWorkerStatus::RuntimeEnvCreationFailed);
  // Call back this function asynchronously to make sure executed in different stack.
  io_service_->post(
      [this, callback, worker, status]() {
        PopWorkerCallbackInternal(callback, worker, status);
      },
      "WorkerPool.PopWorkerCallback");
}

void WorkerPool::PopWorkerCallbackInternal(const PopWorkerCallback &callback,
                                           std::shared_ptr<WorkerInterface> worker,
                                           PopWorkerStatus status) {
  RAY_CHECK(callback);
  auto used = callback(worker, status, /*runtime_env_setup_error_message*/ "");
  if (worker && !used) {
    // The invalid worker not used, restore it to worker pool.
    PushWorker(worker);
  }
}

void WorkerPool::update_worker_startup_token_counter() {
  worker_startup_token_counter_ += 1;
}

void WorkerPool::AddWorkerProcess(
    State &state,
    const int workers_to_start,
    const rpc::WorkerType worker_type,
    const Process &proc,
    const std::chrono::high_resolution_clock::time_point &start,
    const rpc::RuntimeEnvInfo &runtime_env_info) {
  state.worker_processes.emplace(worker_startup_token_counter_,
                                 WorkerProcessInfo{workers_to_start,
                                                   workers_to_start,
                                                   {},
                                                   worker_type,
                                                   proc,
                                                   start,
                                                   runtime_env_info});
}

void WorkerPool::RemoveWorkerProcess(State &state,
                                     const StartupToken &proc_startup_token) {
  state.worker_processes.erase(proc_startup_token);
}

std::tuple<Process, StartupToken> WorkerPool::StartWorkerProcess(
    const Language &language,
    const rpc::WorkerType worker_type,
    const JobID &job_id,
    PopWorkerStatus *status,
    const std::vector<std::string> &dynamic_options,
    const int runtime_env_hash,
    const std::string &serialized_runtime_env_context,
    const std::string &allocated_instances_serialized_json,
    const rpc::RuntimeEnvInfo &runtime_env_info) {
  rpc::JobConfig *job_config = nullptr;
  if (!IsIOWorkerType(worker_type)) {
    RAY_CHECK(!job_id.IsNil());
    auto it = all_jobs_.find(job_id);
    if (it == all_jobs_.end()) {
      RAY_LOG(DEBUG) << "Job config of job " << job_id << " are not local yet.";
      // Will reschedule ready tasks in `NodeManager::HandleJobStarted`.
      *status = PopWorkerStatus::JobConfigMissing;
      process_failed_job_config_missing_++;
      return {Process(), (StartupToken)-1};
    }
    job_config = &it->second;
  }

  auto &state = GetStateForLanguage(language);
  // If we are already starting up too many workers of the same worker type, then return
  // without starting more.
  int starting_workers = 0;
  for (auto &entry : state.worker_processes) {
    if (entry.second.worker_type == worker_type) {
      starting_workers += entry.second.num_starting_workers;
    }
  }

  // Here we consider both task workers and I/O workers.
  if (starting_workers >= maximum_startup_concurrency_) {
    // Workers have been started, but not registered. Force start disabled -- returning.
    RAY_LOG(DEBUG) << "Worker not started, " << starting_workers
                   << " workers of language type " << static_cast<int>(language)
                   << " pending registration";
    *status = PopWorkerStatus::TooManyStartingWorkerProcesses;
    process_failed_rate_limited_++;
    return {Process(), (StartupToken)-1};
  }
  // Either there are no workers pending registration or the worker start is being forced.
  RAY_LOG(DEBUG) << "Starting new worker process of language "
                 << rpc::Language_Name(language) << " and type "
                 << rpc::WorkerType_Name(worker_type) << ", current pool has "
                 << state.idle.size() << " workers";

  int workers_to_start = 1;
  if (dynamic_options.empty()) {
    if (language == Language::JAVA) {
      workers_to_start = job_config->num_java_workers_per_process();
    }
  }

  std::vector<std::string> options;

  // Append Ray-defined per-job options here
  std::string code_search_path;
  if (language == Language::JAVA || language == Language::CPP) {
    if (job_config) {
      std::string code_search_path_str;
      for (int i = 0; i < job_config->code_search_path_size(); i++) {
        auto path = job_config->code_search_path(i);
        if (i != 0) {
          code_search_path_str += ":";
        }
        code_search_path_str += path;
      }
      if (!code_search_path_str.empty()) {
        code_search_path = code_search_path_str;
        if (language == Language::JAVA) {
          code_search_path_str = "-Dray.job.code-search-path=" + code_search_path_str;
        } else if (language == Language::CPP) {
          code_search_path_str = "--ray_code_search_path=" + code_search_path_str;
        } else {
          RAY_LOG(FATAL) << "Unknown language " << Language_Name(language);
        }
        options.push_back(code_search_path_str);
      }
    }
  }

  // Append user-defined per-job options here
  if (language == Language::JAVA) {
    if (!job_config->jvm_options().empty()) {
      options.insert(options.end(),
                     job_config->jvm_options().begin(),
                     job_config->jvm_options().end());
    }
  }

  // Append Ray-defined per-process options here
  if (language == Language::JAVA) {
    options.push_back("-Dray.job.num-java-workers-per-process=" +
                      std::to_string(workers_to_start));
  }

  // Append startup-token for JAVA here
  if (language == Language::JAVA) {
    options.push_back("-Dray.raylet.startup-token=" +
                      std::to_string(worker_startup_token_counter_));
    options.push_back("-Dray.internal.runtime-env-hash=" +
                      std::to_string(runtime_env_hash));
  }

  // Append user-defined per-process options here
  options.insert(options.end(), dynamic_options.begin(), dynamic_options.end());

  // Extract pointers from the worker command to pass into execvpe.
  std::vector<std::string> worker_command_args;
  for (auto const &token : state.worker_command) {
    if (token == kWorkerDynamicOptionPlaceholder) {
      worker_command_args.insert(
          worker_command_args.end(), options.begin(), options.end());
      continue;
    }
    RAY_CHECK(node_manager_port_ != 0)
        << "Node manager port is not set yet. This shouldn't happen unless we are trying "
           "to start a worker process before node manager server is started. In this "
           "case, it's a bug and it should be fixed.";
    auto node_manager_port_position = token.find(kNodeManagerPortPlaceholder);
    if (node_manager_port_position != std::string::npos) {
      auto replaced_token = token;
      replaced_token.replace(node_manager_port_position,
                             strlen(kNodeManagerPortPlaceholder),
                             std::to_string(node_manager_port_));
      worker_command_args.push_back(replaced_token);
      continue;
    }

    worker_command_args.push_back(token);
  }

  if (language == Language::PYTHON) {
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
    worker_command_args.push_back(
        "--object-spilling-config=" +
        absl::Base64Escape(RayConfig::instance().object_spilling_config()));
  }

  if (language == Language::PYTHON) {
    worker_command_args.push_back("--startup-token=" +
                                  std::to_string(worker_startup_token_counter_));
  } else if (language == Language::CPP) {
    worker_command_args.push_back("--startup_token=" +
                                  std::to_string(worker_startup_token_counter_));
  }

  ProcessEnvironment env;
  if (!IsIOWorkerType(worker_type)) {
    // We pass the job ID to worker processes via an environment variable, so we don't
    // need to add a new CLI parameter for both Python and Java workers.
    env.emplace(kEnvVarKeyJobId, job_id.Hex());
  }
  env.emplace(kEnvVarKeyRayletPid, std::to_string(GetPID()));

  // TODO(SongGuyang): Maybe Python and Java also need native library path in future.
  if (language == Language::CPP) {
    // Set native library path for shared library search.
    if (!native_library_path_.empty() || !code_search_path.empty()) {
#if defined(__APPLE__) || defined(__linux__) || defined(_WIN32)
#if defined(__APPLE__)
      static const std::string kLibraryPathEnvName = "DYLD_LIBRARY_PATH";
#elif defined(__linux__)
      static const std::string kLibraryPathEnvName = "LD_LIBRARY_PATH";
#elif defined(_WIN32)
      static const std::string kLibraryPathEnvName = "PATH";
#endif
      auto path_env_p = std::getenv(kLibraryPathEnvName.c_str());
      std::string path_env = native_library_path_;
      if (path_env_p != nullptr && strlen(path_env_p) != 0) {
        path_env.append(":").append(path_env_p);
      }
      // Append per-job code search path to library path.
      if (!code_search_path.empty()) {
        path_env.append(":").append(code_search_path);
      }
      env.emplace(kLibraryPathEnvName, path_env);
#endif
    }
  }

  if (language == Language::PYTHON || language == Language::JAVA) {
    if (serialized_runtime_env_context != "{}" &&
        !serialized_runtime_env_context.empty()) {
      worker_command_args.push_back("--language=" + Language_Name(language));
      worker_command_args.push_back("--runtime-env-hash=" +
                                    std::to_string(runtime_env_hash));

      worker_command_args.push_back("--serialized-runtime-env-context=" +
                                    serialized_runtime_env_context);
    } else if (language == Language::PYTHON && worker_command_args.size() >= 2 &&
               worker_command_args[1].find(kSetupWorkerFilename) != std::string::npos) {
      // Check that the arg really is the path to the setup worker before erasing it, to
      // prevent breaking tests that mock out the worker command args.
      worker_command_args.erase(worker_command_args.begin() + 1,
                                worker_command_args.begin() + 2);
    } else if (language == Language::JAVA) {
      worker_command_args.push_back("--language=" + Language_Name(language));
    }

    if (ray_debugger_external) {
      worker_command_args.push_back("--ray-debugger-external");
    }
  }

  // We use setproctitle to change python worker process title,
  // causing the process's /proc/PID/environ being empty.
  // Add `SPT_NOENV` env to prevent setproctitle breaking /proc/PID/environ.
  // Refer this issue for more details: https://github.com/ray-project/ray/issues/15061
  if (language == Language::PYTHON) {
    env.insert({"SPT_NOENV", "1"});
  }

  if (RayConfig::instance().support_fork()) {
    // Support forking in gRPC.
    env.insert({"GRPC_ENABLE_FORK_SUPPORT", "True"});
    env.insert({"GRPC_POLL_STRATEGY", "poll"});
    // Make sure only the main thread is running in Python workers.
    env.insert({"RAY_start_python_importer_thread", "0"});
  }

  // Start a process and measure the startup time.
  auto start = std::chrono::high_resolution_clock::now();
  Process proc = StartProcess(worker_command_args, env);
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  stats::ProcessStartupTimeMs.Record(duration.count());
  stats::NumWorkersStarted.Record(1);
  RAY_LOG(INFO) << "Started worker process of " << workers_to_start
                << " worker(s) with pid " << proc.GetId() << ", the token "
                << worker_startup_token_counter_;
  AdjustWorkerOomScore(proc.GetId());
  MonitorStartingWorkerProcess(
      proc, worker_startup_token_counter_, language, worker_type);
  AddWorkerProcess(state, workers_to_start, worker_type, proc, start, runtime_env_info);
  StartupToken worker_startup_token = worker_startup_token_counter_;
  update_worker_startup_token_counter();
  if (IsIOWorkerType(worker_type)) {
    auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
    io_worker_state.num_starting_io_workers++;
  }
  return {proc, worker_startup_token};
}

void WorkerPool::AdjustWorkerOomScore(pid_t pid) const {
#ifdef __linux__
  std::ofstream oom_score_file;
  std::string filename("/proc/" + std::to_string(pid) + "/oom_score_adj");
  oom_score_file.open(filename, std::ofstream::out);
  int oom_score_adj = RayConfig::instance().worker_oom_score_adjustment();
  oom_score_adj = std::max(oom_score_adj, 0);
  oom_score_adj = std::min(oom_score_adj, 1000);
  if (oom_score_file.is_open()) {
    // Adjust worker's OOM score so that the OS prioritizes killing these
    // processes over the raylet.
    oom_score_file << std::to_string(oom_score_adj);
  }
  if (oom_score_file.fail()) {
    RAY_LOG(INFO) << "Failed to set OOM score adjustment for worker with PID " << pid
                  << ", error: " << strerror(errno);
  }
  oom_score_file.close();
#endif
}

void WorkerPool::MonitorStartingWorkerProcess(const Process &proc,
                                              StartupToken proc_startup_token,
                                              const Language &language,
                                              const rpc::WorkerType worker_type) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      *io_service_,
      boost::posix_time::seconds(
          RayConfig::instance().worker_register_timeout_seconds()));
  // Capture timer in lambda to copy it once, so that it can avoid destructing timer.
  timer->async_wait([timer, language, proc = proc, proc_startup_token, worker_type, this](
                        const boost::system::error_code e) mutable {
    // check the error code.
    auto &state = this->GetStateForLanguage(language);
    // Since this process times out to start, remove it from worker_processes
    // to avoid the zombie worker.
    auto it = state.worker_processes.find(proc_startup_token);
    if (it != state.worker_processes.end() && it->second.num_starting_workers != 0) {
      RAY_LOG(ERROR)
          << "Some workers of the worker process(" << proc.GetId()
          << ") have not registered within the timeout. "
          << (proc.IsAlive()
                  ? "The process is still alive, probably it's hanging during start."
                  : "The process is dead, probably it crashed during start.");

      if (proc.IsAlive()) {
        proc.Kill();
      }

      PopWorkerStatus status = PopWorkerStatus::WorkerPendingRegistration;
      process_failed_pending_registration_++;
      bool found;
      bool used;
      TaskID task_id;
      InvokePopWorkerCallbackForProcess(state.starting_dedicated_workers_to_tasks,
                                        proc_startup_token,
                                        nullptr,
                                        status,
                                        &found,
                                        &used,
                                        &task_id);
      if (!found) {
        InvokePopWorkerCallbackForProcess(state.starting_workers_to_tasks,
                                          proc_startup_token,
                                          nullptr,
                                          status,
                                          &found,
                                          &used,
                                          &task_id);
      }
      DeleteRuntimeEnvIfPossible(it->second.runtime_env_info.serialized_runtime_env());
      RemoveWorkerProcess(state, proc_startup_token);
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
    std::string debug_info;
    debug_info.append("Starting worker process with command:");
    for (const auto &arg : worker_command_args) {
      debug_info.append(" ").append(arg);
    }
    debug_info.append(", and the envs:");
    for (const auto &entry : env) {
      debug_info.append(" ")
          .append(entry.first)
          .append(":")
          .append(entry.second)
          .append(",");
    }
    if (!env.empty()) {
      // Erase the last ","
      debug_info.pop_back();
    }
    debug_info.append(".");
    RAY_LOG(DEBUG) << debug_info;
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

static bool NeedToEagerInstallRuntimeEnv(const rpc::JobConfig &job_config) {
  if (job_config.has_runtime_env_info() &&
      job_config.runtime_env_info().runtime_env_eager_install()) {
    auto const &runtime_env = job_config.runtime_env_info().serialized_runtime_env();
    return !IsRuntimeEnvEmpty(runtime_env);
  }
  return false;
}

void WorkerPool::HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config) {
  if (all_jobs_.find(job_id) != all_jobs_.end()) {
    RAY_LOG(INFO) << "Job " << job_id << " already started in worker pool.";
    return;
  }
  all_jobs_[job_id] = job_config;
  if (NeedToEagerInstallRuntimeEnv(job_config)) {
    auto const &runtime_env = job_config.runtime_env_info().serialized_runtime_env();
    auto const &runtime_env_config = job_config.runtime_env_info().runtime_env_config();
    // NOTE: Technically `HandleJobStarted` isn't idempotent because we'll
    // increment the ref count multiple times. This is fine because
    // `HandleJobFinished` will also decrement the ref count multiple times.
    RAY_LOG(INFO) << "[Eagerly] Start install runtime environment for job " << job_id
                  << ". The runtime environment was " << runtime_env << ".";
    GetOrCreateRuntimeEnv(
        runtime_env,
        runtime_env_config,
        job_id,
        [job_id](bool successful,
                 const std::string &serialized_runtime_env_context,
                 const std::string &setup_error_message) {
          if (successful) {
            RAY_LOG(INFO) << "[Eagerly] Create runtime env successful for job " << job_id
                          << ". The result context was " << serialized_runtime_env_context
                          << ".";
          } else {
            RAY_LOG(WARNING) << "[Eagerly] Couldn't create a runtime environment for job "
                             << job_id << ". Error message: " << setup_error_message;
          }
        });
  }
}

void WorkerPool::HandleJobFinished(const JobID &job_id) {
  // Currently we don't erase the job from `all_jobs_` , as a workaround for
  // https://github.com/ray-project/ray/issues/11437.
  // unfinished_jobs_.erase(job_id);
  auto job_config = GetJobConfig(job_id);
  RAY_CHECK(job_config);
  // Check eager install here because we only add URI reference when runtime
  // env install really happens.
  if (NeedToEagerInstallRuntimeEnv(*job_config)) {
    DeleteRuntimeEnvIfPossible(job_config->runtime_env_info().serialized_runtime_env());
  }
  finished_jobs_.insert(job_id);
}

boost::optional<const rpc::JobConfig &> WorkerPool::GetJobConfig(
    const JobID &job_id) const {
  auto iter = all_jobs_.find(job_id);
  return iter == all_jobs_.end() ? boost::none
                                 : boost::optional<const rpc::JobConfig &>(iter->second);
}

Status WorkerPool::RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                                  pid_t pid,
                                  StartupToken worker_startup_token,
                                  std::function<void(Status, int)> send_reply_callback) {
  RAY_CHECK(worker);
  auto &state = GetStateForLanguage(worker->GetLanguage());
  auto it = state.worker_processes.find(worker_startup_token);
  if (it == state.worker_processes.end()) {
    RAY_LOG(WARNING) << "Received a register request from an unknown token: "
                     << worker_startup_token;
    Status status = Status::Invalid("Unknown worker");
    send_reply_callback(status, /*port=*/0);
    return status;
  }
  auto process = Process::FromPid(pid);
  worker->SetProcess(process);

  // The port that this worker's gRPC server should listen on. 0 if the worker
  // should bind on a random port.
  int port = 0;
  Status status = GetNextFreePort(&port);
  if (!status.ok()) {
    send_reply_callback(status, /*port=*/0);
    return status;
  }
  auto &starting_process_info = it->second;
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - starting_process_info.start_time);
  STATS_worker_register_time_ms.Record(duration.count());
  RAY_LOG(DEBUG) << "Registering worker " << worker->WorkerId() << " with pid " << pid
                 << ", port: " << port << ", register cost: " << duration.count()
                 << ", worker_type: " << rpc::WorkerType_Name(worker->GetWorkerType());
  worker->SetAssignedPort(port);

  state.registered_workers.insert(worker);

  // Send the reply immediately for worker registrations.
  send_reply_callback(Status::OK(), port);
  return Status::OK();
}

void WorkerPool::OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) {
  auto &state = GetStateForLanguage(worker->GetLanguage());
  const StartupToken worker_startup_token = worker->GetStartupToken();

  auto it = state.worker_processes.find(worker_startup_token);
  if (it != state.worker_processes.end()) {
    it->second.num_starting_workers--;
    it->second.alive_started_workers.insert(worker);
    if (it->second.num_starting_workers == 0) {
      // We may have slots to start more workers now.
      TryStartIOWorkers(worker->GetLanguage());
    }
  }
  const auto &worker_type = worker->GetWorkerType();
  if (IsIOWorkerType(worker_type)) {
    auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
    io_worker_state.started_io_workers.insert(worker);
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
  HandleJobStarted(job_id, job_config);

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
        PopWorkerStatus status;
        StartWorkerProcess(Language::PYTHON, rpc::WorkerType::WORKER, job_id, &status);
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

  if (!io_worker_state.started_io_workers.count(worker)) {
    RAY_LOG(DEBUG)
        << "The IO worker has failed. Skip pushing it to the worker pool. Worker type: "
        << rpc::WorkerType_Name(worker_type) << ", worker id: " << worker->WorkerId();
    return;
  }

  RAY_LOG(DEBUG) << "Pushing an IO worker to the worker pool. Worker type: "
                 << rpc::WorkerType_Name(worker_type)
                 << ", worker id: " << worker->WorkerId();
  if (io_worker_state.pending_io_tasks.empty()) {
    io_worker_state.idle_io_workers.emplace(worker);
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
    RAY_LOG(DEBUG) << "There are no idle workers, try starting a new one. Try starting a "
                      "new one. Worker type: "
                   << rpc::WorkerType_Name(worker_type);
    TryStartIOWorkers(Language::PYTHON, worker_type);
  } else {
    const auto it = io_worker_state.idle_io_workers.begin();
    auto io_worker = *it;
    io_worker_state.idle_io_workers.erase(it);
    RAY_LOG(DEBUG) << "Popped an IO worker. Worker type: "
                   << rpc::WorkerType_Name(worker_type)
                   << ", worker ID: " << io_worker->WorkerId();
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

void WorkerPool::InvokePopWorkerCallbackForProcess(
    absl::flat_hash_map<StartupToken, TaskWaitingForWorkerInfo>
        &starting_workers_to_tasks,
    StartupToken startup_token,
    const std::shared_ptr<WorkerInterface> &worker,
    const PopWorkerStatus &status,
    bool *found,
    bool *worker_used,
    TaskID *task_id) {
  *found = false;
  *worker_used = false;
  auto it = starting_workers_to_tasks.find(startup_token);
  if (it != starting_workers_to_tasks.end()) {
    *found = true;
    *task_id = it->second.task_id;
    const auto &callback = it->second.callback;
    RAY_CHECK(callback);
    // This method shouldn't be invoked when runtime env creation has failed because
    // when runtime env is failed to be created, they are all
    // invoking the callback immediately.
    RAY_CHECK(status != PopWorkerStatus::RuntimeEnvCreationFailed);
    *worker_used = callback(worker, status, /*runtime_env_setup_error_message*/ "");
    starting_workers_to_tasks.erase(it);
  }
}

void WorkerPool::PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
  // Since the worker is now idle, unset its assigned task ID.
  RAY_CHECK(worker->GetAssignedTaskId().IsNil())
      << "Idle workers cannot have an assigned task ID";
  auto &state = GetStateForLanguage(worker->GetLanguage());
  bool found;
  bool used;
  TaskID task_id;
  InvokePopWorkerCallbackForProcess(state.starting_dedicated_workers_to_tasks,
                                    worker->GetStartupToken(),
                                    worker,
                                    PopWorkerStatus::OK,
                                    &found,
                                    &used,
                                    &task_id);
  if (found) {
    // The worker is used for the actor creation task with dynamic options.
    if (!used) {
      // Put it into idle dedicated worker pool.
      // TODO(SongGuyang): This worker will not be used forever. We should kill it.
      state.idle_dedicated_workers[task_id] = worker;
    }
    return;
  }

  InvokePopWorkerCallbackForProcess(state.starting_workers_to_tasks,
                                    worker->GetStartupToken(),
                                    worker,
                                    PopWorkerStatus::OK,
                                    &found,
                                    &used,
                                    &task_id);
  // The worker is not used for the actor creation task with dynamic options.
  if (!used) {
    // Put the worker to the idle pool.
    state.idle.insert(worker);
    int64_t now = get_time_();
    idle_of_all_languages_.emplace_back(worker, now);
    idle_of_all_languages_map_[worker] = now;
  }
}

void WorkerPool::TryKillingIdleWorkers() {
  RAY_CHECK(idle_of_all_languages_.size() == idle_of_all_languages_map_.size());

  int64_t now = get_time_();
  size_t running_size = 0;
  for (const auto &worker : GetAllRegisteredWorkers()) {
    if (!worker->IsDead() && worker->GetWorkerType() == rpc::WorkerType::WORKER) {
      running_size++;
    }
  }
  // Subtract the number of pending exit workers first. This will help us killing more
  // idle workers that it needs to.
  RAY_CHECK(running_size >= pending_exit_idle_workers_.size());
  running_size -= pending_exit_idle_workers_.size();
  // Kill idle workers in FIFO order.
  for (const auto &idle_pair : idle_of_all_languages_) {
    const auto &idle_worker = idle_pair.first;
    const auto &job_id = idle_worker->GetAssignedJobId();
    if (running_size <= static_cast<size_t>(num_workers_soft_limit_)) {
      if (!finished_jobs_.count(job_id)) {
        // Ignore the soft limit for jobs that have already finished, as we
        // should always clean up these workers.
        break;
      }
    }

    if (now - idle_pair.second <
        RayConfig::instance().idle_worker_killing_time_threshold_ms()) {
      break;
    }

    if (idle_worker->IsDead()) {
      // This worker has already been killed.
      // This is possible because a Java worker process may hold multiple workers.
      continue;
    }
    auto worker_startup_token = idle_worker->GetStartupToken();
    auto &worker_state = GetStateForLanguage(idle_worker->GetLanguage());

    auto it = worker_state.worker_processes.find(worker_startup_token);
    if (it != worker_state.worker_processes.end() &&
        it->second.num_starting_workers > 0) {
      // A Java worker process may hold multiple workers.
      // Some workers of this process are pending registration. Skip killing this worker.
      continue;
    }

    auto process = idle_worker->GetProcess();
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

      // Skip killing the worker process if there's any inflight `Exit` RPC requests to
      // this worker process.
      if (pending_exit_idle_workers_.count(worker->WorkerId())) {
        can_be_killed = false;
        break;
      }
    }
    if (!can_be_killed) {
      continue;
    }

    RAY_CHECK(running_size >= workers_in_the_same_process.size());
    if (running_size - workers_in_the_same_process.size() <
        static_cast<size_t>(num_workers_soft_limit_)) {
      // A Java worker process may contain multiple workers. Killing more workers than we
      // expect may slow the job.
      if (!finished_jobs_.count(job_id)) {
        // Ignore the soft limit for jobs that have already finished, as we
        // should always clean up these workers.
        return;
      }
    }

    for (const auto &worker : workers_in_the_same_process) {
      RAY_LOG(DEBUG) << "The worker pool has " << running_size
                     << " registered workers which exceeds the soft limit of "
                     << num_workers_soft_limit_ << ", and worker " << worker->WorkerId()
                     << " with pid " << process.GetId()
                     << " has been idle for a a while. Kill it.";
      // To avoid object lost issue caused by forcibly killing, send an RPC request to the
      // worker to allow it to do cleanup before exiting.
      if (!worker->IsDead()) {
        // Register the worker to pending exit so that we can correctly calculate the
        // running_size.
        // This also means that there's an inflight `Exit` RPC request to the worker.
        pending_exit_idle_workers_.emplace(worker->WorkerId(), worker);
        auto rpc_client = worker->rpc_client();
        RAY_CHECK(rpc_client);
        RAY_CHECK(running_size > 0);
        running_size--;
        rpc::ExitRequest request;
        rpc_client->Exit(
            request, [this, worker](const ray::Status &status, const rpc::ExitReply &r) {
              RAY_CHECK(pending_exit_idle_workers_.erase(worker->WorkerId()));
              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send exit request: " << status.ToString();
              }

              // In case of failed to send request, we remove it from pool as well
              // TODO (iycheng): We should handle the grpc failure in better way.
              if (!status.ok() || r.success()) {
                auto &worker_state = GetStateForLanguage(worker->GetLanguage());
                // If we could kill the worker properly, we remove them from the idle
                // pool.
                RemoveWorker(worker_state.idle, worker);
                // We always mark the worker as dead.
                // If the worker is not idle at this moment, we'd want to mark it as dead
                // so it won't be reused later.
                if (!worker->IsDead()) {
                  worker->MarkDead();
                }
              } else {
                // We re-insert the idle worker to the back of the queue if it fails to
                // kill the worker (e.g., when the worker owns the object). Without this,
                // if the first N workers own objects, it can't kill idle workers that are
                // >= N+1.
                const auto &idle_pair = idle_of_all_languages_.front();
                idle_of_all_languages_.push_back(idle_pair);
                idle_of_all_languages_.pop_front();
                RAY_CHECK(idle_of_all_languages_.size() ==
                          idle_of_all_languages_map_.size());
              }
            });
      } else {
        // Even it's a dead worker, we still need to remove them from the pool.
        RemoveWorker(worker_state.idle, worker);
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

void WorkerPool::PopWorker(const TaskSpecification &task_spec,
                           const PopWorkerCallback &callback,
                           const std::string &allocated_instances_serialized_json) {
  RAY_LOG(DEBUG) << "Pop worker for task " << task_spec.TaskId() << " task name "
                 << task_spec.FunctionDescriptor()->ToString();
  auto &state = GetStateForLanguage(task_spec.GetLanguage());

  std::shared_ptr<WorkerInterface> worker = nullptr;
  auto start_worker_process_fn = [this, allocated_instances_serialized_json](
                                     const TaskSpecification &task_spec,
                                     State &state,
                                     std::vector<std::string> dynamic_options,
                                     bool dedicated,
                                     const std::string &serialized_runtime_env_context,
                                     const PopWorkerCallback &callback) -> Process {
    PopWorkerStatus status = PopWorkerStatus::OK;
    auto [proc, startup_token] = StartWorkerProcess(task_spec.GetLanguage(),
                                                    rpc::WorkerType::WORKER,
                                                    task_spec.JobId(),
                                                    &status,
                                                    dynamic_options,
                                                    task_spec.GetRuntimeEnvHash(),
                                                    serialized_runtime_env_context,
                                                    allocated_instances_serialized_json,
                                                    task_spec.RuntimeEnvInfo());
    if (status == PopWorkerStatus::OK) {
      RAY_CHECK(proc.IsValid());
      WarnAboutSize();
      auto task_info = TaskWaitingForWorkerInfo{task_spec.TaskId(), callback};
      if (dedicated) {
        state.starting_dedicated_workers_to_tasks[startup_token] = std::move(task_info);
      } else {
        state.starting_workers_to_tasks[startup_token] = std::move(task_info);
      }
    } else {
      DeleteRuntimeEnvIfPossible(task_spec.SerializedRuntimeEnv());
      // TODO(SongGuyang): Wait until a worker is pushed or a worker can be started If
      // startup concurrency maxed out or job not started.
      PopWorkerCallbackAsync(callback, nullptr, status);
    }
    return proc;
  };

  if (task_spec.IsActorTask()) {
    // Code path of actor task.
    RAY_CHECK(false) << "Direct call shouldn't reach here.";
  } else if (task_spec.IsActorCreationTask() &&
             !task_spec.DynamicWorkerOptions().empty()) {
    // Code path of task that needs a dedicated worker: an actor creation task with
    // dynamic worker options.
    // Try to pop it from idle dedicated pool.
    auto it = state.idle_dedicated_workers.find(task_spec.TaskId());
    if (it != state.idle_dedicated_workers.end()) {
      // There is an idle dedicated worker for this task.
      worker = std::move(it->second);
      state.idle_dedicated_workers.erase(it);
    } else {
      // We are not pending a registration from a worker for this task,
      // so start a new worker process for this task.
      std::vector<std::string> dynamic_options = {};
      if (task_spec.IsActorCreationTask()) {
        dynamic_options = task_spec.DynamicWorkerOptions();
      }

      if (task_spec.HasRuntimeEnv()) {
        // create runtime env.
        RAY_LOG(DEBUG) << "[dedicated] Creating runtime env for task "
                       << task_spec.TaskId();
        GetOrCreateRuntimeEnv(
            task_spec.SerializedRuntimeEnv(),
            task_spec.RuntimeEnvConfig(),
            task_spec.JobId(),
            [this, start_worker_process_fn, callback, &state, task_spec, dynamic_options](
                bool successful,
                const std::string &serialized_runtime_env_context,
                const std::string &setup_error_message) {
              if (successful) {
                start_worker_process_fn(task_spec,
                                        state,
                                        dynamic_options,
                                        true,
                                        serialized_runtime_env_context,
                                        callback);
              } else {
                process_failed_runtime_env_setup_failed_++;
                callback(nullptr,
                         PopWorkerStatus::RuntimeEnvCreationFailed,
                         /*runtime_env_setup_error_message*/ setup_error_message);
                RAY_LOG(WARNING)
                    << "Create runtime env failed for task " << task_spec.TaskId()
                    << " and couldn't create the dedicated worker.";
              }
            },
            allocated_instances_serialized_json);
      } else {
        start_worker_process_fn(task_spec, state, dynamic_options, true, "", callback);
      }
    }
  } else {
    // Find an available worker which is already assigned to this job and which has
    // the specified runtime env.
    // Try to pop the most recently pushed worker.
    const int runtime_env_hash = task_spec.GetRuntimeEnvHash();
    for (auto it = idle_of_all_languages_.rbegin(); it != idle_of_all_languages_.rend();
         it++) {
      if (task_spec.GetLanguage() != it->first->GetLanguage() ||
          it->first->GetAssignedJobId() != task_spec.JobId() ||
          state.pending_disconnection_workers.count(it->first) > 0 ||
          it->first->IsDead()) {
        continue;
      }
      // These workers are exiting. So skip them.
      if (pending_exit_idle_workers_.count(it->first->WorkerId())) {
        continue;
      }
      // Skip if the runtime env doesn't match.
      if (runtime_env_hash != it->first->GetRuntimeEnvHash()) {
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
      if (task_spec.HasRuntimeEnv()) {
        // create runtime env.
        RAY_LOG(DEBUG) << "Creating runtime env for task " << task_spec.TaskId();
        GetOrCreateRuntimeEnv(
            task_spec.SerializedRuntimeEnv(),
            task_spec.RuntimeEnvConfig(),
            task_spec.JobId(),
            [this, start_worker_process_fn, callback, &state, task_spec](
                bool successful,
                const std::string &serialized_runtime_env_context,
                const std::string &setup_error_message) {
              if (successful) {
                start_worker_process_fn(task_spec,
                                        state,
                                        {},
                                        false,
                                        serialized_runtime_env_context,
                                        callback);
              } else {
                process_failed_runtime_env_setup_failed_++;
                callback(nullptr,
                         PopWorkerStatus::RuntimeEnvCreationFailed,
                         /*runtime_env_setup_error_message*/ setup_error_message);
                RAY_LOG(WARNING)
                    << "Create runtime env failed for task " << task_spec.TaskId()
                    << " and couldn't create the worker.";
              }
            },
            allocated_instances_serialized_json);
      } else {
        start_worker_process_fn(task_spec, state, {}, false, "", callback);
      }
    }
  }

  if (worker) {
    RAY_CHECK(worker->GetAssignedJobId() == task_spec.JobId());
    PopWorkerCallbackAsync(callback, worker);
  }
}

void WorkerPool::PrestartWorkers(const TaskSpecification &task_spec,
                                 int64_t backlog_size,
                                 int64_t num_available_cpus) {
  // Code path of task that needs a dedicated worker.
  if ((task_spec.IsActorCreationTask() && !task_spec.DynamicWorkerOptions().empty()) ||
      task_spec.HasRuntimeEnv()) {
    return;  // Not handled.
    // TODO(architkulkarni): We'd eventually like to prestart workers with the same
    // runtime env to improve initial startup performance.
  }

  auto &state = GetStateForLanguage(task_spec.GetLanguage());
  // The number of available workers that can be used for this task spec.
  int num_usable_workers = state.idle.size();
  for (auto &entry : state.worker_processes) {
    num_usable_workers += entry.second.num_starting_workers;
  }
  // Some existing workers may be holding less than 1 CPU each, so we should
  // start as many workers as needed to fill up the remaining CPUs.
  auto desired_usable_workers = std::min<int64_t>(num_available_cpus, backlog_size);
  if (num_usable_workers < desired_usable_workers) {
    // Account for workers that are idle or already starting.
    int64_t num_needed = desired_usable_workers - num_usable_workers;
    RAY_LOG(DEBUG) << "Prestarting " << num_needed << " workers given task backlog size "
                   << backlog_size << " and available CPUs " << num_available_cpus;
    for (int i = 0; i < num_needed; i++) {
      PopWorkerStatus status;
      StartWorkerProcess(
          task_spec.GetLanguage(), rpc::WorkerType::WORKER, task_spec.JobId(), &status);
    }
  }
}

void WorkerPool::DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                                  rpc::WorkerExitType disconnect_type) {
  MarkPortAsFree(worker->AssignedPort());
  auto &state = GetStateForLanguage(worker->GetLanguage());
  auto it = state.worker_processes.find(worker->GetStartupToken());
  if (it != state.worker_processes.end()) {
    if (!RemoveWorker(it->second.alive_started_workers, worker)) {
      // Worker is either starting or started,
      // if it's not started, we should remove it from starting.
      it->second.num_starting_workers--;
    }
    if (it->second.alive_started_workers.size() == 0 &&
        it->second.num_starting_workers == 0) {
      DeleteRuntimeEnvIfPossible(it->second.runtime_env_info.serialized_runtime_env());
      RemoveWorkerProcess(state, worker->GetStartupToken());
    }
  }
  RAY_CHECK(RemoveWorker(state.registered_workers, worker));

  if (IsIOWorkerType(worker->GetWorkerType())) {
    auto &io_worker_state =
        GetIOWorkerStateFromWorkerType(worker->GetWorkerType(), state);
    if (!RemoveWorker(io_worker_state.started_io_workers, worker)) {
      // IO worker is either starting or started,
      // if it's not started, we should remove it from starting.
      io_worker_state.num_starting_io_workers--;
    }
    RemoveWorker(io_worker_state.idle_io_workers, worker);
    return;
  }

  RAY_UNUSED(RemoveWorker(state.pending_disconnection_workers, worker));

  for (auto it = idle_of_all_languages_.begin(); it != idle_of_all_languages_.end();
       it++) {
    if (it->first == worker) {
      idle_of_all_languages_.erase(it);
      idle_of_all_languages_map_.erase(worker);
      break;
    }
  }
  RemoveWorker(state.idle, worker);
  if (disconnect_type != rpc::WorkerExitType::INTENDED_EXIT) {
    // A Java worker process may have multiple workers. If one of them disconnects
    // unintentionally (which means that the worker process has died), we remove the
    // others from idle pool so that the failed actor will not be rescheduled on the same
    // process.
    auto pid = worker->GetProcess().GetId();
    for (auto worker2 : state.registered_workers) {
      if (worker2->GetProcess().GetId() == pid) {
        // NOTE(kfstorm): We have to use a new field to record these workers (instead of
        // just removing them from idle sets) because they may haven't announced worker
        // port yet. When they announce worker port, they'll be marked idle again. So
        // removing them from idle sets here doesn't really prevent them from being popped
        // later.
        state.pending_disconnection_workers.insert(worker2);
      }
    }
  }
}

void WorkerPool::DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) {
  auto &state = GetStateForLanguage(driver->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_drivers, driver));
  MarkPortAsFree(driver->AssignedPort());
}

inline WorkerPool::State &WorkerPool::GetStateForLanguage(const Language &language) {
  auto state = states_by_lang_.find(language);
  RAY_CHECK(state != states_by_lang_.end())
      << "Required Language isn't supported: " << Language_Name(language);
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
    for (const auto &starting_process : state.worker_processes) {
      num_workers_started_or_registered += starting_process.second.num_starting_workers;
    }
    // Don't count IO workers towards the warning message threshold.
    num_workers_started_or_registered -= RayConfig::instance().max_io_workers() * 2;
    int64_t multiple = num_workers_started_or_registered / state.multiple_for_warning;
    std::stringstream warning_message;
    if (multiple >= 4 && multiple > state.last_warning_multiple) {
      // Push an error message to the user if the worker pool tells us that it is
      // getting too big.
      state.last_warning_multiple = multiple;
      warning_message
          << "WARNING: " << num_workers_started_or_registered << " "
          << Language_Name(entry.first)
          << " worker processes have been started on node: " << node_id_
          << " with address: " << node_address_ << ". "
          << "This could be a result of using "
          << "a large number of actors, or due to tasks blocked in ray.get() calls "
          << "(see https://github.com/ray-project/ray/issues/3644 for "
          << "some discussion of workarounds).";
      std::string warning_message_str = warning_message.str();
      RAY_LOG(WARNING) << warning_message_str;
      auto error_data_ptr = gcs::CreateErrorTableData(
          "worker_pool_large", warning_message_str, get_time_());
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
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

  int available_io_workers_num =
      io_worker_state.num_starting_io_workers + io_worker_state.started_io_workers.size();
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
      PopWorkerStatus status;
      auto [proc, startup_token] =
          StartWorkerProcess(ray::Language::PYTHON, worker_type, JobID::Nil(), &status);
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
  result << "\n- registered jobs: " << all_jobs_.size() - finished_jobs_.size();
  result << "\n- process_failed_job_config_missing: "
         << process_failed_job_config_missing_;
  result << "\n- process_failed_rate_limited: " << process_failed_rate_limited_;
  result << "\n- process_failed_pending_registration: "
         << process_failed_pending_registration_;
  result << "\n- process_failed_runtime_env_setup_failed: "
         << process_failed_runtime_env_setup_failed_;
  for (const auto &entry : states_by_lang_) {
    result << "\n- num " << Language_Name(entry.first)
           << " workers: " << entry.second.registered_workers.size();
    result << "\n- num " << Language_Name(entry.first)
           << " drivers: " << entry.second.registered_drivers.size();
    result << "\n- num object spill callbacks queued: "
           << entry.second.spill_io_worker_state.pending_io_tasks.size();
    result << "\n- num object restore queued: "
           << entry.second.restore_io_worker_state.pending_io_tasks.size();
    result << "\n- num util functions queued: "
           << entry.second.util_io_worker_state.pending_io_tasks.size();
  }
  result << "\n- num idle workers: " << idle_of_all_languages_.size();
  return result.str();
}

WorkerPool::IOWorkerState &WorkerPool::GetIOWorkerStateFromWorkerType(
    const rpc::WorkerType &worker_type, WorkerPool::State &state) const {
  RAY_CHECK(worker_type != rpc::WorkerType::WORKER)
      << worker_type << " type cannot be used to retrieve io_worker_state";
  switch (worker_type) {
  case rpc::WorkerType::SPILL_WORKER:
    return state.spill_io_worker_state;
  case rpc::WorkerType::RESTORE_WORKER:
    return state.restore_io_worker_state;
  default:
    RAY_LOG(FATAL) << "Unknown worker type: " << worker_type;
  }
  UNREACHABLE;
}

void WorkerPool::GetOrCreateRuntimeEnv(
    const std::string &serialized_runtime_env,
    const rpc::RuntimeEnvConfig &runtime_env_config,
    const JobID &job_id,
    const GetOrCreateRuntimeEnvCallback &callback,
    const std::string &serialized_allocated_resource_instances) {
  RAY_LOG(DEBUG) << "GetOrCreateRuntimeEnv " << serialized_runtime_env;
  agent_manager_->GetOrCreateRuntimeEnv(
      job_id,
      serialized_runtime_env,
      runtime_env_config,
      serialized_allocated_resource_instances,
      [job_id,
       serialized_runtime_env = std::move(serialized_runtime_env),
       runtime_env_config = std::move(runtime_env_config),
       callback](bool successful,
                 const std::string &serialized_runtime_env_context,
                 const std::string &setup_error_message) {
        if (successful) {
          callback(true, serialized_runtime_env_context, "");
        } else {
          RAY_LOG(WARNING) << "Couldn't create a runtime environment for job " << job_id
                           << ". The runtime environment was " << serialized_runtime_env
                           << ".";
          callback(false, "", setup_error_message);
        }
      });
}

void WorkerPool::DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env) {
  RAY_LOG(DEBUG) << "DeleteRuntimeEnvIfPossible " << serialized_runtime_env;
  if (!IsRuntimeEnvEmpty(serialized_runtime_env)) {
    agent_manager_->DeleteRuntimeEnvIfPossible(
        serialized_runtime_env, [serialized_runtime_env](bool successful) {
          if (!successful) {
            RAY_LOG(ERROR) << "Delete runtime env failed for " << serialized_runtime_env
                           << ".";
          }
        });
  }
}

}  // namespace raylet

}  // namespace ray
