#include "ray/raylet/worker_pool.h"

#include <sys/wait.h>

#include <algorithm>
#include <thread>

#include "ray/ray_config.h"
#include "ray/stats/stats.h"
#include "ray/status.h"
#include "ray/util/logging.h"

namespace {

// A helper function to get a worker from a list.
std::shared_ptr<ray::raylet::Worker> GetWorker(
    const std::unordered_set<std::shared_ptr<ray::raylet::Worker>> &worker_pool,
    const std::shared_ptr<ray::LocalClientConnection> &connection) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if ((*it)->Connection() == connection) {
      return (*it);
    }
  }
  return nullptr;
}

// A helper function to remove a worker from a list. Returns true if the worker
// was found and removed.
bool RemoveWorker(std::unordered_set<std::shared_ptr<ray::raylet::Worker>> &worker_pool,
                  const std::shared_ptr<ray::raylet::Worker> &worker) {
  return worker_pool.erase(worker) > 0;
}

}  // namespace

namespace ray {

namespace raylet {

/// A constructor that initializes a worker pool with
/// (num_worker_processes * num_workers_per_process) workers for each language.
WorkerPool::WorkerPool(
    int num_worker_processes, int num_workers_per_process,
    int maximum_startup_concurrency,
    const std::unordered_map<Language, std::vector<std::string>> &worker_commands)
    : num_workers_per_process_(num_workers_per_process),
      multiple_for_warning_(std::max(num_worker_processes, maximum_startup_concurrency)),
      maximum_startup_concurrency_(maximum_startup_concurrency),
      last_warning_multiple_(0) {
  RAY_CHECK(num_workers_per_process > 0) << "num_workers_per_process must be positive.";
  RAY_CHECK(maximum_startup_concurrency > 0);
  // Ignore SIGCHLD signals. If we don't do this, then worker processes will
  // become zombies instead of dying gracefully.
  signal(SIGCHLD, SIG_IGN);
  for (const auto &entry : worker_commands) {
    // Initialize the pool state for this language.
    auto &state = states_by_lang_[entry.first];
    // Set worker command for this language.
    state.worker_command = entry.second;
    RAY_CHECK(!state.worker_command.empty()) << "Worker command must not be empty.";
    // Force-start num_workers worker processes for this language.
    for (int i = 0; i < num_worker_processes; i++) {
      StartWorkerProcess(entry.first);
    }
  }
}

WorkerPool::~WorkerPool() {
  std::unordered_set<pid_t> pids_to_kill;
  for (const auto &entry : states_by_lang_) {
    // Kill all registered workers. NOTE(swang): This assumes that the registered
    // workers were started by the pool.
    for (const auto &worker : entry.second.registered_workers) {
      pids_to_kill.insert(worker->Pid());
    }
    // Kill all the workers that have been started but not registered.
    for (const auto &starting_worker : entry.second.starting_worker_processes) {
      pids_to_kill.insert(starting_worker.first);
    }
  }
  for (const auto &pid : pids_to_kill) {
    RAY_CHECK(pid > 0);
    kill(pid, SIGKILL);
  }
  // Waiting for the workers to be killed
  for (const auto &pid : pids_to_kill) {
    waitpid(pid, NULL, 0);
  }
}

uint32_t WorkerPool::Size(const Language &language) const {
  const auto state = states_by_lang_.find(language);
  if (state == states_by_lang_.end()) {
    return 0;
  } else {
    return static_cast<uint32_t>(state->second.idle.size() +
                                 state->second.idle_actor.size());
  }
}

void WorkerPool::StartWorkerProcess(const Language &language) {
  auto &state = GetStateForLanguage(language);
  // If we are already starting up too many workers, then return without starting
  // more.
  if (static_cast<int>(state.starting_worker_processes.size()) >=
      maximum_startup_concurrency_) {
    // Workers have been started, but not registered. Force start disabled -- returning.
    RAY_LOG(DEBUG) << "Worker not started, " << state.starting_worker_processes.size()
                   << " worker processes of language type " << static_cast<int>(language)
                   << " pending registration";
    return;
  }
  // Either there are no workers pending registration or the worker start is being forced.
  RAY_LOG(DEBUG) << "Starting new worker process, current pool has "
                 << state.idle_actor.size() << " actor workers, and " << state.idle.size()
                 << " non-actor workers";

  // Extract pointers from the worker command to pass into execvp.
  std::vector<const char *> worker_command_args;
  for (auto const &token : state.worker_command) {
    worker_command_args.push_back(token.c_str());
  }
  worker_command_args.push_back(nullptr);

  pid_t pid = StartProcess(worker_command_args);
  if (pid < 0) {
    // Failure case.
    RAY_LOG(FATAL) << "Failed to fork worker process: " << strerror(errno);
    return;
  } else if (pid > 0) {
    // Parent process case.
    RAY_LOG(DEBUG) << "Started worker process with pid " << pid;
    state.starting_worker_processes.emplace(
        std::make_pair(pid, num_workers_per_process_));
    return;
  }
}

pid_t WorkerPool::StartProcess(const std::vector<const char *> &worker_command_args) {
  // Launch the process to create the worker.
  pid_t pid = fork();

  if (pid != 0) {
    return pid;
  }

  // Child process case.
  // Reset the SIGCHLD handler for the worker.
  signal(SIGCHLD, SIG_DFL);

  // Try to execute the worker command.
  int rv = execvp(worker_command_args[0],
                  const_cast<char *const *>(worker_command_args.data()));
  // The worker failed to start. This is a fatal error.
  RAY_LOG(FATAL) << "Failed to start worker with return value " << rv << ": "
                 << strerror(errno);
  return 0;
}

void WorkerPool::RegisterWorker(const std::shared_ptr<Worker> &worker) {
  auto pid = worker->Pid();
  RAY_LOG(DEBUG) << "Registering worker with pid " << pid;
  auto &state = GetStateForLanguage(worker->GetLanguage());
  state.registered_workers.insert(std::move(worker));

  auto it = state.starting_worker_processes.find(pid);
  RAY_CHECK(it != state.starting_worker_processes.end());
  it->second--;
  if (it->second == 0) {
    state.starting_worker_processes.erase(it);
  }
}

void WorkerPool::RegisterDriver(const std::shared_ptr<Worker> &driver) {
  RAY_CHECK(!driver->GetAssignedTaskId().is_nil());
  auto &state = GetStateForLanguage(driver->GetLanguage());
  state.registered_drivers.insert(std::move(driver));
}

std::shared_ptr<Worker> WorkerPool::GetRegisteredWorker(
    const std::shared_ptr<LocalClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto worker = GetWorker(entry.second.registered_workers, connection);
    if (worker != nullptr) {
      return worker;
    }
  }
  return nullptr;
}

std::shared_ptr<Worker> WorkerPool::GetRegisteredDriver(
    const std::shared_ptr<LocalClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto driver = GetWorker(entry.second.registered_drivers, connection);
    if (driver != nullptr) {
      return driver;
    }
  }
  return nullptr;
}

void WorkerPool::PushWorker(const std::shared_ptr<Worker> &worker) {
  // Since the worker is now idle, unset its assigned task ID.
  RAY_CHECK(worker->GetAssignedTaskId().is_nil())
      << "Idle workers cannot have an assigned task ID";
  auto &state = GetStateForLanguage(worker->GetLanguage());
  // Add the worker to the idle pool.
  if (worker->GetActorId().is_nil()) {
    state.idle.insert(std::move(worker));
  } else {
    state.idle_actor[worker->GetActorId()] = std::move(worker);
  }
}

std::shared_ptr<Worker> WorkerPool::PopWorker(const TaskSpecification &task_spec) {
  auto &state = GetStateForLanguage(task_spec.GetLanguage());
  const auto &actor_id = task_spec.ActorId();
  std::shared_ptr<Worker> worker = nullptr;
  if (actor_id.is_nil()) {
    if (!state.idle.empty()) {
      worker = std::move(*state.idle.begin());
      state.idle.erase(state.idle.begin());
    }
  } else {
    auto actor_entry = state.idle_actor.find(actor_id);
    if (actor_entry != state.idle_actor.end()) {
      worker = std::move(actor_entry->second);
      state.idle_actor.erase(actor_entry);
    }
  }
  return worker;
}

bool WorkerPool::DisconnectWorker(const std::shared_ptr<Worker> &worker) {
  auto &state = GetStateForLanguage(worker->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_workers, worker));

  stats::CurrentWorker().Record(
      0, {{stats::LanguageKey, EnumNameLanguage(worker->GetLanguage())},
          {stats::WorkerPidKey, std::to_string(worker->Pid())}});

  return RemoveWorker(state.idle, worker);
}

void WorkerPool::DisconnectDriver(const std::shared_ptr<Worker> &driver) {
  auto &state = GetStateForLanguage(driver->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_drivers, driver));
  stats::CurrentDriver().Record(
      0, {{stats::LanguageKey, EnumNameLanguage(driver->GetLanguage())},
          {stats::WorkerPidKey, std::to_string(driver->Pid())}});
}

inline WorkerPool::State &WorkerPool::GetStateForLanguage(const Language &language) {
  auto state = states_by_lang_.find(language);
  RAY_CHECK(state != states_by_lang_.end()) << "Required Language isn't supported.";
  return state->second;
}

std::vector<std::shared_ptr<Worker>> WorkerPool::GetWorkersRunningTasksForDriver(
    const DriverID &driver_id) const {
  std::vector<std::shared_ptr<Worker>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (worker->GetAssignedDriverId() == driver_id) {
        workers.push_back(worker);
      }
    }
  }

  return workers;
}

std::string WorkerPool::WarningAboutSize() {
  int64_t num_workers_started_or_registered = 0;
  for (const auto &entry : states_by_lang_) {
    num_workers_started_or_registered +=
        static_cast<int64_t>(entry.second.registered_workers.size());
    num_workers_started_or_registered +=
        static_cast<int64_t>(entry.second.starting_worker_processes.size());
  }
  int64_t multiple = num_workers_started_or_registered / multiple_for_warning_;
  std::stringstream warning_message;
  if (multiple >= 3 && multiple > last_warning_multiple_) {
    last_warning_multiple_ = multiple;
    warning_message << "WARNING: " << num_workers_started_or_registered
                    << " workers have been started. This could be a result of using "
                    << "a large number of actors, or it could be a consequence of "
                    << "using nested tasks "
                    << "(see https://github.com/ray-project/ray/issues/3644) for "
                    << "some a discussion of workarounds.";
  }
  return warning_message.str();
}

std::string WorkerPool::DebugString() const {
  std::stringstream result;
  result << "WorkerPool:";
  for (const auto &entry : states_by_lang_) {
    result << "\n- num workers: " << entry.second.registered_workers.size();
    result << "\n- num drivers: " << entry.second.registered_drivers.size();
  }
  return result.str();
}

void WorkerPool::RecordMetrics() const {
  for (const auto &entry : states_by_lang_) {
    // Record worker.
    for (auto worker : entry.second.registered_workers) {
      stats::CurrentWorker().Record(
          worker->Pid(), {{stats::LanguageKey, EnumNameLanguage(worker->GetLanguage())},
                          {stats::WorkerPidKey, std::to_string(worker->Pid())}});
    }

    // Record driver.
    for (auto driver : entry.second.registered_drivers) {
      stats::CurrentDriver().Record(
          driver->Pid(), {{stats::LanguageKey, EnumNameLanguage(driver->GetLanguage())},
                          {stats::WorkerPidKey, std::to_string(driver->Pid())}});
    }
  }
}

}  // namespace raylet

}  // namespace ray
