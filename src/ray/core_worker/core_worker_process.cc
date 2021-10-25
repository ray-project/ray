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
#include "ray/stats/stats.h"
#include "ray/util/event.h"
#include "ray/util/util.h"

namespace ray {
namespace core {
namespace {

std::atomic<bool> logging_enabled;

void HandleAtExit() {
  RAY_LOG(DEBUG) << "Stats stop in core worker.";
  // stats::Shutdown();
  if (logging_enabled) {
    RayLog::ShutDownRayLog();
  }
}

/// Threadsafety Singletone state for core_worker_process.
struct State {
  static State &Get() {
    static State state;
    return state;
  }

  /// Check that the core worker environment is initialized for this process.
  void EnsureInitialized() const SHARED_LOCKS_REQUIRED(mutex) {
    RAY_CHECK(core_worker_process != nullptr)
        << "The core worker process is not initialized yet or already "
        << "shutdown.";
  }

  /// The instance of `CoreWorkerProcessImpl`.
  std::unique_ptr<CoreWorkerProcessImpl> core_worker_process GUARDED_BY(mutex);

  /// To protect access to core_worker_process;
  absl::Mutex mutex;
};
}  // namespace

void CoreWorkerProcess::Initialize(const CoreWorkerOptions &options) {
  auto &state = State::Get();
  absl::WriterMutexLock lock(&state.mutex);
  RAY_CHECK(!state.core_worker_process)
      << "The process is already initialized for core worker.";

  logging_enabled = options.enable_logging;

#ifndef _WIN32
  // NOTE(scv119): std::atexit should be put before creation of core_worker_process.
  // We assume that spdlog has been initialized before this line. When the
  // process is exiting, `HandleAtExit` will be invoked before destructing spdlog static
  // variables. We register HandleAtExist before creation of CoreWorkerProcessImpl. This
  // prevents crashing (or hanging) when using `RAY_LOG` in `CoreWorkerProcesImpl`
  // destructor.
  RAY_CHECK(std::atexit(HandleAtExit) == 0);
#endif

  state.core_worker_process.reset(new CoreWorkerProcessImpl(options));
}

void CoreWorkerProcess::Shutdown() {
  auto &state = State::Get();
  absl::WriterMutexLock lock(&state.mutex);
  if (!state.core_worker_process) {
    return;
  }
  state.core_worker_process->ShutdownDriver();
  state.core_worker_process.reset();
}

bool CoreWorkerProcess::IsInitialized() {
  auto &state = State::Get();
  absl::ReaderMutexLock lock(&state.mutex);
  return state.core_worker_process != nullptr;
}

CoreWorker &CoreWorkerProcess::GetCoreWorker() {
  auto &state = State::Get();
  absl::ReaderMutexLock lock(&state.mutex);
  state.EnsureInitialized();
  return state.core_worker_process->GetCoreWorkerForCurrentThread();
}

void CoreWorkerProcess::SetCurrentThreadWorkerId(const WorkerID &worker_id) {
  auto &state = State::Get();
  absl::ReaderMutexLock lock(&state.mutex);
  state.EnsureInitialized();
  state.core_worker_process->SetThreadLocalWorkerById(worker_id);
}

void CoreWorkerProcess::RunTaskExecutionLoop() {
  auto &state = State::Get();
  {
    absl::ReaderMutexLock lock(&state.mutex);
    state.EnsureInitialized();
    state.core_worker_process->RunWorkerTaskExecutionLoop();
  }

  {
    absl::WriterMutexLock lock(&state.mutex);
    state.core_worker_process.reset();
  }
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::TryGetWorker(const WorkerID &worker_id) {
  auto &state = State::Get();
  absl::ReaderMutexLock lock(&state.mutex);
  state.EnsureInitialized();
  if (!state.core_worker_process) {
    return nullptr;
  }
  return state.core_worker_process->GetWorker(worker_id);
}

}  // namespace core
}  // namespace ray
