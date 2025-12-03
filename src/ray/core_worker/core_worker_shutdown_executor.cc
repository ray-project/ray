// Copyright 2025 The Ray Authors.
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

#include "ray/core_worker/core_worker_shutdown_executor.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "ray/core_worker/core_worker.h"

namespace ray {

namespace core {

CoreWorkerShutdownExecutor::CoreWorkerShutdownExecutor(CoreWorker *core_worker)
    : core_worker_(core_worker) {}

void CoreWorkerShutdownExecutor::ExecuteGracefulShutdown(
    std::string_view exit_type,
    std::string_view detail,
    std::chrono::milliseconds timeout_ms) {
  RAY_LOG(DEBUG) << "Executing graceful shutdown: " << exit_type << " - " << detail
                 << " (timeout: " << timeout_ms.count() << "ms)";

  if (core_worker_->options_.worker_type == WorkerType::WORKER) {
    if (!core_worker_->worker_context_->GetCurrentActorID().IsNil()) {
      RAY_CHECK(core_worker_->actor_shutdown_callback_)
          << "actor_shutdown_callback_ must be set for actor workers";
      RAY_LOG(DEBUG) << "Calling actor shutdown callback";
      core_worker_->actor_shutdown_callback_();
    }

    // Actor shutdown callback has run; stop task execution service next.
    core_worker_->task_execution_service_.stop();
  }

  core_worker_->task_event_buffer_->FlushEvents(/*forced=*/true);
  core_worker_->task_event_buffer_->Stop();

  core_worker_->io_service_.stop();
  RAY_LOG(INFO) << "Waiting for joining a core worker io thread. If it hangs here, there "
                   "might be deadlock or a high load in the core worker io service.";
  if (core_worker_->io_thread_.joinable()) {
    // Check if we're already running in the IO thread to avoid self-join deadlock
    if (core_worker_->io_thread_.get_id() != boost::this_thread::get_id()) {
      core_worker_->io_thread_.join();
    } else {
      RAY_LOG(INFO)
          << "Skipping IO thread join since we're already running in the IO thread";
    }
  }

  // Shutdown gRPC server
  core_worker_->core_worker_server_->Shutdown();

  // Now that gcs_client is not used within io service, we can reset the pointer and clean
  // it up.
  if (core_worker_->gcs_client_) {
    RAY_LOG(INFO) << "Disconnecting a GCS client.";
    // TODO(55607): Move the Disconnect() logic to GcsClient destructor.
    // https://github.com/ray-project/ray/issues/55607
    core_worker_->gcs_client_->Disconnect();
    core_worker_->gcs_client_.reset();
  }

  RAY_LOG(INFO) << "Core worker ready to be deallocated.";
}

void CoreWorkerShutdownExecutor::ExecuteForceShutdown(std::string_view exit_type,
                                                      std::string_view detail) {
  KillChildProcessesImmediately();
  DisconnectServices(exit_type, detail, nullptr);
  QuickExit();
}

void CoreWorkerShutdownExecutor::ExecuteExit(
    std::string_view exit_type,
    std::string_view detail,
    std::chrono::milliseconds timeout_ms,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "Executing worker exit: " << exit_type << " - " << detail
                << " (timeout: " << timeout_ms.count() << "ms)";

  {
    absl::MutexLock lock(&core_worker_->mutex_);
    RAY_CHECK_NE(detail, "");
    core_worker_->exiting_detail_ = std::optional<std::string>{detail};
  }

  auto shutdown_callback = [this,
                            exit_type = std::string(exit_type),
                            detail = std::string(detail),
                            creation_task_exception_pb_bytes]() {
    // To avoid problems, make sure shutdown is always called from the same
    // event loop each time.
    core_worker_->task_execution_service_.post(
        [this, exit_type, detail, creation_task_exception_pb_bytes]() {
          rpc::DrainServerCallExecutor();
          KillChildProcessesImmediately();
          DisconnectServices(exit_type, detail, creation_task_exception_pb_bytes);
          ExecuteGracefulShutdown(
              exit_type, "Post-exit graceful shutdown", std::chrono::milliseconds{30000});
        },
        "CoreWorker.Shutdown");
  };

  auto drain_references_callback = [this, shutdown_callback]() {
    // Post to the event loop to avoid a deadlock between the TaskManager and
    // the ReferenceCounter. The deadlock can occur because this callback may
    // get called by the TaskManager while the ReferenceCounter's lock is held,
    // but the callback itself must acquire the ReferenceCounter's lock to
    // drain the object references.
    core_worker_->task_execution_service_.post(
        [this, shutdown_callback]() {
          RAY_LOG(INFO) << "Wait for currently executing tasks in the underlying thread "
                           "pools to finish.";
          // Wait for currently executing tasks in the underlying thread pools to
          // finish. Note that if tasks have been posted to the thread pools but not
          // started yet, they will not be executed.
          core_worker_->task_receiver_->Stop();

          // Release resources only after tasks have stopped executing.
          auto status = core_worker_->raylet_ipc_client_->NotifyWorkerBlocked();
          if (!status.ok()) {
            RAY_LOG(WARNING)
                << "Failed to notify Raylet. The raylet may have already shut down or "
                << "the connection was lost.";
          }

          bool not_actor_task = false;
          {
            absl::MutexLock lock(&core_worker_->mutex_);
            not_actor_task = core_worker_->actor_id_.IsNil();
          }
          if (not_actor_task) {
            // Normal tasks should not hold any object references in the heap after
            // executing, but they could in the case that one was stored as a glob
            // variable (anti-pattern, but possible). We decrement the reference count
            // for all local references to account for this. After this call, the only
            // references left to drain should be those that are in use by remote
            // workers. If these workers hold their references forever, the call to
            // drain the reference counter will hang forever and this process will not
            // exit until it is forcibly removed (e.g., via SIGKILL).
            //
            // NOTE(edoakes): this is only safe to do _after_ we have drained executing
            // tasks in the task_receiver_, otherwise there might still be user code
            // running that relies on the state of the reference counter.
            // See: https://github.com/ray-project/ray/pull/53002.
            RAY_LOG(INFO)
                << "Releasing local references, then draining reference counter.";
            core_worker_->reference_counter_->ReleaseAllLocalReferences();
            core_worker_->reference_counter_->DrainAndShutdown(shutdown_callback);
          } else {
            // If we are an actor, then we may be holding object references in the
            // heap. Then, we should not wait to drain the object references before
            // shutdown since this could hang.
            RAY_LOG(INFO)
                << "Not draining reference counter since this is an actor worker.";
            shutdown_callback();
          }
        },
        "CoreWorker.DrainAndShutdown");
  };

  core_worker_->task_manager_->DrainAndShutdown(drain_references_callback);
}

void CoreWorkerShutdownExecutor::ExecuteExitIfIdle(std::string_view exit_type,
                                                   std::string_view detail,
                                                   std::chrono::milliseconds timeout_ms) {
  RAY_LOG(INFO) << "Executing handle exit: " << exit_type << " - " << detail
                << " (timeout: " << timeout_ms.count() << "ms)";

  if (ShouldWorkerIdleExit()) {
    auto actual_timeout = timeout_ms;
    if (actual_timeout.count() == -1) {
      actual_timeout = std::chrono::milliseconds{10000};  // 10s default
    }

    ExecuteExit(exit_type, detail, actual_timeout, nullptr);
  } else {
    RAY_LOG(INFO) << "Worker not idle, ignoring exit request: " << detail;
  }
}

void CoreWorkerShutdownExecutor::KillChildProcessesImmediately() {
  if (!RayConfig::instance().kill_child_processes_on_worker_exit()) {
    RAY_LOG(DEBUG)
        << "kill_child_processes_on_worker_exit is not true, skipping KillChildProcs";
    return;
  }

  RAY_LOG(DEBUG) << "kill_child_processes_on_worker_exit true, KillChildProcs";
  auto maybe_child_procs = GetAllProcsWithPpid(GetPID());

  // Enumerating child procs is not supported on this platform.
  if (!maybe_child_procs) {
    RAY_LOG(DEBUG) << "Killing leaked procs not supported on this platform.";
    return;
  }

  const auto &child_procs = *maybe_child_procs;
  const auto child_procs_str = absl::StrJoin(child_procs, ",");
  RAY_LOG(INFO) << "Try killing all child processes of this worker as it exits. "
                << "Child process pids: " << child_procs_str;

  for (const auto &child_pid : child_procs) {
    auto maybe_error_code = KillProc(child_pid);
    RAY_CHECK(maybe_error_code)
        << "Expected this path to only be called when KillProc is supported.";
    auto error_code = *maybe_error_code;

    RAY_LOG(INFO) << "Kill result for child pid " << child_pid << ": "
                  << error_code.message() << ", bool " << static_cast<bool>(error_code);
    if (error_code) {
      RAY_LOG(WARNING) << "Unable to kill potentially leaked process " << child_pid
                       << ": " << error_code.message();
    }
  }
}

bool CoreWorkerShutdownExecutor::ShouldWorkerIdleExit() const {
  return core_worker_->IsIdle();
}

void CoreWorkerShutdownExecutor::DisconnectServices(
    std::string_view exit_type,
    std::string_view detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  core_worker_->RecordMetrics();

  if (core_worker_->options_.worker_type == WorkerType::DRIVER &&
      core_worker_->task_event_buffer_->Enabled() &&
      !RayConfig::instance().task_events_skip_driver_for_test()) {
    auto task_event = std::make_unique<worker::TaskStatusEvent>(
        core_worker_->worker_context_->GetCurrentTaskID(),
        core_worker_->worker_context_->GetCurrentJobID(),
        /* attempt_number */ 0,
        rpc::TaskStatus::FINISHED,
        /* timestamp */ absl::GetCurrentTimeNanos(),
        /*is_actor_task_event=*/
        core_worker_->worker_context_->GetCurrentActorID().IsNil(),
        core_worker_->options_.session_name);
    core_worker_->task_event_buffer_->AddTaskEvent(std::move(task_event));
  }

  opencensus::stats::StatsExporter::ExportNow();
  if (core_worker_->connected_) {
    RAY_LOG(INFO) << "Sending disconnect message to the local raylet.";
    core_worker_->connected_ = false;
    if (core_worker_->raylet_ipc_client_) {
      rpc::WorkerExitType worker_exit_type = rpc::WorkerExitType::INTENDED_USER_EXIT;
      if (exit_type == "INTENDED_SYSTEM_EXIT") {
        worker_exit_type = rpc::WorkerExitType::INTENDED_SYSTEM_EXIT;
      } else if (exit_type == "USER_ERROR") {
        worker_exit_type = rpc::WorkerExitType::USER_ERROR;
      } else if (exit_type == "SYSTEM_ERROR") {
        worker_exit_type = rpc::WorkerExitType::SYSTEM_ERROR;
      } else if (exit_type == "NODE_OUT_OF_MEMORY") {
        worker_exit_type = rpc::WorkerExitType::NODE_OUT_OF_MEMORY;
      }

      Status status = core_worker_->raylet_ipc_client_->Disconnect(
          worker_exit_type, std::string(detail), creation_task_exception_pb_bytes);
      if (status.ok()) {
        RAY_LOG(INFO) << "Disconnected from the local raylet.";
      } else {
        RAY_LOG(WARNING) << "Failed to disconnect from the local raylet: " << status;
      }
    }
  }
}

void CoreWorkerShutdownExecutor::QuickExit() {
  RAY_LOG(WARNING) << "Quick exit - terminating process immediately";
  ray::QuickExit();
  RAY_LOG(WARNING) << "Quick exit - this line should never be reached";
}
}  // namespace core
}  // namespace ray
