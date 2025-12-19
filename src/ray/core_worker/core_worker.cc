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

#include <algorithm>
#include <future>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/core_worker/core_worker_shutdown_executor.h"
#include "ray/core_worker/shutdown_coordinator.h"

#ifndef _WIN32
#include <unistd.h>
#endif

#include <google/protobuf/util/json_util.h>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/protobuf_utils.h"
#include "ray/common/ray_config.h"
#include "ray/common/runtime_env_common.h"
#include "ray/common/task/task_util.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/rpc/event_aggregator_client.h"
#include "ray/util/container_util.h"
#include "ray/util/event.h"
#include "ray/util/subreaper.h"
#include "ray/util/time.h"

using json = nlohmann::json;
using MessageType = ray::protocol::MessageType;

namespace ray::core {

namespace {
// Default capacity for serialization caches.
constexpr size_t kDefaultSerializationCacheCap = 500;

// Implements setting the transient RUNNING_IN_RAY_GET and RUNNING_IN_RAY_WAIT states.
// These states override the RUNNING state of a task.
class ScopedTaskMetricSetter {
 public:
  ScopedTaskMetricSetter(const WorkerContext &ctx,
                         TaskCounter &ctr,
                         rpc::TaskStatus status)
      : status_(status), ctr_(ctr) {
    auto task_spec = ctx.GetCurrentTask();
    if (task_spec != nullptr) {
      task_name_ = task_spec->GetName();
      is_retry_ = task_spec->IsRetry();
    } else {
      task_name_ = "Unknown task";
      is_retry_ = false;
    }
    ctr_.SetMetricStatus(task_name_, status, is_retry_);
  }

  ScopedTaskMetricSetter(const ScopedTaskMetricSetter &) = delete;
  ScopedTaskMetricSetter &operator=(const ScopedTaskMetricSetter &) = delete;

  ~ScopedTaskMetricSetter() { ctr_.UnsetMetricStatus(task_name_, status_, is_retry_); }

 private:
  rpc::TaskStatus status_;
  TaskCounter &ctr_;
  std::string task_name_;
  bool is_retry_;
};

using ActorLifetime = ray::rpc::JobConfig_ActorLifetime;

// Helper function converts GetObjectLocationsOwnerReply to ObjectLocation
ObjectLocation CreateObjectLocation(
    const rpc::WorkerObjectLocationsPubMessage &object_info) {
  std::vector<NodeID> node_ids;
  node_ids.reserve(object_info.node_ids_size());
  for (int i = 0; i < object_info.node_ids_size(); ++i) {
    node_ids.push_back(NodeID::FromBinary(object_info.node_ids(i)));
  }
  bool is_spilled = !object_info.spilled_url().empty();
  // If the object size is unknown it's unset, and we use -1 to indicate that.
  int64_t object_size = object_info.object_size() == 0 ? -1 : object_info.object_size();
  return ObjectLocation(NodeID::FromBinary(object_info.primary_node_id()),
                        object_size,
                        std::move(node_ids),
                        is_spilled,
                        object_info.spilled_url(),
                        NodeID::FromBinary(object_info.spilled_node_id()),
                        object_info.did_spill());
}

std::optional<ObjectLocation> TryGetLocalObjectLocation(
    ReferenceCounterInterface &reference_counter, const ObjectID &object_id) {
  if (!reference_counter.HasReference(object_id)) {
    return std::nullopt;
  }
  rpc::WorkerObjectLocationsPubMessage object_info;
  reference_counter.FillObjectInformation(object_id, &object_info);
  // Note: there can be a TOCTOU race condition: HasReference returned true, but before
  // FillObjectInformation the object is released. Hence we check the ref_removed field.
  if (object_info.ref_removed()) {
    return std::nullopt;
  }
  return CreateObjectLocation(object_info);
}

/// Converts rpc::WorkerExitType to ShutdownReason
/// \param exit_type The worker exit type to convert
/// \param is_force_exit If true, INTENDED_USER_EXIT maps to kForcedExit; otherwise
/// kGracefulExit
ShutdownReason ConvertExitTypeToShutdownReason(rpc::WorkerExitType exit_type,
                                               bool is_force_exit = false) {
  switch (exit_type) {
  case rpc::WorkerExitType::INTENDED_SYSTEM_EXIT:
    return ShutdownReason::kIntentionalShutdown;
  case rpc::WorkerExitType::INTENDED_USER_EXIT:
    return is_force_exit ? ShutdownReason::kForcedExit : ShutdownReason::kGracefulExit;
  case rpc::WorkerExitType::USER_ERROR:
    return ShutdownReason::kUserError;
  case rpc::WorkerExitType::SYSTEM_ERROR:
    return ShutdownReason::kUnexpectedError;
  case rpc::WorkerExitType::NODE_OUT_OF_MEMORY:
    return ShutdownReason::kOutOfMemory;
  default:
    return ShutdownReason::kUnexpectedError;
  }
}

}  // namespace

JobID GetProcessJobID(const CoreWorkerOptions &options) {
  if (options.worker_type == WorkerType::DRIVER) {
    RAY_CHECK(!options.job_id.IsNil());
  } else {
    RAY_CHECK(options.job_id.IsNil());
  }

  if (options.worker_type == WorkerType::WORKER) {
    // For workers, the job ID is assigned by Raylet via an environment variable.
    const std::string &job_id_env = RayConfig::instance().JOB_ID();
    RAY_CHECK(!job_id_env.empty());
    return JobID::FromHex(job_id_env);
  }
  return options.job_id;
}

TaskCounter::TaskCounter(ray::observability::MetricInterface &task_by_state_gauge,
                         ray::observability::MetricInterface &actor_by_state_gauge)
    : task_by_state_gauge_(task_by_state_gauge),
      actor_by_state_gauge_(actor_by_state_gauge) {
  counter_.SetOnChangeCallback(
      [this](const std::tuple<std::string, TaskStatusType, bool>
                 &key) ABSL_EXCLUSIVE_LOCKS_REQUIRED(&mu_) mutable {
        if (std::get<1>(key) != TaskStatusType::kRunning) {
          return;
        }
        const auto &func_name = std::get<0>(key);
        const auto is_retry = std::get<2>(key);
        const int64_t running_total = counter_.Get(key);
        const int64_t num_in_get = running_in_get_counter_.Get({func_name, is_retry});
        const int64_t num_in_wait = running_in_wait_counter_.Get({func_name, is_retry});
        const int64_t num_getting_pinning_args =
            pending_getting_and_pinning_args_fetch_counter_.Get({func_name, is_retry});
        const auto is_retry_label = is_retry ? "1" : "0";
        // RUNNING_IN_RAY_GET/WAIT are sub-states of RUNNING, so we need to subtract
        // them out to avoid double-counting.
        task_by_state_gauge_.Record(
            running_total - num_in_get - num_in_wait - num_getting_pinning_args,
            {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING)},
             {"Name", func_name},
             {"IsRetry", is_retry_label},
             {"JobId", job_id_},
             {"Source", "executor"}});
        // Negate the metrics recorded from the submitter process for these tasks.
        task_by_state_gauge_.Record(
            -running_total,
            {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::SUBMITTED_TO_WORKER)},
             {"Name", func_name},
             {"IsRetry", is_retry_label},
             {"JobId", job_id_},
             {"Source", "executor"}});
        // Record sub-state for get.
        task_by_state_gauge_.Record(
            num_in_get,
            {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING_IN_RAY_GET)},
             {"Name", func_name},
             {"IsRetry", is_retry_label},
             {"JobId", job_id_},
             {"Source", "executor"}});
        // Record sub-state for wait.
        task_by_state_gauge_.Record(
            num_in_wait,
            {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::RUNNING_IN_RAY_WAIT)},
             {"Name", func_name},
             {"IsRetry", is_retry_label},
             {"JobId", job_id_},
             {"Source", "executor"}});
        // Record sub-state for pending args fetch.
        task_by_state_gauge_.Record(
            num_getting_pinning_args,
            {{"State", rpc::TaskStatus_Name(rpc::TaskStatus::GETTING_AND_PINNING_ARGS)},
             {"Name", func_name},
             {"IsRetry", is_retry_label},
             {"JobId", job_id_},
             {"Source", "executor"}});
      });
}

void TaskCounter::RecordMetrics() {
  absl::MutexLock l(&mu_);
  counter_.FlushOnChangeCallbacks();
  if (IsActor()) {
    float running_tasks = 0.0;
    float idle = 0.0;
    if (num_tasks_running_ == 0) {
      idle = 1.0;
    } else {
      running_tasks = 1.0;
    }
    actor_by_state_gauge_.Record(idle,
                                 {{"State", "ALIVE_IDLE"},
                                  {"Name", actor_name_},
                                  {"Source", "executor"},
                                  {"JobId", job_id_}});
    actor_by_state_gauge_.Record(running_tasks,
                                 {{"State", "ALIVE_RUNNING_TASKS"},
                                  {"Name", actor_name_},
                                  {"Source", "executor"},
                                  {"JobId", job_id_}});
  }
}

void TaskCounter::SetMetricStatus(const std::string &func_name,
                                  rpc::TaskStatus status,
                                  bool is_retry) {
  absl::MutexLock l(&mu_);
  // Add a no-op increment to counter_ so that
  // it will invoke a callback upon RecordMetrics.
  counter_.Increment({func_name, TaskStatusType::kRunning, is_retry}, 0);
  if (status == rpc::TaskStatus::RUNNING_IN_RAY_GET) {
    running_in_get_counter_.Increment({func_name, is_retry});
  } else if (status == rpc::TaskStatus::RUNNING_IN_RAY_WAIT) {
    running_in_wait_counter_.Increment({func_name, is_retry});
  } else if (status == rpc::TaskStatus::GETTING_AND_PINNING_ARGS) {
    pending_getting_and_pinning_args_fetch_counter_.Increment({func_name, is_retry});
  } else {
    RAY_CHECK(false) << "Unexpected status " << rpc::TaskStatus_Name(status);
  }
}

void TaskCounter::UnsetMetricStatus(const std::string &func_name,
                                    rpc::TaskStatus status,
                                    bool is_retry) {
  absl::MutexLock l(&mu_);
  // Add a no-op decrement to counter_ so that
  // it will invoke a callback upon RecordMetrics.
  counter_.Decrement({func_name, TaskStatusType::kRunning, is_retry}, 0);
  if (status == rpc::TaskStatus::RUNNING_IN_RAY_GET) {
    running_in_get_counter_.Decrement({func_name, is_retry});
  } else if (status == rpc::TaskStatus::RUNNING_IN_RAY_WAIT) {
    running_in_wait_counter_.Decrement({func_name, is_retry});
  } else if (status == rpc::TaskStatus::GETTING_AND_PINNING_ARGS) {
    pending_getting_and_pinning_args_fetch_counter_.Decrement({func_name, is_retry});
  } else {
    RAY_LOG(FATAL) << "Unexpected status " << rpc::TaskStatus_Name(status);
  }
}

CoreWorker::CoreWorker(
    CoreWorkerOptions options,
    std::unique_ptr<WorkerContext> worker_context,
    instrumented_io_context &io_service,
    std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
    std::shared_ptr<rpc::RayletClientPool> raylet_client_pool,
    std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
    std::unique_ptr<rpc::GrpcServer> core_worker_server,
    rpc::Address rpc_address,
    std::shared_ptr<gcs::GcsClient> gcs_client,
    std::shared_ptr<ipc::RayletIpcClientInterface> raylet_ipc_client,
    std::shared_ptr<RayletClientInterface> local_raylet_rpc_client,
    boost::thread &io_thread,
    std::shared_ptr<ReferenceCounterInterface> reference_counter,
    std::shared_ptr<CoreWorkerMemoryStore> memory_store,
    std::shared_ptr<CoreWorkerPlasmaStoreProvider> plasma_store_provider,
    std::shared_ptr<experimental::MutableObjectProviderInterface>
        experimental_mutable_object_provider,
    std::unique_ptr<FutureResolver> future_resolver,
    std::shared_ptr<TaskManager> task_manager,
    std::shared_ptr<ActorCreatorInterface> actor_creator,
    std::unique_ptr<ActorTaskSubmitter> actor_task_submitter,
    std::unique_ptr<pubsub::PublisherInterface> object_info_publisher,
    std::unique_ptr<pubsub::SubscriberInterface> object_info_subscriber,
    std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter,
    std::unique_ptr<NormalTaskSubmitter> normal_task_submitter,
    std::unique_ptr<ObjectRecoveryManager> object_recovery_manager,
    std::unique_ptr<ActorManager> actor_manager,
    instrumented_io_context &task_execution_service,
    std::unique_ptr<worker::TaskEventBuffer> task_event_buffer,
    uint32_t pid,
    ray::observability::MetricInterface &task_by_state_gauge,
    ray::observability::MetricInterface &actor_by_state_gauge)
    : options_(std::move(options)),
      get_call_site_(RayConfig::instance().record_ref_creation_sites()
                         ? options_.get_lang_stack
                         : nullptr),
      worker_context_(std::move(worker_context)),
      io_service_(io_service),
      core_worker_client_pool_(std::move(core_worker_client_pool)),
      raylet_client_pool_(std::move(raylet_client_pool)),
      periodical_runner_(std::move(periodical_runner)),
      core_worker_server_(std::move(core_worker_server)),
      rpc_address_(std::move(rpc_address)),
      connected_(true),
      gcs_client_(std::move(gcs_client)),
      raylet_ipc_client_(std::move(raylet_ipc_client)),
      local_raylet_rpc_client_(std::move(local_raylet_rpc_client)),
      io_thread_(io_thread),
      reference_counter_(std::move(reference_counter)),
      memory_store_(std::move(memory_store)),
      plasma_store_provider_(std::move(plasma_store_provider)),
      experimental_mutable_object_provider_(
          std::move(experimental_mutable_object_provider)),
      future_resolver_(std::move(future_resolver)),
      task_manager_(std::move(task_manager)),
      actor_creator_(std::move(actor_creator)),
      actor_task_submitter_(std::move(actor_task_submitter)),
      object_info_publisher_(std::move(object_info_publisher)),
      object_info_subscriber_(std::move(object_info_subscriber)),
      lease_request_rate_limiter_(std::move(lease_request_rate_limiter)),
      normal_task_submitter_(std::move(normal_task_submitter)),
      object_recovery_manager_(std::move(object_recovery_manager)),
      actor_manager_(std::move(actor_manager)),
      actor_id_(ActorID::Nil()),
      task_queue_length_(0),
      num_executed_tasks_(0),
      num_get_pin_args_in_flight_(0),
      num_failed_get_pin_args_(0),
      task_execution_service_(task_execution_service),
      exiting_detail_(std::nullopt),
      max_direct_call_object_size_(RayConfig::instance().max_direct_call_object_size()),
      task_counter_(task_by_state_gauge, actor_by_state_gauge),
      task_event_buffer_(std::move(task_event_buffer)),
      pid_(pid),
      actor_shutdown_callback_(options_.actor_shutdown_callback),
      runtime_env_json_serialization_cache_(kDefaultSerializationCacheCap),
      free_actor_object_callback_(
          [this, free_actor_object_callback = options_.free_actor_object_callback](
              const ObjectID &object_id) {
            // Need to post to the io service to prevent deadlock because this submits a
            // task and therefore needs to acquire the reference counter lock.
            io_service_.post([free_actor_object_callback,
                              object_id]() { free_actor_object_callback(object_id); },
                             "CoreWorker.FreeActorObjectCallback");
          }) {
  // Initialize task receivers.
  if (options_.worker_type == WorkerType::WORKER || options_.is_local_mode) {
    RAY_CHECK(options_.task_execution_callback != nullptr);
    auto execute_task = std::bind(&CoreWorker::ExecuteTask,
                                  this,
                                  std::placeholders::_1,
                                  std::placeholders::_2,
                                  std::placeholders::_3,
                                  std::placeholders::_4,
                                  std::placeholders::_5,
                                  std::placeholders::_6,
                                  std::placeholders::_7,
                                  std::placeholders::_8);
    task_argument_waiter_ = std::make_unique<DependencyWaiterImpl>(
        [this](const std::vector<rpc::ObjectReference> &dependencies, int64_t tag) {
          return raylet_ipc_client_->WaitForActorCallArgs(dependencies, tag);
        });
    task_receiver_ = std::make_unique<TaskReceiver>(
        task_execution_service_,
        *task_event_buffer_,
        execute_task,
        *task_argument_waiter_,
        options_.initialize_thread_callback,
        [this] { return raylet_ipc_client_->ActorCreationTaskDone(); });
  }

  RegisterToGcs(options_.worker_launch_time_ms, options_.worker_launched_time_ms);

  SubscribeToNodeChanges();

  // Create an entry for the driver task in the task table. This task is
  // added immediately with status RUNNING. This allows us to push errors
  // related to this driver task back to the driver. For example, if the
  // driver creates an object that is later evicted, we should notify the
  // user that we're unable to reconstruct the object, since we cannot
  // rerun the driver.
  if (options_.worker_type == WorkerType::DRIVER) {
    TaskSpecBuilder builder;
    const TaskID task_id = TaskID::ForDriverTask(worker_context_->GetCurrentJobID());
    builder.SetDriverTaskSpec(task_id,
                              options_.language,
                              worker_context_->GetCurrentJobID(),
                              // Driver has no parent task
                              /*parent_task_id=*/TaskID::Nil(),
                              GetCallerId(),
                              rpc_address_,
                              TaskID::Nil());
    // Drivers are never re-executed.
    SetCurrentTaskId(task_id, /*attempt_number=*/0, "driver");

    // Add the driver task info.
    if (task_event_buffer_->Enabled() &&
        !RayConfig::instance().task_events_skip_driver_for_test()) {
      auto spec = std::move(builder).ConsumeAndBuild();
      auto job_id = spec.JobId();
      auto task_event = std::make_unique<worker::TaskStatusEvent>(
          task_id,
          std::move(job_id),
          /*attempt_number=*/0,
          rpc::TaskStatus::RUNNING,
          /*timestamp=*/absl::GetCurrentTimeNanos(),
          /*is_actor_task_event=*/false,
          options_.session_name,
          std::make_shared<const TaskSpecification>(std::move(spec)));
      task_event_buffer_->AddTaskEvent(std::move(task_event));
    }
  }

  if (options_.worker_type != WorkerType::DRIVER) {
    periodical_runner_->RunFnPeriodically(
        [this] { ExitIfParentRayletDies(); },
        RayConfig::instance().raylet_death_check_interval_milliseconds(),
        "CoreWorker.ExitIfParentRayletDies");
  }

  /// If periodic asio stats print is enabled, it will print it.
  const auto event_stats_print_interval_ms =
      RayConfig::instance().event_stats_print_interval_ms();
  if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    periodical_runner_->RunFnPeriodically(
        [this] {
          RAY_LOG(INFO) << "Event stats:\n\n"
                        << io_service_.stats()->StatsString() << "\n\n"
                        << "-----------------\n"
                        << "Task execution event stats:\n"
                        << task_execution_service_.stats()->StatsString() << "\n\n"
                        << "-----------------\n"
                        << "Task Event stats:\n"
                        << task_event_buffer_->DebugString() << "\n";
        },
        event_stats_print_interval_ms,
        "CoreWorker.PrintEventStats");
  }

  periodical_runner_->RunFnPeriodically(
      [this] {
        const auto lost_objects = reference_counter_->FlushObjectsToRecover();
        if (!lost_objects.empty()) {
          // Keep :info_message: in sync with LOG_PREFIX_INFO_MESSAGE in ray_constants.py.
          RAY_LOG(ERROR) << ":info_message: Attempting to recover " << lost_objects.size()
                         << " lost objects by resubmitting their tasks or setting a new "
                            "primary location from existing copies. To disable object "
                            "reconstruction, set @ray.remote(max_retries=0).";
          // Delete the objects from the in-memory store to indicate that they are not
          // available. The object recovery manager will guarantee that a new value
          // will eventually be stored for the objects (either an
          // UnreconstructableError or a value reconstructed from lineage).
          memory_store_->Delete(lost_objects);
          for (const auto &object_id : lost_objects) {
            // NOTE(swang): There is a race condition where this can return false if
            // the reference went out of scope since the call to the ref counter to get
            // the lost objects. It's okay to not mark the object as failed or recover
            // the object since there are no reference holders.
            RAY_UNUSED(object_recovery_manager_->RecoverObject(object_id));
          }
        }
      },
      100,
      "CoreWorker.RecoverObjects");

  periodical_runner_->RunFnPeriodically(
      [this] { InternalHeartbeat(); },
      RayConfig::instance().core_worker_internal_heartbeat_ms(),
      "CoreWorker.InternalHeartbeat");

  periodical_runner_->RunFnPeriodically(
      [this] { RecordMetrics(); },
      RayConfig::instance().metrics_report_interval_ms() / 2,
      "CoreWorker.RecordMetrics");

  periodical_runner_->RunFnPeriodically(
      [this] { TryDelPendingObjectRefStreams(); },
      RayConfig::instance().local_gc_min_interval_s() * 1000,
      "CoreWorker.TryDelPendingObjectRefStreams");

#ifndef _WIN32
  // Doing this last during CoreWorker initialization, so initialization logic like
  // registering with Raylet can finish with higher priority.
  static const bool niced = [this]() {
    if (options_.worker_type != WorkerType::DRIVER) {
      const auto niceness = nice(RayConfig::instance().worker_niceness());
      RAY_LOG(INFO) << "Adjusted worker niceness to " << niceness;
      return true;
    }
    return false;
  }();
  // Verify driver and worker are never mixed in the same process.
  RAY_CHECK_EQ(options_.worker_type != WorkerType::DRIVER, niced);
#endif
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the very end
  // in case there is a problem during construction.
  ConnectToRayletInternal();

  // Initialize shutdown coordinator last - after all services are ready
  // Create concrete shutdown executor that implements real shutdown operations
  auto shutdown_executor = std::make_unique<CoreWorkerShutdownExecutor>(this);
  shutdown_coordinator_ = std::make_unique<ShutdownCoordinator>(
      std::move(shutdown_executor), options_.worker_type);

  RAY_LOG(DEBUG) << "Initialized unified shutdown coordinator with concrete executor for "
                    "worker type: "
                 << WorkerTypeString(options_.worker_type);
}  // NOLINT(readability/fn_size)

CoreWorker::~CoreWorker() { RAY_LOG(INFO) << "Core worker is destructed"; }

void CoreWorker::Shutdown() {
  shutdown_coordinator_->RequestShutdown(
      /*force_shutdown=*/false, ShutdownReason::kGracefulExit, "ray.shutdown() called");
}

void CoreWorker::ConnectToRayletInternal() {
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the
  // very end in case there is a problem during construction.
  if (options_.worker_type == WorkerType::DRIVER) {
    Status status = raylet_ipc_client_->AnnounceWorkerPortForDriver(
        core_worker_server_->GetPort(), options_.entrypoint);
    RAY_CHECK_OK(status) << "Failed to announce driver's port to raylet and GCS";
  } else {
    Status status =
        raylet_ipc_client_->AnnounceWorkerPortForWorker(core_worker_server_->GetPort());
    RAY_CHECK_OK(status) << "Failed to announce worker's port to raylet and GCS";
  }
}

void CoreWorker::Disconnect(
    const rpc::WorkerExitType &exit_type,
    const std::string &exit_detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  // Force stats export before exiting the worker.
  RecordMetrics();

  // Driver exiting.
  if (options_.worker_type == WorkerType::DRIVER && task_event_buffer_->Enabled() &&
      !RayConfig::instance().task_events_skip_driver_for_test()) {
    auto task_event = std::make_unique<worker::TaskStatusEvent>(
        worker_context_->GetCurrentTaskID(),
        worker_context_->GetCurrentJobID(),
        /*attempt_number=*/0,
        rpc::TaskStatus::FINISHED,
        /*timestamp=*/absl::GetCurrentTimeNanos(),
        /*is_actor_task_event=*/worker_context_->GetCurrentActorID().IsNil(),
        options_.session_name);
    task_event_buffer_->AddTaskEvent(std::move(task_event));
  }

  opencensus::stats::StatsExporter::ExportNow();
  if (connected_) {
    RAY_LOG(INFO) << "Sending disconnect message to the local raylet.";
    connected_ = false;
    Status status = raylet_ipc_client_->Disconnect(
        exit_type, exit_detail, creation_task_exception_pb_bytes);
    if (status.ok()) {
      RAY_LOG(INFO) << "Disconnected from the local raylet.";
    } else {
      RAY_LOG(WARNING) << "Failed to disconnect from the local raylet: " << status;
    }
  }
}

void CoreWorker::KillChildProcs() {
  // There are cases where worker processes can "leak" child processes.
  // Basically this means that the worker process (either itself, or via
  // code in a task or actor) spawned a process and did not kill it on termination.
  // The process will continue living beyond the lifetime of the worker process.
  // If that leaked process has expensive resources, such as a CUDA context and associated
  // GPU memory, then those resources will never be cleaned until something else kills the
  // process.
  //
  // This function lists all processes that are direct children of the current worker
  // process, then kills them. This currently only works for the "happy-path"; worker
  // process crashes will still leak processes.
  // TODO(cade) Use more robust method to catch leaked processes even in worker crash
  // scenarios (subreaper).

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

void CoreWorker::Exit(
    const rpc::WorkerExitType exit_type,
    const std::string &detail,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  // Preserve actor creation failure details by marking a distinct shutdown reason
  // when initialization raised an exception. An exception payload is provided.
  ShutdownReason reason = creation_task_exception_pb_bytes != nullptr
                              ? ShutdownReason::kActorCreationFailed
                              : ConvertExitTypeToShutdownReason(exit_type);

  shutdown_coordinator_->RequestShutdown(/*force_shutdown=*/false,
                                         reason,
                                         detail,
                                         ShutdownCoordinator::kInfiniteTimeout,
                                         creation_task_exception_pb_bytes);
}

void CoreWorker::ForceExit(const rpc::WorkerExitType exit_type,
                           const std::string &detail) {
  RAY_LOG(DEBUG) << "ForceExit called: exit_type=" << static_cast<int>(exit_type)
                 << ", detail=" << detail;

  ShutdownReason reason = ConvertExitTypeToShutdownReason(exit_type, true);
  shutdown_coordinator_->RequestShutdown(
      /*force_shutdown=*/true, reason, detail, std::chrono::milliseconds{0}, nullptr);

  RAY_LOG(DEBUG) << "ForceExit: shutdown request completed";
}

const WorkerID &CoreWorker::GetWorkerID() const { return worker_context_->GetWorkerID(); }

void CoreWorker::SetCurrentTaskId(const TaskID &task_id,
                                  uint64_t attempt_number,
                                  const std::string &task_name) {
  worker_context_->SetCurrentTaskId(task_id, attempt_number);
  {
    absl::MutexLock lock(&mutex_);
    main_thread_task_id_ = task_id;
    main_thread_task_name_ = task_name;
  }
}

void CoreWorker::RegisterToGcs(int64_t worker_launch_time_ms,
                               int64_t worker_launched_time_ms) {
  absl::flat_hash_map<std::string, std::string> worker_info;
  const auto &worker_id = GetWorkerID();
  worker_info.emplace("node_ip_address", options_.node_ip_address);
  worker_info.emplace("plasma_store_socket", options_.store_socket);
  worker_info.emplace("raylet_socket", options_.raylet_socket);

  if (options_.worker_type == WorkerType::DRIVER) {
    auto start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    worker_info.emplace("driver_id", worker_id.Binary());
    worker_info.emplace("start_time", absl::StrFormat("%d", start_time));
    if (!options_.driver_name.empty()) {
      worker_info.emplace("name", options_.driver_name);
    }
  }

  auto worker_data = std::make_shared<rpc::WorkerTableData>();
  worker_data->mutable_worker_address()->set_node_id(rpc_address_.node_id());
  worker_data->mutable_worker_address()->set_ip_address(rpc_address_.ip_address());
  worker_data->mutable_worker_address()->set_port(rpc_address_.port());
  worker_data->mutable_worker_address()->set_worker_id(worker_id.Binary());

  worker_data->set_worker_type(options_.worker_type);
  worker_data->mutable_worker_info()->insert(std::make_move_iterator(worker_info.begin()),
                                             std::make_move_iterator(worker_info.end()));

  worker_data->set_is_alive(true);
  worker_data->set_pid(pid_);
  worker_data->set_start_time_ms(current_sys_time_ms());
  worker_data->set_worker_launch_time_ms(worker_launch_time_ms);
  worker_data->set_worker_launched_time_ms(worker_launched_time_ms);

  gcs_client_->Workers().AsyncAdd(worker_data, nullptr);
}

void CoreWorker::SubscribeToNodeChanges() {
  std::call_once(subscribe_to_node_changes_flag_, [this]() {
    // Register a callback to monitor add/removed nodes.
    // Note we capture a shared ownership of reference_counter, rate_limiter,
    // raylet_client_pool, and core_worker_client_pool here to avoid destruction order
    // fiasco between gcs_client, reference_counter_, raylet_client_pool_, and
    // core_worker_client_pool_.
    auto on_node_change = [reference_counter = reference_counter_,
                           rate_limiter = lease_request_rate_limiter_,
                           raylet_client_pool = raylet_client_pool_,
                           core_worker_client_pool = core_worker_client_pool_](
                              const NodeID &node_id,
                              const rpc::GcsNodeAddressAndLiveness &data) {
      if (data.state() == rpc::GcsNodeInfo::DEAD) {
        RAY_LOG(INFO).WithField(node_id)
            << "Node failure. All objects pinned on that node will be lost if object "
               "reconstruction is not enabled.";
        reference_counter->ResetObjectsOnRemovedNode(node_id);
        raylet_client_pool->Disconnect(node_id);
        core_worker_client_pool->Disconnect(node_id);
      }
      auto cluster_size_based_rate_limiter =
          dynamic_cast<ClusterSizeBasedLeaseRequestRateLimiter *>(rate_limiter.get());
      if (cluster_size_based_rate_limiter != nullptr) {
        cluster_size_based_rate_limiter->OnNodeChanges(data);
      }
    };

    gcs_client_->Nodes().AsyncSubscribeToNodeAddressAndLivenessChange(
        std::move(on_node_change), [this](const Status &) {
          {
            std::scoped_lock<std::mutex> lock(gcs_client_node_cache_populated_mutex_);
            gcs_client_node_cache_populated_ = true;
          }
          gcs_client_node_cache_populated_cv_.notify_all();
        });
  });
}

void CoreWorker::ExitIfParentRayletDies() {
  RAY_CHECK(!RayConfig::instance().RAYLET_PID().empty());
  static auto raylet_pid =
      static_cast<pid_t>(std::stoi(RayConfig::instance().RAYLET_PID()));
  bool should_shutdown = !IsProcessAlive(raylet_pid);
  if (should_shutdown) {
    RAY_LOG(WARNING) << "Shutting down the core worker because the local raylet failed. "
                     << "Check out the raylet.out log file. Raylet pid: " << raylet_pid;

    // Kill child procs so that child processes of the workers do not outlive the workers.
    KillChildProcs();

    QuickExit();
  }
}

void CoreWorker::InternalHeartbeat() {
  // Retry tasks.
  std::vector<TaskToRetry> tasks_to_resubmit;
  {
    absl::MutexLock lock(&mutex_);
    const auto current_time = current_time_ms();
    while (!to_resubmit_.empty() && current_time > to_resubmit_.top().execution_time_ms) {
      tasks_to_resubmit.emplace_back(to_resubmit_.top());
      to_resubmit_.pop();
    }
  }

  for (auto &task_to_retry : tasks_to_resubmit) {
    auto &spec = task_to_retry.task_spec;
    if (spec.IsActorTask()) {
      auto actor_handle = actor_manager_->GetActorHandle(spec.ActorId());
      actor_handle->SetResubmittedActorTaskSpec(spec);
      actor_task_submitter_->SubmitTask(spec);
    } else if (spec.IsActorCreationTask()) {
      actor_task_submitter_->SubmitActorCreationTask(spec);
    } else {
      normal_task_submitter_->SubmitTask(spec);
    }
  }

  // Check timeout tasks that are waiting for death info.
  if (actor_task_submitter_ != nullptr) {
    actor_task_submitter_->CheckTimeoutTasks();
  }

  // Periodically report the latest backlog so that
  // local raylet will have the eventually consistent view of worker backlogs
  // even in cases where backlog reports from normal_task_submitter
  // are lost or reordered.
  normal_task_submitter_->ReportWorkerBacklog();

  // Check for unhandled exceptions to raise after a timeout on the driver.
  // Only do this for TTY, since shells like IPython sometimes save references
  // to the result and prevent normal result deletion from handling.
  // See also: https://github.com/ray-project/ray/issues/14485
  if (options_.worker_type == WorkerType::DRIVER && options_.interactive) {
    memory_store_->NotifyUnhandledErrors();
  }
}

void CoreWorker::RecordMetrics() {
  // Record metrics for owned tasks.
  task_manager_->RecordMetrics();
  // Record metrics for executed tasks.
  task_counter_.RecordMetrics();
  // Record worker heap memory metrics.
  memory_store_->RecordMetrics();
  reference_counter_->RecordMetrics();
}

std::unordered_map<ObjectID, std::pair<size_t, size_t>>
CoreWorker::GetAllReferenceCounts() const {
  auto counts = reference_counter_->GetAllReferenceCounts();
  std::vector<ObjectID> actor_handle_ids = actor_manager_->GetActorHandleIDsFromHandles();
  // Strip actor IDs from the ref counts since there is no associated ObjectID
  // in the language frontend.
  for (const auto &actor_handle_id : actor_handle_ids) {
    counts.erase(actor_handle_id);
  }
  return counts;
}

std::vector<TaskID> CoreWorker::GetPendingChildrenTasks(const TaskID &task_id) const {
  return task_manager_->GetPendingChildrenTasks(task_id);
}

const rpc::Address &CoreWorker::GetRpcAddress() const { return rpc_address_; }

bool CoreWorker::HasOwner(const ObjectID &object_id) const {
  return reference_counter_->HasOwner(object_id);
}

rpc::Address CoreWorker::GetOwnerAddressOrDie(const ObjectID &object_id) const {
  rpc::Address owner_address;
  auto status = GetOwnerAddress(object_id, &owner_address);
  RAY_CHECK_OK(status);
  return owner_address;
}

Status CoreWorker::GetOwnerAddress(const ObjectID &object_id,
                                   rpc::Address *owner_address) const {
  auto has_owner = reference_counter_->GetOwner(object_id, owner_address);
  if (!has_owner) {
    std::ostringstream stream;
    stream << "An application is trying to access a Ray object whose owner is unknown"
           << "(" << object_id
           << "). "
              "Please make sure that all Ray objects you are trying to access are part"
              " of the current Ray session. Note that "
              "object IDs generated randomly (ObjectID.from_random()) or out-of-band "
              "(ObjectID.from_binary(...)) cannot be passed as a task argument because"
              " Ray does not know which task created them. "
              "If this was not how your object ID was generated, please file an issue "
              "at https://github.com/ray-project/ray/issues/";
    return Status::ObjectUnknownOwner(stream.str());
  }
  return Status::OK();
}

std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(
    const std::vector<ObjectID> &object_ids) const {
  std::vector<rpc::ObjectReference> refs;
  refs.reserve(object_ids.size());
  for (const auto &object_id : object_ids) {
    rpc::ObjectReference ref;
    ref.set_object_id(object_id.Binary());
    rpc::Address owner_address;
    if (reference_counter_->GetOwner(object_id, &owner_address)) {
      // NOTE(swang): Detached actors do not have an owner address set.
      *ref.mutable_owner_address() = std::move(owner_address);
    }
    refs.emplace_back(std::move(ref));
  }
  return refs;
}

Status CoreWorker::GetOwnershipInfo(const ObjectID &object_id,
                                    rpc::Address *owner_address,
                                    std::string *serialized_object_status) {
  auto has_owner = reference_counter_->GetOwner(object_id, owner_address);
  if (!has_owner) {
    std::ostringstream stream;
    stream << "An application is trying to access a Ray object whose owner is unknown"
           << "(" << object_id
           << "). "
              "Please make sure that all Ray objects you are trying to access are part"
              " of the current Ray session. Note that "
              "object IDs generated randomly (ObjectID.from_random()) or out-of-band "
              "(ObjectID.from_binary(...)) cannot be passed as a task argument because"
              " Ray does not know which task created them. "
              "If this was not how your object ID was generated, please file an issue "
              "at https://github.com/ray-project/ray/issues/";
    return Status::ObjectUnknownOwner(stream.str());
  }

  rpc::GetObjectStatusReply object_status;
  // Optimization: if the object exists, serialize and inline its status. This also
  // resolves some race conditions in resource release (#16025).
  auto existing_object = memory_store_->GetIfExists(object_id);
  if (existing_object != nullptr) {
    PopulateObjectStatus(object_id, existing_object, &object_status);
  }
  *serialized_object_status = object_status.SerializeAsString();
  return Status::OK();
}

void CoreWorker::RegisterOwnershipInfoAndResolveFuture(
    const ObjectID &object_id,
    const ObjectID &outer_object_id,
    const rpc::Address &owner_address,
    const std::string &serialized_object_status) {
  // Add the object's owner to the local metadata in case it gets serialized
  // again.
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);

  rpc::GetObjectStatusReply object_status;
  object_status.ParseFromString(serialized_object_status);

  if (object_status.has_object() && !reference_counter_->OwnedByUs(object_id)) {
    // We already have the inlined object status, process it immediately.
    future_resolver_->ProcessResolvedObject(
        object_id, owner_address, Status::OK(), object_status);
  } else {
    // We will ask the owner about the object until the object is
    // created or we can no longer reach the owner.
    future_resolver_->ResolveFutureAsync(object_id, owner_address);
  }
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       ObjectID *object_id) {
  SubscribeToNodeChanges();
  *object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                   worker_context_->GetNextPutIndex());
  reference_counter_->AddOwnedObject(*object_id,
                                     contained_object_ids,
                                     rpc_address_,
                                     CurrentCallSite(),
                                     object.GetSize(),
                                     /*is_reconstructable=*/false,
                                     /*add_local_ref=*/true,
                                     NodeID::FromBinary(rpc_address_.node_id()));
  auto status = Put(object, contained_object_ids, *object_id, /*pin_object=*/true);
  if (!status.ok()) {
    RemoveLocalReference(*object_id);
  }
  return status;
}

Status CoreWorker::PutInLocalPlasmaStore(const RayObject &object,
                                         const ObjectID &object_id,
                                         bool pin_object) {
  bool object_exists = false;
  RAY_RETURN_NOT_OK(plasma_store_provider_->Put(
      object, object_id, /*owner_address=*/rpc_address_, &object_exists));
  if (!object_exists) {
    if (pin_object) {
      // Tell the raylet to pin the object **after** it is created.
      RAY_LOG(DEBUG).WithField(object_id) << "Pinning put object";
      local_raylet_rpc_client_->PinObjectIDs(
          rpc_address_,
          {object_id},
          /*generator_id=*/ObjectID::Nil(),
          [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
            // RPC to the local raylet should never fail.
            if (!status.ok()) {
              RAY_LOG(ERROR) << "Request to local raylet to pin object failed: "
                             << status.ToString();
              return;
            }
            // Only release the object once the raylet has responded to avoid the race
            // condition that the object could be evicted before the raylet pins it.
            if (!plasma_store_provider_->Release(object_id).ok()) {
              RAY_LOG(ERROR).WithField(object_id)
                  << "Failed to release object, might cause a leak in plasma.";
            }
          });
    } else {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    }
  }
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  return Status::OK();
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       const ObjectID &object_id,
                       bool pin_object) {
  RAY_RETURN_NOT_OK(WaitForActorRegistered(contained_object_ids));
  if (options_.is_local_mode) {
    RAY_LOG(DEBUG).WithField(object_id) << "Put object in memory store";
    memory_store_->Put(object, object_id, reference_counter_->HasReference(object_id));
    return Status::OK();
  }
  return PutInLocalPlasmaStore(object, object_id, pin_object);
}

Status CoreWorker::CreateOwnedAndIncrementLocalRef(
    bool is_experimental_mutable_object,
    const std::shared_ptr<Buffer> &metadata,
    const size_t data_size,
    const std::vector<ObjectID> &contained_object_ids,
    ObjectID *object_id,
    std::shared_ptr<Buffer> *data,
    const std::unique_ptr<rpc::Address> &owner_address,
    bool inline_small_object,
    rpc::TensorTransport tensor_transport) {
  auto status = WaitForActorRegistered(contained_object_ids);
  if (!status.ok()) {
    return status;
  }
  *object_id = ObjectID::FromIndex(worker_context_->GetCurrentInternalTaskId(),
                                   worker_context_->GetNextPutIndex());
  rpc::Address real_owner_address =
      owner_address != nullptr ? *owner_address : rpc_address_;
  bool owned_by_us = real_owner_address.worker_id() == rpc_address_.worker_id();
  if (owned_by_us) {
    SubscribeToNodeChanges();
    reference_counter_->AddOwnedObject(*object_id,
                                       contained_object_ids,
                                       rpc_address_,
                                       CurrentCallSite(),
                                       data_size + metadata->Size(),
                                       /*is_reconstructable=*/false,
                                       /*add_local_ref=*/true,
                                       NodeID::FromBinary(rpc_address_.node_id()),
                                       /*tensor_transport=*/tensor_transport);

    // Register the callback to free the GPU object when it is out of scope.
    if (tensor_transport != rpc::TensorTransport::OBJECT_STORE) {
      reference_counter_->AddObjectOutOfScopeOrFreedCallback(*object_id,
                                                             free_actor_object_callback_);
    }
  } else {
    // Because in the remote worker's `HandleAssignObjectOwner`,
    // a `WaitForRefRemoved` RPC request will be sent back to
    // the current worker. So we need to make sure ref count is > 0
    // by invoking `AddLocalReference` first. Note that in worker.py we set
    // skip_adding_local_ref=True to avoid double referencing the object.
    AddLocalReference(*object_id);
    RAY_UNUSED(
        reference_counter_->AddBorrowedObject(*object_id,
                                              ObjectID::Nil(),
                                              real_owner_address,
                                              /*foreign_owner_already_monitoring=*/true));

    // Remote call `AssignObjectOwner()`.
    rpc::AssignObjectOwnerRequest request;
    request.set_object_id(object_id->Binary());
    request.mutable_borrower_address()->CopyFrom(rpc_address_);
    request.set_call_site(CurrentCallSite());

    for (auto &contained_object_id : contained_object_ids) {
      request.add_contained_object_ids(contained_object_id.Binary());
    }
    request.set_object_size(data_size + metadata->Size());
    auto conn = core_worker_client_pool_->GetOrConnect(real_owner_address);
    std::promise<Status> status_promise;
    conn->AssignObjectOwner(request,
                            [&status_promise](const Status &returned_status,
                                              const rpc::AssignObjectOwnerReply &reply) {
                              status_promise.set_value(returned_status);
                            });
    // Block until the remote call `AssignObjectOwner` returns.
    status = status_promise.get_future().get();
    // Must call `AddNestedObjectIds` after finished assign owner.
    // Otherwise, it will cause the reference count of those contained objects
    // to be less than expected. Details: https://github.com/ray-project/ray/issues/30341
    reference_counter_->AddNestedObjectIds(
        *object_id, contained_object_ids, real_owner_address);
  }

  if (options_.is_local_mode && owned_by_us && inline_small_object) {
    *data = std::make_shared<LocalMemoryBuffer>(data_size);
  } else {
    if (status.ok()) {
      status = plasma_store_provider_->Create(metadata,
                                              data_size,
                                              *object_id,
                                              /*owner_address=*/real_owner_address,
                                              data,
                                              /*created_by_worker=*/true,
                                              is_experimental_mutable_object);
    }
    if (!status.ok()) {
      RemoveLocalReference(*object_id);
      return status;
    } else if (*data == nullptr) {
      // Object already exists in plasma. Store the in-memory value so that the
      // client will check the plasma store.
      memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         *object_id,
                         reference_counter_->HasReference(*object_id));
    }
  }
  return Status::OK();
}

Status CoreWorker::CreateExisting(const std::shared_ptr<Buffer> &metadata,
                                  const size_t data_size,
                                  const ObjectID &object_id,
                                  const rpc::Address &owner_address,
                                  std::shared_ptr<Buffer> *data,
                                  bool created_by_worker) {
  if (options_.is_local_mode) {
    return Status::NotImplemented(
        "Creating an object with a pre-existing ObjectID is not supported in local "
        "mode");
  } else {
    return plasma_store_provider_->Create(
        metadata, data_size, object_id, owner_address, data, created_by_worker);
  }
}

Status CoreWorker::ExperimentalChannelWriteAcquire(
    const ObjectID &object_id,
    const std::shared_ptr<Buffer> &metadata,
    uint64_t data_size,
    int64_t num_readers,
    int64_t timeout_ms,
    std::shared_ptr<Buffer> *data) {
  Status status = experimental_mutable_object_provider_->GetChannelStatus(
      object_id, /*is_reader=*/false);
  if (!status.ok()) {
    return status;
  }
  return experimental_mutable_object_provider_->WriteAcquire(object_id,
                                                             data_size,
                                                             metadata->Data(),
                                                             metadata->Size(),
                                                             num_readers,
                                                             *data,
                                                             timeout_ms);
}

Status CoreWorker::ExperimentalChannelWriteRelease(const ObjectID &object_id) {
  return experimental_mutable_object_provider_->WriteRelease(object_id);
}

Status CoreWorker::ExperimentalChannelSetError(const ObjectID &object_id) {
  return experimental_mutable_object_provider_->SetError(object_id);
}

Status CoreWorker::SealOwned(const ObjectID &object_id,
                             bool pin_object,
                             const std::unique_ptr<rpc::Address> &owner_address) {
  auto status =
      SealExisting(object_id, pin_object, ObjectID::Nil(), std::move(owner_address));
  if (status.ok()) {
    return status;
  }
  RemoveLocalReference(object_id);
  if (reference_counter_->HasReference(object_id)) {
    RAY_LOG(WARNING).WithField(object_id)
        << "Object failed to be put but has a nonzero ref count. This object may leak.";
  }
  return status;
}

Status CoreWorker::SealExisting(const ObjectID &object_id,
                                bool pin_object,
                                const ObjectID &generator_id,
                                const std::unique_ptr<rpc::Address> &owner_address) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    // Tell the raylet to pin the object **after** it is created.
    RAY_LOG(DEBUG).WithField(object_id) << "Pinning sealed object";
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address != nullptr ? *owner_address : rpc_address_,
        {object_id},
        generator_id,
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          // RPC to the local raylet should never fail.
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Request to local raylet to pin object failed: "
                           << status.ToString();
            return;
          }
          // Only release the object once the raylet has responded to avoid the race
          // condition that the object could be evicted before the raylet pins it.
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR).WithField(object_id)
                << "Failed to release object, might cause a leak in plasma.";
          }
        });
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  return Status::OK();
}

void CoreWorker::ExperimentalRegisterMutableObjectWriter(
    const ObjectID &writer_object_id, const std::vector<NodeID> &remote_reader_node_ids) {
  SubscribeToNodeChanges();
  {
    std::unique_lock<std::mutex> lock(gcs_client_node_cache_populated_mutex_);
    if (!gcs_client_node_cache_populated_) {
      gcs_client_node_cache_populated_cv_.wait(
          lock, [this]() { return gcs_client_node_cache_populated_; });
    }
  }
  experimental_mutable_object_provider_->RegisterWriterChannel(writer_object_id,
                                                               remote_reader_node_ids);
}

Status CoreWorker::ExperimentalRegisterMutableObjectReaderRemote(
    const ObjectID &writer_object_id,
    const std::vector<ray::experimental::ReaderRefInfo> &remote_reader_ref_info) {
  if (remote_reader_ref_info.empty()) {
    return Status::OK();
  }

  std::shared_ptr<size_t> num_replied = std::make_shared<size_t>(0);
  size_t num_requests = remote_reader_ref_info.size();
  std::promise<void> promise;
  for (const auto &reader_ref_info : remote_reader_ref_info) {
    const auto &owner_reader_actor_id = reader_ref_info.owner_reader_actor_id;
    const auto &reader_object_id = reader_ref_info.reader_ref_id;
    const auto &num_reader = reader_ref_info.num_reader_actors;
    const auto &addr = actor_task_submitter_->GetActorAddress(owner_reader_actor_id);
    // It can happen if an actor is not created yet. We assume the API is called only when
    // an actor is alive, which is true now.
    RAY_CHECK(addr.has_value());

    std::shared_ptr<rpc::CoreWorkerClientInterface> conn =
        core_worker_client_pool_->GetOrConnect(*addr);

    rpc::RegisterMutableObjectReaderRequest req;
    req.set_writer_object_id(writer_object_id.Binary());
    req.set_num_readers(num_reader);
    req.set_reader_object_id(reader_object_id.Binary());
    rpc::RegisterMutableObjectReaderReply reply;

    // TODO(sang): Add timeout.
    conn->RegisterMutableObjectReader(
        req,
        [&promise, num_replied, num_requests, addr](
            const Status &status, const rpc::RegisterMutableObjectReaderReply &) {
          RAY_CHECK_OK(status);
          *num_replied += 1;
          if (*num_replied == num_requests) {
            promise.set_value();
          }
        });
  }
  promise.get_future().wait();

  return Status::OK();
}

Status CoreWorker::ExperimentalRegisterMutableObjectReader(const ObjectID &object_id) {
  experimental_mutable_object_provider_->RegisterReaderChannel(object_id);
  return Status::OK();
}

Status CoreWorker::Get(const std::vector<ObjectID> &ids,
                       const int64_t timeout_ms,
                       std::vector<std::shared_ptr<RayObject>> &results) {
  std::unique_ptr<ScopedTaskMetricSetter> state = nullptr;
  if (options_.worker_type == WorkerType::WORKER) {
    // We track the state change only from workers.
    state = std::make_unique<ScopedTaskMetricSetter>(
        *worker_context_, task_counter_, rpc::TaskStatus::RUNNING_IN_RAY_GET);
  }
  results.resize(ids.size(), nullptr);

#if defined(__APPLE__) || defined(__linux__)
  // Check whether these are experimental.Channel objects.
  bool is_experimental_channel = false;
  for (const ObjectID &id : ids) {
    Status status =
        experimental_mutable_object_provider_->GetChannelStatus(id, /*is_reader=*/true);
    if (status.ok()) {
      is_experimental_channel = true;
      // We continue rather than break because we want to check that *all* of the
      // objects are either experimental or not experimental. We cannot have a mix of
      // the two.
      continue;
    } else if (status.IsChannelError()) {
      // The channel has been closed.
      return status;
    } else if (is_experimental_channel) {
      return Status::NotImplemented(
          "ray.get can only be called on all normal objects, or all "
          "experimental.Channel objects");
    }
  }

  // ray.get path for experimental.Channel objects.
  if (is_experimental_channel) {
    return GetExperimentalMutableObjects(ids, timeout_ms, results);
  }
#endif

  return GetObjects(ids, timeout_ms, results);
}

Status CoreWorker::GetExperimentalMutableObjects(
    const std::vector<ObjectID> &ids,
    int64_t timeout_ms,
    std::vector<std::shared_ptr<RayObject>> &results) {
  for (size_t i = 0; i < ids.size(); i++) {
    RAY_RETURN_NOT_OK(experimental_mutable_object_provider_->ReadAcquire(
        ids[i], results[i], timeout_ms));
  }
  return Status::OK();
}

Status CoreWorker::GetObjects(const std::vector<ObjectID> &ids,
                              const int64_t timeout_ms,
                              std::vector<std::shared_ptr<RayObject>> &results) {
  // Normal ray.get path for immutable in-memory and shared memory objects.
  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  auto start_time = current_time_ms();

  StatusSet<StatusT::NotFound> objects_have_owners = reference_counter_->HasOwner(ids);

  if (objects_have_owners.has_error()) {
    return std::visit(
        overloaded{[](const StatusT::NotFound &not_found) {
          return Status::ObjectUnknownOwner(absl::StrFormat(
              "You are trying to access Ray objects whose owner is "
              "unknown. Please make sure that all Ray objects you are trying to access "
              "are part of the current Ray session. Note that object IDs generated "
              "randomly (ObjectID.from_random()) or out-of-band "
              "(ObjectID.from_binary(...)) cannot be passed as a task argument because "
              "Ray does not know which task created them. If this was not how your "
              "object ID was generated, please file an issue at "
              "https://github.com/ray-project/ray/issues/. %s",
              not_found.message()));
        }},
        objects_have_owners.error());
  }

  bool got_exception = false;

  if (!memory_object_ids.empty()) {
    RAY_RETURN_NOT_OK(memory_store_->Get(
        memory_object_ids, timeout_ms, *worker_context_, &result_map, &got_exception));
  }

  // Erase any objects that were promoted to plasma from the results. These get
  // requests will be retried at the plasma store.
  for (auto it = result_map.begin(); it != result_map.end();) {
    auto current = it++;
    if (current->second->IsInPlasmaError()) {
      RAY_LOG(DEBUG) << current->first << " in plasma, doing fetch-and-get";
      plasma_object_ids.insert(current->first);
      result_map.erase(current);
    }
  }

  if (!got_exception && !plasma_object_ids.empty()) {
    // If any of the objects have been promoted to plasma, then we retry their
    // gets at the provider plasma. Once we get the objects from plasma, we flip
    // the transport type again and return them for the original direct call ids.

    // Prepare object ids vector and owner addresses vector
    std::vector<ObjectID> object_ids =
        std::vector<ObjectID>(plasma_object_ids.begin(), plasma_object_ids.end());
    auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

    int64_t local_timeout_ms = timeout_ms;
    if (timeout_ms >= 0) {
      local_timeout_ms = std::max(static_cast<int64_t>(0),
                                  timeout_ms - (current_time_ms() - start_time));
    }
    RAY_LOG(DEBUG) << "Plasma GET timeout " << local_timeout_ms;
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(
        object_ids, owner_addresses, local_timeout_ms, &result_map));
  }

  // Loop through `ids` and fill each entry for the `results` vector,
  // this ensures that entries `results` have exactly the same order as
  // they are in `ids`. When there are duplicate object ids, all the entries
  // for the same id are filled in.
  bool missing_result = false;
  bool will_throw_exception = false;
  for (size_t i = 0; i < ids.size(); i++) {
    const auto pair = result_map.find(ids[i]);
    if (pair != result_map.end()) {
      results[i] = pair->second;
      RAY_CHECK(!pair->second->IsInPlasmaError());
      if (pair->second->IsException()) {
        // The language bindings should throw an exception if they see this
        // object.
        will_throw_exception = true;
      }
    } else {
      missing_result = true;
    }
  }

  // If no timeout was set and none of the results will throw an exception,
  // then check that we fetched all results before returning.
  if (timeout_ms < 0 && !will_throw_exception) {
    RAY_CHECK(!missing_result);
  }

  return Status::OK();
}

Status CoreWorker::GetIfLocal(const std::vector<ObjectID> &ids,
                              std::vector<std::shared_ptr<RayObject>> *results) {
  results->resize(ids.size(), nullptr);

  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  RAY_RETURN_NOT_OK(plasma_store_provider_->GetIfLocal(ids, &result_map));
  for (size_t i = 0; i < ids.size(); i++) {
    auto pair = result_map.find(ids[i]);
    // The caller of this method should guarantee that the object exists in the plasma
    // store when this method is called.
    RAY_CHECK(pair != result_map.end());
    RAY_CHECK(pair->second != nullptr);
    (*results)[i] = pair->second;
  }
  return Status::OK();
}

Status CoreWorker::Contains(const ObjectID &object_id,
                            bool *has_object,
                            bool *is_in_plasma) {
  bool found = false;
  bool in_plasma = false;
  found = memory_store_->Contains(object_id, &in_plasma);
  if (in_plasma) {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Contains(object_id, &found));
  }
  *has_object = found;
  if (is_in_plasma != nullptr) {
    *is_in_plasma = found && in_plasma;
  }
  return Status::OK();
}

Status CoreWorker::Wait(const std::vector<ObjectID> &ids,
                        int num_objects,
                        int64_t timeout_ms,
                        std::vector<bool> *results,
                        bool fetch_local) {
  std::unique_ptr<ScopedTaskMetricSetter> state = nullptr;
  if (options_.worker_type == WorkerType::WORKER) {
    // We track the state change only from workers.
    state = std::make_unique<ScopedTaskMetricSetter>(
        *worker_context_, task_counter_, rpc::TaskStatus::RUNNING_IN_RAY_WAIT);
  }

  results->resize(ids.size(), false);

  if (num_objects <= 0 || num_objects > static_cast<int>(ids.size())) {
    return Status::Invalid(
        "Number of objects to wait for must be between 1 and the number of ids.");
  }

  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  if (memory_object_ids.size() != ids.size()) {
    return Status::Invalid("Duplicate object IDs not supported in wait.");
  }

  size_t objs_without_owners = 0;
  size_t objs_with_owners = 0;
  std::ostringstream ids_stream;

  for (size_t i = 0; i < ids.size(); i++) {
    if (!HasOwner(ids[i])) {
      ids_stream << ids[i] << " ";
      ++objs_without_owners;
    } else {
      ++objs_with_owners;
    }
    // enough owned objects to process this batch
    if (objs_with_owners == static_cast<size_t>(num_objects)) {
      break;
    }
    // not enough objects with owners to process the batch
    if (static_cast<size_t>(num_objects) > ids.size() - objs_without_owners) {
      std::ostringstream stream;
      stream << "An application is trying to access a Ray object whose owner is unknown"
             << "(" << ids_stream.str()
             << "). "
                "Please make sure that all Ray objects you are trying to access are part"
                " of the current Ray session. Note that "
                "object IDs generated randomly (ObjectID.from_random()) or out-of-band "
                "(ObjectID.from_binary(...)) cannot be passed as a task argument because"
                " Ray does not know which task created them. "
                "If this was not how your object ID was generated, please file an issue "
                "at https://github.com/ray-project/ray/issues/";
      return Status::ObjectUnknownOwner(stream.str());
    }
  }

  int64_t start_time = current_time_ms();
  absl::flat_hash_set<ObjectID> ready, plasma_object_ids;
  ready.reserve(num_objects);
  RAY_RETURN_NOT_OK(memory_store_->Wait(
      memory_object_ids,
      std::min(static_cast<int>(memory_object_ids.size()), num_objects),
      timeout_ms,
      *worker_context_,
      &ready,
      &plasma_object_ids));
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);
  if (timeout_ms > 0) {
    timeout_ms =
        std::max(0, static_cast<int>(timeout_ms - (current_time_ms() - start_time)));
  }
  if (fetch_local) {
    // With fetch_local we want to start fetching plasma_object_ids from other nodes'
    // plasma stores. We make the request to the plasma store even if we have
    // num_objects ready since we want to at least make the request to start pulling
    // these objects.
    if (!plasma_object_ids.empty()) {
      // Prepare object ids map
      std::vector<ObjectID> object_ids =
          std::vector<ObjectID>(plasma_object_ids.begin(), plasma_object_ids.end());
      auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

      RAY_RETURN_NOT_OK(plasma_store_provider_->Wait(
          object_ids,
          owner_addresses,
          std::min(static_cast<int>(plasma_object_ids.size()),
                   num_objects - static_cast<int>(ready.size())),
          timeout_ms,
          *worker_context_,
          &ready));
    }
  } else {
    // When we don't need to fetch_local, we don't need to wait for the objects to be
    // pulled to the local object store, so we can directly add them to the ready set.
    for (const auto &object_id : plasma_object_ids) {
      if (ready.size() == static_cast<size_t>(num_objects)) {
        break;
      }
      ready.insert(object_id);
    }
  }
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);

  for (size_t i = 0; i < ids.size(); i++) {
    if (ready.find(ids[i]) != ready.end()) {
      results->at(i) = true;
    }
  }

  return Status::OK();
}

Status CoreWorker::Delete(const std::vector<ObjectID> &object_ids, bool local_only) {
  absl::flat_hash_map<WorkerID, std::vector<ObjectID>> by_owner;
  absl::flat_hash_map<WorkerID, rpc::Address> addresses;
  // Group by owner id.
  for (const auto &obj_id : object_ids) {
    auto owner_address = GetOwnerAddressOrDie(obj_id);
    auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
    by_owner[worker_id].push_back(obj_id);
    addresses[worker_id] = owner_address;
  }
  // Send a batch delete call per owner id.
  for (const auto &entry : by_owner) {
    if (entry.first != worker_context_->GetWorkerID()) {
      RAY_LOG(INFO).WithField(entry.first)
          << "Deleting remote objects " << entry.second.size();
      auto conn = core_worker_client_pool_->GetOrConnect(addresses[entry.first]);
      rpc::DeleteObjectsRequest request;
      for (const auto &obj_id : entry.second) {
        request.add_object_ids(obj_id.Binary());
      }
      request.set_local_only(local_only);
      conn->DeleteObjects(
          request,
          [object_ids](const Status &status, const rpc::DeleteObjectsReply &reply) {
            if (status.ok()) {
              RAY_LOG(INFO) << "Completed object delete request " << status;
            } else {
              RAY_LOG(ERROR) << "Failed to delete objects, status: " << status
                             << ", object IDs: " << debug_string(object_ids);
            }
          });
    }
  }
  // Also try to delete all objects locally.
  Status status = DeleteImpl(object_ids, local_only);
  if (status.IsIOError()) {
    return Status::UnexpectedSystemExit(status.ToString());
  } else {
    return status;
  }
}

Status CoreWorker::GetLocationFromOwner(
    const std::vector<ObjectID> &object_ids,
    int64_t timeout_ms,
    std::vector<std::shared_ptr<ObjectLocation>> *results) {
  results->resize(object_ids.size());
  if (object_ids.empty()) {
    return Status::OK();
  }

  absl::flat_hash_map<rpc::Address, std::vector<ObjectID>> objects_by_owner;
  for (const auto &object_id : object_ids) {
    rpc::Address owner_address;
    RAY_RETURN_NOT_OK(GetOwnerAddress(object_id, &owner_address));
    objects_by_owner[owner_address].push_back(object_id);
  }

  auto mutex = std::make_shared<absl::Mutex>();
  auto num_remaining = std::make_shared<size_t>(0);  // Will be incremented per batch
  auto ready_promise = std::make_shared<std::promise<void>>();
  auto location_by_id =
      std::make_shared<absl::flat_hash_map<ObjectID, std::shared_ptr<ObjectLocation>>>();

  for (const auto &owner_and_objects : objects_by_owner) {
    const auto &owner_address = owner_and_objects.first;
    const auto &owner_object_ids = owner_and_objects.second;

    // Calculate the number of batches
    // Use the same config from worker_fetch_request_size
    auto batch_size =
        static_cast<size_t>(RayConfig::instance().worker_fetch_request_size());

    for (size_t batch_start = 0; batch_start < owner_object_ids.size();
         batch_start += batch_size) {
      *num_remaining += 1;
      size_t batch_end = std::min(batch_start + batch_size, owner_object_ids.size());
      auto client = core_worker_client_pool_->GetOrConnect(owner_address);
      rpc::GetObjectLocationsOwnerRequest request;
      request.set_intended_worker_id(owner_address.worker_id());

      // Add object IDs for the current batch to the request
      for (size_t i = batch_start; i < batch_end; ++i) {
        request.add_object_ids(owner_object_ids[i].Binary());
      }

      client->GetObjectLocationsOwner(
          request,
          [owner_object_ids,
           batch_start,
           mutex,
           num_remaining,
           ready_promise,
           location_by_id,
           owner_address](const Status &status,
                          const rpc::GetObjectLocationsOwnerReply &reply) {
            absl::MutexLock lock(mutex.get());
            if (status.ok()) {
              for (int i = 0; i < reply.object_location_infos_size(); ++i) {
                // Map the object ID to its location, adjusting index by batch_start
                location_by_id->emplace(
                    owner_object_ids[batch_start + i],
                    std::make_shared<ObjectLocation>(
                        CreateObjectLocation(reply.object_location_infos(i))));
              }
            } else {
              RAY_LOG(WARNING).WithField(WorkerID::FromBinary(owner_address.worker_id()))
                  << "Failed to query location information for objects "
                  << debug_string(owner_object_ids)
                  << " owned by worker with error: " << status;
            }
            (*num_remaining)--;
            if (*num_remaining == 0) {
              ready_promise->set_value();
            }
          });
    }
  }

  // Wait for all batches to be processed or timeout
  if (timeout_ms < 0) {
    ready_promise->get_future().wait();
  } else if (ready_promise->get_future().wait_for(
                 std::chrono::microseconds(timeout_ms)) != std::future_status::ready) {
    std::ostringstream stream;
    stream << "Failed querying object locations within " << timeout_ms
           << " milliseconds.";
    return Status::TimedOut(stream.str());
  }

  // Fill in the results vector
  for (size_t i = 0; i < object_ids.size(); i++) {
    auto pair = location_by_id->find(object_ids[i]);
    if (pair == location_by_id->end()) {
      continue;
    }
    (*results)[i] = pair->second;
  }

  return Status::OK();
}

void CoreWorker::TriggerGlobalGC() {
  local_raylet_rpc_client_->GlobalGC(
      [](const Status &status, const rpc::GlobalGCReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to send global GC request: " << status;
        }
      });
}

Status CoreWorker::GetPlasmaUsage(std::string &output) {
  StatusOr<std::string> response = plasma_store_provider_->GetMemoryUsage();
  if (response.ok()) {
    output = std::move(response.value());
  }
  return response.status();
}

TaskID CoreWorker::GetCallerId() const {
  TaskID caller_id;
  ActorID actor_id = GetActorId();
  if (!actor_id.IsNil()) {
    caller_id = TaskID::ForActorCreationTask(actor_id);
  } else {
    absl::MutexLock lock(&mutex_);
    caller_id = main_thread_task_id_;
  }
  return caller_id;
}

Status CoreWorker::PushError(const JobID &job_id,
                             const std::string &type,
                             const std::string &error_message,
                             double timestamp) {
  if (options_.is_local_mode) {
    RAY_LOG(ERROR).WithField(job_id)
        << "Pushed Error with job of type: " << type << " with message: " << error_message
        << " at time: " << timestamp;
    return Status::OK();
  }
  return raylet_ipc_client_->PushError(job_id, type, error_message, timestamp);
}

json CoreWorker::OverrideRuntimeEnv(const json &child,
                                    const std::shared_ptr<json> &parent) {
  // By default, the child runtime env inherits non-specified options from the
  // parent. There is one exception to this:
  //     - The env_vars dictionaries are merged, so environment variables
  //       not specified by the child are still inherited from the parent.
  json result_runtime_env = *parent;
  for (auto it = child.cbegin(); it != child.cend(); ++it) {
    if (it.key() == "env_vars" && result_runtime_env.contains("env_vars")) {
      json env_vars = it.value();
      json merged_env_vars = result_runtime_env["env_vars"];
      for (json::iterator nit = env_vars.begin(); nit != env_vars.end(); ++nit) {
        merged_env_vars[nit.key()] = nit.value();
      }
      result_runtime_env["env_vars"] = std::move(merged_env_vars);
    } else {
      result_runtime_env[it.key()] = it.value();
    }
  }
  return result_runtime_env;
}

std::shared_ptr<rpc::RuntimeEnvInfo> CoreWorker::OverrideTaskOrActorRuntimeEnvInfo(
    const std::string &serialized_runtime_env_info) const {
  auto factory = [this](const std::string &runtime_env_info_str) {
    return OverrideTaskOrActorRuntimeEnvInfoImpl(runtime_env_info_str);
  };
  return runtime_env_json_serialization_cache_.GetOrCreate(serialized_runtime_env_info,
                                                           std::move(factory));
}

std::shared_ptr<rpc::RuntimeEnvInfo> CoreWorker::OverrideTaskOrActorRuntimeEnvInfoImpl(
    const std::string &serialized_runtime_env_info) const {
  // TODO(Catch-Bull,SongGuyang): task runtime env not support the field eager_install
  // yet, we will overwrite the filed eager_install when it did.
  std::shared_ptr<json> parent = nullptr;
  std::shared_ptr<rpc::RuntimeEnvInfo> parent_runtime_env_info = nullptr;
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info = nullptr;
  runtime_env_info = std::make_shared<rpc::RuntimeEnvInfo>();

  if (!IsRuntimeEnvInfoEmpty(serialized_runtime_env_info)) {
    RAY_CHECK(google::protobuf::util::JsonStringToMessage(serialized_runtime_env_info,
                                                          runtime_env_info.get())
                  .ok());
  }

  if (options_.worker_type == WorkerType::DRIVER) {
    if (IsRuntimeEnvEmpty(runtime_env_info->serialized_runtime_env())) {
      return std::make_shared<rpc::RuntimeEnvInfo>(
          worker_context_->GetCurrentJobConfig().runtime_env_info());
    }

    auto job_serialized_runtime_env = worker_context_->GetCurrentJobConfig()
                                          .runtime_env_info()
                                          .serialized_runtime_env();
    if (!IsRuntimeEnvEmpty(job_serialized_runtime_env)) {
      parent = std::make_shared<json>(json::parse(job_serialized_runtime_env));
    }
    parent_runtime_env_info = std::make_shared<rpc::RuntimeEnvInfo>(
        worker_context_->GetCurrentJobConfig().runtime_env_info());
  } else {
    if (IsRuntimeEnvEmpty(runtime_env_info->serialized_runtime_env())) {
      return worker_context_->GetCurrentRuntimeEnvInfo();
    }
    parent = worker_context_->GetCurrentRuntimeEnv();
    parent_runtime_env_info = worker_context_->GetCurrentRuntimeEnvInfo();
  }
  if (parent == nullptr) {
    return runtime_env_info;
  }
  std::string serialized_runtime_env = runtime_env_info->serialized_runtime_env();
  json child_runtime_env = json::parse(serialized_runtime_env);
  auto override_runtime_env = OverrideRuntimeEnv(child_runtime_env, parent);
  auto serialized_override_runtime_env = override_runtime_env.dump();
  runtime_env_info->set_serialized_runtime_env(serialized_override_runtime_env);
  if (runtime_env_info->uris().working_dir_uri().empty() &&
      !parent_runtime_env_info->uris().working_dir_uri().empty()) {
    runtime_env_info->mutable_uris()->set_working_dir_uri(
        parent_runtime_env_info->uris().working_dir_uri());
  }
  if (runtime_env_info->uris().py_modules_uris().empty() &&
      !parent_runtime_env_info->uris().py_modules_uris().empty()) {
    runtime_env_info->mutable_uris()->clear_py_modules_uris();
    for (const std::string &uri : parent_runtime_env_info->uris().py_modules_uris()) {
      runtime_env_info->mutable_uris()->add_py_modules_uris(uri);
    }
  }

  runtime_env_json_serialization_cache_.Put(serialized_runtime_env_info,
                                            runtime_env_info);
  return runtime_env_info;
}

void CoreWorker::BuildCommonTaskSpec(
    TaskSpecBuilder &builder,
    const JobID &job_id,
    const TaskID &task_id,
    const std::string &name,
    const TaskID &current_task_id,
    uint64_t task_index,
    const TaskID &caller_id,
    const rpc::Address &address,
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    int64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    const std::string &debugger_breakpoint,
    int64_t depth,
    const std::string &serialized_runtime_env_info,
    const std::string &call_site,
    const TaskID &main_thread_current_task_id,
    const std::string &concurrency_group_name,
    bool include_job_config,
    int64_t generator_backpressure_num_objects,
    bool enable_task_events,
    const std::unordered_map<std::string, std::string> &labels,
    const LabelSelector &label_selector,
    const std::vector<FallbackOption> &fallback_strategy,
    const rpc::TensorTransport &tensor_transport) {
  // Build common task spec.
  auto override_runtime_env_info =
      OverrideTaskOrActorRuntimeEnvInfo(serialized_runtime_env_info);

  bool returns_dynamic = num_returns == -1;
  if (returns_dynamic) {
    // This remote function returns 1 ObjectRef, whose value
    // is a generator of ObjectRefs.
    num_returns = 1;
  }
  // TODO(sang): Remove this and integrate it to
  // nun_returns == -1 once migrating to streaming
  // generator.
  bool is_streaming_generator = num_returns == kStreamingGeneratorReturn;
  if (is_streaming_generator) {
    num_returns = 1;
    // We are using the dynamic return if
    // the streaming generator is used.
    returns_dynamic = true;
  }
  RAY_CHECK(num_returns >= 0);
  builder.SetCommonTaskSpec(
      task_id,
      name,
      function.GetLanguage(),
      function.GetFunctionDescriptor(),
      job_id,
      include_job_config
          ? std::optional<rpc::JobConfig>(worker_context_->GetCurrentJobConfig())
          : std::optional<rpc::JobConfig>(),
      current_task_id,
      task_index,
      caller_id,
      address,
      num_returns,
      returns_dynamic,
      is_streaming_generator,
      generator_backpressure_num_objects,
      required_resources,
      required_placement_resources,
      debugger_breakpoint,
      depth,
      main_thread_current_task_id,
      call_site,
      override_runtime_env_info,
      concurrency_group_name,
      enable_task_events,
      labels,
      label_selector,
      fallback_strategy,
      tensor_transport);
  // Set task arguments.
  for (const auto &arg : args) {
    builder.AddArg(*arg);
  }
}

void CoreWorker::PrestartWorkers(const std::string &serialized_runtime_env_info,
                                 uint64_t keep_alive_duration_secs,
                                 size_t num_workers) {
  rpc::PrestartWorkersRequest request;
  request.set_language(GetLanguage());
  request.set_job_id(GetCurrentJobId().Binary());
  *request.mutable_runtime_env_info() =
      *OverrideTaskOrActorRuntimeEnvInfo(serialized_runtime_env_info);
  request.set_keep_alive_duration_secs(keep_alive_duration_secs);
  request.set_num_workers(num_workers);
  local_raylet_rpc_client_->PrestartWorkers(
      request, [](const Status &status, const rpc::PrestartWorkersReply &reply) {
        if (!status.ok()) {
          RAY_LOG(INFO) << "Failed to prestart workers: " << status;
        }
      });
}

std::vector<rpc::ObjectReference> CoreWorker::SubmitTask(
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options,
    int max_retries,
    bool retry_exceptions,
    const rpc::SchedulingStrategy &scheduling_strategy,
    const std::string &debugger_breakpoint,
    const std::string &serialized_retry_exception_allowlist,
    const std::string &call_site,
    const TaskID current_task_id) {
  SubscribeToNodeChanges();
  RAY_CHECK(scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_->GetNextTaskIndex();
  const auto task_id = TaskID::ForNormalTask(worker_context_->GetCurrentJobID(),
                                             worker_context_->GetCurrentInternalTaskId(),
                                             next_task_index);
  auto constrained_resources =
      AddPlacementGroupConstraint(task_options.resources, scheduling_strategy);

  auto task_name = task_options.name.empty()
                       ? function.GetFunctionDescriptor()->DefaultTaskName()
                       : task_options.name;
  int64_t depth = worker_context_->GetTaskDepth() + 1;
  // TODO(ekl) offload task building onto a thread pool for performance

  BuildCommonTaskSpec(builder,
                      worker_context_->GetCurrentJobID(),
                      task_id,
                      task_name,
                      current_task_id != TaskID::Nil()
                          ? current_task_id
                          : worker_context_->GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      task_options.num_returns,
                      constrained_resources,
                      constrained_resources,
                      debugger_breakpoint,
                      depth,
                      task_options.serialized_runtime_env_info,
                      call_site,
                      worker_context_->GetMainThreadOrActorCreationTaskID(),
                      /*concurrency_group_name=*/"",
                      /*include_job_config=*/true,
                      /*generator_backpressure_num_objects=*/
                      task_options.generator_backpressure_num_objects,
                      /*enable_task_events=*/task_options.enable_task_events,
                      task_options.labels,
                      task_options.label_selector,
                      task_options.fallback_strategy);
  ActorID root_detached_actor_id;
  if (!worker_context_->GetRootDetachedActorID().IsNil()) {
    root_detached_actor_id = worker_context_->GetRootDetachedActorID();
  }
  builder.SetNormalTaskSpec(max_retries,
                            retry_exceptions,
                            serialized_retry_exception_allowlist,
                            scheduling_strategy,
                            root_detached_actor_id);
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
  RAY_LOG(DEBUG) << "Submitting normal task " << task_spec.DebugString();
  std::vector<rpc::ObjectReference> returned_refs;
  if (options_.is_local_mode) {
    returned_refs = ExecuteTaskLocalMode(task_spec);
  } else {
    returned_refs = task_manager_->AddPendingTask(
        task_spec.CallerAddress(), task_spec, CurrentCallSite(), max_retries);

    io_service_.post(
        [this, task_spec = std::move(task_spec)]() mutable {
          normal_task_submitter_->SubmitTask(std::move(task_spec));
        },
        "CoreWorker.SubmitTask");
  }
  return returned_refs;
}

Status CoreWorker::CreateActor(const RayFunction &function,
                               const std::vector<std::unique_ptr<TaskArg>> &args,
                               const ActorCreationOptions &actor_creation_options,
                               const std::string &extension_data,
                               const std::string &call_site,
                               ActorID *return_actor_id) {
  SubscribeToNodeChanges();
  RAY_CHECK(actor_creation_options.scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  if (actor_creation_options.is_asyncio && options_.is_local_mode) {
    return Status::NotImplemented(
        "Async actor is currently not supported for the local mode");
  }

  bool is_detached = false;
  if (!actor_creation_options.is_detached.has_value()) {
    /// Since this actor doesn't have a specified lifetime on creation, let's use
    /// the default value of the job.
    is_detached = worker_context_->GetCurrentJobConfig().default_actor_lifetime() ==
                  ray::rpc::JobConfig_ActorLifetime_DETACHED;
  } else {
    is_detached = actor_creation_options.is_detached.value();
  }

  const auto next_task_index = worker_context_->GetNextTaskIndex();
  const ActorID actor_id = ActorID::Of(worker_context_->GetCurrentJobID(),
                                       worker_context_->GetCurrentTaskID(),
                                       next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_->GetCurrentJobID();
  // Propagate existing environment variable overrides, but override them with any new
  // ones
  TaskSpecBuilder builder;
  auto new_placement_resources =
      AddPlacementGroupConstraint(actor_creation_options.placement_resources,
                                  actor_creation_options.scheduling_strategy);
  auto new_resource = AddPlacementGroupConstraint(
      actor_creation_options.resources, actor_creation_options.scheduling_strategy);
  const auto actor_name = actor_creation_options.name;
  const auto task_name =
      actor_name.empty()
          ? function.GetFunctionDescriptor()->DefaultTaskName()
          : actor_name + ":" + function.GetFunctionDescriptor()->CallString();
  int64_t depth = worker_context_->GetTaskDepth() + 1;
  BuildCommonTaskSpec(builder,
                      job_id,
                      actor_creation_task_id,
                      task_name,
                      worker_context_->GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      /*num_returns=*/0,
                      new_resource,
                      new_placement_resources,
                      /*debugger_breakpoint=*/"",
                      depth,
                      actor_creation_options.serialized_runtime_env_info,
                      call_site,
                      worker_context_->GetMainThreadOrActorCreationTaskID(),
                      /*concurrency_group_name=*/"",
                      /*include_job_config=*/true,
                      /*generator_backpressure_num_objects=*/-1,
                      /*enable_task_events=*/actor_creation_options.enable_task_events,
                      actor_creation_options.labels,
                      actor_creation_options.label_selector,
                      actor_creation_options.fallback_strategy);

  // If the namespace is not specified, get it from the job.
  const auto ray_namespace = (actor_creation_options.ray_namespace.empty()
                                  ? worker_context_->GetCurrentJobConfig().ray_namespace()
                                  : actor_creation_options.ray_namespace);
  auto actor_handle = std::make_unique<ActorHandle>(
      actor_id,
      GetCallerId(),
      rpc_address_,
      job_id,
      /*actor_cursor=*/ObjectID::FromIndex(actor_creation_task_id, 1),
      function.GetLanguage(),
      function.GetFunctionDescriptor(),
      extension_data,
      actor_creation_options.max_task_retries,
      actor_name,
      ray_namespace,
      actor_creation_options.max_pending_calls,
      actor_creation_options.allow_out_of_order_execution,
      actor_creation_options.enable_tensor_transport,
      actor_creation_options.enable_task_events,
      actor_creation_options.labels,
      is_detached);
  std::string serialized_actor_handle;
  actor_handle->Serialize(&serialized_actor_handle);
  ActorID root_detached_actor_id;
  if (is_detached) {
    root_detached_actor_id = actor_id;
  } else if (!worker_context_->GetRootDetachedActorID().IsNil()) {
    root_detached_actor_id = worker_context_->GetRootDetachedActorID();
  }
  builder.SetActorCreationTaskSpec(actor_id,
                                   serialized_actor_handle,
                                   actor_creation_options.scheduling_strategy,
                                   actor_creation_options.max_restarts,
                                   actor_creation_options.max_task_retries,
                                   actor_creation_options.dynamic_worker_options,
                                   actor_creation_options.max_concurrency,
                                   is_detached,
                                   actor_name,
                                   ray_namespace,
                                   actor_creation_options.is_asyncio,
                                   actor_creation_options.concurrency_groups,
                                   extension_data,
                                   actor_creation_options.allow_out_of_order_execution,
                                   root_detached_actor_id);
  // Add the actor handle before we submit the actor creation task, since the
  // actor handle must be in scope by the time the GCS sends the
  // WaitForActorRefDeletedRequest.
  RAY_CHECK(actor_manager_->EmplaceNewActorHandle(
      std::move(actor_handle), CurrentCallSite(), rpc_address_, /*owned=*/!is_detached))
      << "Attempt to emplace new actor handle for the actor being created with actor id: "
      << actor_id
      << " failed because an actor handle with the same actor id has already been added";
  *return_actor_id = actor_id;
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
  RAY_LOG(DEBUG) << "Submitting actor creation task " << task_spec.DebugString();
  if (options_.is_local_mode) {
    // TODO(suquark): Should we consider namespace in local mode? Currently
    // it looks like two actors with two different namespaces become the
    // same actor in local mode. Maybe this is not an issue if we consider
    // the actor name globally unique.
    if (!actor_name.empty()) {
      // Since local mode doesn't pass GCS actor management code path,
      // it just register actor names in memory.
      local_mode_named_actor_registry_.emplace(actor_name, actor_id);
    }
    ExecuteTaskLocalMode(task_spec);
    return Status::OK();
  }

  auto ref_is_detached_actor = [this](const std::string &object_id) {
    auto ref_object_id = ObjectID::FromBinary(object_id);
    if (ObjectID::IsActorID(ref_object_id)) {
      auto ref_actor_id = ObjectID::ToActorID(ref_object_id);
      if (auto ref_actor_handle = actor_manager_->GetActorHandleIfExists(ref_actor_id)) {
        if (ref_actor_handle->IsDetached()) {
          return true;
        }
      }
    }
    return false;
  };
  if (task_spec.MaxActorRestarts() != 0) {
    bool actor_restart_warning = false;
    for (size_t i = 0; i < task_spec.NumArgs(); i++) {
      if (task_spec.ArgByRef(i)) {
        actor_restart_warning = true;
        break;
      }
      if (!task_spec.ArgInlinedRefs(i).empty()) {
        for (const auto &ref : task_spec.ArgInlinedRefs(i)) {
          if (!ref_is_detached_actor(ref.object_id())) {
            // There's an inlined ref that's not a detached actor, so we want to
            // show the warning.
            actor_restart_warning = true;
            break;
          }
        }
      }
      if (actor_restart_warning) {
        break;
      }
    }
    if (actor_restart_warning) {
      RAY_LOG_ONCE_PER_PROCESS(ERROR)
          << "Actor " << (actor_name.empty() ? "" : (actor_name + " "))
          << "with class name: '" << function.GetFunctionDescriptor()->ClassName()
          << "' and ID: '" << task_spec.ActorCreationId()
          << "' has constructor arguments in the object store and max_restarts > 0. If "
             "the arguments in the object store go out of scope or are lost, the "
             "actor restart will fail. See "
             "https://github.com/ray-project/ray/issues/53727 for more details.";
    }
  }

  task_manager_->AddPendingTask(
      rpc_address_,
      task_spec,
      CurrentCallSite(),
      // Actor creation task retry happens on GCS not on core worker.
      /*max_retries=*/0);

  if (actor_name.empty()) {
    io_service_.post(
        [this, task_spec = std::move(task_spec)]() {
          actor_creator_->AsyncRegisterActor(task_spec, [this, task_spec](Status status) {
            if (!status.ok()) {
              RAY_LOG(ERROR).WithField(task_spec.ActorCreationId())
                  << "Failed to register actor. Error message: " << status;
              task_manager_->FailPendingTask(
                  task_spec.TaskId(), rpc::ErrorType::ACTOR_CREATION_FAILED, &status);
            } else {
              actor_task_submitter_->SubmitActorCreationTask(task_spec);
            }
          });
        },
        "ActorCreator.AsyncRegisterActor");
  } else {
    // For named actor, we still go through the sync way because for
    // functions like list actors these actors need to be there, especially
    // for local driver. But the current code all go through the gcs right now.
    auto status = actor_creator_->RegisterActor(task_spec);
    if (!status.ok()) {
      return status;
    }
    io_service_.post(
        [this, task_spec = std::move(task_spec)]() {
          actor_task_submitter_->SubmitActorCreationTask(task_spec);
        },
        "CoreWorker.SubmitTask");
  }
  return Status::OK();
}

Status CoreWorker::CreatePlacementGroup(
    const PlacementGroupCreationOptions &placement_group_creation_options,
    PlacementGroupID *return_placement_group_id) {
  const auto &bundles = placement_group_creation_options.bundles_;
  for (const auto &bundle : bundles) {
    for (const auto &resource : bundle) {
      if (resource.first == kBundle_ResourceLabel) {
        std::ostringstream stream;
        stream << kBundle_ResourceLabel << " is a system reserved resource, which is not "
               << "allowed to be used in placement group. ";
        return Status::Invalid(stream.str());
      }
    }
  }
  const PlacementGroupID placement_group_id = PlacementGroupID::Of(GetCurrentJobId());
  PlacementGroupSpecBuilder builder;
  builder.SetPlacementGroupSpec(placement_group_id,
                                placement_group_creation_options.name_,
                                placement_group_creation_options.bundles_,
                                placement_group_creation_options.strategy_,
                                placement_group_creation_options.is_detached_,
                                placement_group_creation_options.soft_target_node_id_,
                                worker_context_->GetCurrentJobID(),
                                worker_context_->GetCurrentActorID(),
                                worker_context_->CurrentActorDetached(),
                                placement_group_creation_options.bundle_label_selector_);
  PlacementGroupSpecification placement_group_spec = builder.Build();
  *return_placement_group_id = placement_group_id;
  RAY_LOG(INFO).WithField(placement_group_id)
      << "Submitting Placement Group creation to GCS";
  auto status =
      gcs_client_->PlacementGroups().SyncCreatePlacementGroup(placement_group_spec);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in creating the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status;
}

Status CoreWorker::RemovePlacementGroup(const PlacementGroupID &placement_group_id) {
  // Synchronously wait for placement group removal.
  auto status =
      gcs_client_->PlacementGroups().SyncRemovePlacementGroup(placement_group_id);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in removing the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status;
}

Status CoreWorker::WaitPlacementGroupReady(const PlacementGroupID &placement_group_id,
                                           int64_t timeout_seconds) {
  auto status = gcs_client_->PlacementGroups().SyncWaitUntilReady(placement_group_id,
                                                                  timeout_seconds);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in waiting for placement group " << placement_group_id
           << " creation.";
    return Status::TimedOut(stream.str());
  }
  return status;
}

Status CoreWorker::SubmitActorTask(
    const ActorID &actor_id,
    const RayFunction &function,
    const std::vector<std::unique_ptr<TaskArg>> &args,
    const TaskOptions &task_options,
    int max_retries,
    bool retry_exceptions,
    const std::string &serialized_retry_exception_allowlist,
    const std::string &call_site,
    std::vector<rpc::ObjectReference> &task_returns,
    const TaskID current_task_id) {
  SubscribeToNodeChanges();
  absl::ReleasableMutexLock lock(&actor_task_mutex_);
  task_returns.clear();
  if (!actor_task_submitter_->CheckActorExists(actor_id)) {
    std::string err_msg = absl::StrFormat(
        "Can't find actor %s. It might be dead or it's from a different cluster",
        actor_id.Hex());
    return Status::NotFound(err_msg);
  }
  /// Check whether backpressure may happen at the very beginning of submitting a task.
  if (actor_task_submitter_->PendingTasksFull(actor_id)) {
    RAY_LOG(DEBUG).WithField(actor_id)
        << "Back pressure occurred while submitting the actor task. "
        << actor_task_submitter_->DebugString(actor_id);
    return Status::OutOfResource(absl::StrFormat(
        "Too many tasks (%d) pending to be executed for actor %s. Please try later",
        actor_task_submitter_->NumPendingTasks(actor_id),
        actor_id.Hex()));
  }

  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  // Subscribe the actor state when we first submit the actor task. It is to reduce the
  // number of connections. The method is idempotent.
  actor_manager_->SubscribeActorState(actor_id);

  // Build common task spec.
  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_->GetNextTaskIndex();
  const TaskID actor_task_id =
      TaskID::ForActorTask(worker_context_->GetCurrentJobID(),
                           worker_context_->GetCurrentInternalTaskId(),
                           next_task_index,
                           actor_handle->GetActorID());
  const std::unordered_map<std::string, double> required_resources;
  const auto task_name = task_options.name.empty()
                             ? function.GetFunctionDescriptor()->DefaultTaskName()
                             : task_options.name;

  // The depth of the actor task is depth of the caller + 1
  // The caller is not necessarily the creator of the actor.
  int64_t depth = worker_context_->GetTaskDepth() + 1;
  BuildCommonTaskSpec(builder,
                      actor_handle->CreationJobID(),
                      actor_task_id,
                      task_name,
                      current_task_id != TaskID::Nil()
                          ? current_task_id
                          : worker_context_->GetCurrentTaskID(),
                      next_task_index,
                      GetCallerId(),
                      rpc_address_,
                      function,
                      args,
                      task_options.num_returns,
                      task_options.resources,
                      required_resources,
                      /*debugger_breakpoint=*/"",
                      depth,
                      /*serialized_runtime_env_info=*/"{}",
                      call_site,
                      worker_context_->GetMainThreadOrActorCreationTaskID(),
                      task_options.concurrency_group_name,
                      /*include_job_config=*/false,
                      /*generator_backpressure_num_objects=*/
                      task_options.generator_backpressure_num_objects,
                      /*enable_task_events=*/task_options.enable_task_events,
                      /*labels=*/{},
                      /*label_selector=*/{},
                      /*fallback_strategy=*/{},
                      /*tensor_transport=*/task_options.tensor_transport);
  // NOTE: placement_group_capture_child_tasks and runtime_env will
  // be ignored in the actor because we should always follow the actor's option.

  actor_handle->SetActorTaskSpec(builder,
                                 ObjectID::Nil(),
                                 max_retries,
                                 retry_exceptions,
                                 serialized_retry_exception_allowlist);
  // Submit task.
  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
  RAY_LOG(DEBUG) << "Submitting actor task " << task_spec.DebugString();
  std::vector<rpc::ObjectReference> returned_refs;
  if (options_.is_local_mode) {
    /// NOTE: The lock should be released in local mode. The user code may
    /// submit another task when executing the current task locally, which
    /// cause deadlock. The code call chain is:
    /// SubmitActorTask -> python user code -> actor.xx.remote() -> SubmitActorTask
    lock.Release();
    returned_refs = ExecuteTaskLocalMode(task_spec, actor_id);
  } else {
    returned_refs = task_manager_->AddPendingTask(
        rpc_address_, task_spec, CurrentCallSite(), max_retries);

    actor_task_submitter_->SubmitTask(task_spec);
  }
  task_returns = std::move(returned_refs);
  return Status::OK();
}

Status CoreWorker::CancelTask(const ObjectID &object_id,
                              bool force_kill,
                              bool recursive) {
  rpc::Address obj_addr;
  if (!reference_counter_->GetOwner(object_id, &obj_addr)) {
    return Status::Invalid("No owner found for object.");
  }

  if (obj_addr.SerializeAsString() != rpc_address_.SerializeAsString()) {
    // We don't have RequestOwnerToCancelTask for actor_task_submitter_
    // because it requires the same implementation.
    RAY_LOG(DEBUG).WithField(object_id)
        << "Request to cancel a task of object to an owner "
        << obj_addr.SerializeAsString();
    normal_task_submitter_->RequestOwnerToCancelTask(
        object_id, obj_addr, force_kill, recursive);
    return Status::OK();
  }

  auto task_spec = task_manager_->GetTaskSpec(object_id.TaskId());
  if (!task_spec.has_value()) {
    // Task is already finished.
    RAY_LOG(DEBUG).WithField(object_id)
        << "Cancel request is ignored because the task is already canceled "
           "for an object";
    return Status::OK();
  }

  if (task_spec.value().IsActorCreationTask()) {
    RAY_LOG(FATAL) << "Cannot cancel actor creation tasks";
  }

  if (task_spec->IsActorTask()) {
    if (force_kill) {
      return Status::InvalidArgument("force=True is not supported for actor tasks.");
    }

    actor_task_submitter_->CancelTask(task_spec.value(), recursive);
  } else {
    normal_task_submitter_->CancelTask(task_spec.value(), force_kill, recursive);
  }
  return Status::OK();
}

bool CoreWorker::IsTaskCanceled(const TaskID &task_id) const {
  // Check if the task is canceled on executor side. Check the canceled_tasks_ which is
  // populated when CancelTask RPC is received.
  absl::MutexLock lock(&mutex_);
  return canceled_tasks_.find(task_id) != canceled_tasks_.end();
}

Status CoreWorker::CancelChildren(const TaskID &task_id, bool force_kill) {
  absl::flat_hash_set<TaskID> unknown_child_task_ids;
  auto child_task_ids = task_manager_->GetPendingChildrenTasks(task_id);
  for (const auto &child_id : child_task_ids) {
    auto child_spec = task_manager_->GetTaskSpec(child_id);
    if (!child_spec.has_value()) {
      unknown_child_task_ids.insert(child_id);
    } else if (child_spec->IsActorTask()) {
      actor_task_submitter_->CancelTask(std::move(*child_spec), true);
    } else {
      normal_task_submitter_->CancelTask(std::move(*child_spec), force_kill, true);
    }
  }

  if (unknown_child_task_ids.empty()) {
    return Status::OK();
  }

  constexpr size_t kMaxFailedTaskSampleSize = 10;
  std::ostringstream ostr;
  ostr << "Failed to cancel all the children tasks of " << task_id << " recursively.\n"
       << "Here are up to " << kMaxFailedTaskSampleSize
       << " samples tasks that failed to be canceled\n";
  const auto failure_status_str =
      Status::UnknownError("Recursive task cancellation failed--check warning logs.")
          .ToString();
  size_t failures = 0;
  for (const auto &child_id : unknown_child_task_ids) {
    ostr << "\t" << child_id << ", " << failure_status_str << "\n";
    failures += 1;
    if (failures >= kMaxFailedTaskSampleSize) {
      break;
    }
  }
  ostr << "Total Recursive cancelation success: "
       << (child_task_ids.size() - unknown_child_task_ids.size())
       << ", failures: " << unknown_child_task_ids.size();
  return Status::UnknownError(ostr.str());
}

Status CoreWorker::KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) {
  if (options_.is_local_mode) {
    return KillActorLocalMode(actor_id);
  }
  std::promise<Status> p;
  auto f = p.get_future();
  io_service_.post(
      [this, p = &p, actor_id, force_kill, no_restart]() {
        auto cb = [this, p, actor_id, force_kill, no_restart](Status status) mutable {
          if (status.ok()) {
            gcs_client_->Actors().AsyncKillActor(
                actor_id, force_kill, no_restart, nullptr);
          }
          p->set_value(std::move(status));
        };
        if (actor_creator_->IsActorInRegistering(actor_id)) {
          actor_creator_->AsyncWaitForActorRegisterFinish(actor_id, std::move(cb));
        } else if (actor_manager_->CheckActorHandleExists(actor_id)) {
          cb(Status::OK());
        } else {
          std::stringstream stream;
          stream << "Failed to find a corresponding actor handle for " << actor_id;
          cb(Status::Invalid(stream.str()));
        }
      },
      "CoreWorker.KillActor");
  const auto &status = f.get();
  actor_manager_->OnActorKilled(actor_id);
  return status;
}

Status CoreWorker::KillActorLocalMode(const ActorID &actor_id) {
  // KillActor doesn't do anything in local mode. We only remove named actor entry if
  // exists.
  for (auto it = local_mode_named_actor_registry_.begin();
       it != local_mode_named_actor_registry_.end();) {
    auto current = it++;
    if (current->second == actor_id) {
      local_mode_named_actor_registry_.erase(current);
    }
  }
  return Status::OK();
}

void CoreWorker::RemoveActorHandleReference(const ActorID &actor_id) {
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  reference_counter_->RemoveLocalReference(actor_handle_id, nullptr);
}

std::optional<rpc::ActorTableData::ActorState> CoreWorker::GetLocalActorState(
    const ActorID &actor_id) const {
  return actor_task_submitter_->GetLocalActorState(actor_id);
}

ActorID CoreWorker::DeserializeAndRegisterActorHandle(const std::string &serialized,
                                                      const ObjectID &outer_object_id,
                                                      bool add_local_ref) {
  auto actor_handle = std::make_unique<ActorHandle>(serialized);
  return actor_manager_->RegisterActorHandle(std::move(actor_handle),
                                             outer_object_id,
                                             CurrentCallSite(),
                                             rpc_address_,
                                             add_local_ref);
}

Status CoreWorker::SerializeActorHandle(const ActorID &actor_id,
                                        std::string *output,
                                        ObjectID *actor_handle_id) const {
  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  actor_handle->Serialize(output);
  *actor_handle_id = ObjectID::ForActorHandle(actor_id);
  return Status::OK();
}

std::shared_ptr<const ActorHandle> CoreWorker::GetActorHandle(
    const ActorID &actor_id) const {
  return actor_manager_->GetActorHandle(actor_id);
}

std::pair<std::shared_ptr<const ActorHandle>, Status> CoreWorker::GetNamedActorHandle(
    const std::string &name, const std::string &ray_namespace) {
  RAY_CHECK(!name.empty());
  if (options_.is_local_mode) {
    return GetNamedActorHandleLocalMode(name);
  }

  return actor_manager_->GetNamedActorHandle(
      name,
      ray_namespace.empty() ? worker_context_->GetCurrentJobConfig().ray_namespace()
                            : ray_namespace,
      CurrentCallSite(),
      rpc_address_);
}

std::pair<std::vector<std::pair<std::string, std::string>>, Status>
CoreWorker::ListNamedActors(bool all_namespaces) {
  if (options_.is_local_mode) {
    return ListNamedActorsLocalMode();
  }

  std::vector<std::pair<std::string, std::string>> actors;

  // This call needs to be blocking because we can't return until we get the
  // response from the RPC.
  const auto ray_namespace = worker_context_->GetCurrentJobConfig().ray_namespace();
  auto status =
      gcs_client_->Actors().SyncListNamedActors(all_namespaces, ray_namespace, actors);
  if (status.IsTimedOut()) {
    std::ostringstream stream;
    stream << "There was timeout in getting the list of named actors, "
              "probably because the GCS server is dead or under high load .";
    return std::make_pair(std::move(actors), Status::TimedOut(stream.str()));
  }
  return std::make_pair(std::move(actors), std::move(status));
}

std::pair<std::shared_ptr<const ActorHandle>, Status>
CoreWorker::GetNamedActorHandleLocalMode(const std::string &name) {
  auto it = local_mode_named_actor_registry_.find(name);
  if (it == local_mode_named_actor_registry_.end()) {
    std::string err_msg = absl::StrFormat("Failed to look up actor with name %s", name);
    return std::make_pair(nullptr, Status::NotFound(err_msg));
  }

  return std::make_pair(GetActorHandle(it->second), Status::OK());
}

std::pair<std::vector<std::pair<std::string, std::string>>, Status>
CoreWorker::ListNamedActorsLocalMode() {
  std::vector<std::pair<std::string, std::string>> actors;
  actors.reserve(local_mode_named_actor_registry_.size());
  for (auto it = local_mode_named_actor_registry_.begin();
       it != local_mode_named_actor_registry_.end();
       it++) {
    actors.emplace_back(/*namespace=*/"", it->first);
  }
  return std::make_pair(std::move(actors), Status::OK());
}

std::string CoreWorker::GetActorName() const {
  absl::MutexLock lock(&mutex_);
  return actor_manager_->GetActorHandle(actor_id_)->GetName();
}

ResourceMappingType CoreWorker::GetResourceIDs() const {
  absl::MutexLock lock(&mutex_);
  return resource_ids_;
}

std::unique_ptr<worker::ProfileEvent> CoreWorker::CreateProfileEvent(
    const std::string &event_name) {
  return std::make_unique<worker::ProfileEvent>(
      *task_event_buffer_, *worker_context_, options_.node_ip_address, event_name);
}

void CoreWorker::RunTaskExecutionLoop() {
  auto signal_checker = PeriodicalRunner::Create(task_execution_service_);
  if (options_.check_signals) {
    signal_checker->RunFnPeriodically(
        [this] {
          /// The overhead of this is only a single digit microsecond.
          if (worker_context_->GetCurrentActorShouldExit()) {
            Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                 "User requested to exit the actor.",
                 nullptr);
          }
          auto status = options_.check_signals();
          if (status.IsIntentionalSystemExit()) {
            Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                 absl::StrCat("Worker exits by a signal. ", status.message()),
                 nullptr);
          }
          if (status.IsUnexpectedSystemExit()) {
            Exit(
                rpc::WorkerExitType::SYSTEM_ERROR,
                absl::StrCat("Worker exits unexpectedly by a signal. ", status.message()),
                nullptr);
          }
        },
        10,
        "CoreWorker.CheckSignal");
  }
  task_execution_service_.run();
  RAY_CHECK(shutdown_coordinator_ && shutdown_coordinator_->IsShuttingDown())
      << "Task execution loop was terminated without calling shutdown API.";
}

Status CoreWorker::AllocateReturnObject(const ObjectID &object_id,
                                        const size_t &data_size,
                                        const std::shared_ptr<Buffer> &metadata,
                                        const std::vector<ObjectID> &contained_object_ids,
                                        const rpc::Address &caller_address,
                                        int64_t *task_output_inlined_bytes,
                                        std::shared_ptr<RayObject> *return_object) {
  rpc::Address owner_address(options_.is_local_mode ? rpc::Address() : caller_address);

  bool object_already_exists = false;
  std::shared_ptr<Buffer> data_buffer;
  if (data_size > 0) {
    RAY_LOG(DEBUG).WithField(object_id) << "Creating return object";
    // Mark this object as containing other object IDs. The ref counter will
    // keep the inner IDs in scope until the outer one is out of scope.
    if (!contained_object_ids.empty() && !options_.is_local_mode) {
      // Due to response loss caused by network failures,
      // this method may be called multiple times for the same return object
      // but it's fine since AddNestedObjectIds is idempotent.
      // See https://github.com/ray-project/ray/issues/57997
      reference_counter_->AddNestedObjectIds(
          object_id, contained_object_ids, owner_address);
    }

    // Allocate a buffer for the return object.
    if (options_.is_local_mode ||
        (static_cast<int64_t>(data_size) < max_direct_call_object_size_ &&
         // ensure we don't exceed the limit if we allocate this object inline.
         (*task_output_inlined_bytes + static_cast<int64_t>(data_size) <=
          RayConfig::instance().task_rpc_inlined_bytes_limit()))) {
      data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
      *task_output_inlined_bytes += static_cast<int64_t>(data_size);
    } else {
      RAY_RETURN_NOT_OK(CreateExisting(metadata,
                                       data_size,
                                       object_id,
                                       owner_address,
                                       &data_buffer,
                                       /*created_by_worker=*/true));
      object_already_exists = data_buffer == nullptr;
    }
  }
  // Leave the return object as a nullptr if the object already exists.
  if (!object_already_exists) {
    auto contained_refs = GetObjectRefs(contained_object_ids);
    *return_object =
        std::make_shared<RayObject>(data_buffer, metadata, std::move(contained_refs));
  }

  return Status::OK();
}

Status CoreWorker::ExecuteTask(
    const TaskSpecification &task_spec,
    std::optional<ResourceMappingType> resource_ids,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
    std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_return_objects,
    std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
    ReferenceCounterInterface::ReferenceTableProto *borrowed_refs,
    bool *is_retryable_error,
    std::string *application_error) {
  RAY_LOG(DEBUG) << "Executing task, task info = " << task_spec.DebugString();

  // If the worker is exited via Exit API, we shouldn't execute tasks anymore.
  if (IsExiting()) {
    absl::MutexLock lock(&mutex_);
    return Status::IntentionalSystemExit(
        absl::StrCat("Worker has already exited. Detail: ", exiting_detail_.value()));
  }

  std::vector<std::shared_ptr<RayObject>> args;
  std::vector<rpc::ObjectReference> arg_refs;
  // This includes all IDs that were passed by reference and any IDs that were
  // inlined in the task spec. These references will be pinned during the task
  // execution and unpinned once the task completes. We will notify the caller
  // about any IDs that we are still borrowing by the time the task completes.
  std::vector<ObjectID> borrowed_ids;

  // Extract function name and retry status for metrics reporting.
  std::string func_name = task_spec.FunctionDescriptor()->CallString();
  bool is_retry = task_spec.IsRetry();

  ++num_get_pin_args_in_flight_;
  task_counter_.SetMetricStatus(
      func_name, rpc::TaskStatus::GETTING_AND_PINNING_ARGS, is_retry);
  Status pin_args_request_status =
      GetAndPinArgsForExecutor(task_spec, &args, &arg_refs, &borrowed_ids);
  task_counter_.UnsetMetricStatus(
      func_name, rpc::TaskStatus::GETTING_AND_PINNING_ARGS, is_retry);
  --num_get_pin_args_in_flight_;
  if (!pin_args_request_status.ok()) {
    ++num_failed_get_pin_args_;
    // If this has happened, it's because we are unable to talk to our local raylet.
    // This very likely means that the raylet has shutdown before this worker
    // unexpectedly. In whic case we'll trigger shut down.
    Exit(rpc::WorkerExitType::SYSTEM_ERROR,
         absl::StrCat("Worker failed to get and pin task arguments! Error message: ",
                      pin_args_request_status.message()),
         nullptr);
    return pin_args_request_status;
  }

  task_queue_length_ -= 1;
  num_executed_tasks_ += 1;

  // Modify the worker's per function counters.
  std::string actor_repr_name;
  {
    absl::MutexLock lock(&mutex_);
    actor_repr_name = actor_repr_name_;
  }
  if (!options_.is_local_mode) {
    task_counter_.MovePendingToRunning(func_name, is_retry);

    const auto update =
        (task_spec.IsActorTask() && !actor_repr_name.empty())
            ? worker::TaskStatusEvent::TaskStateUpdate(actor_repr_name, pid_)
            : worker::TaskStatusEvent::TaskStateUpdate(pid_);
    RAY_UNUSED(
        task_event_buffer_->RecordTaskStatusEventIfNeeded(task_spec.TaskId(),
                                                          task_spec.JobId(),
                                                          task_spec.AttemptNumber(),
                                                          task_spec,
                                                          rpc::TaskStatus::RUNNING,
                                                          /*include_task_info=*/false,
                                                          update));

    worker_context_->SetCurrentTask(task_spec);
    SetCurrentTaskId(task_spec.TaskId(), task_spec.AttemptNumber(), task_spec.GetName());
  }
  {
    absl::MutexLock lock(&mutex_);
    running_tasks_.emplace(task_spec.TaskId(), task_spec);
    if (resource_ids.has_value()) {
      resource_ids_ = std::move(*resource_ids);
    }
  }

  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  for (size_t i = 0; i < task_spec.NumReturns(); i++) {
    return_objects->emplace_back(task_spec.ReturnId(i), nullptr);
  }
  // For dynamic tasks, pass the return IDs that were dynamically generated on
  // the first execution.
  if (!task_spec.ReturnsDynamic()) {
    dynamic_return_objects = nullptr;
  } else if (task_spec.AttemptNumber() > 0) {
    for (const auto &dynamic_return_id : task_spec.DynamicReturnIds()) {
      // Increase the put index so that when the generator creates a new obj
      // the object id won't conflict.
      worker_context_->GetNextPutIndex();
      dynamic_return_objects->emplace_back(dynamic_return_id,
                                           std::shared_ptr<RayObject>());
      RAY_LOG(DEBUG) << "Re-executed task " << task_spec.TaskId()
                     << " should return dynamic object " << dynamic_return_id;

      AddLocalReference(dynamic_return_id, "<temporary (DynamicObjectRefGenerator)>");
      reference_counter_->AddBorrowedObject(
          dynamic_return_id, ObjectID::Nil(), task_spec.CallerAddress());
    }
  }

  TaskType task_type = TaskType::NORMAL_TASK;
  if (task_spec.IsActorCreationTask()) {
    task_type = TaskType::ACTOR_CREATION_TASK;
    SetActorId(task_spec.ActorCreationId());
    task_counter_.BecomeActor(task_spec.FunctionDescriptor()->ClassName());
    {
      auto self_actor_handle =
          std::make_unique<ActorHandle>(task_spec.GetSerializedActorHandle());
      // Register the handle to the current actor itself.
      actor_manager_->RegisterActorHandle(std::move(self_actor_handle),
                                          ObjectID::Nil(),
                                          CurrentCallSite(),
                                          rpc_address_,
                                          /*add_local_ref=*/false,
                                          /*is_self=*/true);
    }
    RAY_LOG(INFO).WithField(task_spec.ActorCreationId()) << "Creating actor";
  } else if (task_spec.IsActorTask()) {
    task_type = TaskType::ACTOR_TASK;
  }

  std::shared_ptr<LocalMemoryBuffer> creation_task_exception_pb_bytes = nullptr;

  std::vector<ConcurrencyGroup> defined_concurrency_groups = {};
  std::string name_of_concurrency_group_to_execute;
  if (task_spec.IsActorCreationTask()) {
    defined_concurrency_groups = task_spec.ConcurrencyGroups();
  } else if (task_spec.IsActorTask()) {
    name_of_concurrency_group_to_execute = task_spec.ConcurrencyGroupName();
  }

  Status status = options_.task_execution_callback(
      task_spec.CallerAddress(),
      task_type,
      task_spec.GetName(),
      func,
      task_spec.GetRequiredResources().GetResourceUnorderedMap(),
      args,
      arg_refs,
      task_spec.GetDebuggerBreakpoint(),
      task_spec.GetSerializedRetryExceptionAllowlist(),
      return_objects,
      dynamic_return_objects,
      streaming_generator_returns,
      creation_task_exception_pb_bytes,
      is_retryable_error,
      application_error,
      defined_concurrency_groups,
      name_of_concurrency_group_to_execute,
      /*is_reattempt=*/task_spec.AttemptNumber() > 0,
      /*is_streaming_generator=*/task_spec.IsStreamingGenerator(),
      /*retry_exception=*/task_spec.ShouldRetryExceptions(),
      /*generator_backpressure_num_objects=*/
      task_spec.GeneratorBackpressureNumObjects(),
      /*tensor_transport=*/task_spec.TensorTransport());

  // Get the reference counts for any IDs that we borrowed during this task,
  // remove the local reference for these IDs, and return the ref count info to
  // the caller. This will notify the caller of any IDs that we (or a nested
  // task) are still borrowing. It will also notify the caller of any new IDs
  // that were contained in a borrowed ID that we (or a nested task) are now
  // borrowing.
  std::vector<ObjectID> deleted;
  if (!borrowed_ids.empty()) {
    reference_counter_->PopAndClearLocalBorrowers(borrowed_ids, borrowed_refs, &deleted);
  }
  if (dynamic_return_objects != nullptr) {
    for (const auto &dynamic_return : *dynamic_return_objects) {
      reference_counter_->PopAndClearLocalBorrowers(
          {dynamic_return.first}, borrowed_refs, &deleted);
    }
  }
  memory_store_->Delete(deleted);

  if (task_spec.IsNormalTask() && reference_counter_->NumObjectIDsInScope() != 0) {
    RAY_LOG(DEBUG).WithField(task_spec.TaskId())
        << "There were " << reference_counter_->NumObjectIDsInScope()
        << " ObjectIDs left in scope after executing task. "
           "This is either caused by keeping references to ObjectIDs in Python "
           "between "
           "tasks (e.g., in global variables) or indicates a problem with Ray's "
           "reference counting, and may cause problems in the object store.";
  }

  if (!options_.is_local_mode) {
    SetCurrentTaskId(TaskID::Nil(), /*attempt_number=*/0, /*task_name=*/"");
    worker_context_->ResetCurrentTask();
  }
  {
    absl::MutexLock lock(&mutex_);
    size_t erased = running_tasks_.erase(task_spec.TaskId());
    RAY_CHECK(erased == 1);
    // Clean up cancellation state for this task
    canceled_tasks_.erase(task_spec.TaskId());
    if (task_spec.IsNormalTask()) {
      resource_ids_.clear();
    }
  }

  if (!options_.is_local_mode) {
    task_counter_.MoveRunningToFinished(func_name, task_spec.IsRetry());
  }
  RAY_LOG(DEBUG).WithField(task_spec.TaskId())
      << "Finished executing task, status=" << status;

  std::ostringstream stream;
  if (status.IsCreationTaskError()) {
    Exit(rpc::WorkerExitType::USER_ERROR,
         absl::StrCat(
             "Worker exits because there was an exception in the initialization method "
             "(e.g., __init__). Fix the exceptions from the initialization to resolve "
             "the issue. ",
             status.message()),
         creation_task_exception_pb_bytes);
  } else if (status.IsIntentionalSystemExit()) {
    Exit(rpc::WorkerExitType::INTENDED_USER_EXIT,
         absl::StrCat("Worker exits by an user request. ", status.message()),
         creation_task_exception_pb_bytes);
  } else if (status.IsUnexpectedSystemExit()) {
    Exit(rpc::WorkerExitType::SYSTEM_ERROR,
         absl::StrCat("Worker exits unexpectedly. ", status.message()),
         creation_task_exception_pb_bytes);
  } else {
    RAY_CHECK_OK(status) << "Unexpected task status type : " << status;
  }
  return status;
}

Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    const std::shared_ptr<RayObject> &return_object,
                                    const ObjectID &generator_id,
                                    const rpc::Address &caller_address) {
  RAY_LOG(DEBUG).WithField(return_id) << "Sealing return object";

  RAY_CHECK(return_object);
  RAY_CHECK(!options_.is_local_mode);

  Status status = Status::OK();
  auto caller_address_ptr = std::make_unique<rpc::Address>(caller_address);

  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    status = SealExisting(return_id, true, generator_id, caller_address_ptr);
    if (!status.ok()) {
      RAY_LOG(FATAL).WithField(return_id)
          << "Failed to seal object in store: " << status.message();
    }
  }
  return status;
}

void CoreWorker::AsyncDelObjectRefStream(const ObjectID &generator_id) {
  RAY_LOG(DEBUG).WithField(generator_id) << "AsyncDelObjectRefStream";
  if (task_manager_->TryDelObjectRefStream(generator_id)) {
    return;
  }

  {
    // TryDelObjectRefStream is thread safe so no need to hold the lock above.
    absl::MutexLock lock(&generator_ids_pending_deletion_mutex_);
    generator_ids_pending_deletion_.insert(generator_id);
  }
}

void CoreWorker::TryDelPendingObjectRefStreams() {
  absl::MutexLock lock(&generator_ids_pending_deletion_mutex_);

  std::vector<ObjectID> deleted;
  for (const auto &generator_id : generator_ids_pending_deletion_) {
    RAY_LOG(DEBUG).WithField(generator_id)
        << "TryDelObjectRefStream from generator_ids_pending_deletion_";
    if (task_manager_->TryDelObjectRefStream(generator_id)) {
      deleted.push_back(generator_id);
    }
  }

  for (const auto &generator_id : deleted) {
    generator_ids_pending_deletion_.erase(generator_id);
  }
}

Status CoreWorker::TryReadObjectRefStream(const ObjectID &generator_id,
                                          rpc::ObjectReference *object_ref_out) {
  ObjectID object_id;
  const auto &status = task_manager_->TryReadObjectRefStream(generator_id, &object_id);
  RAY_CHECK(object_ref_out != nullptr);
  object_ref_out->set_object_id(object_id.Binary());
  object_ref_out->mutable_owner_address()->CopyFrom(rpc_address_);
  return status;
}

bool CoreWorker::StreamingGeneratorIsFinished(const ObjectID &generator_id) const {
  return task_manager_->StreamingGeneratorIsFinished(generator_id);
}

std::pair<rpc::ObjectReference, bool> CoreWorker::PeekObjectRefStream(
    const ObjectID &generator_id) {
  auto [object_id, ready] = task_manager_->PeekObjectRefStream(generator_id);
  rpc::ObjectReference object_ref;
  object_ref.set_object_id(object_id.Binary());
  object_ref.mutable_owner_address()->CopyFrom(rpc_address_);
  return {object_ref, ready};
}

bool CoreWorker::PinExistingReturnObject(const ObjectID &return_id,
                                         std::shared_ptr<RayObject> *return_object,
                                         const ObjectID &generator_id,
                                         const rpc::Address &owner_address) {
  // TODO(swang): If there is already an existing copy of this object, then it
  // might not have the same value as the new copy. It would be better to evict
  // the existing copy here.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;

  // Temporarily set the return object's owner's address. This is needed to retrieve the
  // value from plasma.
  reference_counter_->AddLocalReference(return_id, "<temporary (pin return object)>");
  reference_counter_->AddBorrowedObject(return_id, ObjectID::Nil(), owner_address);

  // Resolve owner address of return id
  std::vector<ObjectID> object_ids = {return_id};
  auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);

  Status status =
      plasma_store_provider_->Get(object_ids, owner_addresses, 0, &result_map);
  // Remove the temporary ref.
  RemoveLocalReference(return_id);

  if (result_map.contains(return_id)) {
    *return_object = std::move(result_map[return_id]);
    RAY_LOG(DEBUG) << "Pinning existing return object " << return_id
                   << " owned by worker "
                   << WorkerID::FromBinary(owner_address.worker_id());
    // Keep the object in scope until it's been pinned.
    std::shared_ptr<RayObject> pinned_return_object = *return_object;
    // Asynchronously ask the raylet to pin the object. Note that this can fail
    // if the raylet fails. We expect the owner of the object to handle that
    // case (e.g., by detecting the raylet failure and storing an error).
    local_raylet_rpc_client_->PinObjectIDs(
        owner_address,
        {return_id},
        generator_id,
        [return_id, pinned_return_object](const Status &pin_object_status,
                                          const rpc::PinObjectIDsReply &reply) {
          // RPC to the local raylet should never fail.
          if (!pin_object_status.ok()) {
            RAY_LOG(ERROR) << "Request to local raylet to pin object failed: "
                           << pin_object_status.ToString();
            return;
          }
          if (!reply.successes(0)) {
            RAY_LOG(INFO).WithField(return_id)
                << "Failed to pin existing copy of the task return object. "
                   "This object may get evicted while there are still "
                   "references to it.";
          }
        });
    return true;
  }

  // Failed to get the existing copy of the return object. It must have been
  // evicted before we could pin it.
  // TODO(swang): We should allow the owner to retry this task instead of
  // immediately returning an error to the application.
  return false;
}

ObjectID CoreWorker::AllocateDynamicReturnId(const rpc::Address &owner_address,
                                             const TaskID &task_id,
                                             std::optional<ObjectIDIndexType> put_index) {
  const auto return_id = worker_context_->GetGeneratorReturnId(task_id, put_index);
  AddLocalReference(return_id, "<temporary (DynamicObjectRefGenerator)>");
  reference_counter_->AddBorrowedObject(return_id, ObjectID::Nil(), owner_address);
  return return_id;
}

Status CoreWorker::ReportGeneratorItemReturns(
    const std::pair<ObjectID, std::shared_ptr<RayObject>> &dynamic_return_object,
    const ObjectID &generator_id,
    const rpc::Address &caller_address,
    int64_t item_index,
    uint64_t attempt_number,
    const std::shared_ptr<GeneratorBackpressureWaiter> &waiter) {
  rpc::ReportGeneratorItemReturnsRequest request;
  request.mutable_worker_addr()->CopyFrom(rpc_address_);
  request.set_item_index(item_index);
  request.set_generator_id(generator_id.Binary());
  request.set_attempt_number(attempt_number);
  auto client = core_worker_client_pool_->GetOrConnect(caller_address);

  // This means it is the last report when the task has finished executing.
  if (!dynamic_return_object.first.IsNil()) {
    SerializeReturnObject(dynamic_return_object.first,
                          dynamic_return_object.second,
                          request.mutable_returned_object());
    std::vector<ObjectID> deleted;
    // When we allocate a dynamic return ID (AllocateDynamicReturnId),
    // we borrow the object. When the object value is allocatd, the
    // memory store is updated. We should clear borrowers and memory store
    // here.
    ReferenceCounterInterface::ReferenceTableProto borrowed_refs;
    reference_counter_->PopAndClearLocalBorrowers(
        {dynamic_return_object.first}, &borrowed_refs, &deleted);
    memory_store_->Delete(deleted);
  }
  const auto return_id = dynamic_return_object.first;
  RAY_LOG(DEBUG) << "Write the object ref stream, index: " << item_index
                 << ", id: " << return_id;

  waiter->IncrementObjectGenerated();

  client->ReportGeneratorItemReturns(
      std::move(request),
      [waiter, generator_id, return_id, item_index](
          const Status &status, const rpc::ReportGeneratorItemReturnsReply &reply) {
        RAY_LOG(DEBUG) << "ReportGeneratorItemReturns replied. " << generator_id
                       << "index: " << item_index << ". total_consumed_reported: "
                       << reply.total_num_object_consumed();
        RAY_LOG(DEBUG) << "Total object consumed: " << waiter->TotalObjectConsumed()
                       << ". Total object generated: " << waiter->TotalObjectGenerated();
        int64_t num_objects_consumed = 0;
        if (status.ok()) {
          num_objects_consumed = reply.total_num_object_consumed();
        } else {
          // If the request fails, we should just resume until task finishes without
          // backpressure.
          num_objects_consumed = waiter->TotalObjectGenerated();
          RAY_LOG(WARNING).WithField(return_id)
              << "Failed to report streaming generator return "
                 "to the caller. The yield'ed ObjectRef may not be usable. "
              << status;
        }
        waiter->HandleObjectReported(num_objects_consumed);
      });

  // Backpressure if needed. See task_manager.h and search "backpressure" for protocol
  // details.
  return waiter->WaitUntilObjectConsumed();
}

void CoreWorker::HandleReportGeneratorItemReturns(
    rpc::ReportGeneratorItemReturnsRequest request,
    rpc::ReportGeneratorItemReturnsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto generator_id = ObjectID::FromBinary(request.generator_id());
  auto worker_id = WorkerID::FromBinary(request.worker_addr().worker_id());
  task_manager_->HandleReportGeneratorItemReturns(
      request,
      /*execution_signal_callback=*/
      [reply,
       worker_id = std::move(worker_id),
       generator_id = std::move(generator_id),
       send_reply_callback = std::move(send_reply_callback)](
          const Status &status, int64_t total_num_object_consumed) {
        RAY_LOG(DEBUG) << "Reply HandleReportGeneratorItemReturns to signal "
                          "executor to resume tasks. "
                       << generator_id << ". Worker ID: " << worker_id
                       << ". Total consumed: " << total_num_object_consumed;
        if (!status.ok()) {
          RAY_CHECK_EQ(total_num_object_consumed, -1);
        }

        reply->set_total_num_object_consumed(total_num_object_consumed);
        send_reply_callback(status, nullptr, nullptr);
      });
}

std::vector<rpc::ObjectReference> CoreWorker::ExecuteTaskLocalMode(
    const TaskSpecification &task_spec, const ActorID &actor_id) {
  auto return_objects = std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>();
  auto borrowed_refs = ReferenceCounterInterface::ReferenceTableProto();

  std::vector<rpc::ObjectReference> returned_refs;
  size_t num_returns = task_spec.NumReturns();
  for (size_t i = 0; i < num_returns; i++) {
    if (!task_spec.IsActorCreationTask()) {
      reference_counter_->AddOwnedObject(task_spec.ReturnId(i),
                                         /*contained_ids=*/{},
                                         rpc_address_,
                                         CurrentCallSite(),
                                         -1,
                                         /*is_reconstructable=*/false,
                                         /*add_local_ref=*/true);
    }
    rpc::ObjectReference ref;
    ref.set_object_id(task_spec.ReturnId(i).Binary());
    ref.mutable_owner_address()->CopyFrom(task_spec.CallerAddress());
    returned_refs.push_back(std::move(ref));
  }
  auto old_id = GetActorId();
  SetActorId(actor_id);
  bool is_retryable_error = false;
  std::string application_error;
  // TODO(swang): Support DynamicObjectRefGenerators in local mode?
  std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> dynamic_return_objects;
  std::vector<std::pair<ObjectID, bool>> streaming_generator_returns;
  RAY_UNUSED(ExecuteTask(task_spec,
                         /*resource_ids=*/ResourceMappingType{},
                         &return_objects,
                         &dynamic_return_objects,
                         &streaming_generator_returns,
                         &borrowed_refs,
                         &is_retryable_error,
                         &application_error));
  SetActorId(old_id);
  return returned_refs;
}

Status CoreWorker::GetAndPinArgsForExecutor(const TaskSpecification &task,
                                            std::vector<std::shared_ptr<RayObject>> *args,
                                            std::vector<rpc::ObjectReference> *arg_refs,
                                            std::vector<ObjectID> *borrowed_ids) {
  auto num_args = task.NumArgs();
  args->reserve(num_args);
  arg_refs->reserve(num_args);

  absl::flat_hash_set<ObjectID> by_ref_ids;
  absl::flat_hash_map<ObjectID, std::vector<size_t>> by_ref_indices;

  for (size_t i = 0; i < task.NumArgs(); ++i) {
    if (task.ArgByRef(i)) {
      const auto &arg_ref = task.ArgRef(i);
      const auto arg_id = ObjectID::FromBinary(arg_ref.object_id());
      by_ref_ids.insert(arg_id);
      by_ref_indices[arg_id].push_back(i);
      arg_refs->push_back(arg_ref);
      args->emplace_back();
      // Pin all args passed by reference for the duration of the task.  This
      // ensures that when the task completes, we can retrieve metadata about
      // any borrowed ObjectIDs that were serialized in the argument's value.
      RAY_LOG(DEBUG).WithField(arg_id) << "Incrementing ref for argument ID";
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());
      // Attach the argument's owner's address. This is needed to retrieve the
      // value from plasma.
      reference_counter_->AddBorrowedObject(
          arg_id, ObjectID::Nil(), task.ArgRef(i).owner_address());
      borrowed_ids->push_back(arg_id);
      // We need to put an OBJECT_IN_PLASMA error here so the subsequent call to Get()
      // properly redirects to the plasma store.
      // NOTE: This needs to be done after adding reference to reference counter
      // otherwise, the put is a no-op.
      if (!options_.is_local_mode) {
        memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                           task.ArgObjectId(i),
                           reference_counter_->HasReference(task.ArgObjectId(i)));
      }
    } else {
      // A pass-by-value argument.
      std::shared_ptr<LocalMemoryBuffer> data = nullptr;
      if (task.ArgDataSize(i) != 0u) {
        data = std::make_shared<LocalMemoryBuffer>(const_cast<uint8_t *>(task.ArgData(i)),
                                                   task.ArgDataSize(i));
      }
      std::shared_ptr<LocalMemoryBuffer> metadata = nullptr;
      if (task.ArgMetadataSize(i) != 0u) {
        metadata = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(task.ArgMetadata(i)), task.ArgMetadataSize(i));
      }
      // NOTE: this is a workaround to avoid an extra copy for Java workers.
      // Python workers need this copy to pass test case
      // test_inline_arg_memory_corruption.
      bool copy_data = options_.language == Language::PYTHON;
      rpc::TensorTransport tensor_transport = task.ArgTensorTransport(i);
      args->push_back(std::make_shared<RayObject>(std::move(data),
                                                  std::move(metadata),
                                                  task.ArgInlinedRefs(i),
                                                  copy_data,
                                                  tensor_transport));
      auto &arg_ref = arg_refs->emplace_back();
      arg_ref.set_object_id(task.ArgObjectIdBinary(i));
      // The task borrows all ObjectIDs that were serialized in the inlined
      // arguments. The task will receive references to these IDs, so it is
      // possible for the task to continue borrowing these arguments by the
      // time it finishes.
      for (const auto &inlined_ref : task.ArgInlinedRefs(i)) {
        const auto inlined_id = ObjectID::FromBinary(inlined_ref.object_id());
        RAY_LOG(DEBUG).WithField(inlined_id) << "Incrementing ref for borrowed ID";
        // We do not need to add the ownership information here because it will
        // get added once the language frontend deserializes the value, before
        // the ObjectID can be used.
        reference_counter_->AddLocalReference(inlined_id, task.CallSiteString());
        borrowed_ids->push_back(inlined_id);
      }
    }
  }

  // Fetch by-reference arguments directly from the plasma store.
  bool got_exception = false;
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  if (options_.is_local_mode) {
    RAY_RETURN_NOT_OK(memory_store_->Get(
        by_ref_ids, -1, *worker_context_, &result_map, &got_exception));
  } else {
    // Resolve owner addresses of by-ref ids
    std::vector<ObjectID> object_ids =
        std::vector<ObjectID>(by_ref_ids.begin(), by_ref_ids.end());
    auto owner_addresses = reference_counter_->GetOwnerAddresses(object_ids);
    RAY_RETURN_NOT_OK(
        plasma_store_provider_->Get(object_ids, owner_addresses, -1, &result_map));
  }
  for (const auto &it : result_map) {
    for (size_t idx : by_ref_indices[it.first]) {
      args->at(idx) = it.second;
    }
  }

  return Status::OK();
}

void CoreWorker::HandlePushTask(rpc::PushTaskRequest request,
                                rpc::PushTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG).WithField(TaskID::FromBinary(request.task_spec().task_id()))
      << "Received Handle Push Task";
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Set actor info in the worker context.
  if (request.task_spec().type() == TaskType::ACTOR_CREATION_TASK) {
    auto actor_id =
        ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

    // Handle duplicate actor creation tasks that might be sent from the GCS on restart.
    // Ignore the message and reply OK.
    if (worker_context_->GetCurrentActorID() == actor_id) {
      RAY_LOG(INFO) << "Ignoring duplicate actor creation task for actor " << actor_id
                    << ". This is likely due to a GCS server restart.";
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
    worker_context_->SetCurrentActorId(actor_id);
  }

  // Set job info in the worker context.
  if (request.task_spec().type() == TaskType::ACTOR_CREATION_TASK ||
      request.task_spec().type() == TaskType::NORMAL_TASK) {
    auto job_id = JobID::FromBinary(request.task_spec().job_id());
    worker_context_->MaybeInitializeJobInfo(job_id, request.task_spec().job_config());
    task_counter_.SetJobId(job_id);
  }

  // Increment the task_queue_length and per function counter.
  task_queue_length_ += 1;
  std::string func_name =
      FunctionDescriptorBuilder::FromProto(request.task_spec().function_descriptor())
          ->CallString();
  task_counter_.IncPending(func_name, request.task_spec().attempt_number() > 0);

  // For actor tasks, we just need to post a HandleActorTask instance to the task
  // execution service.
  if (request.task_spec().type() == TaskType::ACTOR_TASK) {
    task_execution_service_.post(
        [this,
         request = std::move(request),
         reply,
         send_reply_callback = std::move(send_reply_callback),
         func_name]() mutable {
          // We have posted an exit task onto the main event loop,
          // so shouldn't bother executing any further work.
          if (IsExiting()) {
            RAY_LOG(INFO) << "Queued task " << func_name
                          << " won't be executed because the worker already exited.";
            return;
          }
          task_receiver_->HandleTask(std::move(request), reply, send_reply_callback);
        },
        "CoreWorker.HandlePushTaskActor");
  } else {
    // Normal tasks are enqueued here, and we post a RunNormalTasksFromQueue instance to
    // the task execution service.
    task_receiver_->HandleTask(std::move(request), reply, send_reply_callback);
    task_execution_service_.post(
        [this, func_name] {
          // We have posted an exit task onto the main event loop,
          // so shouldn't bother executing any further work.
          if (IsExiting()) {
            RAY_LOG(INFO) << "Queued task " << func_name
                          << " won't be executed because the worker already exited.";
            return;
          }
          task_receiver_->RunNormalTasksFromQueue();
        },
        "CoreWorker.HandlePushTask");
  }
}

void CoreWorker::HandleActorCallArgWaitComplete(
    rpc::ActorCallArgWaitCompleteRequest request,
    rpc::ActorCallArgWaitCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Post on the task execution event loop since this may trigger the
  // execution of a task that is now ready to run.
  task_execution_service_.post(
      [this, request = std::move(request)] {
        RAY_LOG(DEBUG) << "Arg wait complete for tag " << request.tag();
        task_argument_waiter_->OnWaitComplete(request.tag());
      },
      "CoreWorker.ArgWaitComplete");

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleRayletNotifyGCSRestart(
    rpc::RayletNotifyGCSRestartRequest request,
    rpc::RayletNotifyGCSRestartReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  gcs_client_->AsyncResubscribe();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// HandleGetObjectStatus is expected to be idempotent
void CoreWorker::HandleGetObjectStatus(rpc::GetObjectStatusRequest request,
                                       rpc::GetObjectStatusReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.owner_worker_id()),
                           send_reply_callback)) {
    RAY_LOG(INFO) << "Handling GetObjectStatus for object produced by a previous worker "
                     "with the same address";
    return;
  }

  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG).WithField(object_id) << "Received GetObjectStatus";

  rpc::Address owner_address;
  auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
  if (!has_owner) {
    // We owned this object, but the object has gone out of scope.
    reply->set_status(rpc::GetObjectStatusReply::OUT_OF_SCOPE);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  RAY_CHECK(owner_address.worker_id() == request.owner_worker_id());
  if (reference_counter_->IsPlasmaObjectFreed(object_id)) {
    reply->set_status(rpc::GetObjectStatusReply::FREED);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  // Send the reply once the value has become available. The value is
  // guaranteed to become available eventually because we own the object and
  // its ref count is > 0.
  memory_store_->GetAsync(object_id,
                          [this, object_id, reply, send_reply_callback](
                              const std::shared_ptr<RayObject> &obj) {
                            PopulateObjectStatus(object_id, obj, reply);
                            send_reply_callback(Status::OK(), nullptr, nullptr);
                          });
}

void CoreWorker::PopulateObjectStatus(const ObjectID &object_id,
                                      const std::shared_ptr<RayObject> &obj,
                                      rpc::GetObjectStatusReply *reply) {
  // If obj is the concrete object value, it is small, so we
  // send the object back to the caller in the GetObjectStatus
  // reply, bypassing a Plasma put and object transfer. If obj
  // is an indicator that the object is in Plasma, we set an
  // in_plasma indicator on the message, and the caller will
  // have to facilitate a Plasma object transfer to get the
  // object value.
  auto *object = reply->mutable_object();
  if (obj->HasData()) {
    const auto &data = obj->GetData();
    object->set_data(data->Data(), data->Size());
  }
  if (obj->HasMetadata()) {
    const auto &metadata = obj->GetMetadata();
    object->set_metadata(metadata->Data(), metadata->Size());
  }
  for (const auto &nested_ref : obj->GetNestedRefs()) {
    object->add_nested_inlined_refs()->CopyFrom(nested_ref);
  }
  reply->set_status(rpc::GetObjectStatusReply::CREATED);
  // Set locality data.
  const auto &locality_data = reference_counter_->GetLocalityData(object_id);
  if (locality_data.has_value()) {
    for (const auto &node_id : locality_data.value().nodes_containing_object) {
      reply->add_node_ids(node_id.Binary());
    }
    reply->set_object_size(locality_data.value().object_size);
  }
}

void CoreWorker::HandleWaitForActorRefDeleted(
    rpc::WaitForActorRefDeletedRequest request,
    rpc::WaitForActorRefDeletedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto actor_id = ActorID::FromBinary(request.actor_id());

  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Send a response to trigger cleaning up the actor state once the handle is
  // no longer in scope.
  auto respond = [send_reply_callback](const ActorID &respond_actor_id) {
    RAY_LOG(DEBUG).WithField(respond_actor_id)
        << "Replying to HandleWaitForActorRefDeleted";
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  /// The callback for each request is stored in the reference counter due to retries
  /// and message reordering where the callback of the retry of the request could be
  /// overwritten by the callback of the initial request.
  if (actor_creator_->IsActorInRegistering(actor_id)) {
    actor_creator_->AsyncWaitForActorRegisterFinish(
        actor_id, [this, actor_id, respond = std::move(respond)](const auto &status) {
          if (!status.ok()) {
            respond(actor_id);
          } else {
            RAY_LOG(DEBUG).WithField(actor_id) << "Received HandleWaitForActorRefDeleted";
            actor_manager_->WaitForActorRefDeleted(actor_id, std::move(respond));
          }
        });
  } else {
    RAY_LOG(DEBUG).WithField(actor_id) << "Received HandleWaitForActorRefDeleted";
    actor_manager_->WaitForActorRefDeleted(actor_id, std::move(respond));
  }
}

void CoreWorker::ProcessSubscribeForObjectEviction(
    const rpc::WorkerObjectEvictionSubMessage &message) {
  // Send a response to trigger unpinning the object when it is no longer in scope.
  auto unpin_object = [this](const ObjectID &object_id) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object is deleted. Unpinning the object.";

    rpc::PubMessage pub_message;
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
    pub_message.mutable_worker_object_eviction_message()->set_object_id(
        object_id.Binary());

    object_info_publisher_->Publish(std::move(pub_message));
  };

  const auto object_id = ObjectID::FromBinary(message.object_id());
  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  if (intended_worker_id != worker_context_->GetWorkerID()) {
    RAY_LOG(INFO).WithField(object_id)
        << "The SubscribeForObjectEviction message for object is for worker "
        << intended_worker_id << ", but the current worker is "
        << worker_context_->GetWorkerID() << ". The RPC will be no-op.";
    unpin_object(object_id);
    return;
  }

  if (message.has_generator_id()) {
    // For dynamically generated return values, the raylet may subscribe to
    // eviction events before we know about the object. This can happen when we
    // receive the subscription request before the reply from the task that
    // created the object. Add the dynamically created object to our ref
    // counter so that we know that it exists.
    const auto generator_id = ObjectID::FromBinary(message.generator_id());
    RAY_CHECK(!generator_id.IsNil());
    if (task_manager_->ObjectRefStreamExists(generator_id)) {
      // ObjectRefStreamExists is used to distinguigsh num_returns="dynamic" vs
      // "streaming".
      task_manager_->TemporarilyOwnGeneratorReturnRefIfNeeded(object_id, generator_id);
    } else {
      reference_counter_->AddDynamicReturn(object_id, generator_id);
    }
  }

  // Returns true if the object was present and the callback was added. It might have
  // already been evicted by the time we get this request, in which case we should
  // respond immediately so the raylet unpins the object.
  if (!reference_counter_->AddObjectOutOfScopeOrFreedCallback(object_id, unpin_object)) {
    // If the object is already evicted (callback cannot be set), unregister the
    // subscription & publish the message so that the subscriber knows it.
    unpin_object(object_id);
    RAY_LOG(DEBUG).WithField(object_id) << "Reference for object has already been freed.";
  }
}

void CoreWorker::ProcessSubscribeMessage(const rpc::SubMessage &sub_message,
                                         rpc::ChannelType channel_type,
                                         const std::string &key_id,
                                         const NodeID &subscriber_id) {
  object_info_publisher_->RegisterSubscription(channel_type, subscriber_id, key_id);

  if (sub_message.has_worker_object_eviction_message()) {
    ProcessSubscribeForObjectEviction(sub_message.worker_object_eviction_message());
  } else if (sub_message.has_worker_ref_removed_message()) {
    ProcessSubscribeForRefRemoved(sub_message.worker_ref_removed_message());
  } else if (sub_message.has_worker_object_locations_message()) {
    ProcessSubscribeObjectLocations(sub_message.worker_object_locations_message());
  } else {
    RAY_LOG(FATAL)
        << "Invalid command has received: "
        << static_cast<int>(sub_message.sub_message_one_of_case())
        << " has received. If you see this message, please report to Ray Github.";
  }
}

void CoreWorker::ProcessPubsubCommands(const Commands &commands,
                                       const NodeID &subscriber_id) {
  for (const auto &command : commands) {
    if (command.has_unsubscribe_message()) {
      object_info_publisher_->UnregisterSubscription(
          command.channel_type(), subscriber_id, command.key_id());
    } else if (command.has_subscribe_message()) {
      ProcessSubscribeMessage(command.subscribe_message(),
                              command.channel_type(),
                              command.key_id(),
                              subscriber_id);
    } else {
      RAY_LOG(FATAL) << "Invalid command has received, "
                     << static_cast<int>(command.command_message_one_of_case())
                     << ". If you see this message, please "
                        "report to Ray "
                        "Github.";
    }
  }
}

void CoreWorker::HandlePubsubLongPolling(rpc::PubsubLongPollingRequest request,
                                         rpc::PubsubLongPollingReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = NodeID::FromBinary(request.subscriber_id());
  RAY_LOG(DEBUG).WithField(subscriber_id) << "Got a long polling request from a node";
  object_info_publisher_->ConnectToSubscriber(request,
                                              reply->mutable_publisher_id(),
                                              reply->mutable_pub_messages(),
                                              std::move(send_reply_callback));
}

void CoreWorker::HandlePubsubCommandBatch(rpc::PubsubCommandBatchRequest request,
                                          rpc::PubsubCommandBatchReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = NodeID::FromBinary(request.subscriber_id());
  ProcessPubsubCommands(request.commands(), subscriber_id);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleUpdateObjectLocationBatch(
    rpc::UpdateObjectLocationBatchRequest request,
    rpc::UpdateObjectLocationBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &worker_id = request.intended_worker_id();
  if (HandleWrongRecipient(WorkerID::FromBinary(worker_id), send_reply_callback)) {
    return;
  }
  const auto &node_id = NodeID::FromBinary(request.node_id());
  const auto &object_location_updates = request.object_location_updates();

  for (const auto &object_location_update : object_location_updates) {
    const auto &object_id = ObjectID::FromBinary(object_location_update.object_id());

    if (object_location_update.has_spilled_location_update()) {
      AddSpilledObjectLocationOwner(
          object_id,
          object_location_update.spilled_location_update().spilled_url(),
          object_location_update.spilled_location_update().spilled_to_local_storage()
              ? node_id
              : NodeID::Nil(),
          object_location_update.has_generator_id()
              ? std::optional<ObjectID>(
                    ObjectID::FromBinary(object_location_update.generator_id()))
              : std::nullopt);
    }

    if (object_location_update.has_plasma_location_update()) {
      if (object_location_update.plasma_location_update() ==
          rpc::ObjectPlasmaLocationUpdate::ADDED) {
        AddObjectLocationOwner(object_id, node_id);
      } else if (object_location_update.plasma_location_update() ==
                 rpc::ObjectPlasmaLocationUpdate::REMOVED) {
        RemoveObjectLocationOwner(object_id, node_id);
      } else {
        RAY_LOG(FATAL) << "Invalid object plasma location update "
                       << object_location_update.plasma_location_update()
                       << " has been received.";
      }
    }
  }

  send_reply_callback(Status::OK(),
                      /*success_callback_on_reply=*/nullptr,
                      /*failure_callback_on_reply=*/nullptr);
}

void CoreWorker::AddSpilledObjectLocationOwner(
    const ObjectID &object_id,
    const std::string &spilled_url,
    const NodeID &spilled_node_id,
    const std::optional<ObjectID> &generator_id) {
  RAY_LOG(DEBUG).WithField(object_id).WithField(spilled_node_id)
      << "Received object spilled location update for object, which has been spilled "
         "to "
      << spilled_url << " on node";
  if (generator_id.has_value()) {
    // For dynamically generated return values, the raylet may spill the
    // primary copy before we know about the object. This can happen when the
    // object is spilled before the reply from the task that created the
    // object. Add the dynamically created object to our ref counter so that we
    // know that it exists.
    if (task_manager_->ObjectRefStreamExists(*generator_id)) {
      // ObjectRefStreamExists is used to distinguigsh num_returns="dynamic" vs
      // "streaming".
      task_manager_->TemporarilyOwnGeneratorReturnRefIfNeeded(object_id, *generator_id);
    } else {
      reference_counter_->AddDynamicReturn(object_id, *generator_id);
    }
  }

  auto reference_exists =
      reference_counter_->HandleObjectSpilled(object_id, spilled_url, spilled_node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object not found";
  }
}

void CoreWorker::AddObjectLocationOwner(const ObjectID &object_id,
                                        const NodeID &node_id) {
  if (gcs_client_->Nodes().IsNodeDead(node_id)) {
    RAY_LOG(DEBUG).WithField(node_id).WithField(object_id)
        << "Attempting to add object location for a dead node. Ignoring this request.";
    return;
  }
  auto reference_exists = reference_counter_->AddObjectLocation(object_id, node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object not found";
  }

  // For generator tasks where we haven't yet received the task reply, the
  // internal ObjectRefs may not be added yet, so we don't find out about these
  // until the task finishes.
  const auto &maybe_generator_id = task_manager_->TaskGeneratorId(object_id.TaskId());
  if (!maybe_generator_id.IsNil()) {
    if (task_manager_->ObjectRefStreamExists(maybe_generator_id)) {
      // ObjectRefStreamExists is used to distinguigsh num_returns="dynamic" vs
      // "streaming".
      task_manager_->TemporarilyOwnGeneratorReturnRefIfNeeded(object_id,
                                                              maybe_generator_id);
    } else {
      // The task is a generator and may not have finished yet. Add the internal
      // ObjectID so that we can update its location.
      reference_counter_->AddDynamicReturn(object_id, maybe_generator_id);
    }
    RAY_UNUSED(reference_counter_->AddObjectLocation(object_id, node_id));
  }
}

void CoreWorker::RemoveObjectLocationOwner(const ObjectID &object_id,
                                           const NodeID &node_id) {
  auto reference_exists = reference_counter_->RemoveObjectLocation(object_id, node_id);
  if (!reference_exists) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object not found";
  }
}

void CoreWorker::ProcessSubscribeObjectLocations(
    const rpc::WorkerObjectLocationsSubMessage &message) {
  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  const auto object_id = ObjectID::FromBinary(message.object_id());

  if (intended_worker_id != worker_context_->GetWorkerID()) {
    RAY_LOG(INFO) << "The ProcessSubscribeObjectLocations message is for worker "
                  << intended_worker_id << ", but the current worker is "
                  << worker_context_->GetWorkerID() << ". The RPC will be no-op.";
    object_info_publisher_->PublishFailure(
        rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL, object_id.Binary());
    return;
  }

  // Publish the first object location snapshot when subscribed for the first time.
  reference_counter_->PublishObjectLocationSnapshot(object_id);
}

std::unordered_map<rpc::LineageReconstructionTask, uint64_t>
CoreWorker::GetLocalOngoingLineageReconstructionTasks() const {
  return task_manager_->GetOngoingLineageReconstructionTasks(*actor_manager_);
}

Status CoreWorker::GetLocalObjectLocations(
    const std::vector<ObjectID> &object_ids,
    std::vector<std::optional<ObjectLocation>> *results) {
  results->clear();
  results->reserve(object_ids.size());
  if (object_ids.empty()) {
    return Status::OK();
  }
  for (size_t i = 0; i < object_ids.size(); i++) {
    results->emplace_back(TryGetLocalObjectLocation(*reference_counter_, object_ids[i]));
  }
  return Status::OK();
}

void CoreWorker::HandleGetObjectLocationsOwner(
    rpc::GetObjectLocationsOwnerRequest request,
    rpc::GetObjectLocationsOwnerReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  for (int i = 0; i < request.object_ids_size(); ++i) {
    auto object_id = ObjectID::FromBinary(request.object_ids(i));
    auto *object_info = reply->add_object_location_infos();
    reference_counter_->FillObjectInformation(object_id, object_info);
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::ProcessSubscribeForRefRemoved(
    const rpc::WorkerRefRemovedSubMessage &message) {
  const ObjectID &object_id = ObjectID::FromBinary(message.reference().object_id());

  const auto intended_worker_id = WorkerID::FromBinary(message.intended_worker_id());
  if (intended_worker_id != worker_context_->GetWorkerID()) {
    RAY_LOG(INFO) << "The ProcessSubscribeForRefRemoved message is for worker "
                  << intended_worker_id << ", but the current worker is "
                  << worker_context_->GetWorkerID() << ". The RPC will be no-op.";
    reference_counter_->PublishRefRemoved(object_id);
    return;
  }

  const auto owner_address = message.reference().owner_address();
  ObjectID contained_in_id = ObjectID::FromBinary(message.contained_in_id());
  // So it will call PublishRefRemovedInternal to publish a message when the requested
  // object ID's ref count goes to 0.
  reference_counter_->SubscribeRefRemoved(object_id, contained_in_id, owner_address);
}

void CoreWorker::HandleRequestOwnerToCancelTask(
    rpc::RequestOwnerToCancelTaskRequest request,
    rpc::RequestOwnerToCancelTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = CancelTask(ObjectID::FromBinary(request.remote_object_id()),
                           request.force_kill(),
                           request.recursive());
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleCancelTask(rpc::CancelTaskRequest request,
                                  rpc::CancelTaskReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.intended_task_id());
  bool force_kill = request.force_kill();
  bool recursive = request.recursive();
  const auto &current_actor_id = worker_context_->GetCurrentActorID();
  const auto caller_worker_id = WorkerID::FromBinary(request.caller_worker_id());

  auto on_cancel_callback = [this,
                             reply,
                             send_reply_callback = std::move(send_reply_callback),
                             force_kill,
                             task_id](bool success, bool requested_task_running) {
    reply->set_attempt_succeeded(success);
    reply->set_requested_task_running(requested_task_running);
    send_reply_callback(Status::OK(), nullptr, nullptr);

    // Do force kill after reply callback sent.
    if (force_kill) {
      // We grab the lock again to make sure that we are force-killing the correct
      // task. This is guaranteed not to deadlock because ForceExit should not
      // require any other locks.
      absl::MutexLock lock(&mutex_);
      if (main_thread_task_id_ == task_id) {
        ForceExit(rpc::WorkerExitType::INTENDED_USER_EXIT,
                  absl::StrCat("The worker exits because the task ",
                               main_thread_task_name_,
                               " has received a force ray.cancel request."));
      }
    }
  };

  if (task_id.ActorId() == current_actor_id) {
    RAY_LOG(INFO).WithField(task_id).WithField(current_actor_id)
        << "Cancel an actor task";
    CancelActorTaskOnExecutor(
        caller_worker_id, task_id, force_kill, recursive, std::move(on_cancel_callback));
  } else {
    RAY_CHECK(current_actor_id.IsNil());
    RAY_LOG(INFO).WithField(task_id) << "Cancel a normal task";
    CancelTaskOnExecutor(task_id, force_kill, recursive, on_cancel_callback);
  }
}

void CoreWorker::CancelTaskOnExecutor(TaskID task_id,
                                      bool force_kill,
                                      bool recursive,
                                      const OnCanceledCallback &on_canceled) {
  bool requested_task_running = false;
  {
    absl::MutexLock lock(&mutex_);
    requested_task_running = main_thread_task_id_ == task_id;

    if (requested_task_running) {
      canceled_tasks_.insert(task_id);
    }
  }
  bool success = requested_task_running;

  // Try non-force kill.
  // NOTE(swang): We do not hold the CoreWorker lock here because the kill
  // callback requires the GIL, which can cause a deadlock with the main task
  // thread. This means that the currently executing task can change by the time
  // the kill callback runs; the kill callback is responsible for also making
  // sure it cancels the right task.
  // See https://github.com/ray-project/ray/issues/29739.
  if (requested_task_running && !force_kill) {
    RAY_LOG(INFO).WithField(task_id) << "Cancelling a running task";
    success = options_.kill_main(task_id);
  } else if (!requested_task_running) {
    RAY_LOG(INFO).WithField(task_id)
        << "Cancelling a task that's not running. Tasks will be removed from a queue.";
    // If the task is not currently running, check if it is in the worker's queue of
    // normal tasks, and remove it if found.
    success = task_receiver_->CancelQueuedNormalTask(task_id);
  }
  if (recursive) {
    auto recursive_cancel = CancelChildren(task_id, force_kill);
    if (!recursive_cancel.ok()) {
      RAY_LOG(ERROR) << recursive_cancel.ToString();
    }
  }

  on_canceled(/*success=*/success, /*requested_task_running=*/requested_task_running);
}

void CoreWorker::CancelActorTaskOnExecutor(WorkerID caller_worker_id,
                                           TaskID task_id,
                                           bool force_kill,
                                           bool recursive,
                                           OnCanceledCallback on_canceled) {
  RAY_CHECK(!force_kill);
  auto is_async_actor = worker_context_->CurrentActorIsAsync();

  auto cancel = [this,
                 task_id,
                 caller_worker_id,
                 on_canceled = std::move(on_canceled),
                 is_async_actor]() {
    // If the task was still queued (not running yet), `CancelQueuedActorTask` will
    // cancel it. If it is already running, we attempt to cancel it.
    bool success = false;
    bool is_running = false;
    bool task_present = task_receiver_->CancelQueuedActorTask(caller_worker_id, task_id);
    if (task_present) {
      {
        absl::MutexLock lock(&mutex_);
        is_running = running_tasks_.find(task_id) != running_tasks_.end();

        if (is_running) {
          canceled_tasks_.insert(task_id);
        }
      }

      // Attempt to cancel the task if it's running.
      // We can't currently interrupt running tasks for non-async actors.
      if (is_running && is_async_actor) {
        success = options_.cancel_async_actor_task(task_id);
      } else {
        // If the task wasn't running, it was successfully cancelled by
        // CancelQueuedActorTask. Else if for non-async actor, we can't interrupt running
        // tasks, but we've marked it as canceled so IsTaskCanceled() will return true.
        // Return success so the client won't retry.
        success = true;
      }
    }

    on_canceled(success, is_running);
  };

  if (is_async_actor) {
    // If it is an async actor, post it to an execution service
    // to avoid thread issues. Note that when it is an async actor
    // task_execution_service_ won't actually run a task but it will
    // just create coroutines.
    task_execution_service_.post([cancel = std::move(cancel)]() { cancel(); },
                                 "CoreWorker.CancelActorTaskOnExecutor");
  } else {
    // For regular actor, we cannot post it to task_execution_service because
    // main thread is blocked. Threaded actor can do both (dispatching to
    // task execution service, or just directly call it in io_service).
    // There's no special reason why we don't dispatch
    // cancel to task_execution_service_ for threaded actors.
    cancel();
  }

  if (recursive) {
    auto recursive_cancel = CancelChildren(task_id, force_kill);
    if (!recursive_cancel.ok()) {
      RAY_LOG(ERROR) << recursive_cancel.ToString();
    }
  };
}

void CoreWorker::HandleKillActor(rpc::KillActorRequest request,
                                 rpc::KillActorReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  ActorID intended_actor_id = ActorID::FromBinary(request.intended_actor_id());
  if (intended_actor_id != worker_context_->GetCurrentActorID()) {
    std::ostringstream stream;
    stream << "Mismatched ActorID: ignoring KillActor for previous actor "
           << intended_actor_id
           << ", current actor ID: " << worker_context_->GetCurrentActorID();
    const auto &msg = stream.str();
    RAY_LOG(ERROR) << msg;
    send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
    return;
  }

  const auto &kill_actor_reason =
      gcs::GenErrorMessageFromDeathCause(request.death_cause());

  if (request.force_kill()) {
    RAY_LOG(INFO) << "Force kill actor request has received. exiting immediately... "
                  << kill_actor_reason;
    RAY_LOG(DEBUG) << "HandleKillActor: About to call ForceExit";
    // If we don't need to restart this actor, we notify raylet before force killing it.
    ForceExit(
        rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
        absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
    RAY_LOG(DEBUG) << "HandleKillActor: ForceExit completed";
  } else {
    RAY_LOG(DEBUG) << "HandleKillActor: About to call Exit";
    Exit(rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
         absl::StrCat("Worker exits because the actor is killed. ", kill_actor_reason));
  }
}

void CoreWorker::HandleRegisterMutableObjectReader(
    rpc::RegisterMutableObjectReaderRequest request,
    rpc::RegisterMutableObjectReaderReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  local_raylet_rpc_client_->RegisterMutableObjectReader(
      ObjectID::FromBinary(request.writer_object_id()),
      request.num_readers(),
      ObjectID::FromBinary(request.reader_object_id()),
      [send_reply_callback](const Status &status,
                            const rpc::RegisterMutableObjectReply &r) {
        RAY_CHECK_OK(status);
        send_reply_callback(Status::OK(), nullptr, nullptr);
      });
}

int64_t CoreWorker::GetLocalMemoryStoreBytesUsed() const {
  MemoryStoreStats memory_store_stats = memory_store_->GetMemoryStoreStatisticalData();
  return memory_store_stats.num_local_objects_bytes;
}

void CoreWorker::HandleGetCoreWorkerStats(rpc::GetCoreWorkerStatsRequest request,
                                          rpc::GetCoreWorkerStatsReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  auto limit = request.has_limit() ? request.limit() : -1;
  auto stats = reply->mutable_core_worker_stats();
  // TODO(swang): Differentiate between tasks that are currently pending
  // execution and tasks that have finished but may be retried.
  stats->set_num_pending_tasks(task_manager_->NumSubmissibleTasks());
  stats->set_task_queue_length(task_queue_length_);
  stats->set_num_executed_tasks(num_executed_tasks_);
  stats->set_num_object_refs_in_scope(reference_counter_->NumObjectIDsInScope());
  stats->set_num_owned_objects(reference_counter_->NumObjectsOwnedByUs());
  stats->set_num_owned_actors(reference_counter_->NumActorsOwnedByUs());
  stats->set_ip_address(rpc_address_.ip_address());
  stats->set_port(rpc_address_.port());
  stats->set_pid(getpid());
  stats->set_language(options_.language);
  stats->set_job_id(worker_context_->GetCurrentJobID().Binary());
  stats->set_worker_id(worker_context_->GetWorkerID().Binary());
  stats->set_actor_id(actor_id_.Binary());
  stats->set_worker_type(worker_context_->GetWorkerType());
  stats->set_num_running_tasks(running_tasks_.size());
  stats->set_num_in_flight_arg_pinning_requests(num_get_pin_args_in_flight_);
  stats->set_num_of_failed_arg_pinning_requests(num_failed_get_pin_args_);
  auto *used_resources_map = stats->mutable_used_resources();
  for (auto const &[resource_name, resource_allocations] : resource_ids_) {
    rpc::ResourceAllocations allocations;
    for (auto const &[cur_resource_slot, cur_resource_alloc] : resource_allocations) {
      auto resource_slot = allocations.add_resource_slots();
      resource_slot->set_slot(cur_resource_slot);
      resource_slot->set_allocation(cur_resource_alloc);
    }
    (*used_resources_map)[resource_name] = allocations;
  }
  google::protobuf::Map<std::string, std::string> webui_map(webui_display_.begin(),
                                                            webui_display_.end());
  (*stats->mutable_webui_display()) = webui_map;

  MemoryStoreStats memory_store_stats = memory_store_->GetMemoryStoreStatisticalData();
  stats->set_num_in_plasma(memory_store_stats.num_in_plasma);
  stats->set_num_local_objects(memory_store_stats.num_local_objects);
  stats->set_used_object_store_memory(memory_store_stats.num_local_objects_bytes);

  if (request.include_memory_info()) {
    reference_counter_->AddObjectRefStats(
        plasma_store_provider_->UsedObjectsList(), stats, limit);
    task_manager_->AddTaskStatusInfo(stats);
  }

  if (request.include_task_info()) {
    task_manager_->FillTaskInfo(reply, limit);
    for (const auto &current_running_task : running_tasks_) {
      reply->add_running_task_ids(current_running_task.second.TaskIdBinary());
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleLocalGC(rpc::LocalGCRequest request,
                               rpc::LocalGCReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  if (options_.gc_collect != nullptr) {
    options_.gc_collect();
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("GC callback not defined"), nullptr, nullptr);
  }
}

void CoreWorker::HandleDeleteObjects(rpc::DeleteObjectsRequest request,
                                     rpc::DeleteObjectsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  for (const auto &obj_id : request.object_ids()) {
    object_ids.push_back(ObjectID::FromBinary(obj_id));
  }
  auto status = DeleteImpl(object_ids, request.local_only());
  send_reply_callback(status, nullptr, nullptr);
}

Status CoreWorker::DeleteImpl(const std::vector<ObjectID> &object_ids, bool local_only) {
  // Release the object from plasma. This does not affect the object's ref
  // count. If this was called from a non-owning worker, then a warning will be
  // logged and the object will not get released.
  reference_counter_->FreePlasmaObjects(object_ids);

  // Store an error in the in-memory store to indicate that the plasma value is
  // no longer reachable.
  memory_store_->Delete(object_ids);
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG).WithField(object_id) << "Freeing object";
    memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_FREED),
                       object_id,
                       reference_counter_->HasReference(object_id));
  }

  // We only delete from plasma, which avoids hangs (issue #7105). In-memory
  // objects can only be deleted once the ref count goes to 0.
  absl::flat_hash_set<ObjectID> plasma_object_ids(object_ids.begin(), object_ids.end());
  return plasma_store_provider_->Delete(plasma_object_ids, local_only);
}

void CoreWorker::HandleSpillObjects(rpc::SpillObjectsRequest request,
                                    rpc::SpillObjectsReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  if (options_.spill_objects != nullptr) {
    auto object_refs = VectorFromProtobuf<rpc::ObjectReference>(
        std::move(*request.mutable_object_refs_to_spill()));
    std::vector<std::string> object_urls = options_.spill_objects(object_refs);
    for (size_t i = 0; i < object_urls.size(); i++) {
      reply->add_spilled_objects_url(std::move(object_urls[i]));
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Spill objects callback not defined"), nullptr, nullptr);
  }
}

void CoreWorker::HandleRestoreSpilledObjects(rpc::RestoreSpilledObjectsRequest request,
                                             rpc::RestoreSpilledObjectsReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  if (options_.restore_spilled_objects != nullptr) {
    // Get a list of object ids.
    std::vector<rpc::ObjectReference> object_refs_to_restore;
    object_refs_to_restore.reserve(request.object_ids_to_restore_size());
    for (const auto &id_binary : request.object_ids_to_restore()) {
      rpc::ObjectReference ref;
      ref.set_object_id(id_binary);
      object_refs_to_restore.push_back(std::move(ref));
    }
    // Get a list of spilled_object_urls.
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    auto total =
        options_.restore_spilled_objects(object_refs_to_restore, spilled_objects_url);
    reply->set_bytes_restored_total(total);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Restore spilled objects callback not defined"),
        nullptr,
        nullptr);
  }
}

void CoreWorker::HandleDeleteSpilledObjects(rpc::DeleteSpilledObjectsRequest request,
                                            rpc::DeleteSpilledObjectsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  if (options_.delete_spilled_objects != nullptr) {
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    options_.delete_spilled_objects(spilled_objects_url,
                                    worker_context_->GetWorkerType());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Delete spilled objects callback not defined"),
        nullptr,
        nullptr);
  }
}

void CoreWorker::HandleExit(rpc::ExitRequest request,
                            rpc::ExitReply *reply,
                            rpc::SendReplyCallback send_reply_callback) {
  bool is_idle = IsIdle();
  bool force_exit = request.force_exit();
  RAY_LOG(DEBUG) << "Exiting: is_idle: " << is_idle << " force_exit: " << force_exit;
  if (!is_idle) {
    const size_t num_pending_tasks = task_manager_->NumPendingTasks();
    const int64_t pins_in_flight = local_raylet_rpc_client_->GetPinsInFlight();
    RAY_LOG_EVERY_MS(INFO, 60000)
        << "Worker is not idle: reference counter: " << reference_counter_->DebugString()
        << " # pins in flight: " << pins_in_flight
        << " # pending tasks: " << num_pending_tasks;
    if (force_exit) {
      RAY_LOG(INFO) << "Force exiting worker that's not idle. "
                    << "reference counter: " << reference_counter_->DebugString()
                    << " # Pins in flight: " << pins_in_flight
                    << " # pending tasks: " << num_pending_tasks;
    }
  }
  const bool will_exit = is_idle || force_exit;
  reply->set_success(will_exit);
  send_reply_callback(
      Status::OK(),
      [this, will_exit, force_exit]() {
        if (!will_exit) {
          return;
        }

        ShutdownReason reason;
        std::string detail;

        if (force_exit) {
          reason = ShutdownReason::kForcedExit;
          detail = "Worker force exited because its job has finished";
        } else {
          reason = ShutdownReason::kIdleTimeout;
          detail = "Worker exited because it was idle for a long time";
        }

        shutdown_coordinator_->RequestShutdown(force_exit, reason, detail);
      },
      // Fallback on RPC failure - still attempt shutdown
      [this]() {
        shutdown_coordinator_->RequestShutdown(
            /*force_shutdown=*/false,
            ShutdownReason::kIdleTimeout,
            "Worker exited due to RPC failure during idle exit");
      });
}

void CoreWorker::HandleAssignObjectOwner(rpc::AssignObjectOwnerRequest request,
                                         rpc::AssignObjectOwnerReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  SubscribeToNodeChanges();
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  const auto &borrower_address = request.borrower_address();
  const std::string &call_site = request.call_site();
  // Get a list of contained object ids.
  std::vector<ObjectID> contained_object_ids;
  contained_object_ids.reserve(request.contained_object_ids_size());
  for (const auto &id_binary : request.contained_object_ids()) {
    contained_object_ids.push_back(ObjectID::FromBinary(id_binary));
  }
  reference_counter_->AddOwnedObject(
      object_id,
      contained_object_ids,
      rpc_address_,
      call_site,
      request.object_size(),
      /*is_reconstructable=*/false,
      /*add_local_ref=*/false,
      /*pinned_at_node_id=*/NodeID::FromBinary(borrower_address.node_id()));
  reference_counter_->AddBorrowerAddress(object_id, borrower_address);
  memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                     object_id,
                     reference_counter_->HasReference(object_id));
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// Handle RPC for TaskManager::NumPendingTasks().
void CoreWorker::HandleNumPendingTasks(rpc::NumPendingTasksRequest request,
                                       rpc::NumPendingTasksReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Received NumPendingTasks request.";
  reply->set_num_pending_tasks(task_manager_->NumPendingTasks());
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::YieldCurrentFiber(FiberEvent &event) {
  RAY_CHECK(worker_context_->CurrentActorIsAsync());
  boost::this_fiber::yield();
  event.Wait();
}

void CoreWorker::GetAsync(const ObjectID &object_id,
                          SetResultCallback success_callback,
                          void *python_user_callback) {
  auto fallback_callback = std::bind(&CoreWorker::PlasmaCallback,
                                     this,
                                     success_callback,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3);

  memory_store_->GetAsync(
      object_id,
      // This callback is posted to io_service_ by memory store.
      [object_id,
       python_user_callback,
       success_callback = std::move(success_callback),
       fallback_callback =
           std::move(fallback_callback)](std::shared_ptr<RayObject> ray_object) {
        if (ray_object->IsInPlasmaError()) {
          fallback_callback(ray_object, object_id, python_user_callback);
        } else {
          success_callback(ray_object, object_id, python_user_callback);
        }
      });
}

void CoreWorker::PlasmaCallback(const SetResultCallback &success,
                                const std::shared_ptr<RayObject> &ray_object,
                                ObjectID object_id,
                                void *py_future) {
  RAY_CHECK(ray_object->IsInPlasmaError());

  // First check if the object is available in local plasma store.
  // Note that we are using Contains instead of Get so it won't trigger pull request
  // to remote nodes.
  bool object_is_local = false;
  if (Contains(object_id, &object_is_local).ok() && object_is_local) {
    std::vector<std::shared_ptr<RayObject>> vec;
    if (Get(std::vector<ObjectID>{object_id}, 0, vec).ok()) {
      RAY_CHECK(!vec.empty())
          << "Failed to get local object but Raylet notified object is local.";
      return success(vec.front(), object_id, py_future);
    }
  }

  // Object is not available locally. We now add the callback to listener queue.
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto plasma_arrived_callback = [this, success, object_id, py_future]() {
      // This callback is invoked on the io_service_ event loop, so it cannot call
      // blocking call like Get(). We used GetAsync here, which should immediate call
      // PlasmaCallback again with object available locally.
      GetAsync(object_id, success, py_future);
    };

    async_plasma_callbacks_[object_id].emplace_back(std::move(plasma_arrived_callback));
  }

  // Ask raylet to subscribe to object notification. Raylet will call this core worker
  // when the object is local (and it will fire the callback immediately if the object
  // exists). CoreWorker::HandlePlasmaObjectReady handles such request.
  auto owner_address = GetOwnerAddressOrDie(object_id);
  raylet_ipc_client_->SubscribePlasmaReady(object_id, owner_address);
}

void CoreWorker::HandlePlasmaObjectReady(rpc::PlasmaObjectReadyRequest request,
                                         rpc::PlasmaObjectReadyReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::function<void(void)>> callbacks;
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto it = async_plasma_callbacks_.extract(ObjectID::FromBinary(request.object_id()));
    callbacks = it.mapped();
  }
  for (const auto &callback : callbacks) {
    // This callback needs to be asynchronous because it runs on the io_service_, so no
    // RPCs can be processed while it's running. This can easily lead to deadlock (for
    // example if the callback calls ray.get() on an object that is dependent on an RPC
    // to be ready).
    callback();
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::SetActorId(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  if (!options_.is_local_mode) {
    RAY_CHECK(actor_id_.IsNil());
  }
  actor_id_ = actor_id;
}

void CoreWorker::SetWebuiDisplay(const std::string &key, const std::string &message) {
  absl::MutexLock lock(&mutex_);
  webui_display_[key] = message;
}

void CoreWorker::SetActorReprName(const std::string &repr_name) {
  RAY_CHECK(task_receiver_ != nullptr);
  task_receiver_->SetActorReprName(repr_name);

  absl::MutexLock lock(&mutex_);
  actor_repr_name_ = repr_name;
}

rpc::JobConfig CoreWorker::GetJobConfig() const {
  return worker_context_->GetCurrentJobConfig();
}

bool CoreWorker::IsExiting() const { return shutdown_coordinator_->ShouldEarlyExit(); }

bool CoreWorker::IsIdle(size_t num_objects_with_references,
                        int64_t pins_in_flight,
                        size_t num_pending_tasks) const {
  return (num_objects_with_references == 0) && (pins_in_flight == 0) &&
         (num_pending_tasks == 0);
}

bool CoreWorker::IsIdle() const {
  const size_t num_objects_with_references = reference_counter_->Size();
  const size_t num_pending_tasks = task_manager_->NumPendingTasks();
  const int64_t pins_in_flight = local_raylet_rpc_client_->GetPinsInFlight();
  return IsIdle(num_objects_with_references, pins_in_flight, num_pending_tasks);
}

Status CoreWorker::WaitForActorRegistered(const std::vector<ObjectID> &ids) {
  std::vector<ActorID> actor_ids;
  for (const auto &id : ids) {
    if (ObjectID::IsActorID(id)) {
      actor_ids.emplace_back(ObjectID::ToActorID(id));
    }
  }
  if (actor_ids.empty()) {
    return Status::OK();
  }
  std::promise<void> promise;
  auto future = promise.get_future();
  std::vector<Status> ret;
  int counter = 0;
  // Post to service pool to avoid mutex
  io_service_.post(
      [&, this]() {
        for (const auto &id : actor_ids) {
          if (actor_creator_->IsActorInRegistering(id)) {
            ++counter;
            actor_creator_->AsyncWaitForActorRegisterFinish(
                id, [&counter, &promise, &ret](const Status &status) {
                  ret.push_back(status);
                  --counter;
                  if (counter == 0) {
                    promise.set_value();
                  }
                });
          }
        }
        if (counter == 0) {
          promise.set_value();
        }
      },
      "CoreWorker.WaitForActorRegistered");
  future.wait();
  for (const auto &s : ret) {
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

std::vector<ObjectID> CoreWorker::GetCurrentReturnIds(int num_returns,
                                                      const ActorID &callee_actor_id) {
  std::vector<ObjectID> return_ids(num_returns);
  const auto next_task_index = worker_context_->GetTaskIndex() + 1;
  TaskID task_id;
  if (callee_actor_id.IsNil()) {
    /// Return ids for normal task call.
    task_id = TaskID::ForNormalTask(worker_context_->GetCurrentJobID(),
                                    worker_context_->GetCurrentInternalTaskId(),
                                    next_task_index);
  } else {
    /// Return ids for actor task call.
    task_id = TaskID::ForActorTask(worker_context_->GetCurrentJobID(),
                                   worker_context_->GetCurrentInternalTaskId(),
                                   next_task_index,
                                   callee_actor_id);
  }
  for (int i = 0; i < num_returns; i++) {
    return_ids[i] = ObjectID::FromIndex(task_id, i + 1);
  }
  return return_ids;
}

void CoreWorker::RecordTaskLogStart(const TaskID &task_id,
                                    int32_t attempt_number,
                                    const std::string &stdout_path,
                                    const std::string &stderr_path,
                                    int64_t stdout_start_offset,
                                    int64_t stderr_start_offset) const {
  if (options_.is_local_mode) {
    return;
  }
  rpc::TaskLogInfo task_log_info;
  task_log_info.set_stdout_file(stdout_path);
  task_log_info.set_stderr_file(stderr_path);
  task_log_info.set_stdout_start(stdout_start_offset);
  task_log_info.set_stderr_start(stderr_start_offset);

  auto current_task = worker_context_->GetCurrentTask();
  RAY_CHECK(current_task)
      << "We should have set the current task spec while executing the task.";
  RAY_UNUSED(task_event_buffer_->RecordTaskStatusEventIfNeeded(
      task_id,
      worker_context_->GetCurrentJobID(),
      attempt_number,
      *current_task,
      rpc::TaskStatus::NIL,
      /*include_task_info=*/false,
      worker::TaskStatusEvent::TaskStateUpdate(task_log_info)));
}

void CoreWorker::RecordTaskLogEnd(const TaskID &task_id,
                                  int32_t attempt_number,
                                  int64_t stdout_end_offset,
                                  int64_t stderr_end_offset) const {
  if (options_.is_local_mode) {
    return;
  }
  rpc::TaskLogInfo task_log_info;
  task_log_info.set_stdout_end(stdout_end_offset);
  task_log_info.set_stderr_end(stderr_end_offset);

  auto current_task = worker_context_->GetCurrentTask();
  RAY_CHECK(current_task)
      << "We should have set the current task spec before executing the task.";
  RAY_UNUSED(task_event_buffer_->RecordTaskStatusEventIfNeeded(
      task_id,
      worker_context_->GetCurrentJobID(),
      attempt_number,
      *current_task,
      rpc::TaskStatus::NIL,
      /*include_task_info=*/false,
      worker::TaskStatusEvent::TaskStateUpdate(task_log_info)));
}

void CoreWorker::UpdateTaskIsDebuggerPaused(const TaskID &task_id,
                                            const bool is_debugger_paused) {
  absl::MutexLock lock(&mutex_);
  auto running_task_it = running_tasks_.find(task_id);
  RAY_CHECK(running_task_it != running_tasks_.end())
      << "We should have set the running task spec before running the task.";
  RAY_LOG(DEBUG).WithField(running_task_it->second.TaskId())
      << "Task is paused by debugger set to " << is_debugger_paused;
  RAY_UNUSED(task_event_buffer_->RecordTaskStatusEventIfNeeded(
      task_id,
      worker_context_->GetCurrentJobID(),
      running_task_it->second.AttemptNumber(),
      running_task_it->second,
      rpc::TaskStatus::NIL,
      /*include_task_info=*/false,
      worker::TaskStatusEvent::TaskStateUpdate(is_debugger_paused)));
}

void CoreWorker::AsyncRetryTask(TaskSpecification &spec, uint32_t delay_ms) {
  spec.GetMutableMessage().set_attempt_number(spec.AttemptNumber() + 1);
  absl::MutexLock lock(&mutex_);
  TaskToRetry task_to_retry{current_time_ms() + delay_ms, spec};
  RAY_LOG(INFO) << "Will resubmit task after a " << delay_ms
                << "ms delay: " << spec.DebugString();
  to_resubmit_.push(std::move(task_to_retry));
}

}  // namespace ray::core
