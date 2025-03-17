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

#include "ray/raylet/node_manager.h"

#include <cctype>
#include <csignal>
#include <cstddef>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/buffer.h"
#include "ray/common/client_connection.h"
#include "ray/common/common_protocol.h"
#include "ray/common/constants.h"
#include "ray/common/memory_monitor.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/status.h"
#include "ray/common/task/task_common.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/pb_util.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/raylet/worker_killing_policy.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/cmd_line_utils.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include "ray/util/util.h"

namespace {

#define RAY_CHECK_ENUM(x, y) \
  static_assert(static_cast<int>(x) == static_cast<int>(y), "protocol mismatch")

struct ActorStats {
  int live_actors = 0;
  int dead_actors = 0;
  int restarting_actors = 0;
};

inline ray::rpc::ObjectReference FlatbufferToSingleObjectReference(
    const flatbuffers::String &object_id, const ray::protocol::Address &address) {
  ray::rpc::ObjectReference ref;
  ref.set_object_id(object_id.str());
  ref.mutable_owner_address()->set_raylet_id(address.raylet_id()->str());
  ref.mutable_owner_address()->set_ip_address(address.ip_address()->str());
  ref.mutable_owner_address()->set_port(address.port());
  ref.mutable_owner_address()->set_worker_id(address.worker_id()->str());
  return ref;
}

std::vector<ray::rpc::ObjectReference> FlatbufferToObjectReference(
    const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> &object_ids,
    const flatbuffers::Vector<flatbuffers::Offset<ray::protocol::Address>>
        &owner_addresses) {
  RAY_CHECK(object_ids.size() == owner_addresses.size());
  std::vector<ray::rpc::ObjectReference> refs;
  for (int64_t i = 0; i < object_ids.size(); i++) {
    ray::rpc::ObjectReference ref;
    ref.set_object_id(object_ids.Get(i)->str());
    const auto &addr = owner_addresses.Get(i);
    ref.mutable_owner_address()->set_raylet_id(addr->raylet_id()->str());
    ref.mutable_owner_address()->set_ip_address(addr->ip_address()->str());
    ref.mutable_owner_address()->set_port(addr->port());
    ref.mutable_owner_address()->set_worker_id(addr->worker_id()->str());
    refs.emplace_back(std::move(ref));
  }
  return refs;
}

}  // namespace

namespace ray::raylet {

void NodeManagerConfig::AddDefaultLabels(const std::string &self_node_id) {
  std::vector<std::string> default_keys = {kLabelKeyNodeID};

  for (const auto &key : default_keys) {
    RAY_CHECK(!labels.contains(key))
        << "The label key name " << key << " should never be set by the user.";
  }
  labels[kLabelKeyNodeID] = self_node_id;
}

NodeManager::NodeManager(
    instrumented_io_context &io_service,
    const NodeID &self_node_id,
    const std::string &self_node_name,
    const NodeManagerConfig &config,
    const ObjectManagerConfig &object_manager_config,
    std::shared_ptr<gcs::GcsClient> gcs_client,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully)
    : self_node_id_(self_node_id),
      self_node_name_(self_node_name),
      io_service_(io_service),
      gcs_client_(gcs_client),
      shutdown_raylet_gracefully_(shutdown_raylet_gracefully),
      worker_pool_(
          io_service,
          self_node_id_,
          config.node_manager_address,
          [this, config]() {
            // Callback to determine the maximum number of idle workers to keep
            // around.
            if (config.num_workers_soft_limit >= 0) {
              return config.num_workers_soft_limit;
            }
            // If no limit is provided, use the available number of CPUs,
            // assuming that each incoming task will likely require 1 CPU.
            // We floor the available CPUs to the nearest integer to avoid starting too
            // many workers when there is less than 1 CPU left. Otherwise, we could end
            // up repeatedly starting the worker, then killing it because it idles for
            // too long. The downside is that we will be slower to schedule tasks that
            // could use a fraction of a CPU.
            return static_cast<int64_t>(
                cluster_resource_scheduler_->GetLocalResourceManager()
                    .GetLocalAvailableCpus());
          },
          config.num_prestart_python_workers,
          config.maximum_startup_concurrency,
          config.min_worker_port,
          config.max_worker_port,
          config.worker_ports,
          gcs_client_,
          config.worker_commands,
          config.native_library_path,
          /*starting_worker_timeout_callback=*/
          [this] { cluster_task_manager_->ScheduleAndDispatchTasks(); },
          config.ray_debugger_external,
          /*get_time=*/[]() { return absl::Now(); }),
      client_call_manager_(io_service),
      worker_rpc_pool_([this](const rpc::Address &addr) {
        return std::make_shared<rpc::CoreWorkerClient>(addr, client_call_manager_, []() {
          RAY_LOG(FATAL) << "Raylet doesn't call any retryable core worker grpc methods.";
        });
      }),
      core_worker_subscriber_(std::make_unique<pubsub::Subscriber>(
          self_node_id_,
          /*channels=*/
          std::vector<rpc::ChannelType>{
              rpc::ChannelType::WORKER_OBJECT_EVICTION,
              rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
              rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
          RayConfig::instance().max_command_batch_size(),
          /*get_client=*/
          [this](const rpc::Address &address) {
            return worker_rpc_pool_.GetOrConnect(address);
          },
          &io_service_)),
      object_directory_(std::make_unique<OwnershipBasedObjectDirectory>(
          io_service_,
          gcs_client_,
          core_worker_subscriber_.get(),
          /*owner_client_pool=*/&worker_rpc_pool_,
          /*max_object_report_batch_size=*/
          RayConfig::instance().max_object_report_batch_size(),
          [this](const ObjectID &obj_id, const ErrorType &error_type) {
            rpc::ObjectReference ref;
            ref.set_object_id(obj_id.Binary());
            MarkObjectsAsFailed(error_type, {ref}, JobID::Nil());
          })),
      object_manager_(
          io_service,
          self_node_id,
          object_manager_config,
          object_directory_.get(),
          [this](const ObjectID &object_id,
                 int64_t object_size,
                 const std::string &object_url,
                 std::function<void(const ray::Status &)> callback) {
            GetLocalObjectManager().AsyncRestoreSpilledObject(
                object_id, object_size, object_url, callback);
          },
          /*get_spilled_object_url=*/
          [this](const ObjectID &object_id) {
            return GetLocalObjectManager().GetLocalSpilledObjectURL(object_id);
          },
          /*spill_objects_callback=*/
          [this]() {
            // This callback is called from the plasma store thread.
            // NOTE: It means the local object manager should be thread-safe.
            io_service_.post(
                [this]() { GetLocalObjectManager().SpillObjectUptoMaxThroughput(); },
                "NodeManager.SpillObjects");
            return GetLocalObjectManager().IsSpillingInProgress();
          },
          /*object_store_full_callback=*/
          [this]() {
            // Post on the node manager's event loop since this
            // callback is called from the plasma store thread.
            // This will help keep node manager lock-less.
            io_service_.post([this]() { TriggerGlobalGC(); }, "NodeManager.GlobalGC");
          },
          /*add_object_callback=*/
          [this](const ObjectInfo &object_info) { HandleObjectLocal(object_info); },
          /*delete_object_callback=*/
          [this](const ObjectID &object_id) { HandleObjectMissing(object_id); },
          /*pin_object=*/
          [this](const ObjectID &object_id) {
            std::vector<ObjectID> object_ids = {object_id};
            std::vector<std::unique_ptr<RayObject>> results;
            std::unique_ptr<RayObject> result;
            if (GetObjectsFromPlasma(object_ids, &results) && results.size() > 0) {
              result = std::move(results[0]);
            }
            return result;
          },
          /*fail_pull_request=*/
          [this](const ObjectID &object_id, rpc::ErrorType error_type) {
            rpc::ObjectReference ref;
            ref.set_object_id(object_id.Binary());
            MarkObjectsAsFailed(error_type, {ref}, JobID::Nil());
          }),
      store_client_(std::make_unique<plasma::PlasmaClient>()),
      periodical_runner_(PeriodicalRunner::Create(io_service)),
      report_resources_period_ms_(config.report_resources_period_ms),
      temp_dir_(config.temp_dir),
      initial_config_(config),
      dependency_manager_(object_manager_),
      wait_manager_(/*is_object_local*/
                    [this](const ObjectID &object_id) {
                      return dependency_manager_.CheckObjectLocal(object_id);
                    },
                    /*delay_executor*/
                    [this](std::function<void()> fn, int64_t delay_ms) {
                      RAY_UNUSED(execute_after(
                          io_service_, fn, std::chrono::milliseconds(delay_ms)));
                    }),
      node_manager_server_("NodeManager",
                           config.node_manager_port,
                           config.node_manager_address == "127.0.0.1"),
      node_manager_service_(io_service, *this),
      local_object_manager_(
          self_node_id_,
          config.node_manager_address,
          config.node_manager_port,
          io_service_,
          RayConfig::instance().free_objects_batch_size(),
          RayConfig::instance().free_objects_period_milliseconds(),
          worker_pool_,
          worker_rpc_pool_,
          /*max_io_workers*/ config.max_io_workers,
          /*min_spilling_size*/ config.min_spilling_size,
          /*is_external_storage_type_fs*/
          RayConfig::instance().is_external_storage_type_fs(),
          /*max_fused_object_count*/ RayConfig::instance().max_fused_object_count(),
          /*on_objects_freed*/
          [this](const std::vector<ObjectID> &object_ids) {
            object_manager_.FreeObjects(object_ids,
                                        /*local_only=*/false);
          },
          /*is_plasma_object_spillable*/
          [this](const ObjectID &object_id) {
            return object_manager_.IsPlasmaObjectSpillable(object_id);
          },
          /*core_worker_subscriber_=*/core_worker_subscriber_.get(),
          object_directory_.get()),
      high_plasma_storage_usage_(RayConfig::instance().high_plasma_storage_usage()),
      local_gc_run_time_ns_(absl::GetCurrentTimeNanos()),
      local_gc_throttler_(RayConfig::instance().local_gc_min_interval_s() * 1e9),
      global_gc_throttler_(RayConfig::instance().global_gc_min_interval_s() * 1e9),
      local_gc_interval_ns_(RayConfig::instance().local_gc_interval_s() * 1e9),
      record_metrics_period_ms_(config.record_metrics_period_ms),
      next_resource_seq_no_(0),
      ray_syncer_(io_service_, self_node_id_.Binary()),
      ray_syncer_service_(ray_syncer_),
      worker_killing_policy_(
          CreateWorkerKillingPolicy(RayConfig::instance().worker_killing_policy())),
      memory_monitor_(std::make_unique<MemoryMonitor>(
          io_service,
          RayConfig::instance().memory_usage_threshold(),
          RayConfig::instance().min_memory_free_bytes(),
          RayConfig::instance().memory_monitor_refresh_ms(),
          CreateMemoryUsageRefreshCallback())) {
  RAY_LOG(INFO).WithField(kLogKeyNodeID, self_node_id_) << "Initializing NodeManager";
  cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
      io_service,
      scheduling::NodeID(self_node_id_.Binary()),
      config.resource_config.GetResourceMap(),
      /*is_node_available_fn*/
      [this](scheduling::NodeID node_id) {
        return gcs_client_->Nodes().Get(NodeID::FromBinary(node_id.Binary())) != nullptr;
      },
      /*get_used_object_store_memory*/
      [this]() {
        if (RayConfig::instance().scheduler_report_pinned_bytes_only()) {
          // Get the current bytes used by local primary object copies.  This
          // is used to help node scale down decisions. A node can only be
          // safely drained when this function reports zero.
          int64_t bytes_used = local_object_manager_.GetPrimaryBytes();
          // Report nonzero if we have objects spilled to the local filesystem.
          if (bytes_used == 0 && local_object_manager_.HasLocallySpilledObjects()) {
            bytes_used = 1;
          }
          return bytes_used;
        }
        return object_manager_.GetUsedMemory();
      },
      /*get_pull_manager_at_capacity*/
      [this]() { return object_manager_.PullManagerHasPullsQueued(); },
      shutdown_raylet_gracefully,
      /*labels*/
      config.labels);

  auto get_node_info_func = [this](const NodeID &node_id) {
    return gcs_client_->Nodes().Get(node_id);
  };
  auto announce_infeasible_task = [this](const RayTask &task) {
    PublishInfeasibleTaskError(task);
  };
  RAY_CHECK(RayConfig::instance().max_task_args_memory_fraction() > 0 &&
            RayConfig::instance().max_task_args_memory_fraction() <= 1)
      << "max_task_args_memory_fraction must be a nonzero fraction.";
  auto max_task_args_memory =
      static_cast<int64_t>(static_cast<float>(object_manager_.GetMemoryCapacity()) *
                           RayConfig::instance().max_task_args_memory_fraction());
  if (max_task_args_memory <= 0) {
    RAY_LOG(WARNING)
        << "Max task args should be a fraction of the object store capacity, but object "
           "store capacity is zero or negative. Allowing task args to use 100% of the "
           "local object store. This can cause ObjectStoreFullErrors if the tasks' "
           "return values are greater than the remaining capacity.";
    max_task_args_memory = 0;
  }
  local_task_manager_ = std::make_unique<LocalTaskManager>(
      self_node_id_,
      *std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_),
      dependency_manager_,
      [this](const WorkerID &owner_worker_id, const NodeID &owner_node_id) {
        return !this->IsWorkerDead(owner_worker_id, owner_node_id);
      },
      get_node_info_func,
      worker_pool_,
      leased_workers_,
      [this](const std::vector<ObjectID> &object_ids,
             std::vector<std::unique_ptr<RayObject>> *results) {
        return GetObjectsFromPlasma(object_ids, results);
      },
      max_task_args_memory);
  cluster_task_manager_ = std::make_shared<ClusterTaskManager>(
      self_node_id_,
      *std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_),
      get_node_info_func,
      announce_infeasible_task,
      *local_task_manager_);
  placement_group_resource_manager_ = std::make_shared<NewPlacementGroupResourceManager>(
      std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_));

  periodical_runner_->RunFnPeriodically(
      [this]() { cluster_task_manager_->ScheduleAndDispatchTasks(); },
      RayConfig::instance().worker_cap_initial_backoff_delay_ms(),
      "NodeManager.ScheduleAndDispatchTasks");

  periodical_runner_->RunFnPeriodically(
      [this]() { CheckForUnexpectedWorkerDisconnects(); },
      RayConfig::instance().raylet_check_for_unexpected_worker_disconnect_interval_ms(),
      "NodeManager.CheckForUnexpectedWorkerDisconnects");

  RAY_CHECK_OK(store_client_->Connect(config.store_socket_name));
  // Run the node manager rpc server.
  node_manager_server_.RegisterService(node_manager_service_, false);
  node_manager_server_.RegisterService(ray_syncer_service_);
  node_manager_server_.Run();
  // GCS will check the health of the service named with the node id.
  // Fail to setup this will lead to the health check failure.
  node_manager_server_.GetServer().GetHealthCheckService()->SetServingStatus(
      self_node_id_.Hex(), true);
  worker_pool_.SetNodeManagerPort(GetServerPort());

  dashboard_agent_manager_ = CreateDashboardAgentManager(self_node_id, config);
  runtime_env_agent_manager_ = CreateRuntimeEnvAgentManager(self_node_id, config);

  auto runtime_env_agent_client = RuntimeEnvAgentClient::Create(
      io_service_,
      config.node_manager_address,
      config.runtime_env_agent_port, /*delay_executor=*/
      [this](std::function<void()> task, uint32_t delay_ms) {
        return execute_after(
            io_service_, std::move(task), std::chrono::milliseconds(delay_ms));
      },
      shutdown_raylet_gracefully_);

  worker_pool_.SetRuntimeEnvAgentClient(std::move(runtime_env_agent_client));
  worker_pool_.Start();
  periodical_runner_->RunFnPeriodically([this]() { GCTaskFailureReason(); },
                                        RayConfig::instance().task_failure_entry_ttl_ms(),
                                        "NodeManager.GCTaskFailureReason");

  mutable_object_provider_ = std::make_unique<core::experimental::MutableObjectProvider>(
      *store_client_, absl::bind_front(&NodeManager::CreateRayletClient, this), nullptr);
}

std::shared_ptr<raylet::RayletClient> NodeManager::CreateRayletClient(
    const NodeID &node_id, rpc::ClientCallManager &client_call_manager) {
  const rpc::GcsNodeInfo *node_info = gcs_client_->Nodes().Get(node_id);
  RAY_CHECK(node_info) << "No GCS info for node " << node_id;
  std::shared_ptr<ray::rpc::NodeManagerWorkerClient> grpc_client =
      rpc::NodeManagerWorkerClient::make(node_info->node_manager_address(),
                                         node_info->node_manager_port(),
                                         client_call_manager);
  return std::make_shared<raylet::RayletClient>(std::move(grpc_client));
};

bool NodeManager::IsWorkerDead(const WorkerID &worker_id, const NodeID &node_id) const {
  return failed_workers_cache_.count(worker_id) > 0 ||
         failed_nodes_cache_.count(node_id) > 0;
}

ray::Status NodeManager::RegisterGcs() {
  auto on_node_change = [this](const NodeID &node_id, const GcsNodeInfo &data) {
    if (data.state() == GcsNodeInfo::ALIVE) {
      NodeAdded(data);
    } else {
      RAY_CHECK(data.state() == GcsNodeInfo::DEAD);
      NodeRemoved(node_id);
    }
  };

  // If the node resource message is received first and then the node message is received,
  // ForwardTask will throw exception, because it can't get node info.
  auto on_node_change_subscribe_done = [this](Status status) {
    RAY_CHECK_OK(status);

    // Register resource manager and scheduler
    ray_syncer_.Register(
        /* message_type */ syncer::MessageType::RESOURCE_VIEW,
        /* reporter */ &cluster_resource_scheduler_->GetLocalResourceManager(),
        /* receiver */ this,
        /* pull_from_reporter_interval_ms */
        RayConfig::instance().raylet_report_resources_period_milliseconds());

    // Register a commands channel.
    // It's only used for GC right now.
    ray_syncer_.Register(
        /* message_type */ syncer::MessageType::COMMANDS,
        /* reporter */ this,
        /* receiver */ this,
        /* pull_from_reporter_interval_ms */ 0);

    auto gcs_channel = gcs_client_->GetGcsRpcClient().GetChannel();
    ray_syncer_.Connect(kGCSNodeID.Binary(), gcs_channel);
    periodical_runner_->RunFnPeriodically(
        [this] {
          auto triggered_by_global_gc = TryLocalGC();
          // If plasma store is under high pressure, we should try to schedule a global
          // gc.
          if (triggered_by_global_gc) {
            ray_syncer_.OnDemandBroadcasting(syncer::MessageType::COMMANDS);
          }
        },
        RayConfig::instance().raylet_check_gc_period_milliseconds(),
        "NodeManager.CheckGC");
  };
  // Register a callback to monitor new nodes and a callback to monitor removed nodes.
  RAY_RETURN_NOT_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(
      on_node_change, on_node_change_subscribe_done));

  // Subscribe to all unexpected failure notifications from the local and
  // remote raylets. Note that this does not include workers that failed due to
  // node failure. These workers can be identified by comparing the raylet_id
  // in their rpc::Address to the ID of a failed raylet.
  const auto &worker_failure_handler =
      [this](const rpc::WorkerDeltaData &worker_failure_data) {
        HandleUnexpectedWorkerFailure(worker_failure_data);
      };
  RAY_CHECK_OK(gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
      worker_failure_handler, nullptr));

  // Subscribe to job updates.
  const auto job_subscribe_handler = [this](const JobID &job_id,
                                            const JobTableData &job_data) {
    // HandleJobStarted is idempotent so it's ok to call it again when the job
    // finishes. We always need to call `HandleJobStarted` even when a job has
    // finished, because we may have missed the started event (for example,
    // because the node wasn't up when the job started). JobStarted +
    // JobFinished events both need to be processed because we need to persist
    // the job config of dead jobs in order for detached actors to function
    // properly.
    HandleJobStarted(job_id, job_data);
    if (job_data.is_dead()) {
      HandleJobFinished(job_id, job_data);
    }
  };
  RAY_RETURN_NOT_OK(
      gcs_client_->Jobs().AsyncSubscribeAll(job_subscribe_handler, nullptr));

  periodical_runner_->RunFnPeriodically(
      [this] {
        DumpDebugState();
        WarnResourceDeadlock();
      },
      RayConfig::instance().debug_dump_period_milliseconds(),
      "NodeManager.deadline_timer.debug_state_dump");
  uint64_t now_ms = current_time_ms();
  last_metrics_recorded_at_ms_ = now_ms;
  periodical_runner_->RunFnPeriodically([this] { RecordMetrics(); },
                                        record_metrics_period_ms_,
                                        "NodeManager.deadline_timer.record_metrics");
  if (RayConfig::instance().free_objects_period_milliseconds() > 0) {
    periodical_runner_->RunFnPeriodically(
        [this] { local_object_manager_.FlushFreeObjects(); },
        RayConfig::instance().free_objects_period_milliseconds(),
        "NodeManager.deadline_timer.flush_free_objects");
    periodical_runner_->RunFnPeriodically(
        [this] { SpillIfOverPrimaryObjectsThreshold(); },
        RayConfig::instance().free_objects_period_milliseconds(),
        "NodeManager.deadline_timer.spill_objects_when_over_threshold");
  }
  /// If periodic asio stats print is enabled, it will print it.
  const auto event_stats_print_interval_ms =
      RayConfig::instance().event_stats_print_interval_ms();
  if (event_stats_print_interval_ms != -1 && RayConfig::instance().event_stats()) {
    periodical_runner_->RunFnPeriodically(
        [this] {
          std::stringstream debug_msg;
          debug_msg << DebugString() << "\n\n";
          RAY_LOG(INFO) << AppendToEachLine(debug_msg.str(), "[state-dump] ");
          ReportWorkerOOMKillStats();
        },
        event_stats_print_interval_ms,
        "NodeManager.deadline_timer.print_event_loop_stats");
  }
  // Raylet periodically check whether it's alive in GCS.
  // For failure cases, GCS might think this raylet dead, but this
  // raylet still think it's alive. This could happen when the cluster setup is wrong,
  // for example, there is data loss in the DB.
  periodical_runner_->RunFnPeriodically(
      [this] {
        // Flag to see whether a request is running.
        static bool checking = false;
        if (checking) {
          return;
        }
        checking = true;
        RAY_CHECK_OK(gcs_client_->Nodes().AsyncCheckSelfAlive(
            // capture checking ptr here because vs17 fail to compile
            [this, checking_ptr = &checking](auto status, auto alive) mutable {
              if ((status.ok() && !alive)) {
                // GCS think this raylet is dead. Fail the node
                RAY_LOG(FATAL)
                    << "GCS consider this node to be dead. This may happen when "
                    << "GCS is not backed by a DB and restarted or there is data loss "
                    << "in the DB.";
              } else if (status.IsAuthError()) {
                RAY_LOG(FATAL)
                    << "GCS returned an authentication error. This may happen when "
                    << "GCS is not backed by a DB and restarted or there is data loss "
                    << "in the DB. Local cluster ID: " << gcs_client_->GetClusterId();
              }
              *checking_ptr = false;
            },
            /* timeout_ms = */ 30000));
      },
      RayConfig::instance().raylet_liveness_self_check_interval_ms(),
      "NodeManager.GcsCheckAlive");
  return ray::Status::OK();
}

void NodeManager::KillWorker(std::shared_ptr<WorkerInterface> worker, bool force) {
  if (force) {
    worker->GetProcess().Kill();
    return;
  }
#ifdef _WIN32
// TODO(mehrdadn): implement graceful process termination mechanism
#else
  // If we're just cleaning up a single worker, allow it some time to clean
  // up its state before force killing. The client socket will be closed
  // and the worker struct will be freed after the timeout.
  kill(worker->GetProcess().GetId(), SIGTERM);
#endif

  auto retry_timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
  auto retry_duration = boost::posix_time::milliseconds(
      RayConfig::instance().kill_worker_timeout_milliseconds());
  retry_timer->expires_from_now(retry_duration);
  retry_timer->async_wait([retry_timer, worker](const boost::system::error_code &error) {
    RAY_LOG(DEBUG) << "Send SIGKILL to worker, pid=" << worker->GetProcess().GetId();
    // Force kill worker
    worker->GetProcess().Kill();
  });
}

void NodeManager::DestroyWorker(std::shared_ptr<WorkerInterface> worker,
                                rpc::WorkerExitType disconnect_type,
                                const std::string &disconnect_detail,
                                bool force) {
  // We should disconnect the client first. Otherwise, we'll remove bundle resources
  // before actual resources are returned. Subsequent disconnect request that comes
  // due to worker dead will be ignored.
  DisconnectClient(
      worker->Connection(), /*graceful=*/false, disconnect_type, disconnect_detail);
  worker->MarkDead();
  KillWorker(worker, force);
  if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR) {
    number_workers_killed_++;
  } else if (disconnect_type == rpc::WorkerExitType::NODE_OUT_OF_MEMORY) {
    number_workers_killed_by_oom_++;
  }
}

void NodeManager::HandleJobStarted(const JobID &job_id, const JobTableData &job_data) {
  RAY_LOG(DEBUG).WithField(job_id)
      << "HandleJobStarted Driver pid " << job_data.driver_pid()
      << " is dead: " << job_data.is_dead()
      << " driver address: " << job_data.driver_address().ip_address();
  worker_pool_.HandleJobStarted(job_id, job_data.config());
  // Tasks of this job may already arrived but failed to pop a worker because the job
  // config is not local yet. So we trigger dispatching again here to try to
  // reschedule these tasks.
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleJobFinished(const JobID &job_id, const JobTableData &job_data) {
  RAY_LOG(DEBUG).WithField(job_id) << "HandleJobFinished";
  RAY_CHECK(job_data.is_dead());
  // Force kill all the worker processes belonging to the finished job
  // so that no worker processes is leaked.
  for (const auto &pair : leased_workers_) {
    auto &worker = pair.second;
    RAY_CHECK(!worker->GetAssignedJobId().IsNil());
    if (worker->GetRootDetachedActorId().IsNil() &&
        (worker->GetAssignedJobId() == job_id)) {
      // Don't kill worker processes belonging to the detached actor
      // since those are expected to outlive the job.
      RAY_LOG(INFO).WithField(worker->WorkerId())
          << "The leased worker "
          << " is killed because the job " << job_id << " finished.";
      rpc::ExitRequest request;
      request.set_force_exit(true);
      worker->rpc_client()->Exit(
          request, [this, worker](const ray::Status &status, const rpc::ExitReply &r) {
            if (!status.ok()) {
              RAY_LOG(WARNING).WithField(worker->WorkerId())
                  << "Failed to send exit request to worker "
                  << ": " << status.ToString() << ". Killing it using SIGKILL instead.";
              // Just kill-9 as a last resort.
              KillWorker(worker, /* force */ true);
            }
          });
    }
  }
  worker_pool_.HandleJobFinished(job_id);
}

// TODO(edoakes): the connection management and logic to destroy a worker should live
// inside of the WorkerPool. We also need to unify the destruction paths between
// DestroyWorker, DisconnectWorker, and KillWorker.
void NodeManager::CheckForUnexpectedWorkerDisconnects() {
  std::vector<std::shared_ptr<ClientConnection>> all_connections;
  std::vector<std::shared_ptr<WorkerInterface>> all_workers =
      worker_pool_.GetAllRegisteredWorkers();
  all_connections.reserve(all_workers.size());
  for (const auto &worker : all_workers) {
    all_connections.push_back(worker->Connection());
  }
  for (const auto &driver : worker_pool_.GetAllRegisteredDrivers()) {
    all_workers.push_back(driver);
    all_connections.push_back(driver->Connection());
  }

  RAY_CHECK_EQ(all_connections.size(), all_workers.size());

  // Check if there are any unexpected disconnects on the worker socket connections.
  // This will close the connection without processing remaining messages.
  std::vector<bool> disconnects = CheckForClientDisconnects(all_connections);
  for (size_t i = 0; i < disconnects.size(); i++) {
    if (disconnects[i]) {
      std::string msg = "Worker connection closed unexpectedly.";
      RAY_LOG(DEBUG).WithField(all_workers[i]->WorkerId()) << msg;
      DestroyWorker(all_workers[i], rpc::WorkerExitType::SYSTEM_ERROR, msg);
    }
  }
}

void NodeManager::DoLocalGC(bool triggered_by_global_gc) {
  auto all_workers = worker_pool_.GetAllRegisteredWorkers();
  for (const auto &driver : worker_pool_.GetAllRegisteredDrivers()) {
    all_workers.push_back(driver);
  }
  RAY_LOG(INFO) << "Sending Python GC request to " << all_workers.size()
                << " local workers to clean up Python cyclic references.";
  for (const auto &worker : all_workers) {
    rpc::LocalGCRequest request;
    request.set_triggered_by_global_gc(triggered_by_global_gc);
    worker->rpc_client()->LocalGC(
        request, [](const ray::Status &status, const rpc::LocalGCReply &r) {
          if (!status.ok()) {
            RAY_LOG(DEBUG) << "Failed to send local GC request: " << status.ToString();
          }
        });
  }
  local_gc_run_time_ns_ = absl::GetCurrentTimeNanos();
}

void NodeManager::HandleReleaseUnusedBundles(rpc::ReleaseUnusedBundlesRequest request,
                                             rpc::ReleaseUnusedBundlesReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Releasing unused bundles.";
  std::unordered_set<BundleID, pair_hash> in_use_bundles;
  for (int index = 0; index < request.bundles_in_use_size(); ++index) {
    const auto &bundle_id = request.bundles_in_use(index).bundle_id();
    in_use_bundles.emplace(PlacementGroupID::FromBinary(bundle_id.placement_group_id()),
                           bundle_id.bundle_index());
    // Add -1 one to the in_use_bundles. It's ok to add it more than one times since it's
    // a set.
    in_use_bundles.emplace(PlacementGroupID::FromBinary(bundle_id.placement_group_id()),
                           -1);
  }

  // Cancel lease requests that are waiting for workers
  // to free the acquired pg bundle resources
  // so that pg bundle can be returned.
  local_task_manager_->CancelTasks(
      [&](const std::shared_ptr<internal::Work> &work) {
        const auto bundle_id = work->task.GetTaskSpecification().PlacementGroupBundleId();
        return !bundle_id.first.IsNil() && (0 == in_use_bundles.count(bundle_id)) &&
               (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER);
      },
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      "The lease request is cancelled because it uses placement group bundles that are "
      "not "
      "registered to GCS. It can happen upon GCS restart.");

  // Kill all workers that are currently associated with the unused bundles.
  // NOTE: We can't traverse directly with `leased_workers_`, because `DestroyWorker` will
  // delete the element of `leased_workers_`. So we need to filter out
  // `workers_associated_with_unused_bundles` separately.
  std::vector<std::shared_ptr<WorkerInterface>> workers_associated_with_unused_bundles;
  for (const auto &worker_it : leased_workers_) {
    auto &worker = worker_it.second;
    const auto &bundle_id = worker->GetBundleId();
    // We need to filter out the workers used by placement group.
    if (!bundle_id.first.IsNil() && 0 == in_use_bundles.count(bundle_id)) {
      workers_associated_with_unused_bundles.emplace_back(worker);
    }
  }

  for (const auto &worker : workers_associated_with_unused_bundles) {
    RAY_LOG(DEBUG)
            .WithField(worker->GetBundleId().first)
            .WithField(worker->GetAssignedTaskId())
            .WithField(worker->GetActorId())
            .WithField(worker->WorkerId())
        << "Destroying worker since its bundle was unused, bundle index: "
        << worker->GetBundleId().second;
    DestroyWorker(worker,
                  rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                  "Worker exits because it uses placement group bundles that are not "
                  "registered to GCS. It can happen upon GCS restart.");
  }

  // Return unused bundle resources.
  placement_group_resource_manager_->ReturnUnusedBundle(in_use_bundles);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleGetObjectsInfo(rpc::GetObjectsInfoRequest request,
                                       rpc::GetObjectsInfoReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Received a HandleGetObjectsInfo request";
  auto total = std::make_shared<int>(0);
  auto count = std::make_shared<int>(0);
  auto limit = request.has_limit() ? request.limit() : -1;

  // Each worker query will have limit as well.
  // At the end there will be limit * num_workers entries returned at max.
  QueryAllWorkerStates(
      /*on_replied*/
      [reply, total, count, limit](const ray::Status &status,
                                   const rpc::GetCoreWorkerStatsReply &r) {
        *total += r.core_worker_stats().objects_total();
        if (limit != -1 && *count >= limit) {
          return;
        }
        // Currently, instead of counting object one by one, we add all object refs
        // returned. This means there can be overflow. TODO(sang): Fix it after
        // refactoring this code path.
        *count += r.core_worker_stats().object_refs_size();
        if (status.ok()) {
          reply->add_core_workers_stats()->MergeFrom(r.core_worker_stats());
        } else {
          RAY_LOG(INFO) << "Failed to query object information from a worker.";
        }
      },
      send_reply_callback,
      /*include_memory_info*/ true,
      /*include_task_info*/ false,
      /*limit*/ limit,
      /*on_all_replied*/ [total, reply]() { reply->set_total(*total); });
}

void NodeManager::HandleGetTaskFailureCause(rpc::GetTaskFailureCauseRequest request,
                                            rpc::GetTaskFailureCauseReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  const TaskID task_id = TaskID::FromBinary(request.task_id());
  RAY_LOG(DEBUG) << "Received a HandleGetTaskFailureCause request for task " << task_id;

  auto it = task_failure_reasons_.find(task_id);
  if (it != task_failure_reasons_.end()) {
    RAY_LOG(DEBUG) << "task " << task_id << " has failure reason "
                   << ray::gcs::RayErrorInfoToString(it->second.ray_error_info)
                   << ", fail immediately: " << !it->second.should_retry;
    reply->mutable_failure_cause()->CopyFrom(it->second.ray_error_info);
    reply->set_fail_task_immediately(!it->second.should_retry);
  } else {
    RAY_LOG(INFO) << "didn't find failure cause for task " << task_id;
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleRegisterMutableObject(
    rpc::RegisterMutableObjectRequest request,
    rpc::RegisterMutableObjectReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ObjectID writer_object_id = ObjectID::FromBinary(request.writer_object_id());
  int64_t num_readers = request.num_readers();
  ObjectID reader_object_id = ObjectID::FromBinary(request.reader_object_id());

  mutable_object_provider_->HandleRegisterMutableObject(
      writer_object_id, num_readers, reader_object_id);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandlePushMutableObject(rpc::PushMutableObjectRequest request,
                                          rpc::PushMutableObjectReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  mutable_object_provider_->HandlePushMutableObject(request, reply);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::QueryAllWorkerStates(
    const std::function<void(const ray::Status &, const rpc::GetCoreWorkerStatsReply &)>
        &on_replied,
    rpc::SendReplyCallback &send_reply_callback,
    bool include_memory_info,
    bool include_task_info,
    int64_t limit,
    const std::function<void()> &on_all_replied) {
  auto all_workers = worker_pool_.GetAllRegisteredWorkers(/* filter_dead_worker */ true,
                                                          /*filter_io_workers*/ true);
  for (auto driver :
       worker_pool_.GetAllRegisteredDrivers(/* filter_dead_driver */ true)) {
    all_workers.push_back(driver);
  }

  if (all_workers.empty()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  // Sort workers for the consistent ordering.
  auto sort_func = [](std::shared_ptr<WorkerInterface> worker_a,
                      std::shared_ptr<WorkerInterface> worker_b) {
    // Prioritize drivers over workers. It is because drivers usually have data users care
    // more. Note the enum values Driver == 1, Worker == 0.
    return (worker_a->GetWorkerType() > worker_b->GetWorkerType())
           // If the worker type is the same, order it based on pid (just for consistent
           // ordering).
           || ((worker_a->GetWorkerType() == worker_b->GetWorkerType()) &&
               (worker_a->GetProcess().GetId() < worker_b->GetProcess().GetId()));
  };
  std::sort(all_workers.begin(), all_workers.end(), sort_func);

  // Query all workers.
  auto rpc_replied = std::make_shared<size_t>(0);
  auto num_workers = all_workers.size();
  bool all_dead = true;
  for (const auto &worker : all_workers) {
    if (worker->IsDead()) {
      *rpc_replied += 1;
      continue;
    }
    all_dead = false;
    rpc::GetCoreWorkerStatsRequest request;
    request.set_intended_worker_id(worker->WorkerId().Binary());
    request.set_include_memory_info(include_memory_info);
    request.set_include_task_info(include_task_info);
    request.set_limit(limit);
    // TODO(sang): Add timeout to the RPC call.
    worker->rpc_client()->GetCoreWorkerStats(
        request,
        [num_workers,
         rpc_replied,
         send_reply_callback,
         on_replied = std::move(on_replied),
         on_all_replied](const ray::Status &status,
                         const rpc::GetCoreWorkerStatsReply &r) {
          *rpc_replied += 1;
          on_replied(status, r);
          if (*rpc_replied == num_workers) {
            if (on_all_replied) {
              on_all_replied();
            }
            send_reply_callback(Status::OK(), nullptr, nullptr);
          }
        });
  }
  if (all_dead) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
}

// This warns users that there could be the resource deadlock. It works this way;
// - If there's no available workers for scheduling
// - But if there are still pending tasks waiting for resource acquisition
// It means the cluster might not have enough resources to be in progress.
// Note that this can print the false negative messages
// e.g., there are many actors taking up resources for a long time.
void NodeManager::WarnResourceDeadlock() {
  ray::RayTask exemplar;
  bool any_pending = false;
  int pending_actor_creations = 0;
  int pending_tasks = 0;
  std::string available_resources;

  // Check if any progress is being made on this raylet.
  for (const auto &worker : worker_pool_.GetAllRegisteredWorkers()) {
    if (worker->IsAvailableForScheduling()) {
      // Progress is being made in a task, don't warn.
      resource_deadlock_warned_ = 0;
      return;
    }
  }

  // Check if any tasks are blocked on resource acquisition.
  if (!cluster_task_manager_->AnyPendingTasksForResourceAcquisition(
          &exemplar, &any_pending, &pending_actor_creations, &pending_tasks)) {
    // No pending tasks, no need to warn.
    resource_deadlock_warned_ = 0;
    return;
  }

  // Push an warning to the driver that a task is blocked trying to acquire resources.
  // To avoid spurious triggers, only take action starting with the second time.
  // case resource_deadlock_warned_:  0 => first time, don't do anything yet
  // case resource_deadlock_warned_:  1 => second time, print a warning
  // case resource_deadlock_warned_: >1 => global gc but don't print any warnings
  if (any_pending && resource_deadlock_warned_++ > 0) {
    // Actor references may be caught in cycles, preventing them from being deleted.
    // Trigger global GC to hopefully free up resource slots.
    TriggerGlobalGC();

    // Suppress duplicates warning messages.
    if (resource_deadlock_warned_ > 2) {
      return;
    }

    std::ostringstream error_message;
    error_message
        << "The actor or task with ID " << exemplar.GetTaskSpecification().TaskId()
        << " cannot be scheduled right now. You can ignore this message if this "
        << "Ray cluster is expected to auto-scale or if you specified a "
        << "runtime_env for this actor or task, which may take time to install.  "
        << "Otherwise, this is likely due to all cluster resources being claimed "
        << "by actors. To resolve the issue, consider creating fewer actors or "
        << "increasing the resources available to this Ray cluster.\n"
        << "Required resources for this actor or task: "
        << exemplar.GetTaskSpecification().GetRequiredPlacementResources().DebugString()
        << "\n"
        << "Available resources on this node: "
        << cluster_resource_scheduler_->GetClusterResourceManager()
               .GetNodeResourceViewString(scheduling::NodeID(self_node_id_.Binary()))
        << " In total there are " << pending_tasks << " pending tasks and "
        << pending_actor_creations << " pending actors on this node.";

    std::string error_message_str = error_message.str();
    RAY_LOG(WARNING) << error_message_str;
    RAY_LOG_EVERY_MS(WARNING, 10 * 1000) << cluster_task_manager_->DebugStr();
  }
  // Try scheduling tasks. Without this, if there's no more tasks coming in, deadlocked
  // tasks are never be scheduled.
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::NodeAdded(const GcsNodeInfo &node_info) {
  const NodeID node_id = NodeID::FromBinary(node_info.node_id());

  RAY_LOG(DEBUG).WithField(node_id) << "[NodeAdded] Received callback from node id ";
  if (node_id == self_node_id_) {
    return;
  }

  // Store address of the new node manager for rpc requests.
  remote_node_manager_addresses_[node_id] =
      std::make_pair(node_info.node_manager_address(), node_info.node_manager_port());

  // Set node labels when node added.
  absl::flat_hash_map<std::string, std::string> labels(node_info.labels().begin(),
                                                       node_info.labels().end());
  cluster_resource_scheduler_->GetClusterResourceManager().SetNodeLabels(
      scheduling::NodeID(node_id.Binary()), labels);

  // TODO: Always use the message from ray syncer.
  ResourceRequest resources;
  for (auto &resource_entry : node_info.resources_total()) {
    resources.Set(scheduling::ResourceID(resource_entry.first),
                  FixedPoint(resource_entry.second));
  }
  if (ResourceCreateUpdated(node_id, resources)) {
    cluster_task_manager_->ScheduleAndDispatchTasks();
  }
  // Update the resource view if a new message has been sent.
  if (auto sync_msg = ray_syncer_.GetSyncMessage(node_id.Binary(),
                                                 syncer::MessageType::RESOURCE_VIEW)) {
    if (sync_msg) {
      ConsumeSyncMessage(sync_msg);
    }
  }
}

void NodeManager::NodeRemoved(const NodeID &node_id) {
  // TODO(swang): If we receive a notification for our own death, clean up and
  // exit immediately.
  RAY_LOG(DEBUG).WithField(node_id) << "[NodeRemoved] Received callback from node id ";

  if (node_id == self_node_id_) {
    if (!is_shutdown_request_received_) {
      std::ostringstream error_message;
      error_message
          << "[Timeout] Exiting because this node manager has mistakenly been marked as "
             "dead by the "
          << "GCS: GCS failed to check the health of this node for "
          << RayConfig::instance().health_check_failure_threshold() << " times."
          << " This is likely because the machine or raylet has become overloaded.";
      RAY_EVENT(FATAL, "RAYLET_MARKED_DEAD").WithField("node_id", self_node_id_.Hex())
          << error_message.str();
      RAY_LOG(FATAL) << error_message.str();
    } else {
      // No-op since this node already starts to be drained, and GCS already knows about
      // it.
      RAY_LOG(INFO).WithField(node_id)
          << "Node is marked as dead by GCS because the node is drained.";
      return;
    }
  }

  // Below, when we remove node_id from all of these data structures, we could
  // check that it is actually removed, or log a warning otherwise, but that may
  // not be necessary.
  // Remove the node from the resource map.
  if (!cluster_resource_scheduler_->GetClusterResourceManager().RemoveNode(
          scheduling::NodeID(node_id.Binary()))) {
    RAY_LOG(DEBUG).WithField(node_id)
        << "Received NodeRemoved callback for an unknown node.";
    return;
  }

  // Remove the node manager address.
  const auto node_entry = remote_node_manager_addresses_.find(node_id);
  if (node_entry != remote_node_manager_addresses_.end()) {
    remote_node_manager_addresses_.erase(node_entry);
  }

  // Notify the object directory that the node has been removed so that it
  // can remove it from any cached locations.
  object_directory_->HandleNodeRemoved(node_id);

  // Clean up workers that were owned by processes that were on the failed
  // node.
  rpc::WorkerDeltaData data;
  data.set_raylet_id(node_id.Binary());
  HandleUnexpectedWorkerFailure(data);
}

void NodeManager::HandleUnexpectedWorkerFailure(const rpc::WorkerDeltaData &data) {
  const WorkerID worker_id = WorkerID::FromBinary(data.worker_id());
  const NodeID node_id = NodeID::FromBinary(data.raylet_id());
  if (!worker_id.IsNil()) {
    RAY_LOG(DEBUG).WithField(worker_id) << "Worker failed";
    failed_workers_cache_.insert(worker_id);
  } else {
    RAY_CHECK(!node_id.IsNil());
    failed_nodes_cache_.insert(node_id);
  }

  cluster_task_manager_->CancelAllTaskOwnedBy(worker_id);

  for (const auto &pair : leased_workers_) {
    auto &worker = pair.second;
    const auto owner_worker_id =
        WorkerID::FromBinary(worker->GetOwnerAddress().worker_id());
    const auto owner_node_id = NodeID::FromBinary(worker->GetOwnerAddress().raylet_id());
    RAY_LOG(DEBUG).WithField(worker->WorkerId())
        << "Lease worker owned by " << owner_worker_id;
    RAY_CHECK(!owner_worker_id.IsNil() && !owner_node_id.IsNil());
    if (!worker->IsDetachedActor()) {
      if (!worker_id.IsNil()) {
        // If the failed worker was a leased worker's owner, then kill the leased worker.
        if (owner_worker_id == worker_id) {
          std::ostringstream stream;
          stream << "The leased worker " << worker->WorkerId()
                 << " is killed because the owner process " << owner_worker_id
                 << " died.";
          const auto &err_msg = stream.str();
          RAY_LOG(INFO) << err_msg;
          KillWorker(worker);
        }
      } else if (owner_node_id == node_id) {
        // If the leased worker's owner was on the failed node, then kill the leased
        // worker.
        std::ostringstream stream;
        stream << "The leased worker " << worker->WorkerId()
               << " is killed because the owner node " << owner_node_id << " died.";
        const auto &err_msg = stream.str();
        RAY_LOG(INFO) << err_msg;
        KillWorker(worker);
      }
    }
  }
}

bool NodeManager::ResourceCreateUpdated(const NodeID &node_id,
                                        const ResourceRequest &createUpdatedResources) {
  RAY_LOG(DEBUG).WithField(node_id)
      << "[ResourceCreateUpdated] received callback from node with created or updated "
         "resources: "
      << createUpdatedResources.DebugString()
      << ". Updating resource map. skip=" << (node_id == self_node_id_);

  // Skip updating local node since local node always has the latest information.
  // Updating local node could result in a inconsistence view in cluster resource
  // scheduler which could make task hang.
  if (node_id == self_node_id_) {
    return false;
  }

  for (const auto &resource_id : createUpdatedResources.ResourceIds()) {
    cluster_resource_scheduler_->GetClusterResourceManager().UpdateResourceCapacity(
        scheduling::NodeID(node_id.Binary()),
        resource_id,
        createUpdatedResources.Get(resource_id).Double());
  }
  RAY_LOG(DEBUG) << "[ResourceCreateUpdated] Updated cluster_resource_map.";
  return true;
}

bool NodeManager::ResourceDeleted(const NodeID &node_id,
                                  const std::vector<std::string> &resource_names) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    std::ostringstream oss;
    for (auto &resource_name : resource_names) {
      oss << resource_name << ", ";
    }
    RAY_LOG(DEBUG).WithField(node_id)
        << "[ResourceDeleted] received callback from node with deleted resources: "
        << oss.str() << ". Updating resource map. skip=" << (node_id == self_node_id_);
  }

  // Skip updating local node since local node always has the latest information.
  // Updating local node could result in a inconsistence view in cluster resource
  // scheduler which could make task hang.
  if (node_id == self_node_id_) {
    return false;
  }

  std::vector<scheduling::ResourceID> resource_ids;
  for (const auto &resource_label : resource_names) {
    resource_ids.emplace_back(scheduling::ResourceID(resource_label));
  }
  cluster_resource_scheduler_->GetClusterResourceManager().DeleteResources(
      scheduling::NodeID(node_id.Binary()), resource_ids);
  return true;
}

void NodeManager::HandleNotifyGCSRestart(rpc::NotifyGCSRestartRequest request,
                                         rpc::NotifyGCSRestartReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  // When GCS restarts, it'll notify raylet to do some initialization work
  // (resubscribing). Raylet will also notify all workers to do this job. Workers are
  // registered to raylet first (blocking call) and then connect to GCS, so there is no
  // race condition here.
  gcs_client_->AsyncResubscribe();
  auto workers = worker_pool_.GetAllRegisteredWorkers(/* filter_dead_worker */ true);
  for (auto worker : workers) {
    worker->AsyncNotifyGCSRestart();
  }
  auto drivers = worker_pool_.GetAllRegisteredDrivers(/* filter_dead_drivers */ true);
  for (auto driver : drivers) {
    driver->AsyncNotifyGCSRestart();
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

bool NodeManager::UpdateResourceUsage(
    const NodeID &node_id,
    const syncer::ResourceViewSyncMessage &resource_view_sync_message) {
  if (!cluster_resource_scheduler_->GetClusterResourceManager().UpdateNode(
          scheduling::NodeID(node_id.Binary()), resource_view_sync_message)) {
    RAY_LOG(INFO).WithField(node_id)
        << "[UpdateResourceUsage]: received resource usage from unknown node.";
    return false;
  }

  return true;
}

void NodeManager::HandleClientConnectionError(std::shared_ptr<ClientConnection> client,
                                              const boost::system::error_code &error) {
  const std::string err_msg = absl::StrCat(
      "Worker unexpectedly exits with a connection error code ",
      error.value(),
      ". ",
      error.message(),
      ". There are some potential root causes. (1) The process is killed by "
      "SIGKILL by OOM killer due to high memory usage. (2) ray stop --force is "
      "called. (3) The worker is crashed unexpectedly due to SIGSEGV or other "
      "unexpected errors.");

  // Disconnect the client and don't process more messages.
  DisconnectClient(
      client, /*graceful=*/false, ray::rpc::WorkerExitType::SYSTEM_ERROR, err_msg);
}

void NodeManager::ProcessClientMessage(const std::shared_ptr<ClientConnection> &client,
                                       int64_t message_type,
                                       const uint8_t *message_data) {
  auto registered_worker = worker_pool_.GetRegisteredWorker(client);
  auto message_type_value = static_cast<protocol::MessageType>(message_type);
  RAY_LOG(DEBUG) << "[Worker] Message "
                 << protocol::EnumNameMessageType(message_type_value) << "("
                 << message_type << ") from worker with PID "
                 << (registered_worker
                         ? std::to_string(registered_worker->GetProcess().GetId())
                         : "nil");

  if (registered_worker && registered_worker->IsDead()) {
    // For a worker that is marked as dead (because the job has died already),
    // all the messages are ignored except DisconnectClient.
    if (message_type_value != protocol::MessageType::DisconnectClientRequest) {
      // Listen for more messages.
      client->ProcessMessages();
      return;
    }
  }

  switch (message_type_value) {
  case protocol::MessageType::RegisterClientRequest: {
    ProcessRegisterClientRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::AnnounceWorkerPort: {
    ProcessAnnounceWorkerPortMessage(client, message_data);
  } break;
  case protocol::MessageType::RegisterWorkerWithPortRequest: {
    ProcessRegisterClientAndAnnouncePortMessage(client, message_data);
  } break;
  case protocol::MessageType::ActorCreationTaskDone: {
    if (registered_worker) {
      // Worker may send this message after it was disconnected.
      HandleWorkerAvailable(registered_worker);
    }
  } break;
  case protocol::MessageType::DisconnectClientRequest: {
    ProcessDisconnectClientMessage(client, message_data);
    // We don't need to receive future messages from this client,
    // because it's already disconnected.
    return;
  } break;
  case protocol::MessageType::FetchOrReconstruct: {
    ProcessFetchOrReconstructMessage(client, message_data);
  } break;
  case protocol::MessageType::NotifyDirectCallTaskBlocked: {
    HandleDirectCallTaskBlocked(registered_worker);
  } break;
  case protocol::MessageType::NotifyDirectCallTaskUnblocked: {
    HandleDirectCallTaskUnblocked(registered_worker);
  } break;
  case protocol::MessageType::NotifyUnblocked: {
    // TODO(ekl) this is still used from core worker even in direct call mode to
    // finish up get requests.
    auto message = flatbuffers::GetRoot<protocol::NotifyUnblocked>(message_data);
    AsyncResolveObjectsFinish(client, from_flatbuf<TaskID>(*message->task_id()));
  } break;
  case protocol::MessageType::WaitRequest: {
    ProcessWaitRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::WaitForDirectActorCallArgsRequest: {
    ProcessWaitForDirectActorCallArgsRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::PushErrorRequest: {
    ProcessPushErrorRequestMessage(message_data);
  } break;
  case protocol::MessageType::FreeObjectsInObjectStoreRequest: {
    auto message = flatbuffers::GetRoot<protocol::FreeObjectsRequest>(message_data);
    std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
    // Clean up objects from the object store.
    object_manager_.FreeObjects(object_ids, message->local_only());
  } break;
  case protocol::MessageType::SubscribePlasmaReady: {
    ProcessSubscribePlasmaReady(client, message_data);
  } break;
  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  // Listen for more messages.
  client->ProcessMessages();
}

void NodeManager::ProcessRegisterClientRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto *message = flatbuffers::GetRoot<protocol::RegisterClientRequest>(message_data);
  RAY_UNUSED(
      ProcessRegisterClientRequestMessageImpl(client, message, /*port=*/std::nullopt));
}

Status NodeManager::ProcessRegisterClientRequestMessageImpl(
    const std::shared_ptr<ClientConnection> &client,
    const ray::protocol::RegisterClientRequest *message,
    std::optional<int> port) {
  client->Register();

  Language language = static_cast<Language>(message->language());
  const JobID job_id = from_flatbuf<JobID>(*message->job_id());
  const int runtime_env_hash = static_cast<int>(message->runtime_env_hash());
  WorkerID worker_id = from_flatbuf<WorkerID>(*message->worker_id());
  pid_t pid = message->worker_pid();
  StartupToken worker_startup_token = message->startup_token();
  std::string worker_ip_address = string_from_flatbuf(*message->ip_address());
  // TODO(suquark): Use `WorkerType` in `common.proto` without type converting.
  rpc::WorkerType worker_type = static_cast<rpc::WorkerType>(message->worker_type());
  if (worker_type == rpc::WorkerType::DRIVER) {
    RAY_CHECK(!job_id.IsNil());
  } else if (worker_type == rpc::WorkerType::SPILL_WORKER ||
             worker_type == rpc::WorkerType::RESTORE_WORKER) {
    RAY_CHECK(job_id.IsNil());
  }

  auto worker = std::static_pointer_cast<WorkerInterface>(
      std::make_shared<Worker>(job_id,
                               runtime_env_hash,
                               worker_id,
                               language,
                               worker_type,
                               worker_ip_address,
                               client,
                               client_call_manager_,
                               worker_startup_token));

  std::function<void(Status, int)> send_reply_callback;
  if (port.has_value()) {
    worker->SetAssignedPort(*port);
  } else {
    send_reply_callback = [this, client](Status status, int assigned_port) {
      flatbuffers::FlatBufferBuilder fbb;
      auto reply =
          ray::protocol::CreateRegisterClientReply(fbb,
                                                   status.ok(),
                                                   fbb.CreateString(status.ToString()),
                                                   to_flatbuf(fbb, self_node_id_),
                                                   assigned_port);
      fbb.Finish(reply);
      client->WriteMessageAsync(
          static_cast<int64_t>(protocol::MessageType::RegisterClientReply),
          fbb.GetSize(),
          fbb.GetBufferPointer(),
          [this, client](const ray::Status &status) {
            if (!status.ok()) {
              DisconnectClient(client,
                               /*graceful=*/false,
                               rpc::WorkerExitType::SYSTEM_ERROR,
                               "Worker is failed because the raylet couldn't reply the "
                               "registration request: " +
                                   status.ToString());
            }
          });
    };
  }

  if (worker_type == rpc::WorkerType::WORKER ||
      worker_type == rpc::WorkerType::SPILL_WORKER ||
      worker_type == rpc::WorkerType::RESTORE_WORKER) {
    return RegisterForNewWorker(
        worker, pid, worker_startup_token, std::move(send_reply_callback));
  }
  return RegisterForNewDriver(
      worker, pid, job_id, message, std::move(send_reply_callback));
}

Status NodeManager::RegisterForNewWorker(
    std::shared_ptr<WorkerInterface> worker,
    pid_t pid,
    const StartupToken &worker_startup_token,
    std::function<void(Status, int)> send_reply_callback) {
  Status status = Status::OK();
  if (send_reply_callback) {
    status = worker_pool_.RegisterWorker(
        worker, pid, worker_startup_token, send_reply_callback);
  } else {
    status = worker_pool_.RegisterWorker(worker, pid, worker_startup_token);
  }

  if (!status.ok()) {
    // If the worker failed to register to Raylet, trigger task dispatching here to
    // allow new worker processes to be started (if capped by
    // maximum_startup_concurrency).
    cluster_task_manager_->ScheduleAndDispatchTasks();
  }
  return status;
}

Status NodeManager::RegisterForNewDriver(
    std::shared_ptr<WorkerInterface> worker,
    pid_t pid,
    const JobID &job_id,
    const ray::protocol::RegisterClientRequest *message,
    std::function<void(Status, int)> send_reply_callback) {
  RAY_CHECK_GE(pid, 0);
  RAY_CHECK(send_reply_callback);

  worker->SetProcess(Process::FromPid(pid));
  // Compute a dummy driver task id from a given driver.
  // The task id set in the worker here should be consistent with the task
  // id set in the core worker.
  const TaskID driver_task_id = TaskID::ForDriverTask(job_id);
  worker->AssignTaskId(driver_task_id);
  rpc::JobConfig job_config;
  job_config.ParseFromString(message->serialized_job_config()->str());

  return worker_pool_.RegisterDriver(worker, job_config, send_reply_callback);
}

void NodeManager::ProcessAnnounceWorkerPortMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto *message = flatbuffers::GetRoot<protocol::AnnounceWorkerPort>(message_data);
  ProcessAnnounceWorkerPortMessageImpl(client, message);
}

void NodeManager::ProcessAnnounceWorkerPortMessageImpl(
    const std::shared_ptr<ClientConnection> &client,
    const ray::protocol::AnnounceWorkerPort *message) {
  bool is_worker = true;
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (worker == nullptr) {
    is_worker = false;
    worker = worker_pool_.GetRegisteredDriver(client);
  }
  RAY_CHECK(worker != nullptr) << "No worker exists for CoreWorker with client: "
                               << client->DebugString();

  int port = message->port();
  worker->Connect(port);
  if (is_worker) {
    worker_pool_.OnWorkerStarted(worker);
    HandleWorkerAvailable(worker);
  } else {
    // Driver is ready. Add the job to GCS.
    JobID job_id = worker->GetAssignedJobId();
    boost::optional<const rpc::JobConfig &> job_config =
        worker_pool_.GetJobConfig(job_id);
    RAY_CHECK(job_config.has_value());

    rpc::Address driver_address;
    // Assume raylet ID is the same as the node ID.
    driver_address.set_raylet_id(self_node_id_.Binary());
    driver_address.set_ip_address(worker->IpAddress());
    driver_address.set_port(port);
    driver_address.set_worker_id(worker->WorkerId().Binary());
    auto job_data_ptr =
        gcs::CreateJobTableData(job_id,
                                /*is_dead=*/false,
                                driver_address,
                                worker->GetProcess().GetId(),
                                string_from_flatbuf(*message->entrypoint()),
                                *job_config);

    RAY_CHECK_OK(
        gcs_client_->Jobs().AsyncAdd(job_data_ptr, [this, client](Status status) {
          SendPortAnnouncementResponse(client, std::move(status));
        }));
  }
}

void NodeManager::SendPortAnnouncementResponse(
    const std::shared_ptr<ClientConnection> &client, Status status) {
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to add job to GCS: " << status.ToString();
  }
  // Write the reply back.
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateAnnounceWorkerPortReply(
      fbb, status.ok(), fbb.CreateString(status.ToString()));
  fbb.Finish(message);

  client->WriteMessageAsync(
      static_cast<int64_t>(protocol::MessageType::AnnounceWorkerPortReply),
      fbb.GetSize(),
      fbb.GetBufferPointer(),
      [this, client](const ray::Status &status) {
        if (!status.ok()) {
          DisconnectClient(
              client,
              /*graceful=*/false,
              rpc::WorkerExitType::SYSTEM_ERROR,
              "Failed to send AnnounceWorkerPortReply to client: " + status.ToString());
        }
      });
}

void NodeManager::ProcessRegisterClientAndAnnouncePortMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto *message =
      flatbuffers::GetRoot<protocol::RegisterWorkerWithPortRequest>(message_data);
  const ray::protocol::RegisterClientRequest *register_client_request =
      message->request_client_request();
  auto status = ProcessRegisterClientRequestMessageImpl(
      client, register_client_request, register_client_request->port());
  if (!status.ok()) {
    SendRegisterClientAndAnnouncePortResponse(client, std::move(status));
    return;
  }
  ProcessAnnounceWorkerPortMessageImpl(client, message->announcement_port_request());

  // TODO(hjiang): In the next PR, `ProcessAnnounceWorkerPortMessageImpl` should split
  // into two parts, one for worker, another for driver.
  SendRegisterClientAndAnnouncePortResponse(client, Status::OK());
}

void NodeManager::SendRegisterClientAndAnnouncePortResponse(
    const std::shared_ptr<ClientConnection> &client, Status status) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateRegisterWorkerWithPortReply(
      fbb, status.ok(), fbb.CreateString(status.ToString()));
  fbb.Finish(message);

  client->WriteMessageAsync(
      static_cast<int64_t>(protocol::MessageType::RegisterWorkerWithPortReply),
      fbb.GetSize(),
      fbb.GetBufferPointer(),
      [this, client](const ray::Status &status) {
        if (!status.ok()) {
          DisconnectClient(client,
                           /*graceful=*/false,
                           rpc::WorkerExitType::SYSTEM_ERROR,
                           "Failed to send RegisterWorkerWithPortReply to client: " +
                               status.ToString());
        }
      });
}

void NodeManager::HandleWorkerAvailable(const std::shared_ptr<WorkerInterface> &worker) {
  RAY_CHECK(worker);

  if (worker->GetWorkerType() == rpc::WorkerType::SPILL_WORKER) {
    // Return the worker to the idle pool.
    worker_pool_.PushSpillWorker(worker);
    return;
  }

  if (worker->GetWorkerType() == rpc::WorkerType::RESTORE_WORKER) {
    // Return the worker to the idle pool.
    worker_pool_.PushRestoreWorker(worker);
    return;
  }

  bool worker_idle = true;

  // If the worker was assigned a task, mark it as finished.
  if (!worker->GetAssignedTaskId().IsNil()) {
    worker_idle = FinishAssignedTask(worker);
  }

  if (worker_idle) {
    // Return the worker to the idle pool.
    worker_pool_.PushWorker(worker);
  }

  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void SendDisconnectClientReply(const WorkerID &worker_id,
                               const std::shared_ptr<ClientConnection> &client) {
  flatbuffers::FlatBufferBuilder fbb;
  auto reply = protocol::CreateDisconnectClientReply(fbb);
  fbb.Finish(reply);

  // NOTE(edoakes): it's important to use sync WriteMessage here to ensure the message
  // is written to the socket before it's closed.
  const auto status = client->WriteMessage(
      static_cast<int64_t>(protocol::MessageType::DisconnectClientReply),
      fbb.GetSize(),
      fbb.GetBufferPointer());
  if (!status.ok()) {
    RAY_LOG(WARNING).WithField(worker_id)
        << "Failed to send disconnect reply to worker: " << status.ToString();
  }
}

void NodeManager::DisconnectClient(const std::shared_ptr<ClientConnection> &client,
                                   bool graceful,
                                   rpc::WorkerExitType disconnect_type,
                                   const std::string &disconnect_detail,
                                   const rpc::RayException *creation_task_exception) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  bool is_worker = false, is_driver = false;
  if (worker) {
    // The client is a worker.
    is_worker = true;
  } else {
    worker = worker_pool_.GetRegisteredDriver(client);
    if (worker) {
      // The client is a driver.
      is_driver = true;
    } else {
      RAY_LOG(INFO)
          << "Not disconnecting client disconnect it has already been disconnected.";
      return;
    }
  }

  RAY_LOG(INFO).WithField(worker->WorkerId())
      << "Disconnecting client, graceful=" << std::boolalpha << graceful
      << ", disconnect_type=" << disconnect_type
      << ", has_creation_task_exception=" << std::boolalpha
      << bool(creation_task_exception != nullptr);

  RAY_CHECK(worker != nullptr);
  RAY_CHECK(!(is_worker && is_driver));
  // Clean up any open ray.get or ray.wait calls that the worker made.
  dependency_manager_.CancelGetRequest(worker->WorkerId());
  dependency_manager_.CancelWaitRequest(worker->WorkerId());

  // Erase any lease metadata.
  ReleaseWorker(worker->WorkerId());

  if (creation_task_exception != nullptr) {
    RAY_LOG(INFO).WithField(worker->WorkerId())
        << "Formatted creation task exception: "
        << creation_task_exception->formatted_exception_string();
  }
  // Publish the worker failure.
  auto worker_failure_data_ptr =
      gcs::CreateWorkerFailureData(worker->WorkerId(),
                                   self_node_id_,
                                   initial_config_.node_manager_address,
                                   time(nullptr),
                                   disconnect_type,
                                   disconnect_detail,
                                   worker->GetProcess().GetId(),
                                   creation_task_exception);
  RAY_CHECK_OK(
      gcs_client_->Workers().AsyncReportWorkerFailure(worker_failure_data_ptr, nullptr));

  if (is_worker) {
    const ActorID &actor_id = worker->GetActorId();
    const TaskID &task_id = worker->GetAssignedTaskId();
    // If the worker was running a task or actor, clean up the task and push an
    // error to the driver, unless the worker is already dead.
    if ((!task_id.IsNil() || !actor_id.IsNil()) && !worker->IsDead()) {
      // If the worker was an actor, it'll be cleaned by GCS.
      if (actor_id.IsNil()) {
        // Return the resources that were being used by this worker.
        RayTask task;
        local_task_manager_->TaskFinished(worker, &task);
      }

      if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR) {
        // Push the error to driver.
        const JobID &job_id = worker->GetAssignedJobId();
        // TODO(rkn): Define this constant somewhere else.
        std::string type = "worker_died";
        std::ostringstream error_message;
        error_message << "A worker died or was killed while executing a task by an "
                         "unexpected system "
                         "error. To troubleshoot the problem, check the logs for the "
                         "dead worker."
                      << " RayTask ID: " << task_id
                      << " Worker ID: " << worker->WorkerId()
                      << " Node ID: " << self_node_id_
                      << " Worker IP address: " << worker->IpAddress()
                      << " Worker port: " << worker->Port()
                      << " Worker PID: " << worker->GetProcess().GetId()
                      << " Worker exit type: "
                      << rpc::WorkerExitType_Name(disconnect_type)
                      << " Worker exit detail: " << disconnect_detail;
        std::string error_message_str = error_message.str();
        RAY_EVENT(ERROR, EL_RAY_WORKER_FAILURE)
                .WithField("worker_id", worker->WorkerId().Hex())
                .WithField("node_id", self_node_id_.Hex())
                .WithField("job_id", worker->GetAssignedJobId().Hex())
            << error_message_str;
        auto error_data_ptr = gcs::CreateErrorTableData(
            type, error_message_str, absl::FromUnixMillis(current_time_ms()), job_id);
        RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
      }
    }

    // Remove the dead client from the pool and stop listening for messages.
    worker_pool_.DisconnectWorker(worker, disconnect_type);

    // Return the resources that were being used by this worker.
    local_task_manager_->ReleaseWorkerResources(worker);

    // Since some resources may have been released, we can try to dispatch more tasks.
    cluster_task_manager_->ScheduleAndDispatchTasks();
  } else if (is_driver) {
    // The client is a driver.
    const auto job_id = worker->GetAssignedJobId();
    RAY_CHECK(!job_id.IsNil());
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFinished(job_id, nullptr));
    worker_pool_.DisconnectDriver(worker);

    RAY_LOG(INFO).WithField(worker->WorkerId()).WithField(worker->GetAssignedJobId())
        << "Driver (pid=" << worker->GetProcess().GetId() << ") is disconnected.";
    if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR) {
      RAY_EVENT(ERROR, EL_RAY_DRIVER_FAILURE)
              .WithField("node_id", self_node_id_.Hex())
              .WithField("job_id", worker->GetAssignedJobId().Hex())
          << "Driver " << worker->WorkerId() << " died. Address: " << worker->IpAddress()
          << ":" << worker->Port() << ", Pid: " << worker->GetProcess().GetId()
          << ", JobId: " << worker->GetAssignedJobId();
    }
  }

  local_task_manager_->ClearWorkerBacklog(worker->WorkerId());
  cluster_task_manager_->CancelAllTaskOwnedBy(worker->WorkerId());

  if (graceful) {
    // Graceful disconnects are initiated by a request from the worker and
    // it blocks waiting for this reply.
    SendDisconnectClientReply(worker->WorkerId(), client);
  }
  client->Close();

  // TODO(rkn): Tell the object manager that this client has disconnected so
  // that it can clean up the wait requests for this client. Currently I think
  // these can be leaked.
}

void NodeManager::ProcessDisconnectClientMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::DisconnectClientRequest>(message_data);
  auto disconnect_type = static_cast<rpc::WorkerExitType>(message->disconnect_type());
  const auto &disconnect_detail = message->disconnect_detail()->str();
  const flatbuffers::Vector<uint8_t> *exception_pb =
      message->creation_task_exception_pb();

  std::unique_ptr<rpc::RayException> creation_task_exception = nullptr;
  if (exception_pb != nullptr) {
    creation_task_exception = std::make_unique<rpc::RayException>();
    creation_task_exception->ParseFromString(std::string(
        reinterpret_cast<const char *>(exception_pb->data()), exception_pb->size()));
  }
  DisconnectClient(client,
                   /*graceful=*/true,
                   disconnect_type,
                   disconnect_detail,
                   creation_task_exception.get());
}

void NodeManager::ProcessFetchOrReconstructMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::FetchOrReconstruct>(message_data);
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());
  // TODO(ekl) we should be able to remove the fetch only flag along with the legacy
  // non-direct call support.
  if (message->fetch_only()) {
    std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
    if (!worker) {
      worker = worker_pool_.GetRegisteredDriver(client);
    }
    // Fetch requests can get re-ordered after the worker finishes, so make sure to
    // check the worker is still assigned a task to avoid leaks.
    if (worker && !worker->GetAssignedTaskId().IsNil()) {
      // This will start a fetch for the objects that gets canceled once the
      // objects are local, or if the worker dies.
      dependency_manager_.StartOrUpdateGetRequest(worker->WorkerId(), refs);
    }
  } else {
    // The values are needed. Add all requested objects to the list to
    // subscribe to in the task dependency manager. These objects will be
    // pulled from remote node managers. If an object's owner dies, an error
    // will be stored as the object's value.
    const TaskID task_id = from_flatbuf<TaskID>(*message->task_id());
    AsyncResolveObjects(client,
                        refs,
                        task_id,
                        /*ray_get=*/true);
  }
}

void NodeManager::ProcessWaitRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message = flatbuffers::GetRoot<protocol::WaitRequest>(message_data);
  std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());

  bool resolve_objects = false;
  for (auto const &object_id : object_ids) {
    if (!dependency_manager_.CheckObjectLocal(object_id)) {
      // At least one object requires resolution.
      resolve_objects = true;
    }
  }

  const TaskID &current_task_id = from_flatbuf<TaskID>(*message->task_id());
  if (resolve_objects) {
    // Resolve any missing objects. This is a no-op for any objects that are
    // already local. Missing objects will be pulled from remote node managers.
    // If an object's owner dies, an error will be stored as the object's
    // value.
    AsyncResolveObjects(client,
                        refs,
                        current_task_id,
                        /*ray_get=*/false);
  }
  if (message->num_required_objects() == 0) {
    // If we don't need to wait for any, return immediately after making the pull
    // requests through AsyncResolveObjects above.
    flatbuffers::FlatBufferBuilder fbb;
    auto wait_reply = protocol::CreateWaitReply(fbb,
                                                to_flatbuf(fbb, std::vector<ObjectID>{}),
                                                to_flatbuf(fbb, std::vector<ObjectID>{}));
    fbb.Finish(wait_reply);
    const auto status =
        client->WriteMessage(static_cast<int64_t>(protocol::MessageType::WaitReply),
                             fbb.GetSize(),
                             fbb.GetBufferPointer());
    if (status.ok()) {
      if (resolve_objects) {
        AsyncResolveObjectsFinish(client, current_task_id);
      }
    } else {
      // We failed to write to the client, so disconnect the client.
      std::ostringstream stream;
      stream << "Failed to write WaitReply to the client. Status " << status
             << ", message: " << status.message();
      DisconnectClient(
          client, /*graceful=*/false, rpc::WorkerExitType::SYSTEM_ERROR, stream.str());
    }
    return;
  }
  uint64_t num_required_objects = static_cast<uint64_t>(message->num_required_objects());
  wait_manager_.Wait(object_ids,
                     message->timeout(),
                     num_required_objects,
                     [this, resolve_objects, client, current_task_id](
                         std::vector<ObjectID> ready, std::vector<ObjectID> remaining) {
                       // Write the data.
                       flatbuffers::FlatBufferBuilder fbb;
                       flatbuffers::Offset<protocol::WaitReply> wait_reply =
                           protocol::CreateWaitReply(
                               fbb, to_flatbuf(fbb, ready), to_flatbuf(fbb, remaining));
                       fbb.Finish(wait_reply);

                       auto status = client->WriteMessage(
                           static_cast<int64_t>(protocol::MessageType::WaitReply),
                           fbb.GetSize(),
                           fbb.GetBufferPointer());
                       if (status.ok()) {
                         // The client is unblocked now because the wait call has
                         // returned.
                         if (resolve_objects) {
                           AsyncResolveObjectsFinish(client, current_task_id);
                         }
                       } else {
                         // We failed to write to the client, so disconnect the client.
                         std::ostringstream stream;
                         stream << "Failed to write WaitReply to the client. Status "
                                << status << ", message: " << status.message();
                         DisconnectClient(client,
                                          /*graceful=*/false,
                                          rpc::WorkerExitType::SYSTEM_ERROR,
                                          stream.str());
                       }
                     });
}

void NodeManager::ProcessWaitForDirectActorCallArgsRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message =
      flatbuffers::GetRoot<protocol::WaitForDirectActorCallArgsRequest>(message_data);
  std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
  int64_t tag = message->tag();
  // Resolve any missing objects. This will pull the objects from remote node
  // managers or store an error if the objects have failed.
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());
  AsyncResolveObjects(client,
                      refs,
                      TaskID::Nil(),
                      /*ray_get=*/false);
  // De-duplicate the object IDs.
  absl::flat_hash_set<ObjectID> object_id_set(object_ids.begin(), object_ids.end());
  object_ids.assign(object_id_set.begin(), object_id_set.end());
  wait_manager_.Wait(
      object_ids,
      -1,
      object_ids.size(),
      [this, client, tag](std::vector<ObjectID> ready, std::vector<ObjectID> remaining) {
        RAY_CHECK(remaining.empty());
        std::shared_ptr<WorkerInterface> worker =
            worker_pool_.GetRegisteredWorker(client);
        if (!worker) {
          RAY_LOG(ERROR) << "Lost worker for wait request " << client;
        } else {
          worker->DirectActorCallArgWaitComplete(tag);
        }
      });
}

void NodeManager::ProcessPushErrorRequestMessage(const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::PushErrorRequest>(message_data);

  auto const &type = string_from_flatbuf(*message->type());
  auto const &error_message = string_from_flatbuf(*message->error_message());
  // TODO(hjiang): Figure out what's the unit for `PushErrorRequest`.
  double timestamp = message->timestamp();
  JobID job_id = from_flatbuf<JobID>(*message->job_id());
  auto error_data_ptr = gcs::CreateErrorTableData(
      type, error_message, absl::FromUnixMillis(timestamp), job_id);
  RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
}

void NodeManager::HandleGetResourceLoad(rpc::GetResourceLoadRequest request,
                                        rpc::GetResourceLoadReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  auto resources_data = reply->mutable_resources();
  resources_data->set_node_id(self_node_id_.Binary());
  resources_data->set_node_manager_address(initial_config_.node_manager_address);
  cluster_task_manager_->FillResourceUsage(*resources_data);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleCancelTasksWithResourceShapes(
    rpc::CancelTasksWithResourceShapesRequest request,
    rpc::CancelTasksWithResourceShapesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &resource_shapes = request.resource_shapes();
  std::vector<ResourceSet> target_resource_shapes;
  for (const auto &resource_shape : resource_shapes) {
    target_resource_shapes.emplace_back(
        ResourceSet(MapFromProtobuf(resource_shape.resource_shape())));
  }

  cluster_task_manager_->CancelTasksWithResourceShapes(target_resource_shapes);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleReportWorkerBacklog(rpc::ReportWorkerBacklogRequest request,
                                            rpc::ReportWorkerBacklogReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  HandleReportWorkerBacklog(
      request, reply, send_reply_callback, worker_pool_, *local_task_manager_);
}

void NodeManager::HandleReportWorkerBacklog(rpc::ReportWorkerBacklogRequest request,
                                            rpc::ReportWorkerBacklogReply *reply,
                                            rpc::SendReplyCallback send_reply_callback,
                                            WorkerPoolInterface &worker_pool,
                                            ILocalTaskManager &local_task_manager) {
  const WorkerID worker_id = WorkerID::FromBinary(request.worker_id());
  if (worker_pool.GetRegisteredWorker(worker_id) == nullptr &&
      worker_pool.GetRegisteredDriver(worker_id) == nullptr) {
    // The worker is already disconnected.
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  local_task_manager.ClearWorkerBacklog(worker_id);
  std::unordered_set<SchedulingClass> seen;
  for (const auto &backlog_report : request.backlog_reports()) {
    const TaskSpecification resource_spec(backlog_report.resource_spec());
    const SchedulingClass scheduling_class = resource_spec.GetSchedulingClass();
    RAY_CHECK(seen.find(scheduling_class) == seen.end());
    local_task_manager.SetWorkerBacklog(
        scheduling_class, worker_id, backlog_report.backlog_size());
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleRequestWorkerLease(rpc::RequestWorkerLeaseRequest request,
                                           rpc::RequestWorkerLeaseReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  RayTask task{std::move(*request.mutable_resource_spec())};

  const auto caller_worker =
      WorkerID::FromBinary(task.GetTaskSpecification().CallerAddress().worker_id());
  const auto caller_node =
      NodeID::FromBinary(task.GetTaskSpecification().CallerAddress().raylet_id());
  if (!task.GetTaskSpecification().IsDetachedActor() &&
      IsWorkerDead(caller_worker, caller_node)) {
    RAY_LOG(INFO).WithField(caller_worker).WithField(caller_node)
        << "Caller of RequestWorkerLease is dead. Skip leasing.";
    reply->set_canceled(true);
    reply->set_failure_type(rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED);
    reply->set_scheduling_failure_message(
        "Cancelled leasing because the caller worker is dead.");
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  };

  const bool is_actor_creation_task = task.GetTaskSpecification().IsActorCreationTask();
  ActorID actor_id = ActorID::Nil();
  metrics_num_task_scheduled_ += 1;

  if (is_actor_creation_task) {
    actor_id = task.GetTaskSpecification().ActorCreationId();
  }

  const auto &task_spec = task.GetTaskSpecification();
  worker_pool_.PrestartWorkers(task_spec, request.backlog_size());

  auto send_reply_callback_wrapper =
      [this, is_actor_creation_task, actor_id, reply, send_reply_callback](
          Status status, std::function<void()> success, std::function<void()> failure) {
        if (reply->rejected() && is_actor_creation_task) {
          auto resources_data = reply->mutable_resources_data();
          resources_data->set_node_id(self_node_id_.Binary());
          // If resources are not enough due to normal tasks' preemption
          // for GCS based actor scheduling, return
          // with normal task resource usages so GCS can fast update
          // its resource view of this raylet.
          if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
            auto normal_task_resources = local_task_manager_->CalcNormalTaskResources();
            RAY_LOG(DEBUG).WithField(actor_id)
                << "Reject leasing as the raylet has no enough resources. "
                   "normal_task_resources = "
                << normal_task_resources.DebugString() << ", local_resoruce_view = "
                << cluster_resource_scheduler_->GetClusterResourceManager()
                       .GetNodeResourceViewString(
                           scheduling::NodeID(self_node_id_.Binary()));
            resources_data->set_resources_normal_task_changed(true);
            auto resource_map = normal_task_resources.GetResourceMap();
            resources_data->mutable_resources_normal_task()->insert(resource_map.begin(),
                                                                    resource_map.end());
            resources_data->set_resources_normal_task_timestamp(
                absl::GetCurrentTimeNanos());
          }
        }
        send_reply_callback(status, success, failure);
      };

  cluster_task_manager_->QueueAndScheduleTask(std::move(task),
                                              request.grant_or_reject(),
                                              request.is_selected_based_on_locality(),
                                              reply,
                                              std::move(send_reply_callback_wrapper));
}

void NodeManager::HandlePrestartWorkers(rpc::PrestartWorkersRequest request,
                                        rpc::PrestartWorkersReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  auto pop_worker_request = std::make_shared<PopWorkerRequest>(
      request.language(),
      rpc::WorkerType::WORKER,
      request.has_job_id() ? JobID::FromBinary(request.job_id()) : JobID::Nil(),
      /*root_detached_actor_id=*/ActorID::Nil(),
      /*gpu=*/std::nullopt,
      /*actor_worker=*/std::nullopt,
      request.runtime_env_info(),
      /*runtime_env_hash=*/
      CalculateRuntimeEnvHash(request.runtime_env_info().serialized_runtime_env()),
      /*options=*/std::vector<std::string>{},
      absl::Seconds(request.keep_alive_duration_secs()),
      /*callback=*/
      [request](const std::shared_ptr<WorkerInterface> &worker,
                PopWorkerStatus status,
                const std::string &runtime_env_setup_error_message) {
        // This callback does not use the worker.
        RAY_LOG(DEBUG).WithField(worker->WorkerId())
            << "Prestart worker started! token " << worker->GetStartupToken()
            << ", status " << status << ", runtime_env_setup_error_message "
            << runtime_env_setup_error_message;
        return false;
      });

  for (uint64_t i = 0; i < request.num_workers(); i++) {
    worker_pool_.StartNewWorker(pop_worker_request);
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandlePrepareBundleResources(
    rpc::PrepareBundleResourcesRequest request,
    rpc::PrepareBundleResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::shared_ptr<const BundleSpecification>> bundle_specs;
  for (int index = 0; index < request.bundle_specs_size(); index++) {
    bundle_specs.emplace_back(
        std::make_shared<BundleSpecification>(request.bundle_specs(index)));
  }
  RAY_LOG(DEBUG) << "Request to prepare resources for bundles: "
                 << GetDebugStringForBundles(bundle_specs);
  auto prepared = placement_group_resource_manager_->PrepareBundles(bundle_specs);
  reply->set_success(prepared);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleCommitBundleResources(
    rpc::CommitBundleResourcesRequest request,
    rpc::CommitBundleResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::shared_ptr<const BundleSpecification>> bundle_specs;
  for (int index = 0; index < request.bundle_specs_size(); index++) {
    bundle_specs.emplace_back(
        std::make_shared<BundleSpecification>(request.bundle_specs(index)));
  }
  RAY_LOG(DEBUG) << "Request to commit resources for bundles: "
                 << GetDebugStringForBundles(bundle_specs);
  placement_group_resource_manager_->CommitBundles(bundle_specs);
  send_reply_callback(Status::OK(), nullptr, nullptr);

  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleCancelResourceReserve(
    rpc::CancelResourceReserveRequest request,
    rpc::CancelResourceReserveReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto bundle_spec = BundleSpecification(request.bundle_spec());
  RAY_LOG(DEBUG) << "Request to cancel reserved resource is received, "
                 << bundle_spec.DebugString();

  // Cancel lease requests that are waiting for workers
  // to free the acquired pg bundle resources
  // so that pg bundle can be returned.
  local_task_manager_->CancelTasks(
      [&](const std::shared_ptr<internal::Work> &work) {
        const auto bundle_id = work->task.GetTaskSpecification().PlacementGroupBundleId();
        return (bundle_id.first == bundle_spec.PlacementGroupId()) &&
               (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER);
      },
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_PLACEMENT_GROUP_REMOVED,
      absl::StrCat("Required placement group ",
                   bundle_spec.PlacementGroupId().Hex(),
                   " is removed."));

  // Kill all workers that are currently associated with the placement group.
  // NOTE: We can't traverse directly with `leased_workers_`, because `DestroyWorker` will
  // delete the element of `leased_workers_`. So we need to filter out
  // `workers_associated_with_pg` separately.
  std::vector<std::shared_ptr<WorkerInterface>> workers_associated_with_pg;
  for (const auto &worker_it : leased_workers_) {
    auto &worker = worker_it.second;
    if (worker->GetBundleId().first == bundle_spec.PlacementGroupId()) {
      workers_associated_with_pg.emplace_back(worker);
    }
  }
  for (const auto &worker : workers_associated_with_pg) {
    std::ostringstream stream;
    stream
        << "Destroying worker since its placement group was removed. Placement group id: "
        << worker->GetBundleId().first
        << ", bundle index: " << bundle_spec.BundleId().second
        << ", task id: " << worker->GetAssignedTaskId()
        << ", actor id: " << worker->GetActorId()
        << ", worker id: " << worker->WorkerId();
    const auto &message = stream.str();
    RAY_LOG(DEBUG) << message;
    DestroyWorker(worker, rpc::WorkerExitType::INTENDED_SYSTEM_EXIT, message);
  }

  RAY_CHECK_OK(placement_group_resource_manager_->ReturnBundle(bundle_spec));
  cluster_task_manager_->ScheduleAndDispatchTasks();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleReturnWorker(rpc::ReturnWorkerRequest request,
                                     rpc::ReturnWorkerReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  // Read the resource spec submitted by the client.
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  std::shared_ptr<WorkerInterface> worker = leased_workers_[worker_id];

  Status status;
  ReleaseWorker(worker_id);

  if (worker) {
    if (request.disconnect_worker()) {
      // The worker should be destroyed.
      DisconnectClient(
          worker->Connection(),
          /*graceful=*/false,
          rpc::WorkerExitType::SYSTEM_ERROR,
          absl::StrCat("The leased worker has unrecoverable failure. Worker is requested "
                       "to be destroyed when it is returned. ",
                       request.disconnect_worker_error_detail()));
    } else {
      if (worker->IsBlocked()) {
        // Handle the edge case where the worker was returned before we got the
        // unblock RPC by unblocking it immediately (unblock is idempotent).
        HandleDirectCallTaskUnblocked(worker);
      }
      local_task_manager_->ReleaseWorkerResources(worker);
      // If the worker is exiting, don't add it to our pool. The worker will cleanup
      // and terminate itself.
      if (!request.worker_exiting()) {
        HandleWorkerAvailable(worker);
      }
    }
  } else {
    status = Status::Invalid("Returned worker does not exist any more");
  }
  send_reply_callback(status, nullptr, nullptr);
}

void NodeManager::HandleIsLocalWorkerDead(rpc::IsLocalWorkerDeadRequest request,
                                          rpc::IsLocalWorkerDeadReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  reply->set_is_dead(worker_pool_.GetRegisteredWorker(
                         WorkerID::FromBinary(request.worker_id())) == nullptr);
  send_reply_callback(Status::OK(), /*success=*/nullptr, /*failure=*/nullptr);
}

void NodeManager::HandleDrainRaylet(rpc::DrainRayletRequest request,
                                    rpc::DrainRayletReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "Drain raylet RPC has received. Deadline is "
                << request.deadline_timestamp_ms() << ". Drain reason: "
                << rpc::autoscaler::DrainNodeReason_Name(request.reason())
                << ". Drain reason message: " << request.reason_message();

  if (request.reason() ==
      rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_IDLE_TERMINATION) {
    const bool is_idle =
        cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeIdle();
    if (is_idle) {
      cluster_resource_scheduler_->GetLocalResourceManager().SetLocalNodeDraining(
          request);
      reply->set_is_accepted(true);
    } else {
      reply->set_is_accepted(false);
      reply->set_rejection_reason_message(
          "The node to be idle terminated is no longer idle.");
    }
  } else {
    // Non-rejectable draining request.
    RAY_CHECK_EQ(request.reason(),
                 rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
    cluster_resource_scheduler_->GetLocalResourceManager().SetLocalNodeDraining(request);
    reply->set_is_accepted(true);
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleShutdownRaylet(rpc::ShutdownRayletRequest request,
                                       rpc::ShutdownRayletReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO)
      << "Shutdown RPC has received. Shutdown will happen after the RPC is replied.";
  // Exit right away if it is not graceful.
  if (!request.graceful()) {
    std::_Exit(EXIT_SUCCESS);
  }
  if (is_shutdown_request_received_) {
    RAY_LOG(INFO) << "Node already has received the shutdown request. The shutdown "
                     "request RPC is ignored.";
    return;
  }
  auto shutdown_after_reply = [&]() {
    rpc::DrainServerCallExecutor();
    // Note that the callback is posted to the io service after the shutdown GRPC request
    // is replied. Otherwise, the RPC might not be replied to GCS before it shutsdown
    // itself.
    rpc::NodeDeathInfo node_death_info;
    node_death_info.set_reason(rpc::NodeDeathInfo::EXPECTED_TERMINATION);
    node_death_info.set_reason_message("Terminated by autoscaler.");
    shutdown_raylet_gracefully_(node_death_info);
  };
  is_shutdown_request_received_ = true;
  send_reply_callback(Status::OK(), shutdown_after_reply, shutdown_after_reply);
}

void NodeManager::HandleReleaseUnusedActorWorkers(
    rpc::ReleaseUnusedActorWorkersRequest request,
    rpc::ReleaseUnusedActorWorkersReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::unordered_set<WorkerID> in_use_worker_ids;
  for (int index = 0; index < request.worker_ids_in_use_size(); ++index) {
    auto worker_id = WorkerID::FromBinary(request.worker_ids_in_use(index));
    in_use_worker_ids.emplace(worker_id);
  }

  std::vector<std::shared_ptr<WorkerInterface>> unused_actor_workers;
  for (auto &iter : leased_workers_) {
    // We only kill *actor* workers.
    if (!iter.second->GetActorId().IsNil() && !in_use_worker_ids.count(iter.first)) {
      unused_actor_workers.push_back(iter.second);
    }
  }

  for (auto &worker : unused_actor_workers) {
    RAY_LOG(DEBUG).WithField(worker->WorkerId())
        << "GCS requested to release unused actor worker.";
    DestroyWorker(worker,
                  rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
                  "Worker is no longer needed by the GCS.");
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleCancelWorkerLease(rpc::CancelWorkerLeaseRequest request,
                                          rpc::CancelWorkerLeaseReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const TaskID task_id = TaskID::FromBinary(request.task_id());
  bool canceled = cluster_task_manager_->CancelTask(task_id);
  // The task cancellation failed if we did not have the task queued, since
  // this means that we may not have received the task request yet. It is
  // successful if we did have the task queued, since we have now replied to
  // the client that requested the lease.
  reply->set_success(canceled);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::MarkObjectsAsFailed(
    const ErrorType &error_type,
    const std::vector<rpc::ObjectReference> &objects_to_fail,
    const JobID &job_id) {
  // TODO(swang): Ideally we should return the error directly to the client
  // that needs this object instead of storing the object in plasma, which is
  // not guaranteed to succeed. This avoids hanging the client if plasma is not
  // reachable.
  const std::string meta = std::to_string(static_cast<int>(error_type));
  for (const auto &ref : objects_to_fail) {
    ObjectID object_id = ObjectID::FromBinary(ref.object_id());
    RAY_LOG(DEBUG).WithField(object_id)
        << "Mark the object as failed due to " << error_type;
    std::shared_ptr<Buffer> data;
    Status status;
    status = store_client_->TryCreateImmediately(
        object_id,
        ref.owner_address(),
        0,
        reinterpret_cast<const uint8_t *>(meta.c_str()),
        meta.length(),
        &data,
        plasma::flatbuf::ObjectSource::ErrorStoredByRaylet);
    if (status.ok()) {
      status = store_client_->Seal(object_id);
    }
    if (!status.ok() && !status.IsObjectExists()) {
      RAY_LOG(DEBUG).WithField(object_id) << "Marking plasma object failed.";
      // If we failed to save the error code, log a warning and push an error message
      // to the driver.
      std::ostringstream stream;
      stream << "A plasma error (" << status.ToString() << ") occurred while saving"
             << " error code to object " << object_id << ". Anyone who's getting this"
             << " object may hang forever.";
      std::string error_message = stream.str();
      RAY_LOG(ERROR) << error_message;
      auto error_data_ptr = gcs::CreateErrorTableData(
          "task", error_message, absl::FromUnixMillis(current_time_ms()), job_id);
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
}

void NodeManager::HandleDirectCallTaskBlocked(
    const std::shared_ptr<WorkerInterface> &worker) {
  if (!worker || worker->IsBlocked() || worker->GetAssignedTaskId().IsNil()) {
    return;  // The worker may have died or is no longer processing the task.
  }

  local_task_manager_->ReleaseCpuResourcesFromBlockedWorker(worker);
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleDirectCallTaskUnblocked(
    const std::shared_ptr<WorkerInterface> &worker) {
  if (!worker || worker->GetAssignedTaskId().IsNil()) {
    return;  // The worker may have died or is no longer processing the task.
  }

  // First, always release task dependencies. This ensures we don't leak resources even
  // if we don't need to unblock the worker below.
  dependency_manager_.CancelGetRequest(worker->WorkerId());

  if (worker->IsBlocked()) {
    local_task_manager_->ReturnCpuResourcesToUnblockedWorker(worker);
    cluster_task_manager_->ScheduleAndDispatchTasks();
  }
}

void NodeManager::AsyncResolveObjects(
    const std::shared_ptr<ClientConnection> &client,
    const std::vector<rpc::ObjectReference> &required_object_refs,
    const TaskID &current_task_id,
    bool ray_get) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (!worker) {
    // The client is a driver. Drivers do not hold resources, so we simply mark
    // the task as blocked.
    worker = worker_pool_.GetRegisteredDriver(client);
  }

  RAY_CHECK(worker);
  // Subscribe to the objects required by the task. These objects will be
  // fetched and/or restarted as necessary, until the objects become local
  // or are unsubscribed.
  if (ray_get) {
    dependency_manager_.StartOrUpdateGetRequest(worker->WorkerId(), required_object_refs);
  } else {
    dependency_manager_.StartOrUpdateWaitRequest(worker->WorkerId(),
                                                 required_object_refs);
  }
}

void NodeManager::AsyncResolveObjectsFinish(
    const std::shared_ptr<ClientConnection> &client, const TaskID &current_task_id) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (!worker) {
    // The client is a driver. Drivers do not hold resources, so we simply
    // mark the driver as unblocked.
    worker = worker_pool_.GetRegisteredDriver(client);
  }

  RAY_CHECK(worker);
  // Unsubscribe from any `ray.get` objects that the task was blocked on.  Any
  // fetch or reconstruction operations to make the objects local are canceled.
  // `ray.wait` calls will stay active until the objects become local, or the
  // task/actor that called `ray.wait` exits.
  dependency_manager_.CancelGetRequest(worker->WorkerId());
}

bool NodeManager::FinishAssignedTask(const std::shared_ptr<WorkerInterface> &worker_ptr) {
  // TODO (Alex): We should standardize to pass
  // std::shared_ptr<WorkerInterface> instead of refs.
  auto &worker = *worker_ptr;
  TaskID task_id = worker.GetAssignedTaskId();
  RAY_LOG(DEBUG).WithField(task_id) << "Finished task ";

  RayTask task;
  local_task_manager_->TaskFinished(worker_ptr, &task);

  const auto &spec = task.GetTaskSpecification();  //
  if ((spec.IsActorCreationTask())) {
    // If this was an actor or actor creation task, handle the actor's new
    // state.
    FinishAssignedActorCreationTask(worker, task);
  } else {
    // If this was a non-actor task, then cancel any ray.wait calls that were
    // made during the task execution.
    dependency_manager_.CancelWaitRequest(worker.WorkerId());
  }

  // Notify the task dependency manager that this task has finished execution.
  dependency_manager_.CancelGetRequest(worker.WorkerId());

  if (!spec.IsActorCreationTask()) {
    // Unset the worker's assigned task. We keep the assigned task ID for
    // direct actor creation calls because this ID is used later if the actor
    // requires objects from plasma.
    worker.AssignTaskId(TaskID::Nil());
    worker.SetOwnerAddress(rpc::Address());
  }
  // Direct actors will be assigned tasks via the core worker and therefore are
  // not idle.
  return !spec.IsActorCreationTask();
}

void NodeManager::FinishAssignedActorCreationTask(WorkerInterface &worker,
                                                  const RayTask &task) {
  RAY_LOG(DEBUG) << "Finishing assigned actor creation task";
  const TaskSpecification task_spec = task.GetTaskSpecification();
  ActorID actor_id = task_spec.ActorCreationId();

  // This was an actor creation task. Convert the worker to an actor.
  worker.AssignActorId(actor_id);

  if (task_spec.IsDetachedActor()) {
    worker.MarkDetachedActor();
    auto job_id = task.GetTaskSpecification().JobId();
    auto job_config = worker_pool_.GetJobConfig(job_id);
    RAY_CHECK(job_config);
  }
}

void NodeManager::SpillIfOverPrimaryObjectsThreshold() {
  // Trigger object spilling if current usage is above the specified threshold.
  const float allocated_percentage =
      static_cast<float>(local_object_manager_.GetPrimaryBytes()) /
      object_manager_.GetMemoryCapacity();
  if (allocated_percentage >= RayConfig::instance().object_spilling_threshold()) {
    RAY_LOG(INFO) << "Triggering object spilling because current usage "
                  << allocated_percentage * 100 << "% is above threshold "
                  << RayConfig::instance().object_spilling_threshold() * 100 << "%.";
    local_object_manager_.SpillObjectUptoMaxThroughput();
  }
}

void NodeManager::HandleObjectLocal(const ObjectInfo &object_info) {
  const ObjectID &object_id = object_info.object_id;
  // Notify the task dependency manager that this object is local.
  const auto ready_task_ids = dependency_manager_.HandleObjectLocal(object_id);
  RAY_LOG(DEBUG).WithField(object_id).WithField(self_node_id_)
      << "Object local on node, " << ready_task_ids.size() << " tasks ready";
  local_task_manager_->TasksUnblocked(ready_task_ids);

  // Notify the wait manager that this object is local.
  wait_manager_.HandleObjectLocal(object_id);

  auto waiting_workers = absl::flat_hash_set<std::shared_ptr<WorkerInterface>>();
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    auto waiting = this->async_plasma_objects_notification_.extract(object_id);
    if (!waiting.empty()) {
      waiting_workers.swap(waiting.mapped());
    }
  }
  rpc::PlasmaObjectReadyRequest request;
  request.set_object_id(object_id.Binary());

  for (auto worker : waiting_workers) {
    worker->rpc_client()->PlasmaObjectReady(
        request, [](Status status, const rpc::PlasmaObjectReadyReply &reply) {
          if (!status.ok()) {
            RAY_LOG(INFO) << "Problem with telling worker that plasma object is ready"
                          << status.ToString();
          }
        });
  }

  // An object was created so we may be over the spill
  // threshold now.
  SpillIfOverPrimaryObjectsThreshold();
}

bool NodeManager::IsActorCreationTask(const TaskID &task_id) {
  auto actor_id = task_id.ActorId();
  if (!actor_id.IsNil() && task_id == TaskID::ForActorCreationTask(actor_id)) {
    // This task ID corresponds to an actor creation task.
    return true;
  }

  return false;
}

void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is no longer local.
  const auto waiting_task_ids = dependency_manager_.HandleObjectMissing(object_id);
  std::stringstream result;
  result << "Object missing " << object_id << ", "
         << " on " << self_node_id_ << ", " << waiting_task_ids.size()
         << " tasks waiting";
  if (waiting_task_ids.size() > 0) {
    result << ", tasks: ";
    for (const auto &task_id : waiting_task_ids) {
      result << task_id << "  ";
    }
  }
  RAY_LOG(DEBUG) << result.str();
}

void NodeManager::ProcessSubscribePlasmaReady(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  std::shared_ptr<WorkerInterface> associated_worker =
      worker_pool_.GetRegisteredWorker(client);
  if (associated_worker == nullptr) {
    associated_worker = worker_pool_.GetRegisteredDriver(client);
  }
  RAY_CHECK(associated_worker != nullptr)
      << "No worker exists for CoreWorker with client: " << client->DebugString();

  auto message = flatbuffers::GetRoot<protocol::SubscribePlasmaReady>(message_data);
  auto id = from_flatbuf<ObjectID>(*message->object_id());

  if (dependency_manager_.CheckObjectLocal(id)) {
    // Object is already local, so we directly fire the callback to tell the core worker
    // that the plasma object is ready.
    rpc::PlasmaObjectReadyRequest request;
    request.set_object_id(id.Binary());

    RAY_LOG(DEBUG).WithField(id) << "Object is already local, firing callback directly.";
    associated_worker->rpc_client()->PlasmaObjectReady(
        request, [](Status status, const rpc::PlasmaObjectReadyReply &reply) {
          if (!status.ok()) {
            RAY_LOG(INFO) << "Problem with telling worker that plasma object is ready"
                          << status.ToString();
          }
        });
  } else {
    // The object is not local, so we are subscribing to pull and wait for the objects.
    std::vector<rpc::ObjectReference> refs = {FlatbufferToSingleObjectReference(
        *message->object_id(), *message->owner_address())};

    // NOTE(simon): This call will issue a pull request to remote workers and make sure
    // the object will be local.
    // 1. We currently do not allow user to cancel this call. The object will be pulled
    //    even if the `await object_ref` is cancelled.
    // 2. We currently do not handle edge cases with object eviction where the object
    //    is local at this time but when the core worker was notified, the object is
    //    is evicted. The core worker should be able to handle evicted object in this
    //    case.
    dependency_manager_.StartOrUpdateWaitRequest(associated_worker->WorkerId(), refs);

    // Add this worker to the listeners for the object ID.
    {
      absl::MutexLock guard(&plasma_object_notification_lock_);
      async_plasma_objects_notification_[id].insert(associated_worker);
    }
  }
}

void NodeManager::DumpDebugState() const {
  std::fstream fs;
  fs.open(initial_config_.log_dir + "/debug_state.txt",
          std::fstream::out | std::fstream::trunc);
  fs << DebugString();
  fs.close();
}

const NodeManagerConfig &NodeManager::GetInitialConfig() const { return initial_config_; }

std::string NodeManager::DebugString() const {
  std::stringstream result;
  uint64_t now_ms = current_time_ms();
  result << "NodeManager:";
  result << "\nNode ID: " << self_node_id_;
  result << "\nNode name: " << self_node_name_;
  result << "\nInitialConfigResources: " << initial_config_.resource_config.DebugString();
  if (cluster_task_manager_ != nullptr) {
    result << "\nClusterTaskManager:\n";
    result << cluster_task_manager_->DebugStr();
  }
  result << "\nClusterResources:";
  result << "\n" << local_object_manager_.DebugString();
  result << "\n" << object_manager_.DebugString();
  result << "\n" << gcs_client_->DebugString();
  result << "\n" << worker_pool_.DebugString();
  result << "\n" << dependency_manager_.DebugString();
  result << "\n" << wait_manager_.DebugString();
  result << "\n" << core_worker_subscriber_->DebugString();
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    result << "\nnum async plasma notifications: "
           << async_plasma_objects_notification_.size();
  }

  result << "\nRemote node managers: ";
  for (const auto &entry : remote_node_manager_addresses_) {
    result << "\n" << entry.first;
  }

  // Event stats.
  result << "\nEvent stats:" << io_service_.stats().StatsString();

  result << "\nDebugString() time ms: " << (current_time_ms() - now_ms);
  return result.str();
}

bool NodeManager::GetObjectsFromPlasma(const std::vector<ObjectID> &object_ids,
                                       std::vector<std::unique_ptr<RayObject>> *results) {
  // Pin the objects in plasma by getting them and holding a reference to
  // the returned buffer.
  // NOTE: the caller must ensure that the objects already exist in plasma before
  // sending a PinObjectIDs request.
  std::vector<plasma::ObjectBuffer> plasma_results;
  // TODO(swang): This `Get` has a timeout of 0, so the plasma store will not
  // block when serving the request. However, if the plasma store is under
  // heavy load, then this request can still block the NodeManager event loop
  // since we must wait for the plasma store's reply. We should consider using
  // an `AsyncGet` instead.
  if (!store_client_
           ->Get(object_ids, /*timeout_ms=*/0, &plasma_results, /*is_from_worker=*/false)
           .ok()) {
    return false;
  }

  for (const auto &plasma_result : plasma_results) {
    if (plasma_result.data == nullptr) {
      results->push_back(nullptr);
    } else {
      results->emplace_back(std::unique_ptr<RayObject>(
          new RayObject(plasma_result.data, plasma_result.metadata, {})));
    }
  }
  return true;
}

void NodeManager::HandlePinObjectIDs(rpc::PinObjectIDsRequest request,
                                     rpc::PinObjectIDsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  object_ids.reserve(request.object_ids_size());
  for (const auto &object_id_binary : request.object_ids()) {
    object_ids.push_back(ObjectID::FromBinary(object_id_binary));
  }
  std::vector<std::unique_ptr<RayObject>> results;
  if (!GetObjectsFromPlasma(object_ids, &results)) {
    for (size_t i = 0; i < object_ids.size(); ++i) {
      reply->add_successes(false);
    }
  } else {
    RAY_CHECK_EQ(object_ids.size(), results.size());
    auto object_id_it = object_ids.begin();
    auto result_it = results.begin();
    while (object_id_it != object_ids.end()) {
      if (*result_it == nullptr) {
        RAY_LOG(DEBUG).WithField(*object_id_it)
            << "Failed to get object in the object store. This should only happen when "
               "the owner tries to pin a "
            << "secondary copy and it's evicted in the meantime";
        object_id_it = object_ids.erase(object_id_it);
        result_it = results.erase(result_it);
        reply->add_successes(false);
      } else {
        ++object_id_it;
        ++result_it;
        reply->add_successes(true);
      }
    }
    // Wait for the object to be freed by the owner, which keeps the ref count.
    ObjectID generator_id = request.has_generator_id()
                                ? ObjectID::FromBinary(request.generator_id())
                                : ObjectID::Nil();
    local_object_manager_.PinObjectsAndWaitForFree(
        object_ids, std::move(results), request.owner_address(), generator_id);
  }
  RAY_CHECK_EQ(request.object_ids_size(), reply->successes_size());
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleGetSystemConfig(rpc::GetSystemConfigRequest request,
                                        rpc::GetSystemConfigReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  reply->set_system_config(initial_config_.raylet_config);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleGetNodeStats(rpc::GetNodeStatsRequest node_stats_request,
                                     rpc::GetNodeStatsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  // Report object spilling stats.
  local_object_manager_.FillObjectStoreStats(reply);
  // Report object store stats.
  object_manager_.FillObjectStoreStats(reply);
  // As a result of the HandleGetNodeStats, we are collecting information from all
  // workers on this node. This is done by calling GetCoreWorkerStats on each worker. In
  // order to send up-to-date information back, we wait until all workers have replied,
  // and return the information from HandleNodesStatsRequest. The caller of
  // HandleGetNodeStats should set a timeout so that the rpc finishes even if not all
  // workers have replied.
  auto all_workers = worker_pool_.GetAllRegisteredWorkers(/* filter_dead_worker */ true);
  absl::flat_hash_set<WorkerID> driver_ids;
  for (auto driver :
       worker_pool_.GetAllRegisteredDrivers(/* filter_dead_driver */ true)) {
    all_workers.push_back(driver);
    driver_ids.insert(driver->WorkerId());
  }
  if (all_workers.empty()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  for (const auto &worker : all_workers) {
    if (worker->IsDead()) {
      continue;
    }
    rpc::GetCoreWorkerStatsRequest request;
    request.set_intended_worker_id(worker->WorkerId().Binary());
    request.set_include_memory_info(node_stats_request.include_memory_info());
    worker->rpc_client()->GetCoreWorkerStats(
        request,
        [reply, worker, all_workers, driver_ids, send_reply_callback](
            const ray::Status &status, const rpc::GetCoreWorkerStatsReply &r) {
          reply->add_core_workers_stats()->MergeFrom(r.core_worker_stats());
          reply->set_num_workers(reply->num_workers() + 1);
          if (reply->num_workers() == all_workers.size()) {
            send_reply_callback(Status::OK(), nullptr, nullptr);
          }
        });
  }
}

rpc::ObjectStoreStats AccumulateStoreStats(
    std::vector<rpc::GetNodeStatsReply> node_stats) {
  rpc::ObjectStoreStats store_stats;
  for (const auto &reply : node_stats) {
    auto cur_store = reply.store_stats();
    // Use max aggregation for time, since the nodes are spilling concurrently.
    store_stats.set_spill_time_total_s(
        std::max(store_stats.spill_time_total_s(), cur_store.spill_time_total_s()));
    store_stats.set_restore_time_total_s(
        std::max(store_stats.restore_time_total_s(), cur_store.restore_time_total_s()));
    // Use sum aggregation for the rest of the metrics.
    store_stats.set_spilled_bytes_total(store_stats.spilled_bytes_total() +
                                        cur_store.spilled_bytes_total());
    store_stats.set_spilled_objects_total(store_stats.spilled_objects_total() +
                                          cur_store.spilled_objects_total());
    store_stats.set_restored_bytes_total(store_stats.restored_bytes_total() +
                                         cur_store.restored_bytes_total());
    store_stats.set_restored_objects_total(store_stats.restored_objects_total() +
                                           cur_store.restored_objects_total());
    store_stats.set_object_store_bytes_used(store_stats.object_store_bytes_used() +
                                            cur_store.object_store_bytes_used());
    store_stats.set_object_store_bytes_avail(store_stats.object_store_bytes_avail() +
                                             cur_store.object_store_bytes_avail());
    store_stats.set_object_store_bytes_primary_copy(
        store_stats.object_store_bytes_primary_copy() +
        cur_store.object_store_bytes_primary_copy());
    store_stats.set_object_store_bytes_fallback(
        store_stats.object_store_bytes_fallback() +
        cur_store.object_store_bytes_fallback());
    store_stats.set_num_local_objects(store_stats.num_local_objects() +
                                      cur_store.num_local_objects());
    store_stats.set_consumed_bytes(store_stats.consumed_bytes() +
                                   cur_store.consumed_bytes());
    if (cur_store.object_pulls_queued()) {
      store_stats.set_object_pulls_queued(true);
    }
    store_stats.set_cumulative_created_objects(store_stats.cumulative_created_objects() +
                                               cur_store.cumulative_created_objects());
    store_stats.set_cumulative_created_bytes(store_stats.cumulative_created_bytes() +
                                             cur_store.cumulative_created_bytes());
  }
  return store_stats;
}

std::string FormatMemoryInfo(std::vector<rpc::GetNodeStatsReply> node_stats) {
  // First pass to compute object sizes.
  absl::flat_hash_map<ObjectID, int64_t> object_sizes;
  for (const auto &reply : node_stats) {
    for (const auto &core_worker_stats : reply.core_workers_stats()) {
      for (const auto &object_ref : core_worker_stats.object_refs()) {
        auto obj_id = ObjectID::FromBinary(object_ref.object_id());
        if (object_ref.object_size() > 0) {
          object_sizes[obj_id] = object_ref.object_size();
        }
      }
    }
  }

  std::ostringstream builder;
  builder
      << "----------------------------------------------------------------------------"
         "-----------------------------------------\n";
  builder
      << " Object ID                                                Reference Type    "
         "   Object Size  "
         " Reference Creation Site\n";
  builder
      << "============================================================================"
         "=========================================\n";

  // Second pass builds the summary string for each node.
  for (const auto &reply : node_stats) {
    for (const auto &core_worker_stats : reply.core_workers_stats()) {
      bool pid_printed = false;
      for (const auto &object_ref : core_worker_stats.object_refs()) {
        auto obj_id = ObjectID::FromBinary(object_ref.object_id());
        if (!object_ref.pinned_in_memory() && object_ref.local_ref_count() == 0 &&
            object_ref.submitted_task_ref_count() == 0 &&
            object_ref.contained_in_owned_size() == 0) {
          continue;
        }
        if (obj_id.IsNil()) {
          continue;
        }
        if (!pid_printed) {
          if (core_worker_stats.worker_type() == rpc::WorkerType::DRIVER) {
            builder << "; driver pid=" << core_worker_stats.pid() << "\n";
          } else {
            builder << "; worker pid=" << core_worker_stats.pid() << "\n";
          }
          pid_printed = true;
        }
        builder << obj_id.Hex() << "  ";
        // TODO(ekl) we could convey more information about the reference status.
        if (object_ref.pinned_in_memory()) {
          builder << "PINNED_IN_MEMORY     ";
        } else if (object_ref.submitted_task_ref_count() > 0) {
          builder << "USED_BY_PENDING_TASK ";
        } else if (object_ref.local_ref_count() > 0) {
          builder << "LOCAL_REFERENCE      ";
        } else if (object_ref.contained_in_owned_size() > 0) {
          builder << "CAPTURED_IN_OBJECT   ";
        } else {
          builder << "UNKNOWN_STATUS       ";
        }
        builder << std::right << std::setfill(' ') << std::setw(11);
        if (object_sizes.contains(obj_id)) {
          builder << object_sizes[obj_id];
        } else {
          builder << "          ?";
        }
        builder << "   " << object_ref.call_site();
        builder << "\n";
      }
    }
  }
  builder
      << "----------------------------------------------------------------------------"
         "-----------------------------------------\n";

  return builder.str();
}

void NodeManager::HandleFormatGlobalMemoryInfo(
    rpc::FormatGlobalMemoryInfoRequest request,
    rpc::FormatGlobalMemoryInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto replies = std::make_shared<std::vector<rpc::GetNodeStatsReply>>();
  auto local_request = std::make_shared<rpc::GetNodeStatsRequest>();
  auto local_reply = std::make_shared<rpc::GetNodeStatsReply>();
  bool include_memory_info = request.include_memory_info();
  local_request->set_include_memory_info(include_memory_info);

  unsigned int num_nodes = remote_node_manager_addresses_.size() + 1;
  rpc::GetNodeStatsRequest stats_req;
  stats_req.set_include_memory_info(include_memory_info);

  auto store_reply =
      [replies, reply, num_nodes, send_reply_callback, include_memory_info](
          const rpc::GetNodeStatsReply &local_reply) {
        replies->push_back(local_reply);
        if (replies->size() >= num_nodes) {
          if (include_memory_info) {
            reply->set_memory_summary(FormatMemoryInfo(*replies));
          }
          reply->mutable_store_stats()->CopyFrom(AccumulateStoreStats(*replies));
          send_reply_callback(Status::OK(), nullptr, nullptr);
        }
      };

  // Fetch from remote nodes.
  for (const auto &entry : remote_node_manager_addresses_) {
    auto client = std::make_unique<rpc::NodeManagerClient>(
        entry.second.first, entry.second.second, client_call_manager_);
    client->GetNodeStats(stats_req,
                         [replies, store_reply](const ray::Status &status,
                                                const rpc::GetNodeStatsReply &r) {
                           if (!status.ok()) {
                             RAY_LOG(ERROR) << "Failed to get remote node stats: "
                                            << status.ToString();
                           }
                           store_reply(r);
                         });
  }

  // Fetch from the local node.
  HandleGetNodeStats(stats_req,
                     local_reply.get(),
                     [local_reply, store_reply](Status status,
                                                std::function<void()> success,
                                                std::function<void()> failure) mutable {
                       store_reply(*local_reply);
                     });
}

void NodeManager::HandleGlobalGC(rpc::GlobalGCRequest request,
                                 rpc::GlobalGCReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  TriggerGlobalGC();
}

bool NodeManager::TryLocalGC() {
  // If plasma store is under high pressure, we should try to schedule a global gc.
  bool plasma_high_pressure =
      object_manager_.GetUsedMemoryPercentage() > high_plasma_storage_usage_;
  if (plasma_high_pressure && global_gc_throttler_.AbleToRun()) {
    TriggerGlobalGC();
  }

  // Set the global gc bit on the outgoing heartbeat message.
  bool triggered_by_global_gc = false;
  if (should_global_gc_) {
    triggered_by_global_gc = true;
    should_global_gc_ = false;
    global_gc_throttler_.RunNow();
  }

  // Trigger local GC if needed. This throttles the frequency of local GC calls
  // to at most once per heartbeat interval.
  if ((should_local_gc_ ||
       (absl::GetCurrentTimeNanos() - local_gc_run_time_ns_ > local_gc_interval_ns_)) &&
      local_gc_throttler_.AbleToRun()) {
    DoLocalGC(triggered_by_global_gc);
    should_local_gc_ = false;
  }
  return triggered_by_global_gc;
}

void NodeManager::TriggerGlobalGC() {
  should_global_gc_ = true;
  // We won't see our own request, so trigger local GC in the next heartbeat.
  should_local_gc_ = true;
}

void NodeManager::Stop() {
  // This never fails.
  RAY_CHECK_OK(store_client_->Disconnect());
  object_manager_.Stop();
  dashboard_agent_manager_.reset();
  runtime_env_agent_manager_.reset();
}

void NodeManager::RecordMetrics() {
  recorded_metrics_ = true;
  if (stats::StatsConfig::instance().IsStatsDisabled()) {
    return;
  }

  cluster_task_manager_->RecordMetrics();
  object_manager_.RecordMetrics();
  local_object_manager_.RecordMetrics();

  uint64_t current_time = current_time_ms();
  uint64_t duration_ms = current_time - last_metrics_recorded_at_ms_;
  last_metrics_recorded_at_ms_ = current_time;
  object_directory_->RecordMetrics(duration_ms);
  dependency_manager_.RecordMetrics();
}

void NodeManager::ConsumeSyncMessage(
    std::shared_ptr<const syncer::RaySyncMessage> message) {
  if (message->message_type() == syncer::MessageType::RESOURCE_VIEW) {
    syncer::ResourceViewSyncMessage resource_view_sync_message;
    resource_view_sync_message.ParseFromString(message->sync_message());
    NodeID node_id = NodeID::FromBinary(message->node_id());
    if (UpdateResourceUsage(node_id, resource_view_sync_message)) {
      cluster_task_manager_->ScheduleAndDispatchTasks();
    }
  } else if (message->message_type() == syncer::MessageType::COMMANDS) {
    syncer::CommandsSyncMessage commands_sync_message;
    commands_sync_message.ParseFromString(message->sync_message());
    if (commands_sync_message.should_global_gc()) {
      should_local_gc_ = true;
    }
  }
}

std::optional<syncer::RaySyncMessage> NodeManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  RAY_CHECK_EQ(message_type, syncer::MessageType::COMMANDS);

  syncer::CommandsSyncMessage commands_sync_message;
  commands_sync_message.set_should_global_gc(true);
  commands_sync_message.set_cluster_full_of_actors_detected(resource_deadlock_warned_ >=
                                                            1);
  syncer::RaySyncMessage msg;
  msg.set_version(absl::GetCurrentTimeNanos());
  msg.set_node_id(self_node_id_.Binary());
  msg.set_message_type(syncer::MessageType::COMMANDS);
  std::string serialized_msg;
  RAY_CHECK(commands_sync_message.SerializeToString(&serialized_msg));
  msg.set_sync_message(std::move(serialized_msg));
  return std::make_optional(std::move(msg));
}

void NodeManager::PublishInfeasibleTaskError(const RayTask &task) const {
  bool suppress_warning = false;

  if (!task.GetTaskSpecification().PlacementGroupBundleId().first.IsNil()) {
    // If the task is part of a placement group, do nothing. If necessary, the infeasible
    // warning should come from the placement group scheduling, not the task scheduling.
    suppress_warning = true;
  }

  // Push a warning to the task's driver that this task is currently infeasible.
  if (!suppress_warning) {
    std::ostringstream error_message;
    error_message
        << "The actor or task with ID " << task.GetTaskSpecification().TaskId()
        << " cannot be scheduled right now. It requires "
        << task.GetTaskSpecification().GetRequiredPlacementResources().DebugString()
        << " for placement, however the cluster currently cannot provide the requested "
           "resources. The required resources may be added as autoscaling takes place "
           "or placement groups are scheduled. Otherwise, consider reducing the "
           "resource requirements of the task.";
    std::string error_message_str = error_message.str();
    RAY_LOG(WARNING) << error_message_str;
  }
}

// Picks the worker with the latest submitted task and kills the process
// if the memory usage is above the threshold. Allows one in-flight
// process kill at a time as killing a process could sometimes take
// seconds.
// TODO(clarng): potentially kill more aggressively by measuring the
// memory usage of each process and kill enough processes to put it
// below the memory threshold.
MemoryUsageRefreshCallback NodeManager::CreateMemoryUsageRefreshCallback() {
  return [this](bool is_usage_above_threshold,
                MemorySnapshot system_memory,
                float usage_threshold) {
    if (high_memory_eviction_target_ != nullptr) {
      if (!high_memory_eviction_target_->GetProcess().IsAlive()) {
        RAY_LOG(INFO)
                .WithField(high_memory_eviction_target_->WorkerId())
                .WithField(high_memory_eviction_target_->GetAssignedTaskId())
            << "Worker evicted and process killed to reclaim memory. "
            << "worker pid: " << high_memory_eviction_target_->GetProcess().GetId();
        high_memory_eviction_target_ = nullptr;
      }
    }
    if (is_usage_above_threshold) {
      if (high_memory_eviction_target_ != nullptr) {
        RAY_LOG_EVERY_MS(INFO, 1000)
                .WithField(high_memory_eviction_target_->GetAssignedTaskId())
                .WithField(high_memory_eviction_target_->WorkerId())
            << "Memory usage above threshold. "
            << "Still waiting for worker eviction to free up memory. "
            << "worker pid: " << high_memory_eviction_target_->GetProcess().GetId();
      } else {
        system_memory.process_used_bytes = MemoryMonitor::GetProcessMemoryUsage();
        auto workers = worker_pool_.GetAllRegisteredWorkers();
        if (workers.empty()) {
          RAY_LOG_EVERY_MS(WARNING, 5000)
              << "Memory usage above threshold but no workers are available for killing."
              << "This could be due to worker memory leak and"
              << "idle worker are occupying most of the memory.";
          return;
        }
        auto worker_to_kill_and_should_retry =
            worker_killing_policy_->SelectWorkerToKill(workers, system_memory);
        auto worker_to_kill = worker_to_kill_and_should_retry.first;
        bool should_retry = worker_to_kill_and_should_retry.second;
        if (worker_to_kill == nullptr) {
          RAY_LOG_EVERY_MS(WARNING, 5000) << "Worker killer did not select a worker to "
                                             "kill even though memory usage is high.";
        } else {
          high_memory_eviction_target_ = worker_to_kill;

          /// TODO: (clarng) expose these strings in the frontend python error as well.
          std::string oom_kill_details = this->CreateOomKillMessageDetails(
              worker_to_kill, this->self_node_id_, system_memory, usage_threshold);
          std::string oom_kill_suggestions =
              this->CreateOomKillMessageSuggestions(worker_to_kill, should_retry);

          RAY_LOG(INFO)
              << "Killing worker with task "
              << worker_to_kill->GetAssignedTask().GetTaskSpecification().DebugString()
              << "\n\n"
              << oom_kill_details << "\n\n"
              << oom_kill_suggestions;

          std::stringstream worker_exit_message_ss;
          worker_exit_message_ss
              << "Task was killed due to the node running low on memory.\n"
              << oom_kill_details << "\n"
              << oom_kill_suggestions;
          std::string worker_exit_message = worker_exit_message_ss.str();

          // Rerpot the event to the dashboard.
          RAY_EVENT_EVERY_MS(ERROR, "Out of Memory", 10 * 1000) << worker_exit_message;

          // Mark the task as failure and raise an exception from a caller.
          rpc::RayErrorInfo task_failure_reason;
          task_failure_reason.set_error_message(worker_exit_message);
          task_failure_reason.set_error_type(rpc::ErrorType::OUT_OF_MEMORY);
          SetTaskFailureReason(worker_to_kill->GetAssignedTaskId(),
                               std::move(task_failure_reason),
                               should_retry);

          /// since we print the process memory in the message. Destroy should be called
          /// as soon as possible to free up memory.
          DestroyWorker(high_memory_eviction_target_,
                        rpc::WorkerExitType::NODE_OUT_OF_MEMORY,
                        worker_exit_message,
                        true /* force */);

          if (worker_to_kill->GetWorkerType() == rpc::WorkerType::DRIVER) {
            // TODO(sang): Add the job entrypoint to the name.
            ray::stats::STATS_memory_manager_worker_eviction_total.Record(
                1, {{"Type", "MemoryManager.DriverEviction.Total"}, {"Name", ""}});
          } else if (worker_to_kill->GetActorId().IsNil()) {
            const auto &ray_task = worker_to_kill->GetAssignedTask();
            ray::stats::STATS_memory_manager_worker_eviction_total.Record(
                1,
                {{"Type", "MemoryManager.TaskEviction.Total"},
                 {"Name", ray_task.GetTaskSpecification().GetName()}});
          } else {
            const auto &ray_task = worker_to_kill->GetAssignedTask();
            ray::stats::STATS_memory_manager_worker_eviction_total.Record(
                1,
                {{"Type", "MemoryManager.ActorEviction.Total"},
                 {"Name", ray_task.GetTaskSpecification().GetName()}});
          }
        }
      }
    }
  };
}

const std::string NodeManager::CreateOomKillMessageDetails(
    const std::shared_ptr<WorkerInterface> &worker,
    const NodeID &node_id,
    const MemorySnapshot &system_memory,
    float usage_threshold) const {
  float usage_fraction =
      static_cast<float>(system_memory.used_bytes) / system_memory.total_bytes;
  std::string used_bytes_gb = absl::StrFormat(
      "%.2f", static_cast<float>(system_memory.used_bytes) / 1024 / 1024 / 1024);
  std::string total_bytes_gb = absl::StrFormat(
      "%.2f", static_cast<float>(system_memory.total_bytes) / 1024 / 1024 / 1024);
  std::stringstream oom_kill_details_ss;

  auto pid = worker->GetProcess().GetId();
  int64_t used_bytes = 0;
  const auto pid_entry = system_memory.process_used_bytes.find(pid);
  if (pid_entry != system_memory.process_used_bytes.end()) {
    used_bytes = pid_entry->second;
  } else {
    return "";
    RAY_LOG_EVERY_MS(INFO, 60000)
        << "Can't find memory usage for PID, reporting zero. PID: " << pid;
  }
  std::string process_used_bytes_gb =
      absl::StrFormat("%.2f", static_cast<float>(used_bytes) / 1024 / 1024 / 1024);

  oom_kill_details_ss
      << "Memory on the node (IP: " << worker->IpAddress() << ", ID: " << node_id
      << ") where the task (" << worker->GetTaskOrActorIdAsDebugString()
      << ", name=" << worker->GetAssignedTask().GetTaskSpecification().GetName()
      << ", pid=" << worker->GetProcess().GetId()
      << ", memory used=" << process_used_bytes_gb << "GB) was running was "
      << used_bytes_gb << "GB / " << total_bytes_gb << "GB (" << usage_fraction
      << "), which exceeds the memory usage threshold of " << usage_threshold
      << ". Ray killed this worker (ID: " << worker->WorkerId()
      << ") because it was the most recently scheduled task; to see more "
         "information about memory usage on this node, use `ray logs raylet.out "
         "-ip "
      << worker->IpAddress() << "`. To see the logs of the worker, use `ray logs worker-"
      << worker->WorkerId() << "*out -ip " << worker->IpAddress()
      << ". Top 10 memory users:\n"
      << MemoryMonitor::TopNMemoryDebugString(10, system_memory);
  return oom_kill_details_ss.str();
}

const std::string NodeManager::CreateOomKillMessageSuggestions(
    const std::shared_ptr<WorkerInterface> &worker, bool should_retry) const {
  std::stringstream not_retriable_recommendation_ss;
  if (worker && !worker->GetAssignedTask().GetTaskSpecification().IsRetriable()) {
    not_retriable_recommendation_ss << "Set ";
    if (worker->GetAssignedTask().GetTaskSpecification().IsNormalTask()) {
      not_retriable_recommendation_ss << "max_retries";
    } else {
      not_retriable_recommendation_ss << "max_restarts and max_task_retries";
    }
    not_retriable_recommendation_ss
        << " to enable retry when the task crashes due to OOM. ";
  }
  std::stringstream deadlock_recommendation;
  if (!should_retry) {
    deadlock_recommendation
        << "The node has insufficient memory to execute this workload. ";
  }
  std::stringstream oom_kill_suggestions_ss;
  oom_kill_suggestions_ss
      << "Refer to the documentation on how to address the out of memory issue: "
         "https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. "
         "Consider provisioning more memory on this node or reducing task "
         "parallelism by requesting more CPUs per task. "
      << not_retriable_recommendation_ss.str()
      << "To adjust the kill "
         "threshold, set the environment variable "
         "`RAY_memory_usage_threshold` when starting Ray. To disable "
         "worker killing, set the environment variable "
         "`RAY_memory_monitor_refresh_ms` to zero.";
  return oom_kill_suggestions_ss.str();
}

void NodeManager::SetTaskFailureReason(const TaskID &task_id,
                                       const rpc::RayErrorInfo &failure_reason,
                                       bool should_retry) {
  RAY_LOG(DEBUG).WithField(task_id) << "set failure reason for task ";
  ray::TaskFailureEntry entry(failure_reason, should_retry);
  auto result = task_failure_reasons_.emplace(task_id, std::move(entry));
  if (!result.second) {
    RAY_LOG(WARNING).WithField(task_id)
        << "Trying to insert failure reason more than once for the same "
           "task, the previous failure will be removed.";
  }
}

void NodeManager::GCTaskFailureReason() {
  for (const auto &entry : task_failure_reasons_) {
    auto duration = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - entry.second.creation_time)
            .count());
    if (duration > RayConfig::instance().task_failure_entry_ttl_ms()) {
      RAY_LOG(INFO).WithField(entry.first)
          << "Removing task failure reason since it expired";
      task_failure_reasons_.erase(entry.first);
    }
  }
}

void NodeManager::ReportWorkerOOMKillStats() {
  if (number_workers_killed_by_oom_ > 0) {
    RAY_LOG(ERROR) << number_workers_killed_by_oom_
                   << " Workers (tasks / actors) killed due to memory pressure (OOM), "
                   << number_workers_killed_
                   << " Workers crashed due to other reasons at node (ID: "
                   << self_node_id_ << ", IP: " << initial_config_.node_manager_address
                   << ") over the last time period. "
                   << "To see more information about the Workers killed on this node, "
                   << "use `ray logs raylet.out -ip "
                   << initial_config_.node_manager_address << "`\n\n"
                   << CreateOomKillMessageSuggestions({});
  }
  number_workers_killed_by_oom_ = 0;
  number_workers_killed_ = 0;
}

std::unique_ptr<AgentManager> NodeManager::CreateDashboardAgentManager(
    const NodeID &self_node_id, const NodeManagerConfig &config) {
  auto agent_command_line = ParseCommandLine(config.dashboard_agent_command);

  if (agent_command_line.empty()) {
    return nullptr;
  }

  for (auto &arg : agent_command_line) {
    auto node_manager_port_position = arg.find(kNodeManagerPortPlaceholder);
    if (node_manager_port_position != std::string::npos) {
      arg.replace(node_manager_port_position,
                  strlen(kNodeManagerPortPlaceholder),
                  std::to_string(GetServerPort()));
    }
  }
  // Disable metrics report if needed.
  if (!RayConfig::instance().enable_metrics_collection()) {
    agent_command_line.push_back("--disable-metrics-collection");
  }

  // Create a non-zero random agent_id to pass to the child process.
  // We cannot use pid an id because os.getpid() from the python process is not
  // reliable when using a launcher.
  // See https://github.com/ray-project/ray/issues/24361 and Python issue
  // https://github.com/python/cpython/issues/83086
  int agent_id = 0;
  while (agent_id == 0) {
    agent_id = rand();
  };
  std::string agent_id_str = std::to_string(agent_id);
  agent_command_line.push_back("--agent-id");
  agent_command_line.push_back(agent_id_str);

  std::string agent_name = "dashboard_agent/" + agent_id_str;
  // TODO(ryw): after thorough testing, we can disable the fate_shares flag and let a
  // dashboard agent crash no longer lead to a raylet crash.
  auto options = AgentManager::Options({self_node_id,
                                        agent_name,
                                        agent_command_line,
                                        /*fate_shares=*/true});
  return std::make_unique<AgentManager>(
      std::move(options),
      /*delay_executor=*/
      [this](std::function<void()> task, uint32_t delay_ms) {
        return execute_after(io_service_, task, std::chrono::milliseconds(delay_ms));
      },
      shutdown_raylet_gracefully_);
}

std::unique_ptr<AgentManager> NodeManager::CreateRuntimeEnvAgentManager(
    const NodeID &self_node_id, const NodeManagerConfig &config) {
  auto agent_command_line = ParseCommandLine(config.runtime_env_agent_command);

  if (agent_command_line.empty()) {
    return nullptr;
  }

  for (auto &arg : agent_command_line) {
    auto node_manager_port_position = arg.find(kNodeManagerPortPlaceholder);
    if (node_manager_port_position != std::string::npos) {
      arg.replace(node_manager_port_position,
                  strlen(kNodeManagerPortPlaceholder),
                  std::to_string(GetServerPort()));
    }
  }

  std::string agent_name = "runtime_env_agent";

  auto options = AgentManager::Options({self_node_id,
                                        agent_name,
                                        agent_command_line,
                                        /*fate_shares=*/true});
  return std::make_unique<AgentManager>(
      std::move(options),
      /*delay_executor=*/
      [this](std::function<void()> task, uint32_t delay_ms) {
        return execute_after(io_service_, task, std::chrono::milliseconds(delay_ms));
      },
      shutdown_raylet_gracefully_);
}

}  // namespace ray::raylet
