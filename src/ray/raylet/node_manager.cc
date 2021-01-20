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
#include <fstream>
#include <memory>

#include "ray/common/buffer.h"
#include "ray/common/common_protocol.h"
#include "ray/common/constants.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/pb_util.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/stats/stats.h"
#include "ray/util/asio_util.h"
#include "ray/util/sample.h"

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

namespace ray {

namespace raylet {

// A helper function to print the leased workers.
std::string LeasedWorkersSring(
    const std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>>
        &leased_workers) {
  std::stringstream buffer;
  buffer << "  @leased_workers: (";
  for (const auto &pair : leased_workers) {
    auto &worker = pair.second;
    buffer << worker->WorkerId() << ", ";
  }
  buffer << ")";
  return buffer.str();
}

// A helper function to print the workers in worker_pool_.
std::string WorkerPoolString(
    const std::vector<std::shared_ptr<WorkerInterface>> &worker_pool) {
  std::stringstream buffer;
  buffer << "   @worker_pool: (";
  for (const auto &worker : worker_pool) {
    buffer << worker->WorkerId() << ", ";
  }
  buffer << ")";
  return buffer.str();
}

// Helper function to print the worker's owner worker and and node owner.
std::string WorkerOwnerString(std::shared_ptr<WorkerInterface> &worker) {
  std::stringstream buffer;
  const auto owner_worker_id =
      WorkerID::FromBinary(worker->GetOwnerAddress().worker_id());
  const auto owner_node_id = WorkerID::FromBinary(worker->GetOwnerAddress().raylet_id());
  buffer << "leased_worker Lease " << worker->WorkerId() << " owned by "
         << owner_worker_id << " / " << owner_node_id;
  return buffer.str();
}

NodeManager::NodeManager(boost::asio::io_service &io_service, const NodeID &self_node_id,
                         const NodeManagerConfig &config, ObjectManager &object_manager,
                         std::shared_ptr<gcs::GcsClient> gcs_client,
                         std::shared_ptr<ObjectDirectoryInterface> object_directory,
                         std::function<bool(const ObjectID &)> is_plasma_object_spillable)
    : self_node_id_(self_node_id),
      io_service_(io_service),
      object_manager_(object_manager),
      gcs_client_(gcs_client),
      object_directory_(object_directory),
      heartbeat_timer_(io_service),
      heartbeat_period_(std::chrono::milliseconds(config.heartbeat_period_ms)),
      report_resources_timer_(io_service),
      report_resources_period_(
          std::chrono::milliseconds(config.report_resources_period_ms)),
      debug_dump_period_(config.debug_dump_period_ms),
      fair_queueing_enabled_(config.fair_queueing_enabled),
      object_pinning_enabled_(config.object_pinning_enabled),
      temp_dir_(config.temp_dir),
      object_manager_profile_timer_(io_service),
      initial_config_(config),
      local_available_resources_(config.resource_config),
      worker_pool_(io_service, config.num_workers_soft_limit,
                   config.num_initial_python_workers_for_first_job,
                   config.maximum_startup_concurrency, config.min_worker_port,
                   config.max_worker_port, config.worker_ports, gcs_client_,
                   config.worker_commands, config.raylet_config,
                   /*starting_worker_timeout_callback=*/
                   [this] { cluster_task_manager_->ScheduleAndDispatchTasks(); }),
      scheduling_policy_(local_queues_),
      reconstruction_policy_(
          io_service_,
          [this](const TaskID &task_id, const ObjectID &required_object_id) {
            HandleTaskReconstruction(task_id, required_object_id);
          },
          RayConfig::instance().object_timeout_milliseconds(), self_node_id_, gcs_client_,
          object_directory_),
      dependency_manager_(object_manager, reconstruction_policy_),
      node_manager_server_("NodeManager", config.node_manager_port),
      node_manager_service_(io_service, *this),
      agent_manager_service_handler_(
          new DefaultAgentManagerServiceHandler(agent_manager_)),
      agent_manager_service_(io_service, *agent_manager_service_handler_),
      client_call_manager_(io_service),
      worker_rpc_pool_(client_call_manager_),
      local_object_manager_(io_service_, RayConfig::instance().free_objects_batch_size(),
                            RayConfig::instance().free_objects_period_milliseconds(),
                            worker_pool_, gcs_client_->Objects(), worker_rpc_pool_,
                            /* object_pinning_enabled */ config.object_pinning_enabled,
                            /* automatic_object_deletion_enabled */
                            config.automatic_object_deletion_enabled,
                            /*max_io_workers*/ config.max_io_workers,
                            /*min_spilling_size*/ config.min_spilling_size,
                            [this](const std::vector<ObjectID> &object_ids) {
                              object_manager_.FreeObjects(object_ids,
                                                          /*local_only=*/false);
                            },
                            is_plasma_object_spillable),
      report_worker_backlog_(RayConfig::instance().report_worker_backlog()),
      last_local_gc_ns_(absl::GetCurrentTimeNanos()),
      local_gc_interval_ns_(RayConfig::instance().local_gc_interval_s() * 1e9),
      local_gc_min_interval_ns_(RayConfig::instance().local_gc_min_interval_s() * 1e9),
      record_metrics_period_(config.record_metrics_period_ms) {
  RAY_LOG(INFO) << "Initializing NodeManager with ID " << self_node_id_;
  RAY_CHECK(heartbeat_period_.count() > 0);
  // Initialize the resource map with own cluster resource configuration.
  cluster_resource_map_.emplace(self_node_id_,
                                SchedulingResources(config.resource_config));

  RAY_CHECK_OK(object_manager_.SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        ObjectID object_id = ObjectID::FromBinary(object_info.object_id);
        HandleObjectLocal(object_id);
      }));
  RAY_CHECK_OK(object_manager_.SubscribeObjDeleted(
      [this](const ObjectID &object_id) { HandleObjectMissing(object_id); }));

  if (RayConfig::instance().new_scheduler_enabled()) {
    SchedulingResources &local_resources = cluster_resource_map_[self_node_id_];
    cluster_resource_scheduler_ =
        std::shared_ptr<ClusterResourceScheduler>(new ClusterResourceScheduler(
            self_node_id_.Binary(),
            local_resources.GetTotalResources().GetResourceMap()));

    auto get_node_info_func = [this](const NodeID &node_id) {
      return gcs_client_->Nodes().Get(node_id);
    };
    auto is_owner_alive = [this](const WorkerID &owner_worker_id,
                                 const NodeID &owner_node_id) {
      return !(failed_workers_cache_.count(owner_worker_id) > 0 ||
               failed_nodes_cache_.count(owner_node_id) > 0);
    };
    auto announce_infeasible_task = [this](const Task &task) {
      PublishInfeasibleTaskError(task);
    };
    cluster_task_manager_ = std::shared_ptr<ClusterTaskManager>(new ClusterTaskManager(
        self_node_id_,
        std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_),
        dependency_manager_, is_owner_alive, get_node_info_func, announce_infeasible_task,
        worker_pool_, leased_workers_));
    placement_group_resource_manager_ =
        std::make_shared<NewPlacementGroupResourceManager>(
            std::dynamic_pointer_cast<ClusterResourceScheduler>(
                cluster_resource_scheduler_));
  } else {
    cluster_resource_scheduler_ = std::make_shared<OldClusterResourceScheduler>(
        self_node_id_, local_available_resources_, cluster_resource_map_,
        gcs_client_->NodeResources().GetLastResourceUsage());
    cluster_task_manager_ = std::shared_ptr<NodeManager>(this, [](NodeManager *) {
      // If new scheduler is not enabled, the `cluster_task_manager_` is this NodeManager
      // itself. So we create a `shared_ptr` that points to `this` and the deleter does
      // nothing.
    });
    placement_group_resource_manager_ =
        std::make_shared<OldPlacementGroupResourceManager>(
            local_available_resources_, cluster_resource_map_, self_node_id_);
  }

  RAY_CHECK_OK(store_client_.Connect(config.store_socket_name.c_str()));
  // Run the node manger rpc server.
  node_manager_server_.RegisterService(node_manager_service_);
  node_manager_server_.RegisterService(agent_manager_service_);
  node_manager_server_.Run();

  auto options =
      AgentManager::Options({self_node_id, ParseCommandLine(config.agent_command)});
  agent_manager_.reset(
      new AgentManager(std::move(options),
                       /*delay_executor=*/
                       [this](std::function<void()> task, uint32_t delay_ms) {
                         return execute_after(io_service_, task, delay_ms);
                       }));

  RAY_CHECK_OK(SetupPlasmaSubscription());
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
  auto on_done = [this](Status status) {
    RAY_CHECK_OK(status);
    // Subscribe to resource changes.
    const auto &resources_changed =
        [this](const rpc::NodeResourceChange &resource_notification) {
          auto id = NodeID::FromBinary(resource_notification.node_id());
          if (resource_notification.updated_resources_size() != 0) {
            ResourceSet resource_set(
                MapFromProtobuf(resource_notification.updated_resources()));
            ResourceCreateUpdated(id, resource_set);
          }

          if (resource_notification.deleted_resources_size() != 0) {
            ResourceDeleted(
                id, VectorFromProtobuf(resource_notification.deleted_resources()));
          }
        };
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncSubscribeToResources(
        /*subscribe_callback=*/resources_changed,
        /*done_callback=*/nullptr));
  };
  // Register a callback to monitor new nodes and a callback to monitor removed nodes.
  RAY_RETURN_NOT_OK(
      gcs_client_->Nodes().AsyncSubscribeToNodeChange(on_node_change, on_done));

  // Subscribe to resource usage batches from the monitor.
  const auto &resource_usage_batch_added =
      [this](const ResourceUsageBatchData &resource_usage_batch) {
        ResourceUsageBatchReceived(resource_usage_batch);
      };
  RAY_RETURN_NOT_OK(gcs_client_->NodeResources().AsyncSubscribeBatchedResourceUsage(
      resource_usage_batch_added, /*done*/ nullptr));

  // Subscribe to all unexpected failure notifications from the local and
  // remote raylets. Note that this does not include workers that failed due to
  // node failure. These workers can be identified by comparing the raylet_id
  // in their rpc::Address to the ID of a failed raylet.
  const auto &worker_failure_handler =
      [this](const rpc::WorkerDeltaData &worker_failure_data) {
        HandleUnexpectedWorkerFailure(worker_failure_data);
      };
  RAY_CHECK_OK(gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
      worker_failure_handler, /*done_callback=*/nullptr));

  // Subscribe to job updates.
  const auto job_subscribe_handler = [this](const JobID &job_id,
                                            const JobTableData &job_data) {
    if (!job_data.is_dead()) {
      HandleJobStarted(job_id, job_data);
    } else {
      HandleJobFinished(job_id, job_data);
    }
  };
  RAY_RETURN_NOT_OK(
      gcs_client_->Jobs().AsyncSubscribeAll(job_subscribe_handler, nullptr));

  // Start sending heartbeats to the GCS.
  last_heartbeat_at_ms_ = current_time_ms();
  last_debug_dump_at_ms_ = current_time_ms();
  Heartbeat();
  ReportResourceUsage();
  // Start the timer that gets object manager profiling information and sends it
  // to the GCS.
  GetObjectManagerProfileInfo();

  return ray::Status::OK();
}

void NodeManager::KillWorker(std::shared_ptr<WorkerInterface> worker) {
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
                                rpc::WorkerExitType disconnect_type) {
  // We should disconnect the client first. Otherwise, we'll remove bundle resources
  // before actual resources are returned. Subsequent disconnect request that comes
  // due to worker dead will be ignored.
  DisconnectClient(worker->Connection(), disconnect_type);
  worker->MarkDead();
  KillWorker(worker);
}

void NodeManager::HandleJobStarted(const JobID &job_id, const JobTableData &job_data) {
  RAY_LOG(DEBUG) << "HandleJobStarted " << job_id;
  RAY_CHECK(!job_data.is_dead());

  worker_pool_.HandleJobStarted(job_id, job_data.config());
  // Tasks of this job may already arrived but failed to pop a worker because the job
  // config is not local yet. So we trigger dispatching again here to try to
  // reschedule these tasks.
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleJobFinished(const JobID &job_id, const JobTableData &job_data) {
  RAY_LOG(DEBUG) << "HandleJobFinished " << job_id;
  RAY_CHECK(job_data.is_dead());
  worker_pool_.HandleJobFinished(job_id);

  auto workers = worker_pool_.GetWorkersRunningTasksForJob(job_id);
  // Kill all the workers. The actual cleanup for these workers is done
  // later when we receive the DisconnectClient message from them.
  for (const auto &worker : workers) {
    if (!worker->IsDetachedActor()) {
      // Clean up any open ray.wait calls that the worker made.
      dependency_manager_.CancelWaitRequest(worker->WorkerId());
      // Mark the worker as dead so further messages from it are ignored
      // (except DisconnectClient).
      worker->MarkDead();
      // Then kill the worker process.
      KillWorker(worker);
    }
  }
}

void NodeManager::Heartbeat() {
  uint64_t now_ms = current_time_ms();
  uint64_t interval = now_ms - last_heartbeat_at_ms_;
  if (interval > RayConfig::instance().num_heartbeats_warning() *
                     RayConfig::instance().raylet_heartbeat_timeout_milliseconds()) {
    RAY_LOG(WARNING)
        << "Last heartbeat was sent " << interval
        << " ms ago. There might be resource pressure on this node. If heartbeat keeps "
           "lagging, this node can be marked as dead mistakenly.";
  }
  last_heartbeat_at_ms_ = now_ms;
  stats::HeartbeatReportMs.Record(interval);

  auto heartbeat_data = std::make_shared<HeartbeatTableData>();
  heartbeat_data->set_node_id(self_node_id_.Binary());
  RAY_CHECK_OK(
      gcs_client_->Nodes().AsyncReportHeartbeat(heartbeat_data, [](Status status) {
        if (status.IsDisconnected()) {
          RAY_LOG(FATAL) << "This node has beem marked as dead.";
        }
      }));

  if (debug_dump_period_ > 0 &&
      static_cast<int64_t>(now_ms - last_debug_dump_at_ms_) > debug_dump_period_) {
    DumpDebugState();
    WarnResourceDeadlock();
    last_debug_dump_at_ms_ = now_ms;
  }

  if (record_metrics_period_ > 0 &&
      static_cast<int64_t>(now_ms - metrics_last_recorded_time_ms_) >
          record_metrics_period_) {
    RecordMetrics();
    metrics_last_recorded_time_ms_ = now_ms;
  }

  // Evict all copies of freed objects from the cluster.
  local_object_manager_.FlushFreeObjectsIfNeeded(now_ms);

  // Reset the timer.
  heartbeat_timer_.expires_from_now(heartbeat_period_);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    Heartbeat();
  });
}

void NodeManager::ReportResourceUsage() {
  auto resources_data = std::make_shared<rpc::ResourcesData>();
  resources_data->set_node_id(self_node_id_.Binary());
  // Update local chche from gcs remote cache, this is needed when gcs restart.
  // We should always keep the cache view consistent.
  cluster_resource_scheduler_->UpdateLastResourceUsage(
      gcs_client_->NodeResources().GetLastResourceUsage());
  cluster_resource_scheduler_->FillResourceUsage(resources_data);
  cluster_task_manager_->FillResourceUsage(resources_data);

  // Set the global gc bit on the outgoing heartbeat message.
  if (should_global_gc_) {
    resources_data->set_should_global_gc(true);
    should_global_gc_ = false;
  }

  // Trigger local GC if needed. This throttles the frequency of local GC calls
  // to at most once per heartbeat interval.
  auto now = absl::GetCurrentTimeNanos();
  if ((should_local_gc_ || now - last_local_gc_ns_ > local_gc_interval_ns_) &&
      now - last_local_gc_ns_ > local_gc_min_interval_ns_) {
    DoLocalGC();
    should_local_gc_ = false;
    last_local_gc_ns_ = now;
  }

  if (resources_data->resources_total_size() > 0 ||
      resources_data->resources_available_changed() ||
      resources_data->resource_load_changed() || resources_data->should_global_gc()) {
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncReportResourceUsage(resources_data,
                                                                       /*done*/ nullptr));
  }

  // Reset the timer.
  report_resources_timer_.expires_from_now(report_resources_period_);
  report_resources_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    ReportResourceUsage();
  });
}

void NodeManager::DoLocalGC() {
  auto all_workers = worker_pool_.GetAllRegisteredWorkers();
  for (const auto &driver : worker_pool_.GetAllRegisteredDrivers()) {
    all_workers.push_back(driver);
  }
  RAY_LOG(INFO) << "Sending Python GC request to " << all_workers.size()
                << " local workers to clean up Python cyclic references.";
  for (const auto &worker : all_workers) {
    rpc::LocalGCRequest request;
    worker->rpc_client()->LocalGC(
        request, [](const ray::Status &status, const rpc::LocalGCReply &r) {
          if (!status.ok()) {
            RAY_LOG(DEBUG) << "Failed to send local GC request: " << status.ToString();
          }
        });
  }
}

void NodeManager::HandleRequestObjectSpillage(
    const rpc::RequestObjectSpillageRequest &request,
    rpc::RequestObjectSpillageReply *reply, rpc::SendReplyCallback send_reply_callback) {
  local_object_manager_.SpillObjects(
      {ObjectID::FromBinary(request.object_id())},
      [reply, send_reply_callback](const ray::Status &status) {
        if (status.ok()) {
          reply->set_success(true);
        }
        send_reply_callback(Status::OK(), nullptr, nullptr);
      });
}

void NodeManager::HandleReleaseUnusedBundles(
    const rpc::ReleaseUnusedBundlesRequest &request,
    rpc::ReleaseUnusedBundlesReply *reply, rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Releasing unused bundles.";
  std::unordered_set<BundleID, pair_hash> in_use_bundles;
  for (int index = 0; index < request.bundles_in_use_size(); ++index) {
    const auto &bundle_id = request.bundles_in_use(index).bundle_id();
    in_use_bundles.emplace(
        std::make_pair(PlacementGroupID::FromBinary(bundle_id.placement_group_id()),
                       bundle_id.bundle_index()));
  }

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
        << "Destroying worker since its bundle was unused. Placement group id: "
        << worker->GetBundleId().first
        << ", bundle index: " << worker->GetBundleId().second
        << ", task id: " << worker->GetAssignedTaskId()
        << ", actor id: " << worker->GetActorId()
        << ", worker id: " << worker->WorkerId();
    DestroyWorker(worker, rpc::WorkerExitType::UNUSED_RESOURCE_RELEASED);
  }

  // Return unused bundle resources.
  placement_group_resource_manager_->ReturnUnusedBundle(in_use_bundles);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// TODO(edoakes): this function is problematic because it both sends warnings spuriously
// under normal conditions and sometimes doesn't send a warning under actual deadlock
// conditions. The current logic is to push a warning when: all running tasks are
// blocked, there is at least one ready task, and a warning hasn't been pushed in
// debug_dump_period_ milliseconds.
// See https://github.com/ray-project/ray/issues/5790 for details.
void NodeManager::WarnResourceDeadlock() {
  ray::Task exemplar;
  bool any_pending = false;
  int pending_actor_creations = 0;
  int pending_tasks = 0;
  std::string available_resources;

  // Check if any progress is being made on this raylet.
  for (const auto &worker : worker_pool_.GetAllRegisteredWorkers()) {
    if (!worker->IsDead() && !worker->GetAssignedTaskId().IsNil() &&
        !worker->IsBlocked() && worker->GetActorId().IsNil()) {
      // Progress is being made in a task, don't warn.
      resource_deadlock_warned_ = 0;
      return;
    }
  }

  // Check if any tasks are blocked on resource acquisition.
  if (!cluster_task_manager_->AnyPendingTasks(&exemplar, &any_pending,
                                              &pending_actor_creations, &pending_tasks)) {
    // No pending tasks, no need to warn.
    resource_deadlock_warned_ = 0;
    return;
  }
  available_resources = cluster_resource_scheduler_->GetLocalResourceViewString();

  // Push an warning to the driver that a task is blocked trying to acquire resources.
  // To avoid spurious triggers, only take action starting with the second time.
  // case resource_deadlock_warned_:  0 => first time, don't do anything yet
  // case resource_deadlock_warned_:  1 => second time, print a warning
  // case resource_deadlock_warned_: >1 => global gc but don't print any warnings
  std::ostringstream error_message;
  if (any_pending && resource_deadlock_warned_++ > 0) {
    // Actor references may be caught in cycles, preventing them from being deleted.
    // Trigger global GC to hopefully free up resource slots.
    TriggerGlobalGC();

    // Suppress duplicates warning messages.
    if (resource_deadlock_warned_ > 2) {
      return;
    }

    error_message
        << "The actor or task with ID " << exemplar.GetTaskSpecification().TaskId()
        << " cannot be scheduled right now. It requires "
        << exemplar.GetTaskSpecification().GetRequiredPlacementResources().ToString()
        << " for placement, but this node only has remaining " << available_resources
        << ". In total there are " << pending_tasks << " pending tasks and "
        << pending_actor_creations << " pending actors on this node. "
        << "This is likely due to all cluster resources being claimed by actors. "
        << "To resolve the issue, consider creating fewer actors or increase the "
        << "resources available to this Ray cluster. You can ignore this message "
        << "if this Ray cluster is expected to auto-scale.";
    auto error_data_ptr = gcs::CreateErrorTableData(
        "resource_deadlock", error_message.str(), current_time_ms(),
        exemplar.GetTaskSpecification().JobId());
    RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
  }
}

void NodeManager::GetObjectManagerProfileInfo() {
  int64_t start_time_ms = current_time_ms();

  auto profile_info = object_manager_.GetAndResetProfilingInfo();

  if (profile_info->profile_events_size() > 0) {
    RAY_CHECK_OK(gcs_client_->Stats().AsyncAddProfileData(profile_info, nullptr));
  }

  // Reset the timer.
  object_manager_profile_timer_.expires_from_now(heartbeat_period_);
  object_manager_profile_timer_.async_wait(
      [this](const boost::system::error_code &error) {
        RAY_CHECK(!error);
        GetObjectManagerProfileInfo();
      });

  int64_t interval = current_time_ms() - start_time_ms;
  if (interval > RayConfig::instance().handler_warning_timeout_ms()) {
    RAY_LOG(WARNING) << "GetObjectManagerProfileInfo handler took " << interval << " ms.";
  }
}

void NodeManager::NodeAdded(const GcsNodeInfo &node_info) {
  const NodeID node_id = NodeID::FromBinary(node_info.node_id());

  RAY_LOG(DEBUG) << "[NodeAdded] Received callback from node id " << node_id;
  if (1 == cluster_resource_map_.count(node_id)) {
    RAY_LOG(DEBUG) << "Received notification of a new node that already exists: "
                   << node_id;
    return;
  }

  if (node_id == self_node_id_) {
    // We got a notification for ourselves, so we are connected to the GCS now.
    // Save this NodeManager's resource information in the cluster resource map.
    cluster_resource_map_[node_id] = initial_config_.resource_config;
    return;
  }

  // Store address of the new node manager for rpc requests.
  remote_node_manager_addresses_[node_id] =
      std::make_pair(node_info.node_manager_address(), node_info.node_manager_port());

  // Fetch resource info for the remote node and update cluster resource map.
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetResources(
      node_id,
      [this, node_id](
          Status status,
          const boost::optional<gcs::NodeResourceInfoAccessor::ResourceMap> &data) {
        if (data) {
          ResourceSet resource_set;
          for (auto &resource_entry : *data) {
            resource_set.AddOrUpdateResource(resource_entry.first,
                                             resource_entry.second->resource_capacity());
          }
          ResourceCreateUpdated(node_id, resource_set);
        }
      }));
}

void NodeManager::NodeRemoved(const NodeID &node_id) {
  // TODO(swang): If we receive a notification for our own death, clean up and
  // exit immediately.
  RAY_LOG(DEBUG) << "[NodeRemoved] Received callback from node id " << node_id;

  RAY_CHECK(node_id != self_node_id_)
      << "Exiting because this node manager has mistakenly been marked dead by the "
      << "monitor: GCS didn't receive heartbeats within timeout "
      << RayConfig::instance().num_heartbeats_timeout() *
             RayConfig::instance().raylet_heartbeat_timeout_milliseconds()
      << " ms. This is likely since the machine or raylet became overloaded.";

  // Below, when we remove node_id from all of these data structures, we could
  // check that it is actually removed, or log a warning otherwise, but that may
  // not be necessary.

  // Remove the node from the resource map.
  if (!cluster_resource_scheduler_->RemoveNode(node_id.Binary())) {
    RAY_LOG(DEBUG) << "Received NodeRemoved callback for an unknown node: " << node_id
                   << ".";
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
    RAY_LOG(DEBUG) << "Worker " << worker_id << " failed";
    failed_workers_cache_.insert(worker_id);
  } else {
    RAY_CHECK(!node_id.IsNil());
    failed_nodes_cache_.insert(node_id);
  }

  // TODO(swang): Also clean up any lease requests owned by the failed worker
  // from the task queues. This is only necessary for lease requests that are
  // infeasible, since requests that are fulfilled will get canceled during
  // dispatch.
  for (const auto &pair : leased_workers_) {
    auto &worker = pair.second;
    const auto owner_worker_id =
        WorkerID::FromBinary(worker->GetOwnerAddress().worker_id());
    const auto owner_node_id =
        WorkerID::FromBinary(worker->GetOwnerAddress().raylet_id());
    RAY_LOG(DEBUG) << "Lease " << worker->WorkerId() << " owned by " << owner_worker_id;
    RAY_CHECK(!owner_worker_id.IsNil() && !owner_node_id.IsNil());
    if (!worker->IsDetachedActor()) {
      // TODO (Alex): Cancel all pending child tasks of the tasks whose owners have failed
      // because the owner could've submitted lease requests before failing.
      if (!worker_id.IsNil()) {
        // If the failed worker was a leased worker's owner, then kill the leased worker.
        if (owner_worker_id == worker_id) {
          RAY_LOG(INFO) << "Owner process " << owner_worker_id
                        << " died, killing leased worker " << worker->WorkerId();
          KillWorker(worker);
        }
      } else if (owner_node_id == node_id) {
        // If the leased worker's owner was on the failed node, then kill the leased
        // worker.
        RAY_LOG(INFO) << "Owner node " << owner_node_id << " died, killing leased worker "
                      << worker->WorkerId();
        KillWorker(worker);
      }
    }
  }
}

void NodeManager::ResourceCreateUpdated(const NodeID &node_id,
                                        const ResourceSet &createUpdatedResources) {
  RAY_LOG(DEBUG) << "[ResourceCreateUpdated] received callback from node id " << node_id
                 << " with created or updated resources: "
                 << createUpdatedResources.ToString() << ". Updating resource map.";

  // Update local_available_resources_ and SchedulingResources
  for (const auto &resource_pair : createUpdatedResources.GetResourceMap()) {
    const std::string &resource_label = resource_pair.first;
    const double &new_resource_capacity = resource_pair.second;
    cluster_resource_scheduler_->UpdateResourceCapacity(node_id.Binary(), resource_label,
                                                        new_resource_capacity);
  }
  RAY_LOG(DEBUG) << "[ResourceCreateUpdated] Updated cluster_resource_map.";

  if (node_id == self_node_id_) {
    // The resource update is on the local node, check if we can reschedule tasks.
    cluster_task_manager_->ScheduleInfeasibleTasks();
  }
}

void NodeManager::ResourceDeleted(const NodeID &node_id,
                                  const std::vector<std::string> &resource_names) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    std::ostringstream oss;
    for (auto &resource_name : resource_names) {
      oss << resource_name << ", ";
    }
    RAY_LOG(DEBUG) << "[ResourceDeleted] received callback from node id " << node_id
                   << " with deleted resources: " << oss.str()
                   << ". Updating resource map.";
  }

  // Update local_available_resources_ and SchedulingResources
  for (const auto &resource_label : resource_names) {
    cluster_resource_scheduler_->DeleteResource(node_id.Binary(), resource_label);
  }
  return;
}

void NodeManager::TryLocalInfeasibleTaskScheduling() {
  RAY_LOG(DEBUG) << "[LocalResourceUpdateRescheduler] The resource update is on the "
                    "local node, check if we can reschedule tasks";

  SchedulingResources &new_local_resources = cluster_resource_map_[self_node_id_];

  // SpillOver locally to figure out which infeasible tasks can be placed now
  std::vector<TaskID> decision =
      scheduling_policy_.SpillOverInfeasibleTasks(new_local_resources);

  std::unordered_set<TaskID> local_task_ids(decision.begin(), decision.end());

  // Transition locally placed tasks to waiting or ready for dispatch.
  if (local_task_ids.size() > 0) {
    std::vector<Task> tasks = local_queues_.RemoveTasks(local_task_ids);
    for (const auto &t : tasks) {
      EnqueuePlaceableTask(t);
    }
  }
}

void NodeManager::UpdateResourceUsage(const NodeID &node_id,
                                      const rpc::ResourcesData &resource_data) {
  if (!cluster_resource_scheduler_->UpdateNode(node_id.Binary(), resource_data)) {
    RAY_LOG(INFO)
        << "[UpdateResourceUsage]: received resource usage from unknown node id "
        << node_id;
    return;
  }

  // Trigger local GC at the next heartbeat interval.
  if (resource_data.should_global_gc()) {
    should_local_gc_ = true;
  }

  // If light resource usage report enabled, we update remote resources only when related
  // resources map in heartbeat is not empty.
  cluster_task_manager_->OnNodeResourceUsageUpdated(node_id, resource_data);
}

void NodeManager::ResourceUsageBatchReceived(
    const ResourceUsageBatchData &resource_usage_batch) {
  // Update load information provided by each message.
  for (const auto &resource_usage : resource_usage_batch.batch()) {
    const NodeID &node_id = NodeID::FromBinary(resource_usage.node_id());
    if (node_id == self_node_id_) {
      // Skip messages from self.
      continue;
    }
    UpdateResourceUsage(node_id, resource_usage);
  }
}

void NodeManager::ProcessNewClient(ClientConnection &client) {
  // The new client is a worker, so begin listening for messages.
  client.ProcessMessages();
}

// A helper function to create a mapping from task scheduling class to
// tasks with that class from a given list of tasks.
std::unordered_map<SchedulingClass, ordered_set<TaskID>> NodeManager::MakeTasksByClass(
    const std::vector<Task> &tasks) const {
  std::unordered_map<SchedulingClass, ordered_set<TaskID>> result;
  for (const auto &task : tasks) {
    auto spec = task.GetTaskSpecification();
    result[spec.GetSchedulingClass()].push_back(spec.TaskId());
  }
  return result;
}

void NodeManager::DispatchTasks(
    const std::unordered_map<SchedulingClass, ordered_set<TaskID>> &tasks_by_class) {
  // Dispatch tasks in priority order by class. This avoids starvation problems where
  // one class of tasks become stuck behind others in the queue, causing Ray to start
  // many workers. See #3644 for a more detailed description of this issue.
  std::vector<const std::pair<const SchedulingClass, ordered_set<TaskID>> *> fair_order;
  for (auto &it : tasks_by_class) {
    fair_order.emplace_back(&it);
  }
  // Prioritize classes that have fewer currently running tasks. Note that we only
  // sort once per round of task dispatch, which is less fair then it could be, but
  // is simpler and faster.
  if (fair_queueing_enabled_) {
    std::sort(
        std::begin(fair_order), std::end(fair_order),
        [this](const std::pair<const SchedulingClass, ordered_set<ray::TaskID>> *a,
               const std::pair<const SchedulingClass, ordered_set<ray::TaskID>> *b) {
          return local_queues_.NumRunning(a->first) < local_queues_.NumRunning(b->first);
        });
  }
  std::vector<std::function<void()>> post_assign_callbacks;
  // Approximate fair round robin between classes.
  for (const auto &it : fair_order) {
    const auto &task_resources =
        TaskSpecification::GetSchedulingClassDescriptor(it->first);
    // FIFO order within each class.
    for (const auto &task_id : it->second) {
      const auto &task = local_queues_.GetTaskOfState(task_id, TaskState::READY);
      if (!local_available_resources_.Contains(task_resources)) {
        // All the tasks in it.second have the same resource shape, so
        // once the first task is not feasible, we can break out of this loop
        break;
      }

      // Try to get an idle worker to execute this task. If nullptr, there
      // aren't any available workers so we can't assign the task.
      std::shared_ptr<WorkerInterface> worker =
          worker_pool_.PopWorker(task.GetTaskSpecification());
      if (worker != nullptr) {
        AssignTask(worker, task, &post_assign_callbacks);
      }
    }
  }
  // Call the callbacks from the AssignTask calls above. These need to be called
  // after the above loop, as they may alter the scheduling queues and invalidate
  // the loop iterator.
  for (auto &func : post_assign_callbacks) {
    func();
  }
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
    if (message_type_value != protocol::MessageType::DisconnectClient) {
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
  case protocol::MessageType::TaskDone: {
    HandleWorkerAvailable(client);
  } break;
  case protocol::MessageType::DisconnectClient: {
    ProcessDisconnectClientMessage(client, message_data);
    // We don't need to receive future messages from this client,
    // because it's already disconnected.
    return;
  } break;
  case protocol::MessageType::SubmitTask: {
    // For tasks submitted via the raylet path, we must make sure to order the
    // task submission so that tasks are always submitted after the tasks that
    // they depend on.
    ProcessSubmitTaskMessage(message_data);
  } break;
  case protocol::MessageType::SetResourceRequest: {
    ProcessSetResourceRequest(client, message_data);
  } break;
  case protocol::MessageType::FetchOrReconstruct: {
    ProcessFetchOrReconstructMessage(client, message_data);
  } break;
  case protocol::MessageType::NotifyDirectCallTaskBlocked: {
    ProcessDirectCallTaskBlocked(client, message_data);
  } break;
  case protocol::MessageType::NotifyDirectCallTaskUnblocked: {
    std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
    HandleDirectCallTaskUnblocked(worker);
  } break;
  case protocol::MessageType::NotifyUnblocked: {
    auto message = flatbuffers::GetRoot<protocol::NotifyUnblocked>(message_data);
    AsyncResolveObjectsFinish(client, from_flatbuf<TaskID>(*message->task_id()),
                              /*was_blocked*/ true);
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
  case protocol::MessageType::PushProfileEventsRequest: {
    auto fbs_message = flatbuffers::GetRoot<flatbuffers::String>(message_data);
    auto profile_table_data = std::make_shared<rpc::ProfileTableData>();
    RAY_CHECK(
        profile_table_data->ParseFromArray(fbs_message->data(), fbs_message->size()));
    RAY_CHECK_OK(gcs_client_->Stats().AsyncAddProfileData(profile_table_data, nullptr));
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
  client->Register();

  auto message = flatbuffers::GetRoot<protocol::RegisterClientRequest>(message_data);
  Language language = static_cast<Language>(message->language());
  const JobID job_id = from_flatbuf<JobID>(*message->job_id());
  WorkerID worker_id = from_flatbuf<WorkerID>(*message->worker_id());
  pid_t pid = message->worker_pid();
  std::string worker_ip_address = string_from_flatbuf(*message->ip_address());
  // TODO(suquark): Use `WorkerType` in `common.proto` without type converting.
  rpc::WorkerType worker_type = static_cast<rpc::WorkerType>(message->worker_type());
  if (((worker_type != rpc::WorkerType::SPILL_WORKER &&
        worker_type != rpc::WorkerType::RESTORE_WORKER)) ||
      worker_type == rpc::WorkerType::DRIVER) {
    RAY_CHECK(!job_id.IsNil());
  } else {
    RAY_CHECK(job_id.IsNil());
  }
  auto worker = std::dynamic_pointer_cast<WorkerInterface>(
      std::make_shared<Worker>(job_id, worker_id, language, worker_type,
                               worker_ip_address, client, client_call_manager_));

  auto send_reply_callback = [this, client](Status status, int assigned_port) {
    flatbuffers::FlatBufferBuilder fbb;
    std::vector<std::string> system_config_keys;
    std::vector<std::string> system_config_values;
    for (auto kv : initial_config_.raylet_config) {
      system_config_keys.push_back(kv.first);
      system_config_values.push_back(kv.second);
    }
    auto reply = ray::protocol::CreateRegisterClientReply(
        fbb, status.ok(), fbb.CreateString(status.ToString()),
        to_flatbuf(fbb, self_node_id_), assigned_port,
        string_vec_to_flatbuf(fbb, system_config_keys),
        string_vec_to_flatbuf(fbb, system_config_values));
    fbb.Finish(reply);
    client->WriteMessageAsync(
        static_cast<int64_t>(protocol::MessageType::RegisterClientReply), fbb.GetSize(),
        fbb.GetBufferPointer(), [this, client](const ray::Status &status) {
          if (!status.ok()) {
            DisconnectClient(client);
          }
        });
  };

  if (worker_type == rpc::WorkerType::WORKER ||
      worker_type == rpc::WorkerType::SPILL_WORKER ||
      worker_type == rpc::WorkerType::RESTORE_WORKER) {
    // Register the new worker.
    auto status = worker_pool_.RegisterWorker(worker, pid, send_reply_callback);
    if (!status.ok()) {
      // If the worker failed to register to Raylet, trigger task dispatching here to
      // allow new worker processes to be started (if capped by
      // maximum_startup_concurrency).
      cluster_task_manager_->ScheduleAndDispatchTasks();
    }
  } else {
    // Register the new driver.
    RAY_CHECK(pid >= 0);
    worker->SetProcess(Process::FromPid(pid));
    // Compute a dummy driver task id from a given driver.
    const TaskID driver_task_id = TaskID::ComputeDriverTaskId(worker_id);
    worker->AssignTaskId(driver_task_id);
    rpc::JobConfig job_config;
    job_config.ParseFromString(message->serialized_job_config()->str());
    Status status = worker_pool_.RegisterDriver(worker, job_config, send_reply_callback);
    if (status.ok()) {
      local_queues_.AddDriverTaskId(driver_task_id);
      auto job_data_ptr =
          gcs::CreateJobTableData(job_id, /*is_dead*/ false, std::time(nullptr),
                                  worker_ip_address, pid, job_config);
      RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(job_data_ptr, nullptr));
    }
  }
}

void NodeManager::ProcessAnnounceWorkerPortMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  bool is_worker = true;
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (worker == nullptr) {
    is_worker = false;
    worker = worker_pool_.GetRegisteredDriver(client);
  }
  RAY_CHECK(worker != nullptr) << "No worker exists for CoreWorker with client: "
                               << client->DebugString();

  auto message = flatbuffers::GetRoot<protocol::AnnounceWorkerPort>(message_data);
  int port = message->port();
  worker->Connect(port);
  if (is_worker) {
    worker_pool_.OnWorkerStarted(worker);
    HandleWorkerAvailable(worker->Connection());
  }
}

void NodeManager::HandleWorkerAvailable(const std::shared_ptr<ClientConnection> &client) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  HandleWorkerAvailable(worker);
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

  // Local resource availability changed: invoke scheduling policy for local node.
  cluster_resource_map_[self_node_id_].SetLoadResources(
      local_queues_.GetTotalResourceLoad());

  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::DisconnectClient(const std::shared_ptr<ClientConnection> &client,
                                   rpc::WorkerExitType disconnect_type) {
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
      RAY_LOG(INFO) << "Ignoring client disconnect because the client has already "
                    << "been disconnected.";
      return;
    }
  }
  RAY_CHECK(!(is_worker && is_driver));
  // If the client has any blocked tasks, mark them as unblocked. In
  // particular, we are no longer waiting for their dependencies.
  if (is_worker && worker->IsDead()) {
    // If the worker was killed by us because the driver exited,
    // treat it as intentionally disconnected.
    // Don't need to unblock the client if it's a worker and is already dead.
    // Because in this case, its task is already cleaned up.
    RAY_LOG(DEBUG) << "Skip unblocking worker because it's already dead.";
  } else {
    // Clean up any open ray.get calls that the worker made.
    while (!worker->GetBlockedTaskIds().empty()) {
      // NOTE(swang): AsyncResolveObjectsFinish will modify the worker, so it is
      // not safe to pass in the iterator directly.
      const TaskID task_id = *worker->GetBlockedTaskIds().begin();
      AsyncResolveObjectsFinish(client, task_id, true);
    }
    // Clean up any open ray.wait calls that the worker made.
    dependency_manager_.CancelWaitRequest(worker->WorkerId());
  }

  // Erase any lease metadata.
  leased_workers_.erase(worker->WorkerId());

  // Publish the worker failure.
  auto worker_failure_data_ptr =
      gcs::CreateWorkerFailureData(self_node_id_, worker->WorkerId(), worker->IpAddress(),
                                   worker->Port(), time(nullptr), disconnect_type);
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
        Task task;
        static_cast<void>(local_queues_.RemoveTask(task_id, &task));
      }

      if (disconnect_type == rpc::WorkerExitType::SYSTEM_ERROR_EXIT) {
        // Push the error to driver.
        const JobID &job_id = worker->GetAssignedJobId();
        // TODO(rkn): Define this constant somewhere else.
        std::string type = "worker_died";
        std::ostringstream error_message;
        error_message << "A worker died or was killed while executing task " << task_id
                      << ".";
        auto error_data_ptr = gcs::CreateErrorTableData(type, error_message.str(),
                                                        current_time_ms(), job_id);
        RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
      }
    }

    // Remove the dead client from the pool and stop listening for messages.
    worker_pool_.DisconnectWorker(worker);

    // Return the resources that were being used by this worker.
    cluster_task_manager_->ReleaseWorkerResources(worker);

    // Since some resources may have been released, we can try to dispatch more tasks. YYY
    cluster_task_manager_->ScheduleAndDispatchTasks();
  } else if (is_driver) {
    // The client is a driver.
    const auto job_id = worker->GetAssignedJobId();
    RAY_CHECK(!job_id.IsNil());
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFinished(job_id, nullptr));
    const auto driver_id = ComputeDriverIdFromJob(job_id);
    local_queues_.RemoveDriverTaskId(TaskID::ComputeDriverTaskId(driver_id));
    worker_pool_.DisconnectDriver(worker);

    RAY_LOG(INFO) << "Driver (pid=" << worker->GetProcess().GetId()
                  << ") is disconnected. "
                  << "job_id: " << worker->GetAssignedJobId();
  }

  client->Close();

  // TODO(rkn): Tell the object manager that this client has disconnected so
  // that it can clean up the wait requests for this client. Currently I think
  // these can be leaked.
}

void NodeManager::ProcessDisconnectClientMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::DisconnectClient>(message_data);
  auto disconnect_type = static_cast<rpc::WorkerExitType>(message->disconnect_type());
  DisconnectClient(client, disconnect_type);
}

void NodeManager::ProcessFetchOrReconstructMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::FetchOrReconstruct>(message_data);
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());
  if (message->fetch_only()) {
    std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
    if (!worker) {
      worker = worker_pool_.GetRegisteredDriver(client);
    }
    if (worker) {
      // This will start a fetch for the objects that gets canceled once the
      // objects are local, or if the worker dies.
      dependency_manager_.StartOrUpdateWaitRequest(worker->WorkerId(), refs);
    }
  } else {
    // The values are needed. Add all requested objects to the list to
    // subscribe to in the task dependency manager. These objects will be
    // pulled from remote node managers. If an object's owner dies, an error
    // will be stored as the object's value.
    const TaskID task_id = from_flatbuf<TaskID>(*message->task_id());
    AsyncResolveObjects(client, refs, task_id, /*ray_get=*/true,
                        /*mark_worker_blocked*/ message->mark_worker_blocked());
  }
}

void NodeManager::ProcessDirectCallTaskBlocked(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message =
      flatbuffers::GetRoot<protocol::NotifyDirectCallTaskBlocked>(message_data);
  bool release_resources = message->release_resources();
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  HandleDirectCallTaskBlocked(worker, release_resources);
}

void NodeManager::ProcessWaitRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message = flatbuffers::GetRoot<protocol::WaitRequest>(message_data);
  std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
  const auto refs =
      FlatbufferToObjectReference(*message->object_ids(), *message->owner_addresses());
  std::unordered_map<ObjectID, rpc::Address> owner_addresses;
  for (const auto &ref : refs) {
    owner_addresses.emplace(ObjectID::FromBinary(ref.object_id()), ref.owner_address());
  }

  bool resolve_objects = false;
  for (auto const &object_id : object_ids) {
    if (!dependency_manager_.CheckObjectLocal(object_id)) {
      // At least one object requires resolution.
      resolve_objects = true;
    }
  }

  const TaskID &current_task_id = from_flatbuf<TaskID>(*message->task_id());
  bool was_blocked = message->mark_worker_blocked();
  if (resolve_objects) {
    // Resolve any missing objects. This is a no-op for any objects that are
    // already local. Missing objects will be pulled from remote node managers.
    // If an object's owner dies, an error will be stored as the object's
    // value.
    AsyncResolveObjects(client, refs, current_task_id, /*ray_get=*/false,
                        /*mark_worker_blocked*/ was_blocked);
  }
  int64_t wait_ms = message->timeout();
  uint64_t num_required_objects = static_cast<uint64_t>(message->num_ready_objects());
  // TODO Remove in the future since it should have already be done in other place
  ray::Status status = object_manager_.Wait(
      object_ids, owner_addresses, wait_ms, num_required_objects,
      [this, resolve_objects, was_blocked, client, current_task_id](
          std::vector<ObjectID> found, std::vector<ObjectID> remaining) {
        // Write the data.
        flatbuffers::FlatBufferBuilder fbb;
        flatbuffers::Offset<protocol::WaitReply> wait_reply = protocol::CreateWaitReply(
            fbb, to_flatbuf(fbb, found), to_flatbuf(fbb, remaining));
        fbb.Finish(wait_reply);

        auto status =
            client->WriteMessage(static_cast<int64_t>(protocol::MessageType::WaitReply),
                                 fbb.GetSize(), fbb.GetBufferPointer());
        if (status.ok()) {
          // The client is unblocked now because the wait call has returned.
          if (resolve_objects) {
            AsyncResolveObjectsFinish(client, current_task_id, was_blocked);
          }
        } else {
          // We failed to write to the client, so disconnect the client.
          DisconnectClient(client);
        }
      });
  RAY_CHECK_OK(status);
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
  std::unordered_map<ObjectID, rpc::Address> owner_addresses;
  for (const auto &ref : refs) {
    owner_addresses.emplace(ObjectID::FromBinary(ref.object_id()), ref.owner_address());
  }
  AsyncResolveObjects(client, refs, TaskID::Nil(), /*ray_get=*/false,
                      /*mark_worker_blocked*/ false);
  // Reply to the client once a location has been found for all arguments.
  // NOTE(swang): ObjectManager::Wait currently returns as soon as any location
  // has been found, so the object may still be on a remote node when the
  // client receives the reply.
  ray::Status status = object_manager_.Wait(
      object_ids, owner_addresses, -1, object_ids.size(),
      [this, client, tag](std::vector<ObjectID> found, std::vector<ObjectID> remaining) {
        RAY_CHECK(remaining.empty());
        std::shared_ptr<WorkerInterface> worker =
            worker_pool_.GetRegisteredWorker(client);
        if (!worker) {
          RAY_LOG(ERROR) << "Lost worker for wait request " << client;
        } else {
          worker->DirectActorCallArgWaitComplete(tag);
        }
      });
  RAY_CHECK_OK(status);
}

void NodeManager::ProcessPushErrorRequestMessage(const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::PushErrorRequest>(message_data);

  auto const &type = string_from_flatbuf(*message->type());
  auto const &error_message = string_from_flatbuf(*message->error_message());
  double timestamp = message->timestamp();
  JobID job_id = from_flatbuf<JobID>(*message->job_id());
  auto error_data_ptr = gcs::CreateErrorTableData(type, error_message, timestamp, job_id);
  RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
}

void NodeManager::ProcessSubmitTaskMessage(const uint8_t *message_data) {
  // Read the task submitted by the client.
  auto fbs_message = flatbuffers::GetRoot<protocol::SubmitTaskRequest>(message_data);
  rpc::Task task_message;
  RAY_CHECK(task_message.mutable_task_spec()->ParseFromArray(
      fbs_message->task_spec()->data(), fbs_message->task_spec()->size()));

  // Submit the task to the raylet. Since the task was submitted
  // locally, there is no uncommitted lineage.
  SubmitTask(Task(task_message));
}

void NodeManager::HandleRequestWorkerLease(const rpc::RequestWorkerLeaseRequest &request,
                                           rpc::RequestWorkerLeaseReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  rpc::Task task_message;
  task_message.mutable_task_spec()->CopyFrom(request.resource_spec());
  auto backlog_size = -1;
  if (report_worker_backlog_) {
    backlog_size = request.backlog_size();
  }
  Task task(task_message, backlog_size);
  bool is_actor_creation_task = task.GetTaskSpecification().IsActorCreationTask();
  ActorID actor_id = ActorID::Nil();
  metrics_num_task_scheduled_ += 1;

  if (is_actor_creation_task) {
    actor_id = task.GetTaskSpecification().ActorCreationId();

    // Save the actor creation task spec to GCS, which is needed to
    // reconstruct the actor when raylet detect it dies.
    std::shared_ptr<rpc::TaskTableData> data = std::make_shared<rpc::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(
        task.GetTaskSpecification().GetMessage());
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncAdd(data, nullptr));
  }

  if (RayConfig::instance().enable_worker_prestart()) {
    auto task_spec = task.GetTaskSpecification();
    worker_pool_.PrestartWorkers(task_spec, request.backlog_size());
  }

  cluster_task_manager_->QueueAndScheduleTask(task, reply, send_reply_callback);
}

void NodeManager::HandlePrepareBundleResources(
    const rpc::PrepareBundleResourcesRequest &request,
    rpc::PrepareBundleResourcesReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto bundle_spec = BundleSpecification(request.bundle_spec());
  RAY_LOG(DEBUG) << "Request to prepare bundle resources is received, "
                 << bundle_spec.DebugString();

  auto prepared = placement_group_resource_manager_->PrepareBundle(bundle_spec);
  reply->set_success(prepared);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleCommitBundleResources(
    const rpc::CommitBundleResourcesRequest &request,
    rpc::CommitBundleResourcesReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto bundle_spec = BundleSpecification(request.bundle_spec());
  RAY_LOG(DEBUG) << "Request to commit bundle resources is received, "
                 << bundle_spec.DebugString();
  placement_group_resource_manager_->CommitBundle(bundle_spec);
  send_reply_callback(Status::OK(), nullptr, nullptr);

  cluster_task_manager_->ScheduleInfeasibleTasks();
  cluster_task_manager_->ScheduleAndDispatchTasks();
}

void NodeManager::HandleCancelResourceReserve(
    const rpc::CancelResourceReserveRequest &request,
    rpc::CancelResourceReserveReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto bundle_spec = BundleSpecification(request.bundle_spec());
  RAY_LOG(INFO) << "Request to cancel reserved resource is received, "
                << bundle_spec.DebugString();

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
    RAY_LOG(DEBUG)
        << "Destroying worker since its placement group was removed. Placement group id: "
        << worker->GetBundleId().first
        << ", bundle index: " << bundle_spec.BundleId().second
        << ", task id: " << worker->GetAssignedTaskId()
        << ", actor id: " << worker->GetActorId()
        << ", worker id: " << worker->WorkerId();
    DestroyWorker(worker, rpc::WorkerExitType::PLACEMENT_GROUP_REMOVED);
  }

  // Return bundle resources.
  placement_group_resource_manager_->ReturnBundle(bundle_spec);
  cluster_task_manager_->ScheduleInfeasibleTasks();
  cluster_task_manager_->ScheduleAndDispatchTasks();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleReturnWorker(const rpc::ReturnWorkerRequest &request,
                                     rpc::ReturnWorkerReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  // Read the resource spec submitted by the client.
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  std::shared_ptr<WorkerInterface> worker = leased_workers_[worker_id];

  Status status;
  leased_workers_.erase(worker_id);

  if (worker) {
    if (request.disconnect_worker()) {
      DisconnectClient(worker->Connection());
    } else {
      // Handle the edge case where the worker was returned before we got the
      // unblock RPC by unblocking it immediately (unblock is idempotent).
      if (worker->IsBlocked()) {
        HandleDirectCallTaskUnblocked(worker);
      }
      cluster_task_manager_->ReturnWorkerResources(worker);
      HandleWorkerAvailable(worker);
    }
  } else {
    status = Status::Invalid("Returned worker does not exist any more");
  }
  send_reply_callback(status, nullptr, nullptr);
}

void NodeManager::HandleReleaseUnusedWorkers(
    const rpc::ReleaseUnusedWorkersRequest &request,
    rpc::ReleaseUnusedWorkersReply *reply, rpc::SendReplyCallback send_reply_callback) {
  std::unordered_set<WorkerID> in_use_worker_ids;
  for (int index = 0; index < request.worker_ids_in_use_size(); ++index) {
    auto worker_id = WorkerID::FromBinary(request.worker_ids_in_use(index));
    in_use_worker_ids.emplace(worker_id);
  }

  std::vector<WorkerID> unused_worker_ids;
  for (auto &iter : leased_workers_) {
    // We need to exclude workers used by common tasks.
    // Because they are not used by GCS.
    if (!iter.second->GetActorId().IsNil() && !in_use_worker_ids.count(iter.first)) {
      unused_worker_ids.emplace_back(iter.first);
    }
  }

  for (auto &iter : unused_worker_ids) {
    leased_workers_.erase(iter);
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleCancelWorkerLease(const rpc::CancelWorkerLeaseRequest &request,
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

void NodeManager::ProcessSetResourceRequest(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the SetResource message
  auto message = flatbuffers::GetRoot<protocol::SetResourceRequest>(message_data);

  auto const &resource_name = string_from_flatbuf(*message->resource_name());
  double const &capacity = message->capacity();
  bool is_deletion = capacity <= 0;

  NodeID node_id = from_flatbuf<NodeID>(*message->node_id());

  // If the python arg was null, set node_id to the local node id.
  if (node_id.IsNil()) {
    node_id = self_node_id_;
  }

  if (is_deletion &&
      cluster_resource_map_[node_id].GetTotalResources().GetResourceMap().count(
          resource_name) == 0) {
    // Resource does not exist in the cluster resource map, thus nothing to delete.
    // Return..
    RAY_LOG(INFO) << "[ProcessDeleteResourceRequest] Trying to delete resource "
                  << resource_name << ", but it does not exist. Doing nothing..";
    return;
  }

  // Submit to the resource table. This calls the ResourceCreateUpdated or ResourceDeleted
  // callback, which updates cluster_resource_map_.
  if (is_deletion) {
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncDeleteResources(
        node_id, {resource_name}, nullptr));
  } else {
    std::unordered_map<std::string, std::shared_ptr<rpc::ResourceTableData>> data_map;
    auto resource_table_data = std::make_shared<rpc::ResourceTableData>();
    resource_table_data->set_resource_capacity(capacity);
    data_map.emplace(resource_name, resource_table_data);
    RAY_CHECK_OK(
        gcs_client_->NodeResources().AsyncUpdateResources(node_id, data_map, nullptr));
  }
}

void NodeManager::ScheduleTasks(
    std::unordered_map<NodeID, SchedulingResources> &resource_map) {
  // If the resource map contains the local raylet, update load before calling policy.
  if (resource_map.count(self_node_id_) > 0) {
    resource_map[self_node_id_].SetLoadResources(local_queues_.GetTotalResourceLoad());
  }
  // Invoke the scheduling policy.
  auto policy_decision = scheduling_policy_.Schedule(resource_map, self_node_id_);

#ifndef NDEBUG
  RAY_LOG(DEBUG) << "[NM ScheduleTasks] policy decision:";
  for (const auto &task_client_pair : policy_decision) {
    TaskID task_id = task_client_pair.first;
    NodeID node_id = task_client_pair.second;
    RAY_LOG(DEBUG) << task_id << " --> " << node_id;
  }
#endif

  // Extract decision for this raylet.
  std::unordered_set<TaskID> local_task_ids;
  // Iterate over (taskid, nodeid) pairs, extract tasks assigned to the local node.
  for (const auto &task_client_pair : policy_decision) {
    const TaskID &task_id = task_client_pair.first;
    const NodeID &node_id = task_client_pair.second;
    if (node_id == self_node_id_) {
      local_task_ids.insert(task_id);
    } else {
      // TODO(atumanov): need a better interface for task exit on forward.
      // (See design_docs/task_states.rst for the state transition diagram.)
      Task task;
      if (local_queues_.RemoveTask(task_id, &task)) {
        // Attempt to forward the task. If this fails to forward the task,
        // the task will be resubmit locally.
        ForwardTaskOrResubmit(task, node_id);
      }
    }
  }

  // Transition locally placed tasks to waiting or ready for dispatch.
  if (local_task_ids.size() > 0) {
    std::vector<Task> tasks = local_queues_.RemoveTasks(local_task_ids);
    for (const auto &t : tasks) {
      EnqueuePlaceableTask(t);
    }
  }

  // All remaining placeable tasks should be registered with the task dependency
  // manager. TaskDependencyManager::TaskPending() is assumed to be idempotent.
  // TODO(atumanov): evaluate performance implications of registering all new tasks on
  // submission vs. registering remaining queued placeable tasks here.
  std::unordered_set<TaskID> move_task_set;
  for (const auto &task : local_queues_.GetTasks(TaskState::PLACEABLE)) {
    move_task_set.insert(task.GetTaskSpecification().TaskId());
    PublishInfeasibleTaskError(task);
    // Assert that this placeable task is not feasible locally (necessary but not
    // sufficient).
    RAY_CHECK(!task.GetTaskSpecification().GetRequiredPlacementResources().IsSubset(
        cluster_resource_map_[self_node_id_].GetTotalResources()));
  }

  // Assumption: all remaining placeable tasks are infeasible and are moved to the
  // infeasible task queue. Infeasible task queue is checked when new nodes join.
  local_queues_.MoveTasks(move_task_set, TaskState::PLACEABLE, TaskState::INFEASIBLE);
  // Check the invariant that no placeable tasks remain after a call to the policy.
  RAY_CHECK(local_queues_.GetTasks(TaskState::PLACEABLE).size() == 0);
}

void NodeManager::MarkObjectsAsFailed(
    const ErrorType &error_type, const std::vector<rpc::ObjectReference> objects_to_fail,
    const JobID &job_id) {
  const std::string meta = std::to_string(static_cast<int>(error_type));
  for (const auto &ref : objects_to_fail) {
    ObjectID object_id = ObjectID::FromBinary(ref.object_id());
    std::shared_ptr<Buffer> data;
    Status status;
    status = store_client_.TryCreateImmediately(
        object_id, ref.owner_address(), 0,
        reinterpret_cast<const uint8_t *>(meta.c_str()), meta.length(), &data);
    if (status.ok()) {
      status = store_client_.Seal(object_id);
    }
    if (!status.ok() && !status.IsObjectExists()) {
      RAY_LOG(INFO) << "Marking plasma object failed " << object_id;
      // If we failed to save the error code, log a warning and push an error message
      // to the driver.
      std::ostringstream stream;
      stream << "A plasma error (" << status.ToString() << ") occurred while saving"
             << " error code to object " << object_id << ". Anyone who's getting this"
             << " object may hang forever.";
      std::string error_message = stream.str();
      RAY_LOG(ERROR) << error_message;
      auto error_data_ptr =
          gcs::CreateErrorTableData("task", error_message, current_time_ms(), job_id);
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
}

void NodeManager::SubmitTask(const Task &task) {
  const TaskSpecification &spec = task.GetTaskSpecification();
  // Actor tasks should be no longer submitted to raylet.
  RAY_CHECK(!spec.IsActorTask());
  const TaskID &task_id = spec.TaskId();
  RAY_LOG(DEBUG) << "Submitting task: " << task.DebugString();

  if (local_queues_.HasTask(task_id)) {
    if (spec.IsActorCreationTask()) {
      // NOTE(hchen): Normally when raylet receives a duplicated actor creation task
      // from GCS, raylet should just ignore the task. However, due to the hack that
      // we save the RPC reply in task's OnDispatch callback, we have to remove the
      // old task and re-add the new task, to make sure the RPC reply callback is correct.
      RAY_LOG(WARNING) << "Submitted actor creation task " << task_id
                       << " is already queued. This is most likely due to a GCS restart. "
                          "We will remove "
                          "the old one from the queue, and enqueue the new one.";
      std::unordered_set<TaskID> task_ids{task_id};
      local_queues_.RemoveTasks(task_ids);
    } else {
      RAY_LOG(WARNING) << "Submitted task " << task_id
                       << " is already queued and will not be restarted. This is most "
                          "likely due to spurious reconstruction.";
      return;
    }
  }
  // (See design_docs/task_states.rst for the state transition diagram.)
  local_queues_.QueueTasks({task}, TaskState::PLACEABLE);
  ScheduleTasks(cluster_resource_map_);
  // TODO(atumanov): assert that !placeable.isempty() => insufficient available
  // resources locally.
}

void NodeManager::HandleDirectCallTaskBlocked(
    const std::shared_ptr<WorkerInterface> &worker, bool release_resources) {
  if (!worker || worker->IsBlocked() || worker->GetAssignedTaskId().IsNil() ||
      !release_resources) {
    return;  // The worker may have died or is no longer processing the task.
  }

  if (cluster_task_manager_->ReleaseCpuResourcesFromUnblockedWorker(worker)) {
    worker->MarkBlocked();
  }
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
    if (cluster_task_manager_->ReturnCpuResourcesToBlockedWorker(worker)) {
      worker->MarkUnblocked();
    }
    cluster_task_manager_->ScheduleAndDispatchTasks();
  }
}

void NodeManager::AsyncResolveObjects(
    const std::shared_ptr<ClientConnection> &client,
    const std::vector<rpc::ObjectReference> &required_object_refs,
    const TaskID &current_task_id, bool ray_get, bool mark_worker_blocked) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  if (worker) {
    // The client is a worker. If the worker is not already blocked and the
    // blocked task matches the one assigned to the worker, then mark the
    // worker as blocked. This temporarily releases any resources that the
    // worker holds while it is blocked.
    if (mark_worker_blocked && !worker->IsBlocked() &&
        current_task_id == worker->GetAssignedTaskId()) {
      Task task;
      RAY_CHECK(local_queues_.RemoveTask(current_task_id, &task));
      local_queues_.QueueTasks({task}, TaskState::RUNNING);
      // Get the CPU resources required by the running task.
      // Release the CPU resources.
      auto const cpu_resource_ids = worker->ReleaseTaskCpuResources();
      local_available_resources_.Release(cpu_resource_ids);
      cluster_resource_map_[self_node_id_].Release(cpu_resource_ids.ToResourceSet());
      worker->MarkBlocked();
      // Try dispatching tasks since we may have released some resources.
      DispatchTasks(local_queues_.GetReadyTasksByClass());
    }
  } else {
    // The client is a driver. Drivers do not hold resources, so we simply mark
    // the task as blocked.
    worker = worker_pool_.GetRegisteredDriver(client);
  }

  RAY_CHECK(worker);
  // Mark the task as blocked.
  if (mark_worker_blocked) {
    worker->AddBlockedTaskId(current_task_id);
    if (local_queues_.GetBlockedTaskIds().count(current_task_id) == 0) {
      local_queues_.AddBlockedTaskId(current_task_id);
    }
  }

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
    const std::shared_ptr<ClientConnection> &client, const TaskID &current_task_id,
    bool was_blocked) {
  std::shared_ptr<WorkerInterface> worker = worker_pool_.GetRegisteredWorker(client);
  // TODO(swang): Because the object dependencies are tracked in the task
  // dependency manager, we could actually remove this message entirely and
  // instead unblock the worker once all the objects become available.
  if (worker) {
    // The client is a worker. If the worker is not already unblocked and the
    // unblocked task matches the one assigned to the worker, then mark the
    // worker as unblocked. This returns the temporarily released resources to
    // the worker. Workers that have been marked dead have already been cleaned
    // up.
    if (was_blocked && worker->IsBlocked() &&
        current_task_id == worker->GetAssignedTaskId() && !worker->IsDead()) {
      // (See design_docs/task_states.rst for the state transition diagram.)
      Task task;
      RAY_CHECK(local_queues_.RemoveTask(current_task_id, &task));
      local_queues_.QueueTasks({task}, TaskState::RUNNING);
      // Get the CPU resources required by the running task.
      const auto required_resources = task.GetTaskSpecification().GetRequiredResources();
      const ResourceSet cpu_resources = required_resources.GetNumCpus();
      // Check if we can reacquire the CPU resources.
      bool oversubscribed = !local_available_resources_.Contains(cpu_resources);

      if (!oversubscribed) {
        // Reacquire the CPU resources for the worker. Note that care needs to be
        // taken if the user is using the specific CPU IDs since the IDs that we
        // reacquire here may be different from the ones that the task started with.
        auto const resource_ids = local_available_resources_.Acquire(cpu_resources);
        worker->AcquireTaskCpuResources(resource_ids);
        cluster_resource_map_[self_node_id_].Acquire(cpu_resources);
      } else {
        // In this case, we simply don't reacquire the CPU resources for the worker.
        // The worker can keep running and when the task finishes, it will simply
        // not have any CPU resources to release.
        RAY_LOG(WARNING)
            << "Resources oversubscribed: "
            << cluster_resource_map_[self_node_id_].GetAvailableResources().ToString();
      }
      worker->MarkUnblocked();
    }
  } else {
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
  // Mark the task as unblocked.
  if (was_blocked) {
    worker->RemoveBlockedTaskId(current_task_id);
    local_queues_.RemoveBlockedTaskId(current_task_id);
  }
}

void NodeManager::EnqueuePlaceableTask(const Task &task) {
  // TODO(atumanov): add task lookup hashmap and change EnqueuePlaceableTask to take
  // a vector of TaskIDs. Trigger MoveTask internally.
  // Subscribe to the task's dependencies.
  bool args_ready = dependency_manager_.RequestTaskDependencies(
      task.GetTaskSpecification().TaskId(), task.GetDependencies());
  // Enqueue the task. If all dependencies are available, then the task is queued
  // in the READY state, else the WAITING state.
  // (See design_docs/task_states.rst for the state transition diagram.)
  if (args_ready) {
    local_queues_.QueueTasks({task}, TaskState::READY);
    DispatchTasks(MakeTasksByClass({task}));
  } else {
    local_queues_.QueueTasks({task}, TaskState::WAITING);
  }
}

void NodeManager::AssignTask(const std::shared_ptr<WorkerInterface> &worker,
                             const Task &task,
                             std::vector<std::function<void()>> *post_assign_callbacks) {
  // TODO(sang): Modify method names.
  const TaskSpecification &spec = task.GetTaskSpecification();
  RAY_CHECK(post_assign_callbacks);

  RAY_LOG(DEBUG) << "Assigning task " << spec.TaskId() << " to worker with pid "
                 << worker->GetProcess().GetId() << ", worker id: " << worker->WorkerId();
  flatbuffers::FlatBufferBuilder fbb;

  // Resource accounting: acquire resources for the assigned task.
  auto acquired_resources =
      local_available_resources_.Acquire(spec.GetRequiredResources());
  cluster_resource_map_[self_node_id_].Acquire(spec.GetRequiredResources());
  if (spec.IsActorCreationTask()) {
    // Check that the actor's placement resource requirements are satisfied.
    RAY_CHECK(spec.GetRequiredPlacementResources().IsSubset(
        cluster_resource_map_[self_node_id_].GetTotalResources()));
    worker->SetLifetimeResourceIds(acquired_resources);
  } else {
    worker->SetTaskResourceIds(acquired_resources);
  }

  auto task_id = spec.TaskId();
  RAY_CHECK(task.OnDispatch() != nullptr);
  if (task.GetTaskSpecification().IsDetachedActor()) {
    worker->MarkDetachedActor();
  }
  worker->SetBundleId(spec.PlacementGroupBundleId());

  const auto owner_worker_id = WorkerID::FromBinary(spec.CallerAddress().worker_id());
  const auto owner_node_id = NodeID::FromBinary(spec.CallerAddress().raylet_id());
  RAY_CHECK(!owner_worker_id.IsNil());
  RAY_LOG(DEBUG) << "Worker lease request DISPATCH " << task_id << " to worker "
                 << worker->WorkerId() << ", owner ID " << owner_worker_id;

  task.OnDispatch()(worker, initial_config_.node_manager_address, worker->Port(),
                    worker->WorkerId(),
                    spec.IsActorCreationTask() ? worker->GetLifetimeResourceIds()
                                               : worker->GetTaskResourceIds());

  // If the owner has died since this task was queued, cancel the task by
  // killing the worker (unless this task is for a detached actor).
  if (!worker->IsDetachedActor() && (failed_workers_cache_.count(owner_worker_id) > 0 ||
                                     failed_nodes_cache_.count(owner_node_id) > 0)) {
    // TODO(swang): Skip assigning this task to this worker instead of
    // killing the worker?
    RAY_LOG(INFO) << "Owner of assigned task " << task.GetTaskSpecification().TaskId()
                  << " died, killing leased worker " << worker->WorkerId();
    KillWorker(worker);
  }

  post_assign_callbacks->push_back([this, worker, task_id]() {
    RAY_LOG(DEBUG) << "Finished assigning task " << task_id << " to worker "
                   << worker->WorkerId();

    FinishAssignTask(worker, task_id, /*success=*/true);
  });
}

bool NodeManager::FinishAssignedTask(const std::shared_ptr<WorkerInterface> &worker_ptr) {
  // TODO (Alex): We should standardize to pass
  // std::shared_ptr<WorkerInterface> instead of refs.
  auto &worker = *worker_ptr;
  TaskID task_id = worker.GetAssignedTaskId();
  RAY_LOG(DEBUG) << "Finished task " << task_id;

  Task task;
  cluster_task_manager_->TaskFinished(worker_ptr, &task);

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
                                                  const Task &task) {
  RAY_LOG(DEBUG) << "Finishing assigned actor creation task";
  const TaskSpecification task_spec = task.GetTaskSpecification();
  ActorID actor_id = task_spec.ActorCreationId();

  // This was an actor creation task. Convert the worker to an actor.
  worker.AssignActorId(actor_id);

  if (task_spec.IsDetachedActor()) {
    worker.MarkDetachedActor();
  }
}

void NodeManager::HandleTaskReconstruction(const TaskID &task_id,
                                           const ObjectID &required_object_id) {
  // Get the owner's address.
  rpc::Address owner_addr;
  bool has_owner = dependency_manager_.GetOwnerAddress(required_object_id, &owner_addr);
  if (has_owner) {
    if (!RayConfig::instance().object_pinning_enabled()) {
      // LRU eviction is enabled. The object may still be in scope, but we
      // weren't able to fetch the value within the timeout, so the value has
      // most likely been evicted. Mark the object as unreachable.
      rpc::ObjectReference ref;
      ref.set_object_id(required_object_id.Binary());
      ref.mutable_owner_address()->CopyFrom(owner_addr);
      MarkObjectsAsFailed(ErrorType::OBJECT_UNRECONSTRUCTABLE, {ref}, JobID::Nil());
    } else {
      RAY_LOG(DEBUG) << "Required object " << required_object_id
                     << " fetch timed out, asking owner "
                     << WorkerID::FromBinary(owner_addr.worker_id());
      // The owner's address exists. Poll the owner to check if the object is
      // still in scope. If not, mark the object as failed.
      // TODO(swang): If the owner has died, we could also mark the object as
      // failed as soon as we hear about the owner's failure from the GCS,
      // avoiding the raylet's reconstruction timeout.
      auto client = std::unique_ptr<rpc::CoreWorkerClient>(
          new rpc::CoreWorkerClient(owner_addr, client_call_manager_));

      rpc::GetObjectStatusRequest request;
      request.set_object_id(required_object_id.Binary());
      request.set_owner_worker_id(owner_addr.worker_id());
      client->GetObjectStatus(request, [this, required_object_id, owner_addr](
                                           Status status,
                                           const rpc::GetObjectStatusReply &reply) {
        if (!status.ok() || reply.status() == rpc::GetObjectStatusReply::OUT_OF_SCOPE ||
            reply.status() == rpc::GetObjectStatusReply::FREED) {
          // The owner is gone, or the owner replied that the object has
          // gone out of scope (this is an edge case in the distributed ref
          // counting protocol where a borrower dies before it can notify
          // the owner of another borrower), or the object value has been
          // freed. Store an error in the local plasma store so that an
          // exception will be thrown when the worker tries to get the
          // value.
          rpc::ObjectReference ref;
          ref.set_object_id(required_object_id.Binary());
          ref.mutable_owner_address()->CopyFrom(owner_addr);
          MarkObjectsAsFailed(ErrorType::OBJECT_UNRECONSTRUCTABLE, {ref}, JobID::Nil());
        }
        // Do nothing if the owner replied that the object is available. The
        // object manager will continue trying to fetch the object, and this
        // handler will get triggered again if the object is still
        // unavailable after another timeout.
      });
    }
  } else {
    RAY_LOG(WARNING)
        << "Ray cannot get the value of ObjectIDs that are generated "
           "randomly (ObjectID.from_random()) or out-of-band "
           "(ObjectID.from_binary(...)) because Ray "
           "does not know which task will create them. "
           "If this was not how your object ID was generated, please file an "
           "issue "
           "at https://github.com/ray-project/ray/issues/";
    rpc::ObjectReference ref;
    ref.set_object_id(required_object_id.Binary());
    MarkObjectsAsFailed(ErrorType::OBJECT_UNRECONSTRUCTABLE, {ref}, JobID::Nil());
  }
}

void NodeManager::HandleObjectLocal(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is local.
  const auto ready_task_ids = dependency_manager_.HandleObjectLocal(object_id);
  RAY_LOG(DEBUG) << "Object local " << object_id << ", "
                 << " on " << self_node_id_ << ", " << ready_task_ids.size()
                 << " tasks ready";
  cluster_task_manager_->TasksUnblocked(ready_task_ids);
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

  cluster_task_manager_->OnObjectMissing(object_id, waiting_task_ids);
}

void NodeManager::ForwardTaskOrResubmit(const Task &task, const NodeID &node_manager_id) {
  // Attempt to forward the task.
  // TODO(sang): Modify method names.
  ForwardTask(task, node_manager_id,
              [this, node_manager_id](ray::Status error, const Task &task) {
                const TaskID task_id = task.GetTaskSpecification().TaskId();
                RAY_LOG(INFO) << "Failed to forward task " << task_id
                              << " to node manager " << node_manager_id;

                // The task is not for an actor and may therefore be placed on another
                // node immediately. Send it to the scheduling policy to be placed again.
                local_queues_.QueueTasks({task}, TaskState::PLACEABLE);
                ScheduleTasks(cluster_resource_map_);
              });
}

void NodeManager::ForwardTask(
    const Task &task, const NodeID &node_id,
    const std::function<void(const ray::Status &, const Task &)> &on_error) {
  // This method spillbacks lease requests to other nodes.
  // TODO(sang): Modify method names.
  RAY_CHECK(task.OnSpillback() != nullptr);
  auto node_info = gcs_client_->Nodes().Get(node_id);
  RAY_CHECK(node_info)
      << "Spilling back to a node manager, but no GCS info found for node " << node_id;
  task.OnSpillback()(node_id, node_info->node_manager_address(),
                     node_info->node_manager_port());
}

void NodeManager::FinishAssignTask(const std::shared_ptr<WorkerInterface> &worker,
                                   const TaskID &task_id, bool success) {
  // TODO(sang): Modify method names.
  RAY_LOG(DEBUG) << "FinishAssignTask: " << task_id;
  // Remove the ASSIGNED task from the READY queue.
  Task assigned_task;
  TaskState state;
  if (!local_queues_.RemoveTask(task_id, &assigned_task, &state)) {
    // TODO(edoakes): should we be failing silently here?
    return;
  }
  RAY_CHECK(state == TaskState::READY);
  if (success) {
    auto spec = assigned_task.GetTaskSpecification();
    // We successfully assigned the task to the worker.
    worker->AssignTaskId(spec.TaskId());
    worker->SetOwnerAddress(spec.CallerAddress());
    RAY_CHECK(worker->GetAssignedJobId() == spec.JobId());
    // TODO(swang): For actors with multiple actor handles, to
    // guarantee that tasks are replayed in the same order after a
    // failure, we must update the task's execution dependency to be
    // the actor's current execution dependency.

    // Mark the task as running.
    // (See design_docs/task_states.rst for the state transition diagram.)
    assigned_task.OnDispatchInstead(nullptr);
    assigned_task.OnSpillbackInstead(nullptr);
    local_queues_.QueueTasks({assigned_task}, TaskState::RUNNING);
    // Notify the task dependency manager that we no longer need this task's
    // object dependencies.
    dependency_manager_.RemoveTaskDependencies(spec.TaskId());
  } else {
    RAY_LOG(WARNING) << "Failed to send task to worker, disconnecting client";
    // We failed to send the task to the worker, so disconnect the worker.
    DisconnectClient(worker->Connection());
    // Queue this task for future assignment. We need to do this since
    // DispatchTasks() removed it from the ready queue. The task will be
    // assigned to a worker once one becomes available.
    // (See design_docs/task_states.rst for the state transition diagram.)
    local_queues_.QueueTasks({assigned_task}, TaskState::READY);
    DispatchTasks(MakeTasksByClass({assigned_task}));
  }
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
  ObjectID id = from_flatbuf<ObjectID>(*message->object_id());

  if (dependency_manager_.CheckObjectLocal(id)) {
    // Object is already local, so we directly fire the callback to tell the core worker
    // that the plasma object is ready.
    rpc::PlasmaObjectReadyRequest request;
    request.set_object_id(id.Binary());

    RAY_LOG(DEBUG) << "Object " << id << " is already local, firing callback directly.";
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

ray::Status NodeManager::SetupPlasmaSubscription() {
  return object_manager_.SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        ObjectID object_id = ObjectID::FromBinary(object_info.object_id);
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
                  RAY_LOG(INFO)
                      << "Problem with telling worker that plasma object is ready"
                      << status.ToString();
                }
              });
        }
      });
}

void NodeManager::DumpDebugState() const {
  std::fstream fs;
  fs.open(initial_config_.session_dir + "/debug_state.txt",
          std::fstream::out | std::fstream::trunc);
  fs << DebugString();
  fs.close();
}

const NodeManagerConfig &NodeManager::GetInitialConfig() const { return initial_config_; }

std::string NodeManager::DebugString() const {
  std::stringstream result;
  uint64_t now_ms = current_time_ms();
  result << "NodeManager:";
  result << "\nInitialConfigResources: " << initial_config_.resource_config.ToString();
  if (cluster_task_manager_ != nullptr) {
    result << "\nClusterTaskManager:\n";
    result << cluster_task_manager_->DebugStr();
  }
  result << "\nClusterResources:";
  for (auto &pair : cluster_resource_map_) {
    result << "\n" << pair.first.Hex() << ": " << pair.second.DebugString();
  }
  result << "\n" << object_manager_.DebugString();
  result << "\n" << gcs_client_->DebugString();
  result << "\n" << worker_pool_.DebugString();
  result << "\n" << local_queues_.DebugString();
  result << "\n" << reconstruction_policy_.DebugString();
  result << "\n" << dependency_manager_.DebugString();
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    result << "\nnum async plasma notifications: "
           << async_plasma_objects_notification_.size();
  }

  result << "\nRemote node managers: ";
  for (const auto &entry : remote_node_manager_addresses_) {
    result << "\n" << entry.first;
  }

  result << "\nDebugString() time ms: " << (current_time_ms() - now_ms);
  return result.str();
}

// Summarizes a Census view and tag values into a compact string, e.g.,
// "Tag1:Value1,Tag2:Value2,Tag3:Value3".
std::string compact_tag_string(const opencensus::stats::ViewDescriptor &view,
                               const std::vector<std::string> &values) {
  std::stringstream result;
  const auto &keys = view.columns();
  for (size_t i = 0; i < values.size(); i++) {
    result << keys[i].name() << ":" << values[i];
    if (i < values.size() - 1) {
      result << ",";
    }
  }
  return result.str();
}

void NodeManager::HandlePinObjectIDs(const rpc::PinObjectIDsRequest &request,
                                     rpc::PinObjectIDsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  object_ids.reserve(request.object_ids_size());
  for (const auto &object_id_binary : request.object_ids()) {
    object_ids.push_back(ObjectID::FromBinary(object_id_binary));
  }
  if (object_pinning_enabled_) {
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
    if (!store_client_.Get(object_ids, /*timeout_ms=*/0, &plasma_results).ok()) {
      RAY_LOG(WARNING) << "Failed to get objects to be pinned from object store.";
      // TODO(suquark): Maybe "Status::ObjectNotFound" is more accurate here.
      send_reply_callback(Status::Invalid("Failed to get objects."), nullptr, nullptr);
      return;
    }

    std::vector<std::unique_ptr<RayObject>> objects;
    for (int64_t i = 0; i < request.object_ids().size(); i++) {
      if (plasma_results[i].data == nullptr) {
        objects.push_back(nullptr);
      } else {
        objects.emplace_back(std::unique_ptr<RayObject>(
            new RayObject(plasma_results[i].data, plasma_results[i].metadata, {})));
      }
    }
    local_object_manager_.PinObjects(object_ids, std::move(objects));
  }
  // Wait for the object to be freed by the owner, which keeps the ref count.
  local_object_manager_.WaitForObjectFree(request.owner_address(), object_ids);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleGetNodeStats(const rpc::GetNodeStatsRequest &node_stats_request,
                                     rpc::GetNodeStatsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  cluster_task_manager_->FillPendingActorInfo(reply);

  for (const auto &task : local_queues_.GetTasks(TaskState::INFEASIBLE)) {
    if (task.GetTaskSpecification().IsActorCreationTask()) {
      auto infeasible_task = reply->add_infeasible_tasks();
      infeasible_task->ParseFromString(task.GetTaskSpecification().Serialize());
    }
  }
  // Report tasks that are not scheduled because
  // resources are occupied by other actors/tasks.
  for (const auto &task : local_queues_.GetTasks(TaskState::READY)) {
    if (task.GetTaskSpecification().IsActorCreationTask()) {
      auto ready_task = reply->add_ready_tasks();
      ready_task->ParseFromString(task.GetTaskSpecification().Serialize());
    }
  }
  // Report object spilling stats.
  local_object_manager_.FillObjectSpillingStats(reply);
  // Report object store stats.
  object_manager_.FillObjectStoreStats(reply);
  // Ensure we never report an empty set of metrics.
  if (!recorded_metrics_) {
    RecordMetrics();
    RAY_CHECK(recorded_metrics_);
  }
  for (const auto &view : opencensus::stats::StatsExporter::GetViewData()) {
    auto view_data = reply->add_view_data();
    view_data->set_view_name(view.first.name());
    if (view.second.type() == opencensus::stats::ViewData::Type::kInt64) {
      for (const auto &measure : view.second.int_data()) {
        auto measure_data = view_data->add_measures();
        measure_data->set_tags(compact_tag_string(view.first, measure.first));
        measure_data->set_int_value(measure.second);
      }
    } else if (view.second.type() == opencensus::stats::ViewData::Type::kDouble) {
      for (const auto &measure : view.second.double_data()) {
        auto measure_data = view_data->add_measures();
        measure_data->set_tags(compact_tag_string(view.first, measure.first));
        measure_data->set_double_value(measure.second);
      }
    } else {
      RAY_CHECK(view.second.type() == opencensus::stats::ViewData::Type::kDistribution);
      for (const auto &measure : view.second.distribution_data()) {
        auto measure_data = view_data->add_measures();
        measure_data->set_tags(compact_tag_string(view.first, measure.first));
        measure_data->set_distribution_min(measure.second.min());
        measure_data->set_distribution_mean(measure.second.mean());
        measure_data->set_distribution_max(measure.second.max());
        measure_data->set_distribution_count(measure.second.count());
        for (const auto &bound : measure.second.bucket_boundaries().lower_boundaries()) {
          measure_data->add_distribution_bucket_boundaries(bound);
        }
        for (const auto &count : measure.second.bucket_counts()) {
          measure_data->add_distribution_bucket_counts(count);
        }
      }
    }
  }
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
        request, [reply, worker, all_workers, driver_ids, send_reply_callback](
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
    store_stats.set_spill_time_total_s(store_stats.spill_time_total_s() +
                                       cur_store.spill_time_total_s());
    store_stats.set_spilled_bytes_total(store_stats.spilled_bytes_total() +
                                        cur_store.spilled_bytes_total());
    store_stats.set_spilled_objects_total(store_stats.spilled_objects_total() +
                                          cur_store.spilled_objects_total());
    store_stats.set_restore_time_total_s(store_stats.restore_time_total_s() +
                                         cur_store.restore_time_total_s());
    store_stats.set_restored_bytes_total(store_stats.restored_bytes_total() +
                                         cur_store.restored_bytes_total());
    store_stats.set_restored_objects_total(store_stats.restored_objects_total() +
                                           cur_store.restored_objects_total());
    store_stats.set_object_store_bytes_used(store_stats.object_store_bytes_used() +
                                            cur_store.object_store_bytes_used());
    store_stats.set_object_store_bytes_avail(store_stats.object_store_bytes_avail() +
                                             cur_store.object_store_bytes_avail());
    store_stats.set_num_local_objects(store_stats.num_local_objects() +
                                      cur_store.num_local_objects());
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
    const rpc::FormatGlobalMemoryInfoRequest &request,
    rpc::FormatGlobalMemoryInfoReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto replies = std::make_shared<std::vector<rpc::GetNodeStatsReply>>();
  auto local_request = std::make_shared<rpc::GetNodeStatsRequest>();
  auto local_reply = std::make_shared<rpc::GetNodeStatsReply>();
  local_request->set_include_memory_info(true);

  unsigned int num_nodes = remote_node_manager_addresses_.size() + 1;
  rpc::GetNodeStatsRequest stats_req;
  stats_req.set_include_memory_info(true);

  auto store_reply = [replies, reply, num_nodes,
                      send_reply_callback](const rpc::GetNodeStatsReply &local_reply) {
    replies->push_back(local_reply);
    if (replies->size() >= num_nodes) {
      reply->set_memory_summary(FormatMemoryInfo(*replies));
      reply->mutable_store_stats()->CopyFrom(AccumulateStoreStats(*replies));
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }
  };

  // Fetch from remote nodes.
  for (const auto &entry : remote_node_manager_addresses_) {
    std::unique_ptr<rpc::NodeManagerClient> client(new rpc::NodeManagerClient(
        entry.second.first, entry.second.second, client_call_manager_));
    client->GetNodeStats(
        stats_req, [replies, store_reply](const ray::Status &status,
                                          const rpc::GetNodeStatsReply &r) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to get remote node stats: " << status.ToString();
          }
          store_reply(r);
        });
  }

  // Fetch from the local node.
  HandleGetNodeStats(
      stats_req, local_reply.get(),
      [local_reply, store_reply](Status status, std::function<void()> success,
                                 std::function<void()> failure) mutable {
        store_reply(*local_reply);
      });
}

void NodeManager::HandleGlobalGC(const rpc::GlobalGCRequest &request,
                                 rpc::GlobalGCReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  TriggerGlobalGC();
}

void NodeManager::TriggerGlobalGC() {
  should_global_gc_ = true;
  // We won't see our own request, so trigger local GC in the next heartbeat.
  should_local_gc_ = true;
}

void NodeManager::RecordMetrics() {
  recorded_metrics_ = true;
  if (stats::StatsConfig::instance().IsStatsDisabled()) {
    return;
  }
  // Last recorded time will be reset in the caller side.
  uint64_t current_time = current_time_ms();
  uint64_t duration_ms = current_time - metrics_last_recorded_time_ms_;

  // Record available resources of this node.
  const auto &available_resources =
      cluster_resource_map_.at(self_node_id_).GetAvailableResources().GetResourceMap();
  for (const auto &pair : available_resources) {
    stats::LocalAvailableResource().Record(pair.second,
                                           {{stats::ResourceNameKey, pair.first}});
  }
  // Record total resources of this node.
  const auto &total_resources =
      cluster_resource_map_.at(self_node_id_).GetTotalResources().GetResourceMap();
  for (const auto &pair : total_resources) {
    stats::LocalTotalResource().Record(pair.second,
                                       {{stats::ResourceNameKey, pair.first}});
  }

  // Record average number of tasks information per second.
  stats::AvgNumScheduledTasks.Record((double)metrics_num_task_scheduled_ *
                                     (1000.0 / (double)duration_ms));
  metrics_num_task_scheduled_ = 0;
  stats::AvgNumExecutedTasks.Record((double)metrics_num_task_executed_ *
                                    (1000.0 / (double)duration_ms));
  metrics_num_task_executed_ = 0;
  stats::AvgNumSpilledBackTasks.Record((double)metrics_num_task_spilled_back_ *
                                       (1000.0 / (double)duration_ms));
  metrics_num_task_spilled_back_ = 0;

  object_manager_.RecordMetrics();
  local_queues_.RecordMetrics();
}

void NodeManager::PublishInfeasibleTaskError(const Task &task) const {
  // This block is used to suppress infeasible task warning.
  bool suppress_warning = false;
  const auto &required_resources = task.GetTaskSpecification().GetRequiredResources();
  const auto &resources_map = required_resources.GetResourceMap();
  const auto &it = resources_map.begin();
  // It is a hack to suppress infeasible task warning.
  // If the first resource of a task requires this magic number, infeasible warning is
  // suppressed. It is currently only used by placement group ready API. We don't want
  // to have this in ray_config_def.h because the use case is very narrow, and we don't
  // want to expose this anywhere.
  double INFEASIBLE_TASK_SUPPRESS_MAGIC_NUMBER = 0.0101;
  if (it != resources_map.end() && it->second == INFEASIBLE_TASK_SUPPRESS_MAGIC_NUMBER) {
    suppress_warning = true;
  }

  // Push a warning to the task's driver that this task is currently infeasible.
  if (!suppress_warning) {
    // TODO(rkn): Define this constant somewhere else.
    std::string type = "infeasible_task";
    std::ostringstream error_message;
    error_message
        << "The actor or task with ID " << task.GetTaskSpecification().TaskId()
        << " cannot be scheduled right now. It requires "
        << task.GetTaskSpecification().GetRequiredPlacementResources().ToString()
        << " for placement, however the cluster currently cannot provide the requested "
           "resources. The required resources may be added as autoscaling takes place "
           "or placement groups are scheduled. Otherwise, consider reducing the "
           "resource requirements of the task.";
    auto error_data_ptr =
        gcs::CreateErrorTableData(type, error_message.str(), current_time_ms(),
                                  task.GetTaskSpecification().JobId());
    RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
  }
}

}  // namespace raylet

}  // namespace ray
