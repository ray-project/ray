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
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/pb_util.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/stats/stats.h"
#include "ray/util/sample.h"

namespace {

#define RAY_CHECK_ENUM(x, y) \
  static_assert(static_cast<int>(x) == static_cast<int>(y), "protocol mismatch")

/// A helper function to return the expected actor counter for a given actor
/// and actor handle, according to the given actor registry. If a task's
/// counter is less than the returned value, then the task is a duplicate. If
/// the task's counter is equal to the returned value, then the task should be
/// the next to run.
int64_t GetExpectedTaskCounter(
    const std::unordered_map<ray::ActorID, ray::raylet::ActorRegistration>
        &actor_registry,
    const ray::ActorID &actor_id, const ray::TaskID &actor_caller_id) {
  auto actor_entry = actor_registry.find(actor_id);
  RAY_CHECK(actor_entry != actor_registry.end());
  const auto &frontier = actor_entry->second.GetFrontier();
  int64_t expected_task_counter = 0;
  auto frontier_entry = frontier.find(actor_caller_id);
  if (frontier_entry != frontier.end()) {
    expected_task_counter = frontier_entry->second.task_counter;
  }
  return expected_task_counter;
};

struct ActorStats {
  int live_actors = 0;
  int dead_actors = 0;
  int reconstructing_actors = 0;
  int max_num_handles = 0;
};

/// A helper function to return the statistical data of actors in this node manager.
ActorStats GetActorStatisticalData(
    std::unordered_map<ray::ActorID, ray::raylet::ActorRegistration> actor_registry) {
  ActorStats item;
  for (auto &pair : actor_registry) {
    if (pair.second.GetState() == ray::rpc::ActorTableData::ALIVE) {
      item.live_actors += 1;
    } else if (pair.second.GetState() == ray::rpc::ActorTableData::RECONSTRUCTING) {
      item.reconstructing_actors += 1;
    } else {
      item.dead_actors += 1;
    }
    if (pair.second.NumHandles() > item.max_num_handles) {
      item.max_num_handles = pair.second.NumHandles();
    }
  }
  return item;
}

}  // namespace

namespace ray {

namespace raylet {

// A helper function to print the leased workers.
std::string LeasedWorkersSring(
    const std::unordered_map<WorkerID, std::shared_ptr<Worker>> &leased_workers) {
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
std::string WorkerPoolString(const std::vector<std::shared_ptr<Worker>> &worker_pool) {
  std::stringstream buffer;
  buffer << "   @worker_pool: (";
  for (const auto &worker : worker_pool) {
    buffer << worker->WorkerId() << ", ";
  }
  buffer << ")";
  return buffer.str();
}

// Helper function to print the worker's owner worker and and node owner.
std::string WorkerOwnerString(std::shared_ptr<Worker> &worker) {
  std::stringstream buffer;
  const auto owner_worker_id =
      WorkerID::FromBinary(worker->GetOwnerAddress().worker_id());
  const auto owner_node_id = WorkerID::FromBinary(worker->GetOwnerAddress().raylet_id());
  buffer << "leased_worker Lease " << worker->WorkerId() << " owned by "
         << owner_worker_id << " / " << owner_node_id;
  return buffer.str();
}

NodeManager::NodeManager(boost::asio::io_service &io_service,
                         const ClientID &self_node_id, const NodeManagerConfig &config,
                         ObjectManager &object_manager,
                         std::shared_ptr<gcs::GcsClient> gcs_client,
                         std::shared_ptr<ObjectDirectoryInterface> object_directory)
    : self_node_id_(self_node_id),
      io_service_(io_service),
      object_manager_(object_manager),
      gcs_client_(gcs_client),
      object_directory_(object_directory),
      heartbeat_timer_(io_service),
      heartbeat_period_(std::chrono::milliseconds(config.heartbeat_period_ms)),
      debug_dump_period_(config.debug_dump_period_ms),
      free_objects_period_(config.free_objects_period_ms),
      fair_queueing_enabled_(config.fair_queueing_enabled),
      object_pinning_enabled_(config.object_pinning_enabled),
      temp_dir_(config.temp_dir),
      object_manager_profile_timer_(io_service),
      initial_config_(config),
      local_available_resources_(config.resource_config),
      worker_pool_(
          io_service, config.num_initial_workers, config.maximum_startup_concurrency,
          gcs_client_, config.worker_commands, config.raylet_config,
          /*starting_worker_timeout_callback=*/
          [this]() { this->DispatchTasks(this->local_queues_.GetReadyTasksByClass()); }),
      scheduling_policy_(local_queues_),
      reconstruction_policy_(
          io_service_,
          [this](const TaskID &task_id, const ObjectID &required_object_id) {
            HandleTaskReconstruction(task_id, required_object_id);
          },
          RayConfig::instance().initial_reconstruction_timeout_milliseconds(),
          self_node_id_, gcs_client_, object_directory_),
      task_dependency_manager_(
          object_manager, reconstruction_policy_, io_service, self_node_id_,
          RayConfig::instance().initial_reconstruction_timeout_milliseconds(),
          gcs_client_),
      lineage_cache_(self_node_id_, gcs_client_, config.max_lineage_size),
      actor_registry_(),
      node_manager_server_("NodeManager", config.node_manager_port),
      node_manager_service_(io_service, *this),
      client_call_manager_(io_service),
      new_scheduler_enabled_(RayConfig::instance().new_scheduler_enabled()) {
  RAY_LOG(INFO) << "Initializing NodeManager with ID " << self_node_id_;
  RAY_CHECK(heartbeat_period_.count() > 0);
  // Initialize the resource map with own cluster resource configuration.
  cluster_resource_map_.emplace(self_node_id_,
                                SchedulingResources(config.resource_config));

  RAY_CHECK_OK(object_manager_.SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        ObjectID object_id = ObjectID::FromPlasmaIdBinary(object_info.object_id);
        HandleObjectLocal(object_id);
      }));
  RAY_CHECK_OK(object_manager_.SubscribeObjDeleted(
      [this](const ObjectID &object_id) { HandleObjectMissing(object_id); }));

  if (new_scheduler_enabled_) {
    SchedulingResources &local_resources = cluster_resource_map_[self_node_id_];
    new_resource_scheduler_ =
        std::shared_ptr<ClusterResourceScheduler>(new ClusterResourceScheduler(
            self_node_id_.Binary(),
            local_resources.GetTotalResources().GetResourceMap()));
  }

  RAY_ARROW_CHECK_OK(store_client_.Connect(config.store_socket_name.c_str()));
  // Run the node manger rpc server.
  node_manager_server_.RegisterService(node_manager_service_);
  node_manager_server_.Run();

  RAY_CHECK_OK(SetupPlasmaSubscription());
}

ray::Status NodeManager::RegisterGcs() {
  // The TaskLease subscription is done on demand in reconstruction policy.
  // Register a callback to handle actor notifications.
  auto actor_notification_callback = [this](const ActorID &actor_id,
                                            const ActorTableData &data) {
    HandleActorStateTransition(actor_id, ActorRegistration(data));
  };

  RAY_RETURN_NOT_OK(
      gcs_client_->Actors().AsyncSubscribeAll(actor_notification_callback, nullptr));

  auto on_node_change = [this](const ClientID &node_id, const GcsNodeInfo &data) {
    if (data.state() == GcsNodeInfo::ALIVE) {
      NodeAdded(data);
    } else {
      RAY_CHECK(data.state() == GcsNodeInfo::DEAD);
      NodeRemoved(data);
    }
  };
  // Register a callback to monitor new nodes and a callback to monitor removed nodes.
  RAY_RETURN_NOT_OK(
      gcs_client_->Nodes().AsyncSubscribeToNodeChange(on_node_change, nullptr));

  // Subscribe to resource changes.
  const auto &resources_changed =
      [this](const ClientID &id,
             const gcs::ResourceChangeNotification &resource_notification) {
        if (resource_notification.IsAdded()) {
          ResourceSet resource_set;
          for (auto &entry : resource_notification.GetData()) {
            resource_set.AddOrUpdateResource(entry.first,
                                             entry.second->resource_capacity());
          }
          ResourceCreateUpdated(id, resource_set);
        } else {
          RAY_CHECK(resource_notification.IsRemoved());
          std::vector<std::string> resource_names;
          for (auto &entry : resource_notification.GetData()) {
            resource_names.push_back(entry.first);
          }
          ResourceDeleted(id, resource_names);
        }
      };
  RAY_RETURN_NOT_OK(gcs_client_->Nodes().AsyncSubscribeToResources(
      /*subscribe_callback=*/resources_changed,
      /*done_callback=*/nullptr));

  // Subscribe to heartbeat batches from the monitor.
  const auto &heartbeat_batch_added =
      [this](const HeartbeatBatchTableData &heartbeat_batch) {
        HeartbeatBatchAdded(heartbeat_batch);
      };
  RAY_RETURN_NOT_OK(gcs_client_->Nodes().AsyncSubscribeBatchHeartbeat(
      heartbeat_batch_added, /*done*/ nullptr));

  // Subscribe to all unexpected failure notifications from the local and
  // remote raylets. Note that this does not include workers that failed due to
  // node failure. These workers can be identified by comparing the raylet_id
  // in their rpc::Address to the ID of a failed raylet.
  const auto &worker_failure_handler =
      [this](const WorkerID &id, const gcs::WorkerFailureData &worker_failure_data) {
        HandleUnexpectedWorkerFailure(worker_failure_data.worker_address());
      };
  RAY_CHECK_OK(gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
      worker_failure_handler, /*done_callback=*/nullptr));

  // Subscribe to job updates.
  const auto job_subscribe_handler = [this](const JobID &job_id,
                                            const JobTableData &job_data) {
    HandleJobFinished(job_id, job_data);
  };
  RAY_RETURN_NOT_OK(
      gcs_client_->Jobs().AsyncSubscribeToFinishedJobs(job_subscribe_handler, nullptr));

  // Start sending heartbeats to the GCS.
  last_heartbeat_at_ms_ = current_time_ms();
  last_debug_dump_at_ms_ = current_time_ms();
  last_free_objects_at_ms_ = current_time_ms();
  Heartbeat();
  // Start the timer that gets object manager profiling information and sends it
  // to the GCS.
  GetObjectManagerProfileInfo();

  return ray::Status::OK();
}

void NodeManager::KillWorker(std::shared_ptr<Worker> worker) {
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

void NodeManager::HandleJobFinished(const JobID &job_id, const JobTableData &job_data) {
  RAY_LOG(DEBUG) << "HandleJobFinished " << job_id;
  RAY_CHECK(job_data.is_dead());
  auto workers = worker_pool_.GetWorkersRunningTasksForJob(job_id);
  // Kill all the workers. The actual cleanup for these workers is done
  // later when we receive the DisconnectClient message from them.
  for (const auto &worker : workers) {
    if (!worker->IsDetachedActor()) {
      // Clean up any open ray.wait calls that the worker made.
      task_dependency_manager_.UnsubscribeWaitDependencies(worker->WorkerId());
      // Mark the worker as dead so further messages from it are ignored
      // (except DisconnectClient).
      worker->MarkDead();
      // Then kill the worker process.
      KillWorker(worker);
    }
  }

  // Remove all tasks for this job from the scheduling queues, mark
  // the results for these tasks as not required, cancel any attempts
  // at reconstruction. Note that at this time the workers are likely
  // alive because of the delay in killing workers.
  auto tasks_to_remove = local_queues_.GetTaskIdsForJob(job_id);
  task_dependency_manager_.RemoveTasksAndRelatedObjects(tasks_to_remove);
  // NOTE(swang): SchedulingQueue::RemoveTasks modifies its argument so we must
  // call it last.
  local_queues_.RemoveTasks(tasks_to_remove);
}

void NodeManager::Heartbeat() {
  uint64_t now_ms = current_time_ms();
  uint64_t interval = now_ms - last_heartbeat_at_ms_;
  if (interval > RayConfig::instance().num_heartbeats_warning() *
                     RayConfig::instance().raylet_heartbeat_timeout_milliseconds()) {
    RAY_LOG(WARNING) << "Last heartbeat was sent " << interval << " ms ago ";
  }
  last_heartbeat_at_ms_ = now_ms;

  auto heartbeat_data = std::make_shared<HeartbeatTableData>();
  SchedulingResources &local_resources = cluster_resource_map_[self_node_id_];
  heartbeat_data->set_client_id(self_node_id_.Binary());
  // TODO(atumanov): modify the heartbeat table protocol to use the ResourceSet directly.
  // TODO(atumanov): implement a ResourceSet const_iterator.
  for (const auto &resource_pair :
       local_resources.GetAvailableResources().GetResourceMap()) {
    heartbeat_data->add_resources_available_label(resource_pair.first);
    heartbeat_data->add_resources_available_capacity(resource_pair.second);
  }
  for (const auto &resource_pair : local_resources.GetTotalResources().GetResourceMap()) {
    heartbeat_data->add_resources_total_label(resource_pair.first);
    heartbeat_data->add_resources_total_capacity(resource_pair.second);
  }

  local_resources.SetLoadResources(local_queues_.GetResourceLoad());
  for (const auto &resource_pair : local_resources.GetLoadResources().GetResourceMap()) {
    heartbeat_data->add_resource_load_label(resource_pair.first);
    heartbeat_data->add_resource_load_capacity(resource_pair.second);
  }

  // Set the global gc bit on the outgoing heartbeat message.
  if (should_global_gc_) {
    heartbeat_data->set_should_global_gc(true);
    should_global_gc_ = false;
  }

  // Trigger local GC if needed. This throttles the frequency of local GC calls
  // to at most once per heartbeat interval.
  if (should_local_gc_) {
    DoLocalGC();
    should_local_gc_ = false;
  }

  ray::Status status = gcs_client_->Nodes().AsyncReportHeartbeat(heartbeat_data,
                                                                 /*done*/ nullptr);
  RAY_CHECK_OK_PREPEND(status, "Heartbeat failed");

  if (debug_dump_period_ > 0 &&
      static_cast<int64_t>(now_ms - last_debug_dump_at_ms_) > debug_dump_period_) {
    DumpDebugState();
    RecordMetrics();
    WarnResourceDeadlock();
    last_debug_dump_at_ms_ = now_ms;
  }

  // Evict all copies of freed objects from the cluster.
  if (free_objects_period_ > 0 &&
      static_cast<int64_t>(now_ms - last_free_objects_at_ms_) > free_objects_period_) {
    FlushObjectsToFree();
  }

  // Reset the timer.
  heartbeat_timer_.expires_from_now(heartbeat_period_);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    Heartbeat();
  });
}

void NodeManager::DoLocalGC() {
  auto all_workers = worker_pool_.GetAllWorkers();
  for (const auto &driver : worker_pool_.GetAllDrivers()) {
    all_workers.push_back(driver);
  }
  RAY_LOG(WARNING) << "Sending local GC request to " << all_workers.size() << " workers.";
  for (const auto &worker : all_workers) {
    rpc::LocalGCRequest request;
    auto status = worker->rpc_client()->LocalGC(
        request, [](const ray::Status &status, const rpc::LocalGCReply &r) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to send local GC request: " << status.ToString();
          }
        });
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to send local GC request: " << status.ToString();
    }
  }
}

// TODO(edoakes): this function is problematic because it both sends warnings spuriously
// under normal conditions and sometimes doesn't send a warning under actual deadlock
// conditions. The current logic is to push a warning when: all running tasks are
// blocked, there is at least one ready task, and a warning hasn't been pushed in
// debug_dump_period_ milliseconds.
// See https://github.com/ray-project/ray/issues/5790 for details.
void NodeManager::WarnResourceDeadlock() {
  // Check if any progress is being made on this raylet.
  for (const auto &task : local_queues_.GetTasks(TaskState::RUNNING)) {
    // Ignore blocked tasks.
    if (local_queues_.GetBlockedTaskIds().count(task.GetTaskSpecification().TaskId())) {
      continue;
    }
    // Progress is being made, don't warn.
    resource_deadlock_warned_ = false;
    return;
  }

  // suppress duplicates warning messages
  if (resource_deadlock_warned_) {
    return;
  }

  // The node is full of actors and no progress has been made for some time.
  // If there are any pending tasks, build a warning.
  std::ostringstream error_message;
  ray::Task exemplar;
  bool should_warn = false;
  int pending_actor_creations = 0;
  int pending_tasks = 0;

  // See if any tasks are blocked trying to acquire resources.
  for (const auto &task : local_queues_.GetTasks(TaskState::READY)) {
    const TaskSpecification &spec = task.GetTaskSpecification();
    if (spec.IsActorCreationTask()) {
      pending_actor_creations += 1;
    } else {
      pending_tasks += 1;
    }
    if (!should_warn) {
      exemplar = task;
      should_warn = true;
    }
  }

  // Push an warning to the driver that a task is blocked trying to acquire resources.
  if (should_warn) {
    SchedulingResources &local_resources = cluster_resource_map_[self_node_id_];
    error_message
        << "The actor or task with ID " << exemplar.GetTaskSpecification().TaskId()
        << " is pending and cannot currently be scheduled. It requires "
        << exemplar.GetTaskSpecification().GetRequiredResources().ToString()
        << " for execution and "
        << exemplar.GetTaskSpecification().GetRequiredPlacementResources().ToString()
        << " for placement, but this node only has remaining "
        << local_resources.GetAvailableResources().ToString() << ". In total there are "
        << pending_tasks << " pending tasks and " << pending_actor_creations
        << " pending actors on this node. "
        << "This is likely due to all cluster resources being claimed by actors. "
        << "To resolve the issue, consider creating fewer actors or increase the "
        << "resources available to this Ray cluster. You can ignore this message "
        << "if this Ray cluster is expected to auto-scale.";
    auto error_data_ptr = gcs::CreateErrorTableData(
        "resource_deadlock", error_message.str(), current_time_ms(),
        exemplar.GetTaskSpecification().JobId());
    RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    resource_deadlock_warned_ = true;
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
  const ClientID node_id = ClientID::FromBinary(node_info.node_id());

  RAY_LOG(DEBUG) << "[NodeAdded] Received callback from client id " << node_id;
  if (node_id == self_node_id_) {
    // We got a notification for ourselves, so we are connected to the GCS now.
    // Save this NodeManager's resource information in the cluster resource map.
    cluster_resource_map_[node_id] = initial_config_.resource_config;
    return;
  }

  auto entry = remote_node_manager_clients_.find(node_id);
  if (entry != remote_node_manager_clients_.end()) {
    RAY_LOG(DEBUG) << "Received notification of a new client that already exists: "
                   << node_id;
    return;
  }

  // Initialize a rpc client to the new node manager.
  std::unique_ptr<rpc::NodeManagerClient> client(
      new rpc::NodeManagerClient(node_info.node_manager_address(),
                                 node_info.node_manager_port(), client_call_manager_));
  remote_node_manager_clients_.emplace(node_id, std::move(client));

  // Fetch resource info for the remote client and update cluster resource map.
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetResources(
      node_id,
      [this, node_id](Status status,
                      const boost::optional<gcs::NodeInfoAccessor::ResourceMap> &data) {
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

void NodeManager::NodeRemoved(const GcsNodeInfo &node_info) {
  // TODO(swang): If we receive a notification for our own death, clean up and
  // exit immediately.
  const ClientID node_id = ClientID::FromBinary(node_info.node_id());
  RAY_LOG(DEBUG) << "[NodeRemoved] Received callback from client id " << node_id;

  RAY_CHECK(node_id != self_node_id_)
      << "Exiting because this node manager has mistakenly been marked dead by the "
      << "monitor.";

  // Below, when we remove node_id from all of these data structures, we could
  // check that it is actually removed, or log a warning otherwise, but that may
  // not be necessary.

  // Remove the client from the resource map.
  cluster_resource_map_.erase(node_id);

  // Remove the node manager client.
  const auto client_entry = remote_node_manager_clients_.find(node_id);
  if (client_entry != remote_node_manager_clients_.end()) {
    remote_node_manager_clients_.erase(client_entry);
  } else {
    RAY_LOG(WARNING) << "Received NodeRemoved callback for an unknown client " << node_id
                     << ".";
  }

  // For any live actors that were on the dead node, broadcast a notification
  // about the actor's death
  // TODO(swang): This could be very slow if there are many actors.
  for (const auto &actor_entry : actor_registry_) {
    if (actor_entry.second.GetNodeManagerId() == node_id &&
        actor_entry.second.GetState() == ActorTableData::ALIVE) {
      RAY_LOG(INFO) << "Actor " << actor_entry.first
                    << " is disconnected, because its node " << node_id
                    << " is removed from cluster. It may be reconstructed.";
      HandleDisconnectedActor(actor_entry.first, /*was_local=*/false,
                              /*intentional_disconnect=*/false);
    }
  }
  // Notify the object directory that the client has been removed so that it
  // can remove it from any cached locations.
  object_directory_->HandleClientRemoved(node_id);

  // Flush all uncommitted tasks from the local lineage cache. This is to
  // guarantee that all tasks get flushed eventually, in case one of the tasks
  // in our local cache was supposed to be flushed by the node that died.
  lineage_cache_.FlushAllUncommittedTasks();

  // Clean up workers that were owned by processes that were on the failed
  // node.
  rpc::Address address;
  address.set_raylet_id(node_info.node_id());
  HandleUnexpectedWorkerFailure(address);
}

void NodeManager::HandleUnexpectedWorkerFailure(const rpc::Address &address) {
  const WorkerID worker_id = WorkerID::FromBinary(address.worker_id());
  const ClientID node_id = ClientID::FromBinary(address.raylet_id());
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

void NodeManager::ResourceCreateUpdated(const ClientID &client_id,
                                        const ResourceSet &createUpdatedResources) {
  RAY_LOG(DEBUG) << "[ResourceCreateUpdated] received callback from client id "
                 << client_id << " with created or updated resources: "
                 << createUpdatedResources.ToString() << ". Updating resource map.";

  SchedulingResources &cluster_schedres = cluster_resource_map_[client_id];

  // Update local_available_resources_ and SchedulingResources
  for (const auto &resource_pair : createUpdatedResources.GetResourceMap()) {
    const std::string &resource_label = resource_pair.first;
    const double &new_resource_capacity = resource_pair.second;

    cluster_schedres.UpdateResourceCapacity(resource_label, new_resource_capacity);
    if (client_id == self_node_id_) {
      local_available_resources_.AddOrUpdateResource(resource_label,
                                                     new_resource_capacity);
    }
    if (new_scheduler_enabled_) {
      new_resource_scheduler_->UpdateResourceCapacity(client_id.Binary(), resource_label,
                                                      new_resource_capacity);
    }
  }
  RAY_LOG(DEBUG) << "[ResourceCreateUpdated] Updated cluster_resource_map.";

  if (client_id == self_node_id_) {
    // The resource update is on the local node, check if we can reschedule tasks.
    TryLocalInfeasibleTaskScheduling();
  }
  return;
}

void NodeManager::ResourceDeleted(const ClientID &client_id,
                                  const std::vector<std::string> &resource_names) {
  if (RAY_LOG_ENABLED(DEBUG)) {
    std::ostringstream oss;
    for (auto &resource_name : resource_names) {
      oss << resource_name << ", ";
    }
    RAY_LOG(DEBUG) << "[ResourceDeleted] received callback from client id " << client_id
                   << " with deleted resources: " << oss.str()
                   << ". Updating resource map.";
  }

  SchedulingResources &cluster_schedres = cluster_resource_map_[client_id];

  // Update local_available_resources_ and SchedulingResources
  for (const auto &resource_label : resource_names) {
    cluster_schedres.DeleteResource(resource_label);
    if (client_id == self_node_id_) {
      local_available_resources_.DeleteResource(resource_label);
    }
    if (new_scheduler_enabled_) {
      new_resource_scheduler_->DeleteResource(client_id.Binary(), resource_label);
    }
  }
  return;
}

void NodeManager::TryLocalInfeasibleTaskScheduling() {
  RAY_LOG(DEBUG) << "[LocalResourceUpdateRescheduler] The resource update is on the "
                    "local node, check if we can reschedule tasks";
  SchedulingResources &new_local_resources = cluster_resource_map_[self_node_id_];

  // SpillOver locally to figure out which infeasible tasks can be placed now
  std::vector<TaskID> decision = scheduling_policy_.SpillOver(new_local_resources);

  std::unordered_set<TaskID> local_task_ids(decision.begin(), decision.end());

  // Transition locally placed tasks to waiting or ready for dispatch.
  if (local_task_ids.size() > 0) {
    std::vector<Task> tasks = local_queues_.RemoveTasks(local_task_ids);
    for (const auto &t : tasks) {
      EnqueuePlaceableTask(t);
    }
  }
}

void NodeManager::HeartbeatAdded(const ClientID &client_id,
                                 const HeartbeatTableData &heartbeat_data) {
  // Locate the client id in remote client table and update available resources based on
  // the received heartbeat information.
  auto it = cluster_resource_map_.find(client_id);
  if (it == cluster_resource_map_.end()) {
    // Haven't received the client registration for this client yet, skip this heartbeat.
    RAY_LOG(INFO) << "[HeartbeatAdded]: received heartbeat from unknown client id "
                  << client_id;
    return;
  }
  // Trigger local GC at the next heartbeat interval.
  if (heartbeat_data.should_global_gc()) {
    should_local_gc_ = true;
  }

  SchedulingResources &remote_resources = it->second;

  ResourceSet remote_total(VectorFromProtobuf(heartbeat_data.resources_total_label()),
                           VectorFromProtobuf(heartbeat_data.resources_total_capacity()));
  ResourceSet remote_available(
      VectorFromProtobuf(heartbeat_data.resources_available_label()),
      VectorFromProtobuf(heartbeat_data.resources_available_capacity()));
  ResourceSet remote_load(VectorFromProtobuf(heartbeat_data.resource_load_label()),
                          VectorFromProtobuf(heartbeat_data.resource_load_capacity()));
  // TODO(atumanov): assert that the load is a non-empty ResourceSet.
  remote_resources.SetAvailableResources(std::move(remote_available));
  // Extract the load information and save it locally.
  remote_resources.SetLoadResources(std::move(remote_load));

  if (new_scheduler_enabled_ && client_id != self_node_id_) {
    new_resource_scheduler_->AddOrUpdateNode(client_id.Binary(),
                                             remote_total.GetResourceMap(),
                                             remote_available.GetResourceMap());
    NewSchedulerSchedulePendingTasks();
    return;
  }

  // Extract decision for this raylet.
  auto decision = scheduling_policy_.SpillOver(remote_resources);
  std::unordered_set<TaskID> local_task_ids;
  for (const auto &task_id : decision) {
    // (See design_docs/task_states.rst for the state transition diagram.)
    Task task;
    TaskState state;
    if (!local_queues_.RemoveTask(task_id, &task, &state)) {
      return;
    }
    // Since we are spilling back from the ready and waiting queues, we need
    // to unsubscribe the dependencies.
    if (state != TaskState::INFEASIBLE) {
      // Don't unsubscribe for infeasible tasks because we never subscribed in
      // the first place.
      RAY_CHECK(task_dependency_manager_.UnsubscribeGetDependencies(task_id));
    }
    // Attempt to forward the task. If this fails to forward the task,
    // the task will be resubmit locally.
    ForwardTaskOrResubmit(task, client_id);
  }
}

void NodeManager::HeartbeatBatchAdded(const HeartbeatBatchTableData &heartbeat_batch) {
  // Update load information provided by each heartbeat.
  for (const auto &heartbeat_data : heartbeat_batch.batch()) {
    const ClientID &client_id = ClientID::FromBinary(heartbeat_data.client_id());
    if (client_id == self_node_id_) {
      // Skip heartbeats from self.
      continue;
    }
    HeartbeatAdded(client_id, heartbeat_data);
  }
}

void NodeManager::HandleActorStateTransition(const ActorID &actor_id,
                                             ActorRegistration &&actor_registration) {
  // Update local registry.
  auto it = actor_registry_.find(actor_id);
  if (it == actor_registry_.end()) {
    it = actor_registry_.emplace(actor_id, actor_registration).first;
  } else {
    if (RayConfig::instance().gcs_service_enabled() &&
        RayConfig::instance().gcs_actor_service_enabled()) {
      it->second = actor_registration;
    } else {
      // Only process the state transition if it is to a later state than ours.
      if (actor_registration.GetState() > it->second.GetState() &&
          actor_registration.GetRemainingReconstructions() ==
              it->second.GetRemainingReconstructions()) {
        // The new state is later than ours if it is about the same lifetime, but
        // a greater state.
        it->second = actor_registration;
      } else if (actor_registration.GetRemainingReconstructions() <
                 it->second.GetRemainingReconstructions()) {
        // The new state is also later than ours it is about a later lifetime of
        // the actor.
        it->second = actor_registration;
      } else {
        // Our state is already at or past the update, so skip the update.
        return;
      }
    }
  }
  RAY_LOG(DEBUG) << "Actor notification received: actor_id = " << actor_id
                 << ", node_manager_id = " << actor_registration.GetNodeManagerId()
                 << ", state = "
                 << ActorTableData::ActorState_Name(actor_registration.GetState())
                 << ", remaining_reconstructions = "
                 << actor_registration.GetRemainingReconstructions();

  if (actor_registration.GetState() == ActorTableData::ALIVE) {
    // The actor is now alive (created for the first time or reconstructed). We can
    // stop listening for the actor creation task. This is needed because we use
    // `ListenAndMaybeReconstruct` to reconstruct the actor.
    reconstruction_policy_.Cancel(actor_registration.GetActorCreationDependency());
    // The actor's location is now known. Dequeue any methods that were
    // submitted before the actor's location was known.
    // (See design_docs/task_states.rst for the state transition diagram.)
    const auto &methods = local_queues_.GetTasks(TaskState::WAITING_FOR_ACTOR_CREATION);
    std::unordered_set<TaskID> created_actor_method_ids;
    for (const auto &method : methods) {
      if (method.GetTaskSpecification().ActorId() == actor_id) {
        created_actor_method_ids.insert(method.GetTaskSpecification().TaskId());
      }
    }
    // Resubmit the methods that were submitted before the actor's location was
    // known.
    auto created_actor_methods = local_queues_.RemoveTasks(created_actor_method_ids);
    for (const auto &method : created_actor_methods) {
      // Maintain the invariant that if a task is in the
      // MethodsWaitingForActorCreation queue, then it is subscribed to its
      // respective actor creation task. Since the actor location is now known,
      // we can remove the task from the queue and forget its dependency on the
      // actor creation task.
      RAY_CHECK(task_dependency_manager_.UnsubscribeGetDependencies(
          method.GetTaskSpecification().TaskId()));
      // The task's uncommitted lineage was already added to the local lineage
      // cache upon the initial submission, so it's okay to resubmit it with an
      // empty lineage this time.
      SubmitTask(method, Lineage());
    }
  } else if (actor_registration.GetState() == ActorTableData::DEAD) {
    // When an actor dies, loop over all of the queued tasks for that actor
    // and treat them as failed.
    auto tasks_to_remove = local_queues_.GetTaskIdsForActor(actor_id);
    auto removed_tasks = local_queues_.RemoveTasks(tasks_to_remove);
    for (auto const &task : removed_tasks) {
      TreatTaskAsFailed(task, ErrorType::ACTOR_DIED);
    }
  } else if (actor_registration.GetState() == ActorTableData::RECONSTRUCTING) {
    RAY_LOG(DEBUG) << "Actor is being reconstructed: " << actor_id;
    if (!(RayConfig::instance().gcs_service_enabled() &&
          RayConfig::instance().gcs_actor_service_enabled())) {
      // The actor is dead and needs reconstruction. Attempting to reconstruct its
      // creation task.
      reconstruction_policy_.ListenAndMaybeReconstruct(
          actor_registration.GetActorCreationDependency());
    }

    // When an actor fails but can be reconstructed, resubmit all of the queued
    // tasks for that actor. This will mark the tasks as waiting for actor
    // creation.
    auto tasks_to_remove = local_queues_.GetTaskIdsForActor(actor_id);
    auto removed_tasks = local_queues_.RemoveTasks(tasks_to_remove);
    for (auto const &task : removed_tasks) {
      SubmitTask(task, Lineage());
    }
  } else {
    RAY_CHECK(actor_registration.GetState() == ActorTableData::PENDING);
    // Do nothing.
  }
}

void NodeManager::ProcessNewClient(ClientConnection &client) {
  // The new client is a worker, so begin listening for messages.
  client.ProcessMessages();
}

// A helper function to create a mapping from task scheduling class to
// tasks with that class from a given list of tasks.
std::unordered_map<SchedulingClass, ordered_set<TaskID>> MakeTasksByClass(
    const std::vector<Task> &tasks) {
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
  RAY_CHECK(new_scheduler_enabled_ == false);
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
        TaskSpecification::GetSchedulingClassDescriptor(it->first).first;
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
      std::shared_ptr<Worker> worker =
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
    if ((message_type_value != protocol::MessageType::DisconnectClient) &&
        (message_type_value != protocol::MessageType::IntentionalDisconnectClient)) {
      // Listen for more messages.
      client->ProcessMessages();
      return;
    }
  }

  switch (message_type_value) {
  case protocol::MessageType::RegisterClientRequest: {
    ProcessRegisterClientRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::TaskDone: {
    HandleWorkerAvailable(client);
  } break;
  case protocol::MessageType::DisconnectClient: {
    ProcessDisconnectClientMessage(client);
    // We don't need to receive future messages from this client,
    // because it's already disconnected.
    return;
  } break;
  case protocol::MessageType::IntentionalDisconnectClient: {
    ProcessDisconnectClientMessage(client, /* intentional_disconnect = */ true);
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
    std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    HandleDirectCallTaskBlocked(worker);
  } break;
  case protocol::MessageType::NotifyDirectCallTaskUnblocked: {
    std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
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
    if (message->delete_creating_tasks()) {
      // Clean up their creating tasks from GCS.
      std::vector<TaskID> creating_task_ids;
      for (const auto &object_id : object_ids) {
        creating_task_ids.push_back(object_id.TaskId());
      }
      RAY_CHECK_OK(gcs_client_->Tasks().AsyncDelete(creating_task_ids, nullptr));
    }
  } break;
  case protocol::MessageType::PrepareActorCheckpointRequest: {
    ProcessPrepareActorCheckpointRequest(client, message_data);
  } break;
  case protocol::MessageType::NotifyActorResumedFromCheckpoint: {
    ProcessNotifyActorResumedFromCheckpoint(message_data);
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
  flatbuffers::FlatBufferBuilder fbb;
  auto reply =
      ray::protocol::CreateRegisterClientReply(fbb, to_flatbuf(fbb, self_node_id_));
  fbb.Finish(reply);
  client->WriteMessageAsync(
      static_cast<int64_t>(protocol::MessageType::RegisterClientReply), fbb.GetSize(),
      fbb.GetBufferPointer(), [this, client](const ray::Status &status) {
        if (!status.ok()) {
          ProcessDisconnectClientMessage(client);
        }
      });

  auto message = flatbuffers::GetRoot<protocol::RegisterClientRequest>(message_data);
  Language language = static_cast<Language>(message->language());
  WorkerID worker_id = from_flatbuf<WorkerID>(*message->worker_id());
  pid_t pid = message->worker_pid();
  std::string worker_ip_address = string_from_flatbuf(*message->ip_address());
  auto worker = std::make_shared<Worker>(worker_id, language, worker_ip_address,
                                         message->port(), client, client_call_manager_);
  if (message->is_worker()) {
    // Register the new worker.
    if (worker_pool_.RegisterWorker(worker, pid).ok()) {
      HandleWorkerAvailable(worker->Connection());
    }
  } else {
    // Register the new driver.
    RAY_CHECK(pid >= 0);
    worker->SetProcess(Process::FromPid(pid));
    const JobID job_id = from_flatbuf<JobID>(*message->job_id());
    // Compute a dummy driver task id from a given driver.
    const TaskID driver_task_id = TaskID::ComputeDriverTaskId(worker_id);
    worker->AssignTaskId(driver_task_id);
    worker->AssignJobId(job_id);
    Status status = worker_pool_.RegisterDriver(worker);
    if (status.ok()) {
      local_queues_.AddDriverTaskId(driver_task_id);
      auto job_data_ptr = gcs::CreateJobTableData(
          job_id, /*is_dead*/ false, std::time(nullptr), worker_ip_address, pid);
      RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(job_data_ptr, nullptr));
    }
  }
}

void NodeManager::HandleDisconnectedActor(const ActorID &actor_id, bool was_local,
                                          bool intentional_disconnect) {
  if (RayConfig::instance().gcs_service_enabled() &&
      RayConfig::instance().gcs_actor_service_enabled()) {
    // If gcs actor management is enabled, the gcs will take over the status change of all
    // actors.
    return;
  }
  auto actor_entry = actor_registry_.find(actor_id);
  RAY_CHECK(actor_entry != actor_registry_.end());
  auto &actor_registration = actor_entry->second;
  RAY_LOG(DEBUG) << "The actor with ID " << actor_id << " died "
                 << (intentional_disconnect ? "intentionally" : "unintentionally")
                 << ", remaining reconstructions = "
                 << actor_registration.GetRemainingReconstructions();

  // Check if this actor needs to be reconstructed.
  ActorState new_state =
      actor_registration.GetRemainingReconstructions() > 0 && !intentional_disconnect
          ? ActorTableData::RECONSTRUCTING
          : ActorTableData::DEAD;
  if (was_local) {
    // Clean up the dummy objects from this actor.
    RAY_LOG(DEBUG) << "Removing dummy objects for actor: " << actor_id;
    for (auto &dummy_object_pair : actor_entry->second.GetDummyObjects()) {
      HandleObjectMissing(dummy_object_pair.first);
    }
  }
  // Update the actor's state.
  ActorTableData new_actor_info = actor_entry->second.GetTableData();
  new_actor_info.set_state(new_state);
  if (was_local) {
    // If the actor was local, immediately update the state in actor registry.
    // So if we receive any actor tasks before we receive GCS notification,
    // these tasks can be correctly routed to the `MethodsWaitingForActorCreation`
    // queue, instead of being assigned to the dead actor.
    HandleActorStateTransition(actor_id, ActorRegistration(new_actor_info));
  }

  auto done = [was_local, actor_id](Status status) {
    if (was_local && !status.ok()) {
      // If the disconnected actor was local, only this node will try to update actor
      // state. So the update shouldn't fail.
      RAY_LOG(FATAL) << "Failed to update state for actor " << actor_id
                     << ", status: " << status.ToString();
    }
  };
  auto actor_notification = std::make_shared<ActorTableData>(new_actor_info);
  RAY_CHECK_OK(gcs_client_->Actors().AsyncUpdate(actor_id, actor_notification, done));

  if (was_local && new_state == ActorTableData::RECONSTRUCTING) {
    RAY_LOG(INFO) << "A local actor (id = " << actor_id
                  << " ) is dead, reconstructing it.";
    const ObjectID &actor_creation_dummy_object_id =
        actor_registration.GetActorCreationDependency();
    HandleTaskReconstruction(actor_creation_dummy_object_id.TaskId(),
                             actor_creation_dummy_object_id);
  }
}

void NodeManager::HandleWorkerAvailable(const std::shared_ptr<ClientConnection> &client) {
  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
  HandleWorkerAvailable(worker);
}

void NodeManager::HandleWorkerAvailable(const std::shared_ptr<Worker> &worker) {
  RAY_CHECK(worker);
  bool worker_idle = true;

  // If the worker was assigned a task, mark it as finished.
  if (!worker->GetAssignedTaskId().IsNil()) {
    worker_idle = FinishAssignedTask(*worker);
  }

  if (worker_idle) {
    // Return the worker to the idle pool.
    worker_pool_.PushWorker(worker);
  }

  // Local resource availability changed: invoke scheduling policy for local node.
  if (new_scheduler_enabled_) {
    NewSchedulerSchedulePendingTasks();
  } else {
    cluster_resource_map_[self_node_id_].SetLoadResources(
        local_queues_.GetResourceLoad());
    // Call task dispatch to assign work to the new worker.
    DispatchTasks(local_queues_.GetReadyTasksByClass());
  }
}

void NodeManager::ProcessDisconnectClientMessage(
    const std::shared_ptr<ClientConnection> &client, bool intentional_disconnect) {
  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
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
  if (worker) {
    if (is_worker && worker->IsDead()) {
      // If the worker was killed by us because the driver exited,
      // treat it as intentionally disconnected.
      intentional_disconnect = true;
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
      task_dependency_manager_.UnsubscribeWaitDependencies(worker->WorkerId());
    }

    // Erase any lease metadata.
    leased_workers_.erase(worker->WorkerId());

    // Publish the worker failure.
    auto worker_failure_data_ptr = gcs::CreateWorkerFailureData(
        self_node_id_, worker->WorkerId(), worker->IpAddress(), worker->Port(),
        time(nullptr), intentional_disconnect);
    RAY_CHECK_OK(gcs_client_->Workers().AsyncReportWorkerFailure(worker_failure_data_ptr,
                                                                 nullptr));
  }

  if (is_worker) {
    const ActorID &actor_id = worker->GetActorId();
    if (!actor_id.IsNil()) {
      // If the worker was an actor, update actor state, reconstruct the actor if needed,
      // and clean up actor's tasks if the actor is permanently dead.
      HandleDisconnectedActor(actor_id, true, intentional_disconnect);
    }

    const TaskID &task_id = worker->GetAssignedTaskId();
    // If the worker was running a task or actor, clean up the task and push an
    // error to the driver, unless the worker is already dead.
    if ((!task_id.IsNil() || !actor_id.IsNil()) && !worker->IsDead()) {
      // If the worker was an actor, the task was already cleaned up in
      // `HandleDisconnectedActor`.
      if (actor_id.IsNil()) {
        Task task;
        if (local_queues_.RemoveTask(task_id, &task)) {
          TreatTaskAsFailed(task, ErrorType::WORKER_DIED);
        }
      }

      if (!intentional_disconnect) {
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
    if (new_scheduler_enabled_) {
      new_resource_scheduler_->SubtractCPUResourceInstances(
          worker->GetBorrowedCPUInstances());
      new_resource_scheduler_->FreeLocalTaskResources(worker->GetAllocatedInstances());
      worker->ClearAllocatedInstances();
      new_resource_scheduler_->FreeLocalTaskResources(
          worker->GetLifetimeAllocatedInstances());
      worker->ClearLifetimeAllocatedInstances();
    } else {
      auto const &task_resources = worker->GetTaskResourceIds();
      local_available_resources_.ReleaseConstrained(
          task_resources, cluster_resource_map_[self_node_id_].GetTotalResources());
      cluster_resource_map_[self_node_id_].Release(task_resources.ToResourceSet());
      worker->ResetTaskResourceIds();

      auto const &lifetime_resources = worker->GetLifetimeResourceIds();
      local_available_resources_.ReleaseConstrained(
          lifetime_resources, cluster_resource_map_[self_node_id_].GetTotalResources());
      cluster_resource_map_[self_node_id_].Release(lifetime_resources.ToResourceSet());
      worker->ResetLifetimeResourceIds();
    }

    // Since some resources may have been released, we can try to dispatch more tasks. YYY
    if (new_scheduler_enabled_) {
      NewSchedulerSchedulePendingTasks();
    } else {
      DispatchTasks(local_queues_.GetReadyTasksByClass());
    }
  } else if (is_driver) {
    // The client is a driver.
    const auto job_id = worker->GetAssignedJobId();
    RAY_CHECK(!job_id.IsNil());
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFinished(job_id, nullptr));
    const auto driver_id = ComputeDriverIdFromJob(job_id);
    local_queues_.RemoveDriverTaskId(TaskID::ComputeDriverTaskId(driver_id));
    worker_pool_.DisconnectDriver(worker);

    RAY_LOG(DEBUG) << "Driver (pid=" << worker->GetProcess().GetId()
                   << ") is disconnected. "
                   << "job_id: " << worker->GetAssignedJobId();
  }

  client->Close();

  // TODO(rkn): Tell the object manager that this client has disconnected so
  // that it can clean up the wait requests for this client. Currently I think
  // these can be leaked.
}

void NodeManager::ProcessFetchOrReconstructMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::FetchOrReconstruct>(message_data);
  std::vector<ObjectID> required_object_ids;
  for (int64_t i = 0; i < message->object_ids()->size(); ++i) {
    ObjectID object_id = from_flatbuf<ObjectID>(*message->object_ids()->Get(i));
    if (message->fetch_only()) {
      // If only a fetch is required, then do not subscribe to the
      // dependencies to the task dependency manager.
      if (!task_dependency_manager_.CheckObjectLocal(object_id)) {
        // Fetch the object if it's not already local.
        RAY_CHECK_OK(object_manager_.Pull(object_id));
      }
    } else {
      // If reconstruction is also required, then add any requested objects to
      // the list to subscribe to in the task dependency manager. These objects
      // will be pulled from remote node managers and reconstructed if
      // necessary.
      required_object_ids.push_back(object_id);
    }
  }

  if (!required_object_ids.empty()) {
    const TaskID task_id = from_flatbuf<TaskID>(*message->task_id());
    AsyncResolveObjects(client, required_object_ids, task_id, /*ray_get=*/true,
                        /*mark_worker_blocked*/ message->mark_worker_blocked());
  }
}

void NodeManager::ProcessWaitRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message = flatbuffers::GetRoot<protocol::WaitRequest>(message_data);
  std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
  int64_t wait_ms = message->timeout();
  uint64_t num_required_objects = static_cast<uint64_t>(message->num_ready_objects());
  bool wait_local = message->wait_local();

  std::vector<ObjectID> required_object_ids;
  for (auto const &object_id : object_ids) {
    if (!task_dependency_manager_.CheckObjectLocal(object_id)) {
      // Add any missing objects to the list to subscribe to in the task
      // dependency manager. These objects will be pulled from remote node
      // managers and reconstructed if necessary.
      required_object_ids.push_back(object_id);
    }
  }

  const TaskID &current_task_id = from_flatbuf<TaskID>(*message->task_id());
  bool resolve_objects = !required_object_ids.empty();
  bool was_blocked = message->mark_worker_blocked();
  if (resolve_objects) {
    AsyncResolveObjects(client, required_object_ids, current_task_id, /*ray_get=*/false,
                        /*mark_worker_blocked*/ was_blocked);
  }

  ray::Status status = object_manager_.Wait(
      object_ids, wait_ms, num_required_objects, wait_local,
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
          ProcessDisconnectClientMessage(client);
        }
      });
  RAY_CHECK_OK(status);
}

void NodeManager::ProcessWaitForDirectActorCallArgsRequestMessage(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message =
      flatbuffers::GetRoot<protocol::WaitForDirectActorCallArgsRequest>(message_data);
  int64_t tag = message->tag();
  std::vector<ObjectID> object_ids = from_flatbuf<ObjectID>(*message->object_ids());
  std::vector<ObjectID> required_object_ids;
  for (auto const &object_id : object_ids) {
    if (!task_dependency_manager_.CheckObjectLocal(object_id)) {
      // Add any missing objects to the list to subscribe to in the task
      // dependency manager. These objects will be pulled from remote node
      // managers and reconstructed if necessary.
      required_object_ids.push_back(object_id);
    }
  }

  ray::Status status = object_manager_.Wait(
      object_ids, -1, object_ids.size(), false,
      [this, client, tag](std::vector<ObjectID> found, std::vector<ObjectID> remaining) {
        RAY_CHECK(remaining.empty());
        std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
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

void NodeManager::ProcessPrepareActorCheckpointRequest(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  auto message =
      flatbuffers::GetRoot<protocol::PrepareActorCheckpointRequest>(message_data);
  ActorID actor_id = from_flatbuf<ActorID>(*message->actor_id());
  RAY_LOG(DEBUG) << "Preparing checkpoint for actor " << actor_id;
  const auto &actor_entry = actor_registry_.find(actor_id);
  RAY_CHECK(actor_entry != actor_registry_.end());

  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
  RAY_CHECK(worker && worker->GetActorId() == actor_id);

  std::shared_ptr<ActorCheckpointData> checkpoint_data =
      actor_entry->second.GenerateCheckpointData(actor_entry->first, nullptr);

  // Write checkpoint data to GCS.
  RAY_CHECK_OK(gcs_client_->Actors().AsyncAddCheckpoint(
      checkpoint_data, [worker, checkpoint_data](Status status) {
        ActorCheckpointID checkpoint_id =
            ActorCheckpointID::FromBinary(checkpoint_data->checkpoint_id());
        RAY_CHECK(status.ok()) << "Add checkpoint failed, actor is "
                               << worker->GetActorId() << " checkpoint_id is "
                               << checkpoint_id;
        RAY_LOG(DEBUG) << "Checkpoint " << checkpoint_id << " saved for actor "
                       << worker->GetActorId();
        // Send reply to worker.
        flatbuffers::FlatBufferBuilder fbb;
        auto reply = ray::protocol::CreatePrepareActorCheckpointReply(
            fbb, to_flatbuf(fbb, checkpoint_id));
        fbb.Finish(reply);
        worker->Connection()->WriteMessageAsync(
            static_cast<int64_t>(protocol::MessageType::PrepareActorCheckpointReply),
            fbb.GetSize(), fbb.GetBufferPointer(), [](const ray::Status &status) {
              if (!status.ok()) {
                RAY_LOG(WARNING)
                    << "Failed to send PrepareActorCheckpointReply to client";
              }
            });
      }));
}

void NodeManager::ProcessNotifyActorResumedFromCheckpoint(const uint8_t *message_data) {
  auto message =
      flatbuffers::GetRoot<protocol::NotifyActorResumedFromCheckpoint>(message_data);
  ActorID actor_id = from_flatbuf<ActorID>(*message->actor_id());
  ActorCheckpointID checkpoint_id =
      from_flatbuf<ActorCheckpointID>(*message->checkpoint_id());
  RAY_LOG(DEBUG) << "Actor " << actor_id << " was resumed from checkpoint "
                 << checkpoint_id;
  checkpoint_id_to_restore_.emplace(actor_id, checkpoint_id);
}

void NodeManager::ProcessSubmitTaskMessage(const uint8_t *message_data) {
  // Read the task submitted by the client.
  auto fbs_message = flatbuffers::GetRoot<protocol::SubmitTaskRequest>(message_data);
  rpc::Task task_message;
  RAY_CHECK(task_message.mutable_task_spec()->ParseFromArray(
      fbs_message->task_spec()->data(), fbs_message->task_spec()->size()));

  // Submit the task to the raylet. Since the task was submitted
  // locally, there is no uncommitted lineage.
  SubmitTask(Task(task_message), Lineage());
}

void NodeManager::DispatchScheduledTasksToWorkers() {
  RAY_CHECK(new_scheduler_enabled_);

  // Check every task in task_to_dispatch queue to see
  // whether it can be dispatched and ran. This avoids head-of-line
  // blocking where a task which cannot be dispatched because
  // there are not enough available resources blocks other
  // tasks from being dispatched.
  for (size_t queue_size = tasks_to_dispatch_.size(); queue_size > 0; queue_size--) {
    auto task = tasks_to_dispatch_.front();
    auto reply = task.first;
    auto spec = task.second.GetTaskSpecification();
    tasks_to_dispatch_.pop_front();

    std::shared_ptr<Worker> worker = worker_pool_.PopWorker(spec);
    if (!worker) {
      // No worker available to schedule this task.
      // Put the task back in the dispatch queue.
      tasks_to_dispatch_.push_front(task);
      return;
    }

    std::shared_ptr<TaskResourceInstances> allocated_instances(
        new TaskResourceInstances());
    bool schedulable = new_resource_scheduler_->AllocateLocalTaskResources(
        spec.GetRequiredResources().GetResourceMap(), allocated_instances);
    if (!schedulable) {
      // Not enough resources to schedule this task.
      // Put it back at the end of the dispatch queue.
      tasks_to_dispatch_.push_back(task);
      worker_pool_.PushWorker(worker);
      // Try next task in the dispatch queue.
      continue;
    }
    worker->SetOwnerAddress(spec.CallerAddress());
    if (spec.IsActorCreationTask()) {
      // The actor belongs to this worker now.
      worker->AssignActorId(spec.ActorCreationId());
      worker->SetLifetimeAllocatedInstances(allocated_instances);
    } else {
      worker->SetAllocatedInstances(allocated_instances);
    }
    worker->AssignTaskId(spec.TaskId());
    worker->AssignJobId(spec.JobId());
    worker->SetAssignedTask(task.second);

    reply(worker, ClientID::Nil(), "", -1);
  }
}

void NodeManager::NewSchedulerSchedulePendingTasks() {
  RAY_CHECK(new_scheduler_enabled_);
  size_t queue_size = tasks_to_schedule_.size();

  // Check every task in task_to_schedule queue to see
  // whether it can be scheduled. This avoids head-of-line
  // blocking where a task which cannot be scheduled because
  // there are not enough available resources blocks other
  // tasks from being scheduled.
  while (queue_size > 0) {
    if (queue_size == 0) {
      return;
    } else {
      queue_size--;
    }
    auto work = tasks_to_schedule_.front();
    auto task = work.second;
    auto request_resources =
        task.GetTaskSpecification().GetRequiredResources().GetResourceMap();
    int64_t violations = 0;
    std::string node_id_string =
        new_resource_scheduler_->GetBestSchedulableNode(request_resources, &violations);
    if (node_id_string.empty()) {
      /// There is no node that has available resources to run the request.
      tasks_to_schedule_.pop_front();
      tasks_to_schedule_.push_back(work);
      continue;
    } else {
      if (node_id_string == self_node_id_.Binary()) {
        WaitForTaskArgsRequests(work);
      } else {
        new_resource_scheduler_->AllocateRemoteTaskResources(node_id_string,
                                                             request_resources);

        ClientID node_id = ClientID::FromBinary(node_id_string);
        auto node_info_opt = gcs_client_->Nodes().Get(node_id);
        RAY_CHECK(node_info_opt)
            << "Spilling back to a node manager, but no GCS info found for node "
            << node_id;
        work.first(nullptr, node_id, node_info_opt->node_manager_address(),
                   node_info_opt->node_manager_port());
      }
      tasks_to_schedule_.pop_front();
    }
  }
  DispatchScheduledTasksToWorkers();
}

void NodeManager::WaitForTaskArgsRequests(std::pair<ScheduleFn, Task> &work) {
  RAY_CHECK(new_scheduler_enabled_);
  const Task &task = work.second;
  std::vector<ObjectID> object_ids = task.GetTaskSpecification().GetDependencies();

  if (object_ids.size() > 0) {
    bool args_ready = task_dependency_manager_.SubscribeGetDependencies(
        task.GetTaskSpecification().TaskId(), task.GetDependencies());
    if (args_ready) {
      task_dependency_manager_.UnsubscribeGetDependencies(
          task.GetTaskSpecification().TaskId());
      tasks_to_dispatch_.push_back(work);
    } else {
      waiting_tasks_[task.GetTaskSpecification().TaskId()] = work;
    }
  } else {
    tasks_to_dispatch_.push_back(work);
  }
}

void NodeManager::HandleRequestWorkerLease(const rpc::RequestWorkerLeaseRequest &request,
                                           rpc::RequestWorkerLeaseReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  rpc::Task task_message;
  task_message.mutable_task_spec()->CopyFrom(request.resource_spec());
  Task task(task_message);
  bool is_actor_creation_task = task.GetTaskSpecification().IsActorCreationTask();
  ActorID actor_id = ActorID::Nil();

  if (is_actor_creation_task) {
    actor_id = task.GetTaskSpecification().ActorCreationId();

    // Save the actor creation task spec to GCS, which is needed to
    // reconstruct the actor when raylet detect it dies.
    std::shared_ptr<rpc::TaskTableData> data = std::make_shared<rpc::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(
        task.GetTaskSpecification().GetMessage());
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncAdd(data, nullptr));
  }

  if (new_scheduler_enabled_) {
    auto task_spec = task.GetTaskSpecification();
    auto work = std::make_pair(
        [this, task_spec, reply, send_reply_callback](std::shared_ptr<Worker> worker,
                                                      ClientID spillback_to,
                                                      std::string address, int port) {
          if (worker != nullptr) {
            reply->mutable_worker_address()->set_ip_address(worker->IpAddress());
            reply->mutable_worker_address()->set_port(worker->Port());
            reply->mutable_worker_address()->set_worker_id(worker->WorkerId().Binary());
            reply->mutable_worker_address()->set_raylet_id(self_node_id_.Binary());
            RAY_CHECK(leased_workers_.find(worker->WorkerId()) == leased_workers_.end());
            leased_workers_[worker->WorkerId()] = worker;
// TODO (Ion): Fix handling floating point errors, maybe by moving to integers.
#define ZERO_CAPACITY 1.0e-5
            std::shared_ptr<TaskResourceInstances> allocated_resources;
            if (task_spec.IsActorCreationTask()) {
              allocated_resources = worker->GetLifetimeAllocatedInstances();
            } else {
              allocated_resources = worker->GetAllocatedInstances();
            }
            auto predefined_resources = allocated_resources->predefined_resources;
            ::ray::rpc::ResourceMapEntry *resource;
            for (size_t res_idx = 0; res_idx < predefined_resources.size(); res_idx++) {
              bool first = true;  // Set resource name only if at least one of its
                                  // instances has available capacity.
              for (size_t inst_idx = 0; inst_idx < predefined_resources[res_idx].size();
                   inst_idx++) {
                if (std::abs(predefined_resources[res_idx][inst_idx]) > ZERO_CAPACITY) {
                  if (first) {
                    resource = reply->add_resource_mapping();
                    resource->set_name(
                        new_resource_scheduler_->GetResourceNameFromIndex(res_idx));
                    first = false;
                  }
                  auto rid = resource->add_resource_ids();
                  rid->set_index(inst_idx);
                  rid->set_quantity(predefined_resources[res_idx][inst_idx]);
                }
              }
            }
            auto custom_resources = allocated_resources->custom_resources;
            for (auto it = custom_resources.begin(); it != custom_resources.end(); ++it) {
              bool first = true;  // Set resource name only if at least one of its
                                  // instances has available capacity.
              for (size_t inst_idx = 0; inst_idx < it->second.size(); inst_idx++) {
                if (std::abs(it->second[inst_idx]) > ZERO_CAPACITY) {
                  if (first) {
                    resource = reply->add_resource_mapping();
                    resource->set_name(
                        new_resource_scheduler_->GetResourceNameFromIndex(it->first));
                    first = false;
                  }
                  auto rid = resource->add_resource_ids();
                  rid->set_index(inst_idx);
                  rid->set_quantity(it->second[inst_idx]);
                }
              }
            }
          } else {
            reply->mutable_retry_at_raylet_address()->set_ip_address(address);
            reply->mutable_retry_at_raylet_address()->set_port(port);
            reply->mutable_retry_at_raylet_address()->set_raylet_id(
                spillback_to.Binary());
          }
          send_reply_callback(Status::OK(), nullptr, nullptr);
        },
        task);
    tasks_to_schedule_.push_back(work);
    NewSchedulerSchedulePendingTasks();
    return;
  }

  // Override the task dispatch to call back to the client instead of executing the
  // task directly on the worker.
  RAY_LOG(DEBUG) << "Worker lease request " << task.GetTaskSpecification().TaskId();
  TaskID task_id = task.GetTaskSpecification().TaskId();
  rpc::Address owner_address = task.GetTaskSpecification().CallerAddress();
  task.OnDispatchInstead(
      [this, owner_address, reply, send_reply_callback](
          const std::shared_ptr<void> granted, const std::string &address, int port,
          const WorkerID &worker_id, const ResourceIdSet &resource_ids) {
        reply->mutable_worker_address()->set_ip_address(address);
        reply->mutable_worker_address()->set_port(port);
        reply->mutable_worker_address()->set_worker_id(worker_id.Binary());
        reply->mutable_worker_address()->set_raylet_id(self_node_id_.Binary());
        for (const auto &mapping : resource_ids.AvailableResources()) {
          auto resource = reply->add_resource_mapping();
          resource->set_name(mapping.first);
          for (const auto &id : mapping.second.WholeIds()) {
            auto rid = resource->add_resource_ids();
            rid->set_index(id);
            rid->set_quantity(1.0);
          }
          for (const auto &id : mapping.second.FractionalIds()) {
            auto rid = resource->add_resource_ids();
            rid->set_index(id.first);
            rid->set_quantity(id.second.ToDouble());
          }
        }
        send_reply_callback(Status::OK(), nullptr, nullptr);
        RAY_CHECK(leased_workers_.find(worker_id) == leased_workers_.end())
            << "Worker is already leased out " << worker_id;

        auto worker = std::static_pointer_cast<Worker>(granted);
        leased_workers_[worker_id] = worker;
      });
  task.OnSpillbackInstead(
      [reply, task_id, send_reply_callback](const ClientID &spillback_to,
                                            const std::string &address, int port) {
        RAY_LOG(DEBUG) << "Worker lease request SPILLBACK " << task_id;
        reply->mutable_retry_at_raylet_address()->set_ip_address(address);
        reply->mutable_retry_at_raylet_address()->set_port(port);
        reply->mutable_retry_at_raylet_address()->set_raylet_id(spillback_to.Binary());
        send_reply_callback(Status::OK(), nullptr, nullptr);
      });
  task.OnCancellationInstead([reply, task_id, send_reply_callback]() {
    RAY_LOG(DEBUG) << "Task lease request canceled " << task_id;
    reply->set_canceled(true);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  });
  SubmitTask(task, Lineage());
}

void NodeManager::HandleReturnWorker(const rpc::ReturnWorkerRequest &request,
                                     rpc::ReturnWorkerReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  // Read the resource spec submitted by the client.
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  std::shared_ptr<Worker> worker = leased_workers_[worker_id];

  Status status;
  leased_workers_.erase(worker_id);

  if (worker) {
    if (request.disconnect_worker()) {
      ProcessDisconnectClientMessage(worker->Connection());
    } else {
      // Handle the edge case where the worker was returned before we got the
      // unblock RPC by unblocking it immediately (unblock is idempotent).
      if (worker->IsBlocked()) {
        HandleDirectCallTaskUnblocked(worker);
      }
      if (new_scheduler_enabled_) {
        new_resource_scheduler_->SubtractCPUResourceInstances(
            worker->GetBorrowedCPUInstances());
        new_resource_scheduler_->FreeLocalTaskResources(worker->GetAllocatedInstances());
        worker->ClearAllocatedInstances();
      }
      HandleWorkerAvailable(worker);
    }
  } else {
    status = Status::Invalid("Returned worker does not exist any more");
  }
  send_reply_callback(status, nullptr, nullptr);
}

void NodeManager::HandleCancelWorkerLease(const rpc::CancelWorkerLeaseRequest &request,
                                          rpc::CancelWorkerLeaseReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const TaskID task_id = TaskID::FromBinary(request.task_id());
  Task removed_task;
  TaskState removed_task_state;
  const auto canceled =
      local_queues_.RemoveTask(task_id, &removed_task, &removed_task_state);
  if (!canceled) {
    // We do not have the task. This could be because we haven't received the
    // lease request yet, or because we already granted the lease request and
    // it has already been returned.
  } else {
    if (removed_task.OnDispatch()) {
      // We have not yet granted the worker lease. Cancel it now.
      removed_task.OnCancellation()();
      task_dependency_manager_.TaskCanceled(task_id);
      task_dependency_manager_.UnsubscribeGetDependencies(task_id);
    } else {
      // We already granted the worker lease and sent the reply. Re-queue the
      // task and wait for the requester to return the leased worker.
      local_queues_.QueueTasks({removed_task}, removed_task_state);
    }
  }
  // The task cancellation failed if we did not have the task queued, since
  // this means that we may not have received the task request yet. It is
  // successful if we did have the task queued, since we have now replied to
  // the client that requested the lease.
  reply->set_success(canceled);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::HandleForwardTask(const rpc::ForwardTaskRequest &request,
                                    rpc::ForwardTaskReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  // Get the forwarded task and its uncommitted lineage from the request.
  TaskID task_id = TaskID::FromBinary(request.task_id());
  Lineage uncommitted_lineage;
  for (int i = 0; i < request.uncommitted_tasks_size(); i++) {
    Task task(request.uncommitted_tasks(i));
    RAY_CHECK(uncommitted_lineage.SetEntry(task, GcsStatus::UNCOMMITTED));
  }
  const Task &task = uncommitted_lineage.GetEntry(task_id)->TaskData();
  RAY_LOG(DEBUG) << "Received forwarded task " << task.GetTaskSpecification().TaskId()
                 << " on node " << self_node_id_
                 << " spillback=" << task.GetTaskExecutionSpec().NumForwards();
  SubmitTask(task, uncommitted_lineage, /* forwarded = */ true);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::ProcessSetResourceRequest(
    const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data) {
  // Read the SetResource message
  auto message = flatbuffers::GetRoot<protocol::SetResourceRequest>(message_data);

  auto const &resource_name = string_from_flatbuf(*message->resource_name());
  double const &capacity = message->capacity();
  bool is_deletion = capacity <= 0;

  ClientID node_id = from_flatbuf<ClientID>(*message->client_id());

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
    RAY_CHECK_OK(
        gcs_client_->Nodes().AsyncDeleteResources(node_id, {resource_name}, nullptr));
  } else {
    std::unordered_map<std::string, std::shared_ptr<gcs::ResourceTableData>> data_map;
    auto resource_table_data = std::make_shared<gcs::ResourceTableData>();
    resource_table_data->set_resource_capacity(capacity);
    data_map.emplace(resource_name, resource_table_data);
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncUpdateResources(node_id, data_map, nullptr));
  }
}

void NodeManager::ScheduleTasks(
    std::unordered_map<ClientID, SchedulingResources> &resource_map) {
  // If the resource map contains the local raylet, update load before calling policy.
  if (resource_map.count(self_node_id_) > 0) {
    resource_map[self_node_id_].SetLoadResources(local_queues_.GetResourceLoad());
  }
  // Invoke the scheduling policy.
  auto policy_decision = scheduling_policy_.Schedule(resource_map, self_node_id_);

#ifndef NDEBUG
  RAY_LOG(DEBUG) << "[NM ScheduleTasks] policy decision:";
  for (const auto &task_client_pair : policy_decision) {
    TaskID task_id = task_client_pair.first;
    ClientID node_id = task_client_pair.second;
    RAY_LOG(DEBUG) << task_id << " --> " << node_id;
  }
#endif

  // Extract decision for this raylet.
  std::unordered_set<TaskID> local_task_ids;
  // Iterate over (taskid, clientid) pairs, extract tasks assigned to the local node.
  for (const auto &task_client_pair : policy_decision) {
    const TaskID &task_id = task_client_pair.first;
    const ClientID &node_id = task_client_pair.second;
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
    task_dependency_manager_.TaskPending(task);
    move_task_set.insert(task.GetTaskSpecification().TaskId());
    // Push a warning to the task's driver that this task is currently infeasible.
    {
      // TODO(rkn): Define this constant somewhere else.
      std::string type = "infeasible_task";
      std::ostringstream error_message;
      error_message
          << "The actor or task with ID " << task.GetTaskSpecification().TaskId()
          << " is infeasible and cannot currently be scheduled. It requires "
          << task.GetTaskSpecification().GetRequiredResources().ToString()
          << " for execution and "
          << task.GetTaskSpecification().GetRequiredPlacementResources().ToString()
          << " for placement, however there are no nodes in the cluster that can "
          << "provide the requested resources. To resolve this issue, consider "
          << "reducing the resource requests of this task or add nodes that "
          << "can fit the task.";
      auto error_data_ptr =
          gcs::CreateErrorTableData(type, error_message.str(), current_time_ms(),
                                    task.GetTaskSpecification().JobId());
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
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

bool NodeManager::CheckDependencyManagerInvariant() const {
  std::vector<TaskID> pending_task_ids = task_dependency_manager_.GetPendingTasks();
  // Assert that each pending task in the task dependency manager is in one of the queues.
  for (const auto &task_id : pending_task_ids) {
    if (!local_queues_.HasTask(task_id)) {
      return false;
    }
  }
  // TODO(atumanov): perform the check in the opposite direction.
  return true;
}

void NodeManager::TreatTaskAsFailed(const Task &task, const ErrorType &error_type) {
  const TaskSpecification &spec = task.GetTaskSpecification();
  RAY_LOG(DEBUG) << "Treating task " << spec.TaskId() << " as failed because of error "
                 << ErrorType_Name(error_type) << ".";
  // If this was an actor creation task that tried to resume from a checkpoint,
  // then erase it here since the task did not finish.
  if (spec.IsActorCreationTask()) {
    ActorID actor_id = spec.ActorCreationId();
    checkpoint_id_to_restore_.erase(actor_id);
  }
  // Loop over the return IDs (except the dummy ID) and store a fake object in
  // the object store.
  int64_t num_returns = spec.NumReturns();
  if (spec.IsActorCreationTask() || spec.IsActorTask()) {
    // TODO(rkn): We subtract 1 to avoid the dummy ID. However, this leaks
    // information about the TaskSpecification implementation.
    num_returns -= 1;
  }
  // Determine which IDs should be marked as failed.
  std::vector<plasma::ObjectID> objects_to_fail;
  for (int64_t i = 0; i < num_returns; i++) {
    objects_to_fail.push_back(spec.ReturnId(i, TaskTransportType::RAYLET).ToPlasmaId());
  }
  const JobID job_id = task.GetTaskSpecification().JobId();
  MarkObjectsAsFailed(error_type, objects_to_fail, job_id);
  task_dependency_manager_.TaskCanceled(spec.TaskId());
  // Notify the task dependency manager that we no longer need this task's
  // object dependencies. TODO(swang): Ideally, we would check the return value
  // here. However, we don't know at this point if the task was in the WAITING
  // or READY queue before, in which case we would not have been subscribed to
  // its dependencies.
  task_dependency_manager_.UnsubscribeGetDependencies(spec.TaskId());
}

void NodeManager::MarkObjectsAsFailed(const ErrorType &error_type,
                                      const std::vector<plasma::ObjectID> objects_to_fail,
                                      const JobID &job_id) {
  const std::string meta = std::to_string(static_cast<int>(error_type));
  for (const auto &object_id : objects_to_fail) {
    arrow::Status status = store_client_.CreateAndSeal(object_id, "", meta);
    if (!status.ok() && !plasma::IsPlasmaObjectExists(status)) {
      // If we failed to save the error code, log a warning and push an error message
      // to the driver.
      std::ostringstream stream;
      stream << "An plasma error (" << status.ToString() << ") occurred while saving"
             << " error code to object " << object_id << ". Anyone who's getting this"
             << " object may hang forever.";
      std::string error_message = stream.str();
      RAY_LOG(WARNING) << error_message;
      auto error_data_ptr =
          gcs::CreateErrorTableData("task", error_message, current_time_ms(), job_id);
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
}

void NodeManager::TreatTaskAsFailedIfLost(const Task &task) {
  const TaskSpecification &spec = task.GetTaskSpecification();
  RAY_LOG(DEBUG) << "Treating task " << spec.TaskId()
                 << " as failed if return values lost.";
  // Loop over the return IDs (except the dummy ID) and check whether a
  // location for the return ID exists.
  int64_t num_returns = spec.NumReturns();
  if (spec.IsActorCreationTask() || spec.IsActorTask()) {
    // TODO(rkn): We subtract 1 to avoid the dummy ID. However, this leaks
    // information about the TaskSpecification implementation.
    num_returns -= 1;
  }
  // Use a shared flag to make sure that we only treat the task as failed at
  // most once. This flag will get deallocated once all of the object table
  // lookup callbacks are fired.
  auto task_marked_as_failed = std::make_shared<bool>(false);
  for (int64_t i = 0; i < num_returns; i++) {
    const ObjectID object_id = spec.ReturnId(i, TaskTransportType::RAYLET);
    // Lookup the return value's locations.
    RAY_CHECK_OK(object_directory_->LookupLocations(
        object_id, [this, task_marked_as_failed, task](
                       const ray::ObjectID &object_id,
                       const std::unordered_set<ray::ClientID> &clients) {
          if (!*task_marked_as_failed) {
            // Only process the object locations if we haven't already marked the
            // task as failed.
            if (clients.empty()) {
              // The object does not exist on any nodes but has been created
              // before, so the object has been lost. Mark the task as failed to
              // prevent any tasks that depend on this object from hanging.
              TreatTaskAsFailed(task, ErrorType::OBJECT_UNRECONSTRUCTABLE);
              *task_marked_as_failed = true;
            }
          }
        }));
  }
}

void NodeManager::SubmitTask(const Task &task, const Lineage &uncommitted_lineage,
                             bool forwarded) {
  stats::TaskCountReceived().Record(1);
  const TaskSpecification &spec = task.GetTaskSpecification();
  const TaskID &task_id = spec.TaskId();
  RAY_LOG(DEBUG) << "Submitting task: " << task.DebugString();

  if (local_queues_.HasTask(task_id)) {
    RAY_LOG(WARNING) << "Submitted task " << task_id
                     << " is already queued and will not be reconstructed. This is most "
                        "likely due to spurious reconstruction.";
    return;
  }

  if (spec.IsActorTask()) {
    // Check whether we know the location of the actor.
    const auto actor_entry = actor_registry_.find(spec.ActorId());
    bool seen = actor_entry != actor_registry_.end();
    // If we have already seen this actor and this actor is not being reconstructed,
    // its location is known.
    bool location_known =
        seen && actor_entry->second.GetState() != ActorTableData::RECONSTRUCTING;
    if (location_known) {
      if (actor_entry->second.GetState() == ActorTableData::DEAD) {
        // If this actor is dead, either because the actor process is dead
        // or because its residing node is dead, treat this task as failed.
        TreatTaskAsFailed(task, ErrorType::ACTOR_DIED);
      } else {
        // If this actor is alive, check whether this actor is local.
        auto node_manager_id = actor_entry->second.GetNodeManagerId();
        if (node_manager_id == self_node_id_) {
          // The actor is local.
          int64_t expected_task_counter =
              GetExpectedTaskCounter(actor_registry_, spec.ActorId(), spec.CallerId());
          if (static_cast<int64_t>(spec.ActorCounter()) < expected_task_counter) {
            // A task that has already been executed before has been found. The
            // task will be treated as failed if at least one of the task's
            // return values have been evicted, to prevent the application from
            // hanging.
            // TODO(swang): Clean up the task from the lineage cache? If the
            // task is not marked as failed, then it may never get marked as
            // ready to flush to the GCS.
            RAY_LOG(WARNING) << "A task was resubmitted, so we are ignoring it. This "
                             << "should only happen during reconstruction.";
            TreatTaskAsFailedIfLost(task);
          } else {
            // The task has not yet been executed. Queue the task for local
            // execution, bypassing placement.
            EnqueuePlaceableTask(task);
          }
        } else {
          // The actor is remote. Forward the task to the node manager that owns
          // the actor.
          // Attempt to forward the task. If this fails to forward the task,
          // the task will be resubmit locally.
          ForwardTaskOrResubmit(task, node_manager_id);
        }
      }
    } else {
      ObjectID actor_creation_dummy_object;
      if (!seen) {
        // We do not have a registered location for the object, so either the
        // actor has not yet been created or we missed the notification for the
        // actor creation because this node joined the cluster after the actor
        // was already created. Look up the actor's registered location in case
        // we missed the creation notification.
        const ActorID &actor_id = spec.ActorId();
        auto lookup_callback =
            [this, actor_id](Status status, const boost::optional<ActorTableData> &data) {
              if (data) {
                // The actor has been created. We only need the last entry, because
                // it represents the latest state of this actor.
                HandleActorStateTransition(actor_id, ActorRegistration(*data));
              }
            };
        RAY_CHECK_OK(gcs_client_->Actors().AsyncGet(actor_id, lookup_callback));
        actor_creation_dummy_object = spec.ActorCreationDummyObjectId();
      } else {
        actor_creation_dummy_object = actor_entry->second.GetActorCreationDependency();
      }

      // Keep the task queued until we discover the actor's location.
      // (See design_docs/task_states.rst for the state transition diagram.)
      local_queues_.QueueTasks({task}, TaskState::WAITING_FOR_ACTOR_CREATION);
      // The actor has not yet been created and may have failed. To make sure
      // that the actor is eventually recreated, we maintain the invariant that
      // if a task is in the MethodsWaitingForActorCreation queue, then it is
      // subscribed to its respective actor creation task and that task only.
      // Once the actor has been created and this method removed from the
      // waiting queue, the caller must make the corresponding call to
      // UnsubscribeGetDependencies.
      task_dependency_manager_.SubscribeGetDependencies(spec.TaskId(),
                                                        {actor_creation_dummy_object});
      // Mark the task as pending. It will be canceled once we discover the
      // actor's location and either execute the task ourselves or forward it
      // to another node.
      task_dependency_manager_.TaskPending(task);
    }
  } else {
    // This is a non-actor task. Queue the task for a placement decision or for dispatch
    // if the task was forwarded.
    if (forwarded) {
      // Check for local dependencies and enqueue as waiting or ready for dispatch.
      EnqueuePlaceableTask(task);
    } else {
      // (See design_docs/task_states.rst for the state transition diagram.)
      local_queues_.QueueTasks({task}, TaskState::PLACEABLE);
      ScheduleTasks(cluster_resource_map_);
      // TODO(atumanov): assert that !placeable.isempty() => insufficient available
      // resources locally.
    }
  }
}

void NodeManager::HandleDirectCallTaskBlocked(const std::shared_ptr<Worker> &worker) {
  if (new_scheduler_enabled_) {
    if (!worker) {
      return;
    }
    std::vector<double> cpu_instances;
    if (worker->GetAllocatedInstances() != nullptr) {
      cpu_instances = worker->GetAllocatedInstances()->GetCPUInstances();
    }
    if (cpu_instances.size() > 0) {
      std::vector<double> borrowed_cpu_instances =
          new_resource_scheduler_->AddCPUResourceInstances(cpu_instances);
      worker->SetBorrowedCPUInstances(borrowed_cpu_instances);
      worker->MarkBlocked();
    }
    NewSchedulerSchedulePendingTasks();
    return;
  }

  if (!worker || worker->GetAssignedTaskId().IsNil() || worker->IsBlocked()) {
    return;  // The worker may have died or is no longer processing the task.
  }
  auto const cpu_resource_ids = worker->ReleaseTaskCpuResources();
  local_available_resources_.Release(cpu_resource_ids);
  cluster_resource_map_[self_node_id_].Release(cpu_resource_ids.ToResourceSet());
  worker->MarkBlocked();
  DispatchTasks(local_queues_.GetReadyTasksByClass());
}

void NodeManager::HandleDirectCallTaskUnblocked(const std::shared_ptr<Worker> &worker) {
  if (new_scheduler_enabled_) {
    if (!worker) {
      return;
    }
    std::vector<double> cpu_instances;
    if (worker->GetAllocatedInstances() != nullptr) {
      cpu_instances = worker->GetAllocatedInstances()->GetCPUInstances();
    }
    if (cpu_instances.size() > 0) {
      new_resource_scheduler_->SubtractCPUResourceInstances(cpu_instances);
      new_resource_scheduler_->AddCPUResourceInstances(worker->GetBorrowedCPUInstances());
      worker->MarkUnblocked();
    }
    NewSchedulerSchedulePendingTasks();
    return;
  }

  if (!worker || worker->GetAssignedTaskId().IsNil()) {
    return;  // The worker may have died or is no longer processing the task.
  }
  TaskID task_id = worker->GetAssignedTaskId();

  // First, always release task dependencies. This ensures we don't leak resources even
  // if we don't need to unblock the worker below.
  task_dependency_manager_.UnsubscribeGetDependencies(task_id);

  if (!worker->IsBlocked()) {
    return;  // Don't need to unblock the worker.
  }

  Task task = local_queues_.GetTaskOfState(task_id, TaskState::RUNNING);
  const auto required_resources = task.GetTaskSpecification().GetRequiredResources();
  const ResourceSet cpu_resources = required_resources.GetNumCpus();
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

void NodeManager::AsyncResolveObjects(const std::shared_ptr<ClientConnection> &client,
                                      const std::vector<ObjectID> &required_object_ids,
                                      const TaskID &current_task_id, bool ray_get,
                                      bool mark_worker_blocked) {
  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
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
  // fetched and/or reconstructed as necessary, until the objects become local
  // or are unsubscribed.
  if (ray_get) {
    // TODO(ekl) using the assigned task id is a hack to handle unsubscription for
    // HandleDirectCallUnblocked.
    auto &task_id = mark_worker_blocked ? current_task_id : worker->GetAssignedTaskId();
    if (!task_id.IsNil()) {
      task_dependency_manager_.SubscribeGetDependencies(task_id, required_object_ids);
    }
  } else {
    task_dependency_manager_.SubscribeWaitDependencies(worker->WorkerId(),
                                                       required_object_ids);
  }
}

void NodeManager::AsyncResolveObjectsFinish(
    const std::shared_ptr<ClientConnection> &client, const TaskID &current_task_id,
    bool was_blocked) {
  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);

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

  // Unsubscribe from any `ray.get` objects that the task was blocked on.  Any
  // fetch or reconstruction operations to make the objects local are canceled.
  // `ray.wait` calls will stay active until the objects become local, or the
  // task/actor that called `ray.wait` exits.
  task_dependency_manager_.UnsubscribeGetDependencies(current_task_id);
  // Mark the task as unblocked.
  RAY_CHECK(worker);
  if (was_blocked) {
    worker->RemoveBlockedTaskId(current_task_id);
    local_queues_.RemoveBlockedTaskId(current_task_id);
  }
}

void NodeManager::EnqueuePlaceableTask(const Task &task) {
  // TODO(atumanov): add task lookup hashmap and change EnqueuePlaceableTask to take
  // a vector of TaskIDs. Trigger MoveTask internally.
  // Subscribe to the task's dependencies.
  bool args_ready = task_dependency_manager_.SubscribeGetDependencies(
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
  // Mark the task as pending. Once the task has finished execution, or once it
  // has been forwarded to another node, the task must be marked as canceled in
  // the TaskDependencyManager.
  task_dependency_manager_.TaskPending(task);
}

void NodeManager::AssignTask(const std::shared_ptr<Worker> &worker, const Task &task,
                             std::vector<std::function<void()>> *post_assign_callbacks) {
  const TaskSpecification &spec = task.GetTaskSpecification();
  RAY_CHECK(post_assign_callbacks);

  // If this is an actor task, check that the new task has the correct counter.
  if (spec.IsActorTask()) {
    // An actor task should only be ready to be assigned if it matches the
    // expected task counter.
    int64_t expected_task_counter =
        GetExpectedTaskCounter(actor_registry_, spec.ActorId(), spec.CallerId());
    RAY_CHECK(static_cast<int64_t>(spec.ActorCounter()) == expected_task_counter)
        << "Expected actor counter: " << expected_task_counter << ", task "
        << spec.TaskId() << " has: " << spec.ActorCounter();
  }

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
  if (task.OnDispatch() != nullptr) {
    if (task.GetTaskSpecification().IsDetachedActor()) {
      worker->MarkDetachedActor();
    }

    const auto owner_worker_id = WorkerID::FromBinary(spec.CallerAddress().worker_id());
    const auto owner_node_id = ClientID::FromBinary(spec.CallerAddress().raylet_id());
    RAY_CHECK(!owner_worker_id.IsNil());
    RAY_LOG(DEBUG) << "Worker lease request DISPATCH " << task_id << " to worker "
                   << worker->WorkerId() << ", owner ID " << owner_worker_id;

    task.OnDispatch()(worker, initial_config_.node_manager_address, worker->Port(),
                      worker->WorkerId(),
                      spec.IsActorCreationTask() ? worker->GetLifetimeResourceIds()
                                                 : worker->GetTaskResourceIds());

    // If the owner has died since this task was queued, cancel the task by
    // killing the worker.
    if (failed_workers_cache_.count(owner_worker_id) > 0 ||
        failed_nodes_cache_.count(owner_node_id) > 0) {
      // TODO(swang): Skip assigning this task to this worker instead of
      // killing the worker?
      KillWorker(worker);
    }

    post_assign_callbacks->push_back([this, worker, task_id]() {
      RAY_LOG(DEBUG) << "Finished assigning task " << task_id << " to worker "
                     << worker->WorkerId();

      FinishAssignTask(worker, task_id, /*success=*/true);
    });
  } else {
    ResourceIdSet resource_id_set =
        worker->GetTaskResourceIds().Plus(worker->GetLifetimeResourceIds());
    if (worker->AssignTask(task, resource_id_set).ok()) {
      RAY_LOG(DEBUG) << "Assigned task " << task_id << " to worker "
                     << worker->WorkerId();
      post_assign_callbacks->push_back([this, worker, task_id]() {
        FinishAssignTask(worker, task_id, /*success=*/true);
      });
    } else {
      RAY_LOG(ERROR) << "Failed to assign task " << task_id << " to worker "
                     << worker->WorkerId() << ", disconnecting client";
      post_assign_callbacks->push_back([this, worker, task_id]() {
        FinishAssignTask(worker, task_id, /*success=*/false);
      });
    }
  }
}

bool NodeManager::FinishAssignedTask(Worker &worker) {
  TaskID task_id = worker.GetAssignedTaskId();
  RAY_LOG(DEBUG) << "Finished task " << task_id;

  Task task;
  if (new_scheduler_enabled_) {
    task = worker.GetAssignedTask();
    // leased_workers_.erase(worker.WorkerId()); // Maybe RAY_CHECK ???
    if (worker.GetAllocatedInstances() != nullptr) {
      new_resource_scheduler_->SubtractCPUResourceInstances(
          worker.GetBorrowedCPUInstances());
      new_resource_scheduler_->FreeLocalTaskResources(worker.GetAllocatedInstances());
      worker.ClearAllocatedInstances();
    }
  } else {
    // (See design_docs/task_states.rst for the state transition diagram.)
    RAY_CHECK(local_queues_.RemoveTask(task_id, &task));

    // Release task's resources. The worker's lifetime resources are still held.
    auto const &task_resources = worker.GetTaskResourceIds();
    local_available_resources_.ReleaseConstrained(
        task_resources, cluster_resource_map_[self_node_id_].GetTotalResources());
    cluster_resource_map_[self_node_id_].Release(task_resources.ToResourceSet());
    worker.ResetTaskResourceIds();
  }

  const auto &spec = task.GetTaskSpecification();  //
  if ((spec.IsActorCreationTask() || spec.IsActorTask())) {
    // If this was an actor or actor creation task, handle the actor's new
    // state.
    FinishAssignedActorTask(worker, task);
  } else {
    // If this was a non-actor task, then cancel any ray.wait calls that were
    // made during the task execution.
    task_dependency_manager_.UnsubscribeWaitDependencies(worker.WorkerId());
  }

  // Notify the task dependency manager that this task has finished execution.
  task_dependency_manager_.UnsubscribeGetDependencies(spec.TaskId());
  task_dependency_manager_.TaskCanceled(task_id);

  // Unset the worker's assigned job Id if this is not an actor.
  if (!spec.IsActorCreationTask() && !spec.IsActorTask()) {
    worker.AssignJobId(JobID::Nil());
  }
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

std::shared_ptr<ActorTableData> NodeManager::CreateActorTableDataFromCreationTask(
    const TaskSpecification &task_spec, int port, const WorkerID &worker_id) {
  RAY_CHECK(task_spec.IsActorCreationTask());
  auto actor_id = task_spec.ActorCreationId();
  auto actor_entry = actor_registry_.find(actor_id);
  std::shared_ptr<ActorTableData> actor_info_ptr;
  // TODO(swang): If this is an actor that was reconstructed, and previous
  // actor notifications were delayed, then this node may not have an entry for
  // the actor in actor_regisry_. Then, the fields for the number of
  // reconstructions will be wrong.
  if (actor_entry == actor_registry_.end()) {
    actor_info_ptr.reset(new ActorTableData());
    // Set all of the static fields for the actor. These fields will not
    // change even if the actor fails or is reconstructed.
    actor_info_ptr->set_actor_id(actor_id.Binary());
    actor_info_ptr->set_actor_creation_dummy_object_id(
        task_spec.ActorDummyObject().Binary());
    actor_info_ptr->set_job_id(task_spec.JobId().Binary());
    actor_info_ptr->set_max_reconstructions(task_spec.MaxActorReconstructions());
    // This is the first time that the actor has been created, so the number
    // of remaining reconstructions is the max.
    actor_info_ptr->set_remaining_reconstructions(task_spec.MaxActorReconstructions());
    actor_info_ptr->set_is_detached(task_spec.IsDetachedActor());
    actor_info_ptr->mutable_owner_address()->CopyFrom(
        task_spec.GetMessage().caller_address());
  } else {
    // If we've already seen this actor, it means that this actor was reconstructed.
    // Thus, its previous state must be RECONSTRUCTING.
    // TODO: The following is a workaround for the issue described in
    // https://github.com/ray-project/ray/issues/5524, please see the issue
    // description for more information.
    if (actor_entry->second.GetState() != ActorTableData::RECONSTRUCTING) {
      RAY_LOG(WARNING) << "Actor not in reconstructing state, most likely it "
                       << "died before creation handler could run. Actor state is "
                       << actor_entry->second.GetState();
    }
    // Copy the static fields from the current actor entry.
    actor_info_ptr.reset(new ActorTableData(actor_entry->second.GetTableData()));
    // We are reconstructing the actor, so subtract its
    // remaining_reconstructions by 1.
    actor_info_ptr->set_remaining_reconstructions(
        actor_info_ptr->remaining_reconstructions() - 1);
  }

  // Set the new fields for the actor's state to indicate that the actor is
  // now alive on this node manager.
  actor_info_ptr->mutable_address()->set_ip_address(
      gcs_client_->Nodes().GetSelfInfo().node_manager_address());
  actor_info_ptr->mutable_address()->set_port(port);
  actor_info_ptr->mutable_address()->set_raylet_id(self_node_id_.Binary());
  actor_info_ptr->mutable_address()->set_worker_id(worker_id.Binary());
  actor_info_ptr->set_state(ActorTableData::ALIVE);
  actor_info_ptr->set_timestamp(current_time_ms());
  return actor_info_ptr;
}

void NodeManager::FinishAssignedActorTask(Worker &worker, const Task &task) {
  RAY_LOG(INFO) << "Finishing assigned actor task";
  ActorID actor_id;
  TaskID caller_id;
  const TaskSpecification task_spec = task.GetTaskSpecification();
  bool resumed_from_checkpoint = false;
  if (task_spec.IsActorCreationTask()) {
    actor_id = task_spec.ActorCreationId();
    caller_id = TaskID::Nil();
    if (checkpoint_id_to_restore_.count(actor_id) > 0) {
      resumed_from_checkpoint = true;
    }
  } else {
    actor_id = task_spec.ActorId();
    caller_id = task_spec.CallerId();
  }

  if (task_spec.IsActorCreationTask()) {
    // This was an actor creation task. Convert the worker to an actor.
    worker.AssignActorId(actor_id);

    if (task_spec.IsDetachedActor()) {
      worker.MarkDetachedActor();
    }

    if (RayConfig::instance().gcs_service_enabled() &&
        RayConfig::instance().gcs_actor_service_enabled()) {
      // Gcs server is responsible for notifying other nodes of the changes of actor
      // status, and thus raylet doesn't need to handle this anymore.
      // And if `new_scheduler_enabled_` is true, this function `FinishAssignedActorTask`
      // will not be called because raylet is not aware of the actual task when receiving
      // a worker lease request.
      return;
    }

    // Lookup the parent actor id.
    auto parent_task_id = task_spec.ParentTaskId();
    int port = worker.Port();
    auto worker_id = worker.WorkerId();
    RAY_CHECK_OK(
        gcs_client_->Tasks().AsyncGet(
            parent_task_id,
            /*callback=*/
            [this, task_spec, resumed_from_checkpoint, port, parent_task_id, worker_id](
                Status status, const boost::optional<TaskTableData> &parent_task_data) {
              if (parent_task_data) {
                // The task was in the GCS task table. Use the stored task spec to
                // get the parent actor id.
                Task parent_task(parent_task_data->task());
                ActorID parent_actor_id = ActorID::Nil();
                if (parent_task.GetTaskSpecification().IsActorCreationTask()) {
                  parent_actor_id = parent_task.GetTaskSpecification().ActorCreationId();
                } else if (parent_task.GetTaskSpecification().IsActorTask()) {
                  parent_actor_id = parent_task.GetTaskSpecification().ActorId();
                }
                FinishAssignedActorCreationTask(parent_actor_id, task_spec,
                                                resumed_from_checkpoint, port, worker_id);
                return;
              }
              // The parent task was not in the GCS task table. It should most likely be
              // in the lineage cache.
              ActorID parent_actor_id = ActorID::Nil();
              if (lineage_cache_.ContainsTask(parent_task_id)) {
                // Use a copy of the cached task spec to get the parent actor id.
                Task parent_task = lineage_cache_.GetTaskOrDie(parent_task_id);
                if (parent_task.GetTaskSpecification().IsActorCreationTask()) {
                  parent_actor_id = parent_task.GetTaskSpecification().ActorCreationId();
                } else if (parent_task.GetTaskSpecification().IsActorTask()) {
                  parent_actor_id = parent_task.GetTaskSpecification().ActorId();
                }
              } else {
                RAY_LOG(WARNING)
                    << "Task metadata not found in either GCS or lineage cache. It may "
                       "have "
                       "been "
                       "evicted "
                    << "by the redis LRU configuration. Consider increasing the memory "
                       "allocation via "
                    << "ray.init(redis_max_memory=<max_memory_bytes>).";
              }
              FinishAssignedActorCreationTask(parent_actor_id, task_spec,
                                              resumed_from_checkpoint, port, worker_id);
            }));
  } else {
    auto actor_entry = actor_registry_.find(actor_id);
    RAY_CHECK(actor_entry != actor_registry_.end());
    // Extend the actor's frontier to include the executed task.
    const ObjectID object_to_release =
        actor_entry->second.ExtendFrontier(caller_id, task_spec.ActorDummyObject());
    if (!object_to_release.IsNil()) {
      // If there were no new actor handles created, then no other actor task
      // will depend on this execution dependency, so it safe to release.
      HandleObjectMissing(object_to_release);
    }
    // Mark the dummy object as locally available to indicate that the actor's
    // state has changed and the next method can run. This is not added to the
    // object table, so the update will be invisible to both the local object
    // manager and the other nodes.
    // NOTE(swang): The dummy objects must be marked as local whenever
    // ExtendFrontier is called, and vice versa, so that we can clean up the
    // dummy objects properly in case the actor fails and needs to be
    // reconstructed.
    HandleObjectLocal(task_spec.ActorDummyObject());
  }
}

void NodeManager::FinishAssignedActorCreationTask(const ActorID &parent_actor_id,
                                                  const TaskSpecification &task_spec,
                                                  bool resumed_from_checkpoint, int port,
                                                  const WorkerID &worker_id) {
  // Notify the other node managers that the actor has been created.
  const ActorID actor_id = task_spec.ActorCreationId();
  auto new_actor_info = CreateActorTableDataFromCreationTask(task_spec, port, worker_id);
  new_actor_info->set_parent_id(parent_actor_id.Binary());
  auto update_callback = [actor_id](Status status) {
    if (!status.ok()) {
      // Only one node at a time should succeed at creating or updating the actor.
      RAY_LOG(FATAL) << "Failed to update state to ALIVE for actor " << actor_id;
    }
  };

  if (resumed_from_checkpoint) {
    // This actor was resumed from a checkpoint. In this case, we first look
    // up the checkpoint in GCS and use it to restore the actor registration
    // and frontier.
    const auto checkpoint_id = checkpoint_id_to_restore_[actor_id];
    checkpoint_id_to_restore_.erase(actor_id);
    RAY_LOG(DEBUG) << "Looking up checkpoint " << checkpoint_id << " for actor "
                   << actor_id;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGetCheckpoint(
        checkpoint_id, actor_id,
        [this, checkpoint_id, actor_id, new_actor_info, update_callback](
            Status status, const boost::optional<ActorCheckpointData> &checkpoint_data) {
          RAY_CHECK(checkpoint_data) << "Couldn't find checkpoint " << checkpoint_id
                                     << " for actor " << actor_id << " in GCS.";
          RAY_LOG(INFO) << "Restoring registration for actor " << actor_id
                        << " from checkpoint " << checkpoint_id;
          ActorRegistration actor_registration =
              ActorRegistration(*new_actor_info, *checkpoint_data);
          // Mark the unreleased dummy objects in the checkpoint frontier as local.
          for (const auto &entry : actor_registration.GetDummyObjects()) {
            HandleObjectLocal(entry.first);
          }
          HandleActorStateTransition(actor_id, std::move(actor_registration));
          // The actor was created before.
          RAY_CHECK_OK(gcs_client_->Actors().AsyncUpdate(actor_id, new_actor_info,
                                                         update_callback));
        }));
  } else {
    // The actor did not resume from a checkpoint. Immediately notify the
    // other node managers that the actor has been created.
    HandleActorStateTransition(actor_id, ActorRegistration(*new_actor_info));
    if (actor_registry_.find(actor_id) != actor_registry_.end()) {
      // The actor was created before.
      RAY_CHECK_OK(
          gcs_client_->Actors().AsyncUpdate(actor_id, new_actor_info, update_callback));
    } else {
      // The actor was never created before.
      RAY_CHECK_OK(gcs_client_->Actors().AsyncRegister(new_actor_info, update_callback));
    }
  }
  if (!resumed_from_checkpoint) {
    // The actor was not resumed from a checkpoint. Store the
    // initial dummy object. All future handles to the actor will
    // depend on this object.
    HandleObjectLocal(task_spec.ActorDummyObject());
  }
}

void NodeManager::HandleTaskReconstruction(const TaskID &task_id,
                                           const ObjectID &required_object_id) {
  // Retrieve the task spec in order to re-execute the task.
  RAY_CHECK_OK(gcs_client_->Tasks().AsyncGet(
      task_id,
      /*callback=*/
      [this, required_object_id, task_id](
          Status status, const boost::optional<TaskTableData> &task_data) {
        if (task_data) {
          // The task was in the GCS task table. Use the stored task spec to
          // re-execute the task.
          ResubmitTask(Task(task_data->task()), required_object_id);
          return;
        }
        // The task was not in the GCS task table. It must therefore be in the
        // lineage cache.
        if (lineage_cache_.ContainsTask(task_id)) {
          // Use a copy of the cached task spec to re-execute the task.
          const Task task = lineage_cache_.GetTaskOrDie(task_id);
          ResubmitTask(task, required_object_id);
        } else {
          RAY_LOG(WARNING)
              << "Metadata of task " << task_id
              << " not found in either GCS or lineage cache. It may have been evicted "
              << "by the redis LRU configuration. Consider increasing the memory "
                 "allocation via "
              << "ray.init(redis_max_memory=<max_memory_bytes>).";
          MarkObjectsAsFailed(ErrorType::OBJECT_UNRECONSTRUCTABLE,
                              {required_object_id.ToPlasmaId()}, JobID::Nil());
        }
      }));
}

void NodeManager::ResubmitTask(const Task &task, const ObjectID &required_object_id) {
  RAY_LOG(DEBUG) << "Attempting to resubmit task "
                 << task.GetTaskSpecification().TaskId();

  // Actors should only be recreated if the first initialization failed or if
  // the most recent instance of the actor failed.
  if (task.GetTaskSpecification().IsActorCreationTask()) {
    const auto &actor_id = task.GetTaskSpecification().ActorCreationId();
    const auto it = actor_registry_.find(actor_id);
    if (it != actor_registry_.end() && it->second.GetState() == ActorTableData::ALIVE) {
      // If the actor is still alive, then do not resubmit the task. If the
      // actor actually is dead and a result is needed, then reconstruction
      // for this task will be triggered again.
      RAY_LOG(WARNING)
          << "Actor creation task resubmitted, but the actor is still alive.";
      return;
    }
  }

  // Driver tasks cannot be reconstructed. If this is a driver task, push an
  // error to the driver and do not resubmit it.
  if (task.GetTaskSpecification().IsDriverTask()) {
    // TODO(rkn): Define this constant somewhere else.
    std::string type = "put_reconstruction";
    std::ostringstream error_message;
    error_message << "The task with ID " << task.GetTaskSpecification().TaskId()
                  << " is a driver task and so the object created by ray.put "
                  << "could not be reconstructed.";
    auto error_data_ptr =
        gcs::CreateErrorTableData(type, error_message.str(), current_time_ms(),
                                  task.GetTaskSpecification().JobId());
    RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    MarkObjectsAsFailed(ErrorType::OBJECT_UNRECONSTRUCTABLE,
                        {required_object_id.ToPlasmaId()},
                        task.GetTaskSpecification().JobId());
    return;
  }

  RAY_LOG(INFO) << "Resubmitting task " << task.GetTaskSpecification().TaskId()
                << " on node " << self_node_id_;
  // The task may be reconstructed. Submit it with an empty lineage, since any
  // uncommitted lineage must already be in the lineage cache. At this point,
  // the task should not yet exist in the local scheduling queue. If it does,
  // then this is a spurious reconstruction.
  SubmitTask(task, Lineage());
}

void NodeManager::HandleObjectLocal(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is local.
  const auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(object_id);
  RAY_LOG(DEBUG) << "Object local " << object_id << ", "
                 << " on " << self_node_id_ << ", " << ready_task_ids.size()
                 << " tasks ready";
  // Transition the tasks whose dependencies are now fulfilled to the ready state.
  if (new_scheduler_enabled_) {
    for (auto task_id : ready_task_ids) {
      auto it = waiting_tasks_.find(task_id);
      if (it != waiting_tasks_.end()) {
        task_dependency_manager_.UnsubscribeGetDependencies(task_id);
        tasks_to_dispatch_.push_back(it->second);
        waiting_tasks_.erase(it);
      }
    }
    NewSchedulerSchedulePendingTasks();
  } else {
    if (ready_task_ids.size() > 0) {
      std::unordered_set<TaskID> ready_task_id_set(ready_task_ids.begin(),
                                                   ready_task_ids.end());

      // First filter out the tasks that should not be moved to READY.
      local_queues_.FilterState(ready_task_id_set, TaskState::BLOCKED);
      local_queues_.FilterState(ready_task_id_set, TaskState::RUNNING);
      local_queues_.FilterState(ready_task_id_set, TaskState::DRIVER);
      local_queues_.FilterState(ready_task_id_set, TaskState::WAITING_FOR_ACTOR_CREATION);

      // Make sure that the remaining tasks are all WAITING or direct call
      // actors.
      auto ready_task_id_set_copy = ready_task_id_set;
      local_queues_.FilterState(ready_task_id_set_copy, TaskState::WAITING);
      // Filter out direct call actors. These are not tracked by the raylet and
      // their assigned task ID is the actor ID.
      for (const auto &id : ready_task_id_set_copy) {
        RAY_CHECK(actor_registry_.count(id.ActorId()) > 0);
        ready_task_id_set.erase(id);
      }

      // Queue and dispatch the tasks that are ready to run (i.e., WAITING).
      auto ready_tasks = local_queues_.RemoveTasks(ready_task_id_set);
      local_queues_.QueueTasks(ready_tasks, TaskState::READY);
      DispatchTasks(MakeTasksByClass(ready_tasks));
    }
  }
}

bool NodeManager::IsActorCreationTask(const TaskID &task_id) {
  auto actor_id = task_id.ActorId();
  if (!actor_id.IsNil() && task_id == TaskID::ForActorCreationTask(actor_id)) {
    // This task ID corresponds to an actor creation task.
    auto iter = actor_registry_.find(actor_id);
    if (iter != actor_registry_.end()) {
      // This actor is direct call actor.
      return true;
    }
  }

  return false;
}

void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is no longer local.
  const auto waiting_task_ids = task_dependency_manager_.HandleObjectMissing(object_id);
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

  // Transition any tasks that were in the runnable state and are dependent on
  // this object to the waiting state.
  if (!waiting_task_ids.empty()) {
    std::unordered_set<TaskID> waiting_task_id_set(waiting_task_ids.begin(),
                                                   waiting_task_ids.end());

    // NOTE(zhijunfu): For direct actors, the worker is initially assigned actor
    // creation task ID, which will not be reset after the task finishes. And later tasks
    // of this actor will reuse this task ID to require objects from plasma with
    // FetchOrReconstruct, since direct actor task IDs are not known to raylet.
    // To support actor reconstruction for direct actor, raylet marks actor creation task
    // as completed and removes it from `local_queues_` when it receives `TaskDone`
    // message from worker. This is necessary because the actor creation task will be
    // re-submitted during reconstruction, if the task is not removed previously, the new
    // submitted task will be marked as duplicate and thus ignored.
    // So here we check for direct actor creation task explicitly to allow this case.
    auto iter = waiting_task_id_set.begin();
    while (iter != waiting_task_id_set.end()) {
      if (IsActorCreationTask(*iter)) {
        RAY_LOG(DEBUG) << "Ignoring direct actor creation task " << *iter
                       << " when handling object missing for " << object_id;
        iter = waiting_task_id_set.erase(iter);
      } else {
        ++iter;
      }
    }

    // First filter out any tasks that can't be transitioned to READY. These
    // are running workers or drivers, now blocked in a get.
    local_queues_.FilterState(waiting_task_id_set, TaskState::RUNNING);
    local_queues_.FilterState(waiting_task_id_set, TaskState::DRIVER);
    // Transition the tasks back to the waiting state. They will be made
    // runnable once the deleted object becomes available again.
    local_queues_.MoveTasks(waiting_task_id_set, TaskState::READY, TaskState::WAITING);
    RAY_CHECK(waiting_task_id_set.empty());
    // Moving ready tasks to waiting may have changed the load, making space for placing
    // new tasks locally.
    ScheduleTasks(cluster_resource_map_);
  }
}

void NodeManager::ForwardTaskOrResubmit(const Task &task,
                                        const ClientID &node_manager_id) {
  /// TODO(rkn): Should we check that the node manager is remote and not local?
  /// TODO(rkn): Should we check if the remote node manager is known to be dead?
  // Attempt to forward the task.
  ForwardTask(
      task, node_manager_id,
      [this, node_manager_id](ray::Status error, const Task &task) {
        const TaskID task_id = task.GetTaskSpecification().TaskId();
        RAY_LOG(INFO) << "Failed to forward task " << task_id << " to node manager "
                      << node_manager_id;

        // Mark the failed task as pending to let other raylets know that we still
        // have the task. TaskDependencyManager::TaskPending() is assumed to be
        // idempotent.
        task_dependency_manager_.TaskPending(task);

        // Actor tasks can only be executed at the actor's location, so they are
        // retried after a timeout. All other tasks that fail to be forwarded are
        // deemed to be placeable again.
        if (task.GetTaskSpecification().IsActorTask()) {
          // The task is for an actor on another node.  Create a timer to resubmit
          // the task in a little bit. TODO(rkn): Really this should be a
          // unique_ptr instead of a shared_ptr. However, it's a little harder to
          // move unique_ptrs into lambdas.
          auto retry_timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
          auto retry_duration = boost::posix_time::milliseconds(
              RayConfig::instance()
                  .node_manager_forward_task_retry_timeout_milliseconds());
          retry_timer->expires_from_now(retry_duration);
          retry_timer->async_wait(
              [this, task_id, retry_timer](const boost::system::error_code &error) {
                // Timer killing will receive the boost::asio::error::operation_aborted,
                // we only handle the timeout event.
                RAY_CHECK(!error);
                RAY_LOG(INFO) << "Resubmitting task " << task_id
                              << " because ForwardTask failed.";
                // Remove the RESUBMITTED task from the SWAP queue.
                Task task;
                TaskState state;
                if (local_queues_.RemoveTask(task_id, &task, &state)) {
                  RAY_CHECK(state == TaskState::SWAP);
                  // Submit the task again.
                  SubmitTask(task, Lineage());
                }
              });
          // Temporarily move the RESUBMITTED task to the SWAP queue while the
          // timer is active.
          local_queues_.QueueTasks({task}, TaskState::SWAP);
        } else {
          // The task is not for an actor and may therefore be placed on another
          // node immediately. Send it to the scheduling policy to be placed again.
          local_queues_.QueueTasks({task}, TaskState::PLACEABLE);
          ScheduleTasks(cluster_resource_map_);
        }
      });
}

void NodeManager::ForwardTask(
    const Task &task, const ClientID &node_id,
    const std::function<void(const ray::Status &, const Task &)> &on_error) {
  // Override spillback for direct tasks.
  if (task.OnSpillback() != nullptr) {
    auto node_info = gcs_client_->Nodes().Get(node_id);
    RAY_CHECK(node_info)
        << "Spilling back to a node manager, but no GCS info found for node " << node_id;
    task.OnSpillback()(node_id, node_info->node_manager_address(),
                       node_info->node_manager_port());
    return;
  }

  // Lookup node manager client for this node_id and use it to send the request.
  auto client_entry = remote_node_manager_clients_.find(node_id);
  if (client_entry == remote_node_manager_clients_.end()) {
    // TODO(atumanov): caller must handle failure to ensure tasks are not lost.
    RAY_LOG(INFO) << "No node manager client found for GCS client id " << node_id;
    on_error(ray::Status::IOError("Node manager client not found"), task);
    return;
  }
  auto &client = client_entry->second;

  const auto &spec = task.GetTaskSpecification();
  auto task_id = spec.TaskId();

  if (worker_pool_.HasPendingWorkerForTask(spec.GetLanguage(), task_id)) {
    // There is a worker being starting for this task,
    // so we shouldn't forward this task to another node.
    on_error(ray::Status::Invalid("Already has pending worker for this task"), task);
    return;
  }

  // Get the task's unforwarded, uncommitted lineage.
  Lineage uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id, node_id);
  if (uncommitted_lineage.GetEntries().empty()) {
    // There is no uncommitted lineage. This can happen if the lineage was
    // already evicted before we forwarded the task.
    uncommitted_lineage.SetEntry(task, GcsStatus::NONE);
  }
  auto entry = uncommitted_lineage.GetEntryMutable(task_id);
  Task &lineage_cache_entry_task = entry->TaskDataMutable();
  // Increment forward count for the forwarded task.
  lineage_cache_entry_task.IncrementNumForwards();
  RAY_LOG(DEBUG) << "Forwarding task " << task_id << " from " << self_node_id_ << " to "
                 << node_id << " spillback="
                 << lineage_cache_entry_task.GetTaskExecutionSpec().NumForwards();

  // Prepare the request message.
  rpc::ForwardTaskRequest request;
  request.set_task_id(task_id.Binary());
  for (auto &task_entry : uncommitted_lineage.GetEntries()) {
    auto task = request.add_uncommitted_tasks();
    task->mutable_task_spec()->CopyFrom(
        task_entry.second.TaskData().GetTaskSpecification().GetMessage());
    task->mutable_task_execution_spec()->CopyFrom(
        task_entry.second.TaskData().GetTaskExecutionSpec().GetMessage());
  }

  client->ForwardTask(request, [this, on_error, task, task_id, node_id](
                                   Status status, const rpc::ForwardTaskReply &reply) {
    if (local_queues_.HasTask(task_id)) {
      // It must have been forwarded back to us if it's in the queue again
      // so just return here.
      return;
    }

    if (status.ok()) {
      const auto &spec = task.GetTaskSpecification();
      // Mark as forwarded so that the task and its lineage are not
      // re-forwarded in the future to the receiving node.
      lineage_cache_.MarkTaskAsForwarded(task_id, node_id);

      // Notify the task dependency manager that we are no longer responsible
      // for executing this task.
      task_dependency_manager_.TaskCanceled(task_id);
      // Preemptively push any local arguments to the receiving node. For now, we
      // only do this with actor tasks, since actor tasks must be executed by a
      // specific process and therefore have affinity to the receiving node.
      if (spec.IsActorTask()) {
        // Iterate through the object's arguments. NOTE(swang): We do not include
        // the execution dependencies here since those cannot be transferred
        // between nodes.
        for (size_t i = 0; i < spec.NumArgs(); ++i) {
          int count = spec.ArgIdCount(i);
          for (int j = 0; j < count; j++) {
            ObjectID argument_id = spec.ArgId(i, j);
            // If the argument is local, then push it to the receiving node.
            if (task_dependency_manager_.CheckObjectLocal(argument_id)) {
              object_manager_.Push(argument_id, node_id);
            }
          }
        }
      }
    } else {
      on_error(status, task);
    }
  });
}

void NodeManager::FinishAssignTask(const std::shared_ptr<Worker> &worker,
                                   const TaskID &task_id, bool success) {
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
    worker->AssignJobId(spec.JobId());
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
    RAY_CHECK(task_dependency_manager_.UnsubscribeGetDependencies(spec.TaskId()));
  } else {
    RAY_LOG(WARNING) << "Failed to send task to worker, disconnecting client";
    // We failed to send the task to the worker, so disconnect the worker.
    ProcessDisconnectClientMessage(worker->Connection());
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
  std::shared_ptr<Worker> associated_worker = worker_pool_.GetRegisteredWorker(client);
  if (associated_worker == nullptr) {
    associated_worker = worker_pool_.GetRegisteredDriver(client);
  }
  RAY_CHECK(associated_worker != nullptr)
      << "No worker exists for CoreWorker with client: " << client->DebugString();

  auto message = flatbuffers::GetRoot<protocol::SubscribePlasmaReady>(message_data);
  ObjectID id = from_flatbuf<ObjectID>(*message->object_id());
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    if (!async_plasma_objects_notification_.contains(id)) {
      async_plasma_objects_notification_.emplace(
          id, absl::flat_hash_set<std::shared_ptr<Worker>>());
    }

    // Only insert a worker once
    if (!async_plasma_objects_notification_[id].contains(associated_worker)) {
      async_plasma_objects_notification_[id].insert(associated_worker);
    }
  }
}

ray::Status NodeManager::SetupPlasmaSubscription() {
  return object_manager_.SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        ObjectID object_id = ObjectID::FromPlasmaIdBinary(object_info.object_id);
        auto waiting_workers = absl::flat_hash_set<std::shared_ptr<Worker>>();
        {
          absl::MutexLock guard(&plasma_object_notification_lock_);
          auto waiting = this->async_plasma_objects_notification_.extract(object_id);
          if (!waiting.empty()) {
            waiting_workers.swap(waiting.mapped());
          }
        }
        rpc::PlasmaObjectReadyRequest request;
        request.set_object_id(object_id.Binary());
        request.set_metadata_size(object_info.metadata_size);
        request.set_data_size(object_info.data_size);

        for (auto worker : waiting_workers) {
          RAY_CHECK_OK(worker->rpc_client()->PlasmaObjectReady(
              request, [](Status status, const rpc::PlasmaObjectReadyReply &reply) {
                if (!status.ok()) {
                  RAY_LOG(INFO)
                      << "Problem with telling worker that plasma object is ready"
                      << status.ToString();
                }
              }));
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
  result << "\nClusterResources:";
  for (auto &pair : cluster_resource_map_) {
    result << "\n" << pair.first.Hex() << ": " << pair.second.DebugString();
  }
  result << "\n" << object_manager_.DebugString();
  result << "\n" << gcs_client_->DebugString();
  result << "\n" << worker_pool_.DebugString();
  result << "\n" << local_queues_.DebugString();
  result << "\n" << reconstruction_policy_.DebugString();
  result << "\n" << task_dependency_manager_.DebugString();
  result << "\n" << lineage_cache_.DebugString();
  {
    absl::MutexLock guard(&plasma_object_notification_lock_);
    result << "\nnum async plasma notifications: "
           << async_plasma_objects_notification_.size();
  }
  result << "\nActorRegistry:";

  auto statistical_data = GetActorStatisticalData(actor_registry_);
  result << "\n- num live actors: " << statistical_data.live_actors;
  result << "\n- num reconstructing actors: " << statistical_data.reconstructing_actors;
  result << "\n- num dead actors: " << statistical_data.dead_actors;
  result << "\n- max num handles: " << statistical_data.max_num_handles;

  result << "\nRemote node manager clients: ";
  for (const auto &entry : remote_node_manager_clients_) {
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
  if (!object_pinning_enabled_) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }
  WorkerID worker_id = WorkerID::FromBinary(request.owner_address().worker_id());
  auto it = worker_rpc_clients_.find(worker_id);
  if (it == worker_rpc_clients_.end()) {
    auto client = std::unique_ptr<rpc::CoreWorkerClient>(
        new rpc::CoreWorkerClient(request.owner_address(), client_call_manager_));
    it = worker_rpc_clients_
             .emplace(worker_id,
                      std::make_pair<std::unique_ptr<rpc::CoreWorkerClient>, size_t>(
                          std::move(client), 0))
             .first;
  }

  // Pin the objects in plasma by getting them and holding a reference to
  // the returned buffer.
  // NOTE: the caller must ensure that the objects already exist in plamsa before
  // sending a PinObjectIDs request.
  std::vector<plasma::ObjectID> plasma_ids;
  plasma_ids.reserve(request.object_ids_size());
  for (const auto &object_id_binary : request.object_ids()) {
    plasma_ids.push_back(plasma::ObjectID::from_binary(object_id_binary));
  }
  std::vector<plasma::ObjectBuffer> plasma_results;
  // TODO(swang): This `Get` has a timeout of 0, so the plasma store will not
  // block when serving the request. However, if the plasma store is under
  // heavy load, then this request can still block the NodeManager event loop
  // since we must wait for the plasma store's reply. We should consider using
  // an `AsyncGet` instead.
  if (!store_client_.Get(plasma_ids, /*timeout_ms=*/0, &plasma_results).ok()) {
    RAY_LOG(WARNING) << "Failed to get objects to be pinned from object store.";
    send_reply_callback(Status::Invalid("Failed to get objects."), nullptr, nullptr);
    return;
  }

  // Pin the requested objects until the owner notifies us that the objects can be
  // unpinned by responding to the WaitForObjectEviction message.
  // TODO(edoakes): we should be batching these requests instead of sending one per
  // pinned object.
  size_t i = 0;
  for (const auto &object_id_binary : request.object_ids()) {
    ObjectID object_id = ObjectID::FromBinary(object_id_binary);

    if (plasma_results[i].data == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                     << " was evicted before the raylet could pin it.";
      continue;
    }

    RAY_LOG(DEBUG) << "Pinning object " << object_id;
    RAY_CHECK(
        pinned_objects_
            .emplace(object_id,
                     std::unique_ptr<RayObject>(new RayObject(
                         std::make_shared<PlasmaBuffer>(plasma_results[i].data),
                         std::make_shared<PlasmaBuffer>(plasma_results[i].metadata), {})))
            .second);
    i++;

    // Send a long-running RPC request to the owner for each object. When we get a
    // response or the RPC fails (due to the owner crashing), unpin the object.
    rpc::WaitForObjectEvictionRequest wait_request;
    wait_request.set_object_id(object_id_binary);
    wait_request.set_intended_worker_id(request.owner_address().worker_id());
    worker_rpc_clients_[worker_id].second++;
    RAY_CHECK_OK(it->second.first->WaitForObjectEviction(
        wait_request, [this, worker_id, object_id](
                          Status status, const rpc::WaitForObjectEvictionReply &reply) {
          if (!status.ok()) {
            RAY_LOG(WARNING) << "Worker " << worker_id << " failed. Unpinning object "
                             << object_id;
          }
          RAY_LOG(DEBUG) << "Unpinning object " << object_id;
          pinned_objects_.erase(object_id);

          // Try to evict all copies of the object from the cluster.
          if (free_objects_period_ >= 0) {
            objects_to_free_.push_back(object_id);
          }
          if (objects_to_free_.size() ==
                  RayConfig::instance().free_objects_batch_size() ||
              free_objects_period_ == 0) {
            FlushObjectsToFree();
          }

          // Remove the cached worker client if there are no more pending requests.
          if (--worker_rpc_clients_[worker_id].second == 0) {
            worker_rpc_clients_.erase(worker_id);
          }
        }));
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::FlushObjectsToFree() {
  if (!objects_to_free_.empty()) {
    RAY_LOG(DEBUG) << "Freeing " << objects_to_free_.size() << " out-of-scope objects";
    object_manager_.FreeObjects(objects_to_free_, /*local_only=*/false);
    objects_to_free_.clear();
  }
  last_free_objects_at_ms_ = current_time_ms();
}

void NodeManager::HandleGetNodeStats(const rpc::GetNodeStatsRequest &node_stats_request,
                                     rpc::GetNodeStatsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  // NOTE(sang): Currently reporting only infeasible/ready ActorCreationTask
  // because Ray dashboard only renders actorCreationTask as of Feb 3 2020.
  // TODO(sang): Support dashboard for non-ActorCreationTask.
  for (const auto task : local_queues_.GetTasks(TaskState::INFEASIBLE)) {
    if (task.GetTaskSpecification().IsActorCreationTask()) {
      auto infeasible_task = reply->add_infeasible_tasks();
      infeasible_task->ParseFromString(task.GetTaskSpecification().Serialize());
    }
  }
  // Report tasks that are not scheduled because
  // resources are occupied by other actors/tasks.
  // NOTE(sang): This solution is a workaround. It can be replaced by creating a new state
  // like PENDING_UNTIL_RESOURCE_AVAILABLE.
  for (const auto task : local_queues_.GetTasks(TaskState::READY)) {
    if (task.GetTaskSpecification().IsActorCreationTask()) {
      auto ready_task = reply->add_ready_tasks();
      ready_task->ParseFromString(task.GetTaskSpecification().Serialize());
    }
  }
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
  auto all_workers = worker_pool_.GetAllWorkers();
  absl::flat_hash_set<WorkerID> driver_ids;
  for (auto driver : worker_pool_.GetAllDrivers()) {
    all_workers.push_back(driver);
    driver_ids.insert(driver->WorkerId());
  }
  for (const auto &worker : all_workers) {
    rpc::GetCoreWorkerStatsRequest request;
    request.set_intended_worker_id(worker->WorkerId().Binary());
    request.set_include_memory_info(node_stats_request.include_memory_info());
    auto status = worker->rpc_client()->GetCoreWorkerStats(
        request, [reply, worker, all_workers, driver_ids, send_reply_callback](
                     const ray::Status &status, const rpc::GetCoreWorkerStatsReply &r) {
          auto worker_stats = reply->add_workers_stats();
          worker_stats->set_pid(worker->GetProcess().GetId());
          worker_stats->set_is_driver(driver_ids.contains(worker->WorkerId()));
          reply->set_num_workers(reply->num_workers() + 1);
          if (status.ok()) {
            worker_stats->mutable_core_worker_stats()->MergeFrom(r.core_worker_stats());
          } else {
            RAY_LOG(ERROR) << "Failed to send get core worker stats request: "
                           << status.ToString();
            worker_stats->set_fetch_error(status.ToString());
          }
          if (reply->num_workers() == all_workers.size()) {
            send_reply_callback(Status::OK(), nullptr, nullptr);
          }
        });
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to send get core worker stats request: "
                     << status.ToString();
    }
  }
}

std::string FormatMemoryInfo(std::vector<rpc::GetNodeStatsReply> node_stats) {
  // First pass to compute object sizes.
  absl::flat_hash_map<ObjectID, int64_t> object_sizes;
  for (const auto &reply : node_stats) {
    for (const auto &worker_stats : reply.workers_stats()) {
      for (const auto &object_ref : worker_stats.core_worker_stats().object_refs()) {
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
         "-------------------------\n";
  builder
      << " Object ID                                Reference Type       Object Size  "
         " Reference Creation Site\n";
  builder
      << "============================================================================"
         "=========================\n";

  // Second pass builds the summary string for each node.
  for (const auto &reply : node_stats) {
    for (const auto &worker_stats : reply.workers_stats()) {
      bool pid_printed = false;
      for (const auto &object_ref : worker_stats.core_worker_stats().object_refs()) {
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
          if (worker_stats.is_driver()) {
            builder << "; driver pid=" << worker_stats.pid() << "\n";
          } else {
            builder << "; worker pid=" << worker_stats.pid() << "\n";
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
         "-------------------------\n";

  return builder.str();
}

void NodeManager::HandleFormatGlobalMemoryInfo(
    const rpc::FormatGlobalMemoryInfoRequest &request,
    rpc::FormatGlobalMemoryInfoReply *reply, rpc::SendReplyCallback send_reply_callback) {
  auto replies = std::make_shared<std::vector<rpc::GetNodeStatsReply>>();
  auto local_request = std::make_shared<rpc::GetNodeStatsRequest>();
  auto local_reply = std::make_shared<rpc::GetNodeStatsReply>();
  local_request->set_include_memory_info(true);

  unsigned int num_nodes = remote_node_manager_clients_.size() + 1;
  rpc::GetNodeStatsRequest stats_req;
  stats_req.set_include_memory_info(true);

  auto store_reply = [replies, reply, num_nodes,
                      send_reply_callback](const rpc::GetNodeStatsReply &local_reply) {
    replies->push_back(local_reply);
    if (replies->size() >= num_nodes) {
      reply->set_memory_summary(FormatMemoryInfo(*replies));
      send_reply_callback(Status::OK(), nullptr, nullptr);
    }
  };

  // Fetch from remote nodes.
  for (const auto &entry : remote_node_manager_clients_) {
    entry.second->GetNodeStats(
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
  RAY_LOG(WARNING) << "Broadcasting global GC request to all raylets.";
  should_global_gc_ = true;
  // We won't see our own request, so trigger local GC in the next heartbeat.
  should_local_gc_ = true;
}

void NodeManager::RecordMetrics() {
  recorded_metrics_ = true;
  if (stats::StatsConfig::instance().IsStatsDisabled()) {
    return;
  }

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

  object_manager_.RecordMetrics();
  worker_pool_.RecordMetrics();
  local_queues_.RecordMetrics();
  reconstruction_policy_.RecordMetrics();
  task_dependency_manager_.RecordMetrics();
  lineage_cache_.RecordMetrics();

  auto statistical_data = GetActorStatisticalData(actor_registry_);
  stats::ActorStats().Record(statistical_data.live_actors,
                             {{stats::ValueTypeKey, "live_actors"}});
  stats::ActorStats().Record(statistical_data.reconstructing_actors,
                             {{stats::ValueTypeKey, "reconstructing_actors"}});
  stats::ActorStats().Record(statistical_data.dead_actors,
                             {{stats::ValueTypeKey, "dead_actors"}});
  stats::ActorStats().Record(statistical_data.max_num_handles,
                             {{stats::ValueTypeKey, "max_num_handles"}});
}

}  // namespace raylet

}  // namespace ray
