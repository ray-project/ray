#include "ray/raylet/node_manager.h"

#include <fstream>

#include "ray/status.h"

#include "ray/common/common_protocol.h"
#include "ray/id.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/stats/stats.h"

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
    const ray::ActorID &actor_id, const ray::ActorHandleID &actor_handle_id) {
  auto actor_entry = actor_registry.find(actor_id);
  RAY_CHECK(actor_entry != actor_registry.end());
  const auto &frontier = actor_entry->second.GetFrontier();
  int64_t expected_task_counter = 0;
  auto frontier_entry = frontier.find(actor_handle_id);
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
    if (pair.second.GetState() == ActorState::ALIVE) {
      item.live_actors += 1;
    } else if (pair.second.GetState() == ActorState::RECONSTRUCTING) {
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

NodeManager::NodeManager(boost::asio::io_service &io_service,
                         const NodeManagerConfig &config, ObjectManager &object_manager,
                         std::shared_ptr<gcs::AsyncGcsClient> gcs_client,
                         std::shared_ptr<ObjectDirectoryInterface> object_directory)
    : client_id_(gcs_client->client_table().GetLocalClientId()),
      io_service_(io_service),
      object_manager_(object_manager),
      gcs_client_(std::move(gcs_client)),
      object_directory_(std::move(object_directory)),
      heartbeat_timer_(io_service),
      heartbeat_period_(std::chrono::milliseconds(config.heartbeat_period_ms)),
      debug_dump_period_(config.debug_dump_period_ms),
      temp_dir_(config.temp_dir),
      object_manager_profile_timer_(io_service),
      initial_config_(config),
      local_available_resources_(config.resource_config),
      worker_pool_(config.num_initial_workers, config.num_workers_per_process,
                   config.maximum_startup_concurrency, config.worker_commands),
      scheduling_policy_(local_queues_),
      reconstruction_policy_(
          io_service_,
          [this](const TaskID &task_id) { HandleTaskReconstruction(task_id); },
          RayConfig::instance().initial_reconstruction_timeout_milliseconds(),
          gcs_client_->client_table().GetLocalClientId(), gcs_client_->task_lease_table(),
          object_directory_, gcs_client_->task_reconstruction_log()),
      task_dependency_manager_(
          object_manager, reconstruction_policy_, io_service,
          gcs_client_->client_table().GetLocalClientId(),
          RayConfig::instance().initial_reconstruction_timeout_milliseconds(),
          gcs_client_->task_lease_table()),
      lineage_cache_(gcs_client_->client_table().GetLocalClientId(),
                     gcs_client_->raylet_task_table(), gcs_client_->raylet_task_table(),
                     config.max_lineage_size),
      remote_clients_(),
      remote_server_connections_(),
      actor_registry_() {
  RAY_CHECK(heartbeat_period_.count() > 0);
  // Initialize the resource map with own cluster resource configuration.
  ClientID local_client_id = gcs_client_->client_table().GetLocalClientId();
  cluster_resource_map_.emplace(local_client_id,
                                SchedulingResources(config.resource_config));

  RAY_CHECK_OK(object_manager_.SubscribeObjAdded(
      [this](const object_manager::protocol::ObjectInfoT &object_info) {
        ObjectID object_id = ObjectID::from_binary(object_info.object_id);
        HandleObjectLocal(object_id);
      }));
  RAY_CHECK_OK(object_manager_.SubscribeObjDeleted(
      [this](const ObjectID &object_id) { HandleObjectMissing(object_id); }));

  RAY_ARROW_CHECK_OK(store_client_.Connect(config.store_socket_name.c_str()));
}

ray::Status NodeManager::RegisterGcs() {
  object_manager_.RegisterGcs();

  // Subscribe to task entry commits in the GCS. These notifications are
  // forwarded to the lineage cache, which requests notifications about tasks
  // that were executed remotely.
  const auto task_committed_callback = [this](gcs::AsyncGcsClient *client,
                                              const TaskID &task_id,
                                              const ray::protocol::TaskT &task_data) {
    lineage_cache_.HandleEntryCommitted(task_id);
  };
  RAY_RETURN_NOT_OK(gcs_client_->raylet_task_table().Subscribe(
      DriverID::nil(), gcs_client_->client_table().GetLocalClientId(),
      task_committed_callback, nullptr, nullptr));

  const auto task_lease_notification_callback = [this](gcs::AsyncGcsClient *client,
                                                       const TaskID &task_id,
                                                       const TaskLeaseDataT &task_lease) {
    const ClientID node_manager_id = ClientID::from_binary(task_lease.node_manager_id);
    if (gcs_client_->client_table().IsRemoved(node_manager_id)) {
      // The node manager that added the task lease is already removed. The
      // lease is considered inactive.
      reconstruction_policy_.HandleTaskLeaseNotification(task_id, 0);
    } else {
      // NOTE(swang): The task_lease.timeout is an overestimate of the lease's
      // expiration period since the entry may have been in the GCS for some
      // time already. For a more accurate estimate, the age of the entry in
      // the GCS should be subtracted from task_lease.timeout.
      reconstruction_policy_.HandleTaskLeaseNotification(task_id, task_lease.timeout);
    }
  };
  const auto task_lease_empty_callback = [this](gcs::AsyncGcsClient *client,
                                                const TaskID &task_id) {
    reconstruction_policy_.HandleTaskLeaseNotification(task_id, 0);
  };
  RAY_RETURN_NOT_OK(gcs_client_->task_lease_table().Subscribe(
      DriverID::nil(), gcs_client_->client_table().GetLocalClientId(),
      task_lease_notification_callback, task_lease_empty_callback, nullptr));

  // Register a callback to handle actor notifications.
  auto actor_notification_callback = [this](gcs::AsyncGcsClient *client,
                                            const ActorID &actor_id,
                                            const std::vector<ActorTableDataT> &data) {
    if (!data.empty()) {
      // We only need the last entry, because it represents the latest state of
      // this actor.
      HandleActorStateTransition(actor_id, ActorRegistration(data.back()));
    }
  };

  RAY_RETURN_NOT_OK(gcs_client_->actor_table().Subscribe(
      DriverID::nil(), ClientID::nil(), actor_notification_callback, nullptr));

  // Register a callback on the client table for new clients.
  auto node_manager_client_added = [this](gcs::AsyncGcsClient *client, const UniqueID &id,
                                          const ClientTableDataT &data) {
    ClientAdded(data);
  };
  gcs_client_->client_table().RegisterClientAddedCallback(node_manager_client_added);
  // Register a callback on the client table for removed clients.
  auto node_manager_client_removed = [this](
      gcs::AsyncGcsClient *client, const UniqueID &id, const ClientTableDataT &data) {
    ClientRemoved(data);
  };
  gcs_client_->client_table().RegisterClientRemovedCallback(node_manager_client_removed);

  // Subscribe to heartbeat batches from the monitor.
  const auto &heartbeat_batch_added = [this](
      gcs::AsyncGcsClient *client, const ClientID &id,
      const HeartbeatBatchTableDataT &heartbeat_batch) {
    HeartbeatBatchAdded(heartbeat_batch);
  };
  RAY_RETURN_NOT_OK(gcs_client_->heartbeat_batch_table().Subscribe(
      DriverID::nil(), ClientID::nil(), heartbeat_batch_added,
      /*subscribe_callback=*/nullptr,
      /*done_callback=*/nullptr));

  // Subscribe to driver table updates.
  const auto driver_table_handler = [this](
      gcs::AsyncGcsClient *client, const DriverID &client_id,
      const std::vector<DriverTableDataT> &driver_data) {
    HandleDriverTableUpdate(client_id, driver_data);
  };
  RAY_RETURN_NOT_OK(gcs_client_->driver_table().Subscribe(
      DriverID::nil(), ClientID::nil(), driver_table_handler, nullptr));

  // Start sending heartbeats to the GCS.
  last_heartbeat_at_ms_ = current_time_ms();
  last_debug_dump_at_ms_ = current_time_ms();
  Heartbeat();
  // Start the timer that gets object manager profiling information and sends it
  // to the GCS.
  GetObjectManagerProfileInfo();

  return ray::Status::OK();
}

void NodeManager::KillWorker(std::shared_ptr<Worker> worker) {
  // If we're just cleaning up a single worker, allow it some time to clean
  // up its state before force killing. The client socket will be closed
  // and the worker struct will be freed after the timeout.
  kill(worker->Pid(), SIGTERM);

  auto retry_timer = std::make_shared<boost::asio::deadline_timer>(io_service_);
  auto retry_duration = boost::posix_time::milliseconds(
      RayConfig::instance().kill_worker_timeout_milliseconds());
  retry_timer->expires_from_now(retry_duration);
  retry_timer->async_wait([retry_timer, worker](const boost::system::error_code &error) {
    RAY_LOG(DEBUG) << "Send SIGKILL to worker, pid=" << worker->Pid();
    // Force kill worker. TODO(rkn): Is there some small danger that the worker
    // has already died and the PID has been reassigned to a different process?
    kill(worker->Pid(), SIGKILL);
  });
}

void NodeManager::HandleDriverTableUpdate(
    const DriverID &id, const std::vector<DriverTableDataT> &driver_data) {
  for (const auto &entry : driver_data) {
    RAY_LOG(DEBUG) << "HandleDriverTableUpdate " << UniqueID::from_binary(entry.driver_id)
                   << " " << entry.is_dead;
    if (entry.is_dead) {
      auto driver_id = DriverID::from_binary(entry.driver_id);
      auto workers = worker_pool_.GetWorkersRunningTasksForDriver(driver_id);

      // Kill all the workers. The actual cleanup for these workers is done
      // later when we receive the DisconnectClient message from them.
      for (const auto &worker : workers) {
        // Mark the worker as dead so further messages from it are ignored
        // (except DisconnectClient).
        worker->MarkDead();
        // Then kill the worker process.
        KillWorker(worker);
      }

      // Remove all tasks for this driver from the scheduling queues, mark
      // the results for these tasks as not required, cancel any attempts
      // at reconstruction. Note that at this time the workers are likely
      // alive because of the delay in killing workers.
      CleanUpTasksForDeadDriver(driver_id);
    }
  }
}

void NodeManager::Heartbeat() {
  uint64_t now_ms = current_time_ms();
  uint64_t interval = now_ms - last_heartbeat_at_ms_;
  if (interval > RayConfig::instance().num_heartbeats_warning() *
                     RayConfig::instance().heartbeat_timeout_milliseconds()) {
    RAY_LOG(WARNING) << "Last heartbeat was sent " << interval << " ms ago ";
  }
  last_heartbeat_at_ms_ = now_ms;

  auto &heartbeat_table = gcs_client_->heartbeat_table();
  auto heartbeat_data = std::make_shared<HeartbeatTableDataT>();
  const auto &my_client_id = gcs_client_->client_table().GetLocalClientId();
  SchedulingResources &local_resources = cluster_resource_map_[my_client_id];
  heartbeat_data->client_id = my_client_id.binary();
  // TODO(atumanov): modify the heartbeat table protocol to use the ResourceSet directly.
  // TODO(atumanov): implement a ResourceSet const_iterator.
  for (const auto &resource_pair :
       local_resources.GetAvailableResources().GetResourceMap()) {
    heartbeat_data->resources_available_label.push_back(resource_pair.first);
    heartbeat_data->resources_available_capacity.push_back(resource_pair.second);
  }
  for (const auto &resource_pair : local_resources.GetTotalResources().GetResourceMap()) {
    heartbeat_data->resources_total_label.push_back(resource_pair.first);
    heartbeat_data->resources_total_capacity.push_back(resource_pair.second);
  }

  local_resources.SetLoadResources(local_queues_.GetResourceLoad());
  for (const auto &resource_pair : local_resources.GetLoadResources().GetResourceMap()) {
    heartbeat_data->resource_load_label.push_back(resource_pair.first);
    heartbeat_data->resource_load_capacity.push_back(resource_pair.second);
  }

  ray::Status status = heartbeat_table.Add(
      DriverID::nil(), gcs_client_->client_table().GetLocalClientId(), heartbeat_data,
      /*success_callback=*/nullptr);
  RAY_CHECK_OK_PREPEND(status, "Heartbeat failed");

  if (debug_dump_period_ > 0 &&
      static_cast<int64_t>(now_ms - last_debug_dump_at_ms_) > debug_dump_period_) {
    DumpDebugState();
    RecordMetrics();
    last_debug_dump_at_ms_ = now_ms;
  }

  // Reset the timer.
  heartbeat_timer_.expires_from_now(heartbeat_period_);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    Heartbeat();
  });
}

void NodeManager::GetObjectManagerProfileInfo() {
  int64_t start_time_ms = current_time_ms();

  auto profile_info = object_manager_.GetAndResetProfilingInfo();

  if (profile_info.profile_events.size() > 0) {
    flatbuffers::FlatBufferBuilder fbb;
    auto message = CreateProfileTableData(fbb, &profile_info);
    fbb.Finish(message);
    auto profile_message = flatbuffers::GetRoot<ProfileTableData>(fbb.GetBufferPointer());

    RAY_CHECK_OK(gcs_client_->profile_table().AddProfileEventBatch(*profile_message));
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

void NodeManager::ClientAdded(const ClientTableDataT &client_data) {
  const ClientID client_id = ClientID::from_binary(client_data.client_id);

  RAY_LOG(DEBUG) << "[ClientAdded] Received callback from client id " << client_id;
  if (client_id == gcs_client_->client_table().GetLocalClientId()) {
    // We got a notification for ourselves, so we are connected to the GCS now.
    // Save this NodeManager's resource information in the cluster resource map.
    cluster_resource_map_[client_id] = initial_config_.resource_config;
    return;
  }

  // TODO(atumanov): make remote client lookup O(1)
  if (std::find(remote_clients_.begin(), remote_clients_.end(), client_id) ==
      remote_clients_.end()) {
    remote_clients_.push_back(client_id);
  } else {
    // NodeManager connection to this client was already established.
    RAY_LOG(DEBUG) << "Received a new client connection that already exists: "
                   << client_id;
    return;
  }

  // Establish a new NodeManager connection to this GCS client.
  auto status = ConnectRemoteNodeManager(client_id, client_data.node_manager_address,
                                         client_data.node_manager_port);
  if (!status.ok()) {
    // This is not a fatal error for raylet, but it should not happen.
    // We need to broadcase this message.
    std::string type = "raylet_connection_error";
    std::ostringstream error_message;
    error_message << "Failed to connect to ray node " << client_id
                  << " with status: " << status.ToString()
                  << ". This may be since the node was recently removed.";
    // We use the nil DriverID to broadcast the message to all drivers.
    RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(
        DriverID::nil(), type, error_message.str(), current_time_ms()));
    return;
  }

  ResourceSet resources_total(client_data.resources_total_label,
                              client_data.resources_total_capacity);
  cluster_resource_map_.emplace(client_id, SchedulingResources(resources_total));
}

ray::Status NodeManager::ConnectRemoteNodeManager(const ClientID &client_id,
                                                  const std::string &client_address,
                                                  int32_t client_port) {
  // Establish a new NodeManager connection to this GCS client.
  RAY_LOG(INFO) << "[ConnectClient] Trying to connect to client " << client_id << " at "
                << client_address << ":" << client_port;

  boost::asio::ip::tcp::socket socket(io_service_);
  RAY_RETURN_NOT_OK(TcpConnect(socket, client_address, client_port));

  // The client is connected, now send a connect message to remote node manager.
  auto server_conn = TcpServerConnection::Create(std::move(socket));

  // Prepare client connection info buffer
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateConnectClient(fbb, to_flatbuf(fbb, client_id_));
  fbb.Finish(message);
  // Send synchronously.
  // TODO(swang): Make this a WriteMessageAsync.
  RAY_RETURN_NOT_OK(server_conn->WriteMessage(
      static_cast<int64_t>(protocol::MessageType::ConnectClient), fbb.GetSize(),
      fbb.GetBufferPointer()));

  remote_server_connections_.emplace(client_id, std::move(server_conn));
  return ray::Status::OK();
}

void NodeManager::ClientRemoved(const ClientTableDataT &client_data) {
  // TODO(swang): If we receive a notification for our own death, clean up and
  // exit immediately.
  const ClientID client_id = ClientID::from_binary(client_data.client_id);
  RAY_LOG(DEBUG) << "[ClientRemoved] Received callback from client id " << client_id;

  RAY_CHECK(client_id != gcs_client_->client_table().GetLocalClientId())
      << "Exiting because this node manager has mistakenly been marked dead by the "
      << "monitor.";

  // Below, when we remove client_id from all of these data structures, we could
  // check that it is actually removed, or log a warning otherwise, but that may
  // not be necessary.

  // Remove the client from the list of remote clients.
  std::remove(remote_clients_.begin(), remote_clients_.end(), client_id);

  // Remove the client from the resource map.
  cluster_resource_map_.erase(client_id);

  // Remove the remote server connection.
  const auto connection_entry = remote_server_connections_.find(client_id);
  if (connection_entry != remote_server_connections_.end()) {
    connection_entry->second->Close();
    remote_server_connections_.erase(connection_entry);
  } else {
    RAY_LOG(WARNING) << "Received ClientRemoved callback for an unknown client "
                     << client_id << ".";
  }

  // For any live actors that were on the dead node, broadcast a notification
  // about the actor's death
  // TODO(swang): This could be very slow if there are many actors.
  for (const auto &actor_entry : actor_registry_) {
    if (actor_entry.second.GetNodeManagerId() == client_id &&
        actor_entry.second.GetState() == ActorState::ALIVE) {
      RAY_LOG(INFO) << "Actor " << actor_entry.first
                    << " is disconnected, because its node " << client_id
                    << " is removed from cluster. It may be reconstructed.";
      HandleDisconnectedActor(actor_entry.first, /*was_local=*/false,
                              /*intentional_disconnect=*/false);
    }
  }
  // Notify the object directory that the client has been removed so that it
  // can remove it from any cached locations.
  object_directory_->HandleClientRemoved(client_id);
}

void NodeManager::HeartbeatAdded(const ClientID &client_id,
                                 const HeartbeatTableDataT &heartbeat_data) {
  // Locate the client id in remote client table and update available resources based on
  // the received heartbeat information.
  auto it = cluster_resource_map_.find(client_id);
  if (it == cluster_resource_map_.end()) {
    // Haven't received the client registration for this client yet, skip this heartbeat.
    RAY_LOG(INFO) << "[HeartbeatAdded]: received heartbeat from unknown client id "
                  << client_id;
    return;
  }
  SchedulingResources &remote_resources = it->second;

  ResourceSet remote_available(heartbeat_data.resources_available_label,
                               heartbeat_data.resources_available_capacity);
  ResourceSet remote_load(heartbeat_data.resource_load_label,
                          heartbeat_data.resource_load_capacity);
  // TODO(atumanov): assert that the load is a non-empty ResourceSet.
  remote_resources.SetAvailableResources(std::move(remote_available));
  // Extract the load information and save it locally.
  remote_resources.SetLoadResources(std::move(remote_load));
  // Extract decision for this raylet.
  auto decision = scheduling_policy_.SpillOver(remote_resources);
  std::unordered_set<TaskID> local_task_ids;
  for (const auto &task_id : decision) {
    // (See design_docs/task_states.rst for the state transition diagram.)
    TaskState state;
    const auto task = local_queues_.RemoveTask(task_id, &state);
    // Since we are spilling back from the ready and waiting queues, we need
    // to unsubscribe the dependencies.
    if (state != TaskState::INFEASIBLE) {
      // Don't unsubscribe for infeasible tasks because we never subscribed in
      // the first place.
      RAY_CHECK(task_dependency_manager_.UnsubscribeDependencies(task_id));
    }
    // Attempt to forward the task. If this fails to forward the task,
    // the task will be resubmit locally.
    ForwardTaskOrResubmit(task, client_id);
  }
}

void NodeManager::HeartbeatBatchAdded(const HeartbeatBatchTableDataT &heartbeat_batch) {
  const ClientID &local_client_id = gcs_client_->client_table().GetLocalClientId();
  // Update load information provided by each heartbeat.
  for (const auto &heartbeat_data : heartbeat_batch.batch) {
    const ClientID &client_id = ClientID::from_binary(heartbeat_data->client_id);
    if (client_id == local_client_id) {
      // Skip heartbeats from self.
      continue;
    }
    HeartbeatAdded(client_id, *heartbeat_data);
  }
}

void NodeManager::PublishActorStateTransition(
    const ActorID &actor_id, const ActorTableDataT &data,
    const ray::gcs::ActorTable::WriteCallback &failure_callback) {
  // Copy the actor notification data.
  auto actor_notification = std::make_shared<ActorTableDataT>(data);

  // The actor log starts with an ALIVE entry. This is followed by 0 to N pairs
  // of (RECONSTRUCTING, ALIVE) entries, where N is the maximum number of
  // reconstructions. This is followed optionally by a DEAD entry.
  int log_length = 2 * (actor_notification->max_reconstructions -
                        actor_notification->remaining_reconstructions);
  if (actor_notification->state != ActorState::ALIVE) {
    // RECONSTRUCTING or DEAD entries have an odd index.
    log_length += 1;
  }
  // If we successful appended a record to the GCS table of the actor that
  // has died, signal this to anyone receiving signals from this actor.
  auto success_callback = [](gcs::AsyncGcsClient *client, const ActorID &id,
                             const ActorTableDataT &data) {
    auto redis_context = client->primary_context();
    if (data.state == ActorState::DEAD || data.state == ActorState::RECONSTRUCTING) {
      std::vector<std::string> args = {"XADD", id.hex(), "*", "signal",
                                       "ACTOR_DIED_SIGNAL"};
      RAY_CHECK_OK(redis_context->RunArgvAsync(args));
    }
  };
  RAY_CHECK_OK(gcs_client_->actor_table().AppendAt(DriverID::nil(), actor_id,
                                                   actor_notification, success_callback,
                                                   failure_callback, log_length));
}

void NodeManager::HandleActorStateTransition(const ActorID &actor_id,
                                             ActorRegistration &&actor_registration) {
  // Update local registry.
  auto it = actor_registry_.find(actor_id);
  if (it == actor_registry_.end()) {
    it = actor_registry_.emplace(actor_id, actor_registration).first;
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
  RAY_LOG(DEBUG) << "Actor notification received: actor_id = " << actor_id
                 << ", node_manager_id = " << actor_registration.GetNodeManagerId()
                 << ", state = " << EnumNameActorState(actor_registration.GetState())
                 << ", remaining_reconstructions = "
                 << actor_registration.GetRemainingReconstructions();

  if (actor_registration.GetState() == ActorState::ALIVE) {
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
      if (!lineage_cache_.RemoveWaitingTask(method.GetTaskSpecification().TaskId())) {
        RAY_LOG(WARNING) << "Task " << method.GetTaskSpecification().TaskId()
                         << " already removed from the lineage cache. This is most "
                            "likely due to reconstruction.";
      }
      // Maintain the invariant that if a task is in the
      // MethodsWaitingForActorCreation queue, then it is subscribed to its
      // respective actor creation task. Since the actor location is now known,
      // we can remove the task from the queue and forget its dependency on the
      // actor creation task.
      RAY_CHECK(task_dependency_manager_.UnsubscribeDependencies(
          method.GetTaskSpecification().TaskId()));
      // The task's uncommitted lineage was already added to the local lineage
      // cache upon the initial submission, so it's okay to resubmit it with an
      // empty lineage this time.
      SubmitTask(method, Lineage());
    }
  } else if (actor_registration.GetState() == ActorState::DEAD) {
    // When an actor dies, loop over all of the queued tasks for that actor
    // and treat them as failed.
    auto tasks_to_remove = local_queues_.GetTaskIdsForActor(actor_id);
    auto removed_tasks = local_queues_.RemoveTasks(tasks_to_remove);
    for (auto const &task : removed_tasks) {
      TreatTaskAsFailed(task, ErrorType::ACTOR_DIED);
    }
  } else {
    RAY_CHECK(actor_registration.GetState() == ActorState::RECONSTRUCTING);
    RAY_LOG(DEBUG) << "Actor is being reconstructed: " << actor_id;
    // When an actor fails but can be reconstructed, resubmit all of the queued
    // tasks for that actor. This will mark the tasks as waiting for actor
    // creation.
    auto tasks_to_remove = local_queues_.GetTaskIdsForActor(actor_id);
    auto removed_tasks = local_queues_.RemoveTasks(tasks_to_remove);
    for (auto const &task : removed_tasks) {
      SubmitTask(task, Lineage());
    }
  }
}

void NodeManager::CleanUpTasksForDeadDriver(const DriverID &driver_id) {
  auto tasks_to_remove = local_queues_.GetTaskIdsForDriver(driver_id);
  task_dependency_manager_.RemoveTasksAndRelatedObjects(tasks_to_remove);
  // NOTE(swang): SchedulingQueue::RemoveTasks modifies its argument so we must
  // call it last.
  local_queues_.RemoveTasks(tasks_to_remove);
}

void NodeManager::ProcessNewClient(LocalClientConnection &client) {
  // The new client is a worker, so begin listening for messages.
  client.ProcessMessages();
}

// A helper function to create a mapping from resource shapes to
// tasks with that resource shape from a given list of tasks.
std::unordered_map<ResourceSet, ordered_set<TaskID>> MakeTasksWithResources(
    const std::vector<Task> &tasks) {
  std::unordered_map<ResourceSet, ordered_set<TaskID>> result;
  for (const auto &task : tasks) {
    auto spec = task.GetTaskSpecification();
    result[spec.GetRequiredResources()].push_back(spec.TaskId());
  }
  return result;
}

void NodeManager::DispatchTasks(
    const std::unordered_map<ResourceSet, ordered_set<TaskID>> &tasks_with_resources) {
  std::unordered_set<TaskID> removed_task_ids;
  for (const auto &it : tasks_with_resources) {
    const auto &task_resources = it.first;
    for (const auto &task_id : it.second) {
      const auto &task = local_queues_.GetTaskOfState(task_id, TaskState::READY);
      if (!local_available_resources_.Contains(task_resources)) {
        // All the tasks in it.second have the same resource shape, so
        // once the first task is not feasible, we can break out of this loop
        break;
      }
      if (AssignTask(task)) {
        removed_task_ids.insert(task_id);
      }
    }
  }
  local_queues_.RemoveTasks(removed_task_ids);
}

void NodeManager::ProcessClientMessage(
    const std::shared_ptr<LocalClientConnection> &client, int64_t message_type,
    const uint8_t *message_data) {
  auto registered_worker = worker_pool_.GetRegisteredWorker(client);
  auto message_type_value = static_cast<protocol::MessageType>(message_type);
  RAY_LOG(DEBUG) << "[Worker] Message "
                 << protocol::EnumNameMessageType(message_type_value) << "("
                 << message_type << ") from worker with PID "
                 << (registered_worker ? std::to_string(registered_worker->Pid())
                                       : "nil");
  if (registered_worker && registered_worker->IsDead()) {
    // For a worker that is marked as dead (because the driver has died already),
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
  case protocol::MessageType::GetTask: {
    ProcessGetTaskMessage(client);
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
    ProcessSubmitTaskMessage(message_data);
  } break;
  case protocol::MessageType::FetchOrReconstruct: {
    ProcessFetchOrReconstructMessage(client, message_data);
  } break;
  case protocol::MessageType::NotifyUnblocked: {
    auto message = flatbuffers::GetRoot<protocol::NotifyUnblocked>(message_data);
    HandleTaskUnblocked(client, from_flatbuf<TaskID>(*message->task_id()));
  } break;
  case protocol::MessageType::WaitRequest: {
    ProcessWaitRequestMessage(client, message_data);
  } break;
  case protocol::MessageType::PushErrorRequest: {
    ProcessPushErrorRequestMessage(message_data);
  } break;
  case protocol::MessageType::PushProfileEventsRequest: {
    auto message = flatbuffers::GetRoot<ProfileTableData>(message_data);
    RAY_CHECK_OK(gcs_client_->profile_table().AddProfileEventBatch(*message));
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
        creating_task_ids.push_back(ComputeTaskId(object_id));
      }
      gcs_client_->raylet_task_table().Delete(DriverID::nil(), creating_task_ids);
    }
  } break;
  case protocol::MessageType::PrepareActorCheckpointRequest: {
    ProcessPrepareActorCheckpointRequest(client, message_data);
  } break;
  case protocol::MessageType::NotifyActorResumedFromCheckpoint: {
    ProcessNotifyActorResumedFromCheckpoint(message_data);
  } break;

  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  // Listen for more messages.
  client->ProcessMessages();
}

void NodeManager::ProcessRegisterClientRequestMessage(
    const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::RegisterClientRequest>(message_data);
  client->SetClientID(from_flatbuf<ClientID>(*message->client_id()));
  auto worker =
      std::make_shared<Worker>(message->worker_pid(), message->language(), client);
  if (message->is_worker()) {
    // Register the new worker.
    worker_pool_.RegisterWorker(std::move(worker));
    DispatchTasks(local_queues_.GetReadyTasksWithResources());
  } else {
    // Register the new driver. Note that here the driver_id in RegisterClientRequest
    // message is actually the ID of the driver task, while client_id represents the
    // real driver ID, which can associate all the tasks/actors for a given driver,
    // which is set to the worker ID.
    const DriverID driver_task_id = from_flatbuf<DriverID>(*message->driver_id());
    worker->AssignTaskId(TaskID(driver_task_id));
    worker->AssignDriverId(from_flatbuf<DriverID>(*message->client_id()));
    worker_pool_.RegisterDriver(std::move(worker));
    local_queues_.AddDriverTaskId(TaskID(driver_task_id));
  }
}

void NodeManager::HandleDisconnectedActor(const ActorID &actor_id, bool was_local,
                                          bool intentional_disconnect) {
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
          ? ActorState::RECONSTRUCTING
          : ActorState::DEAD;
  if (was_local) {
    // Clean up the dummy objects from this actor.
    RAY_LOG(DEBUG) << "Removing dummy objects for actor: " << actor_id;
    for (auto &dummy_object_pair : actor_entry->second.GetDummyObjects()) {
      HandleObjectMissing(dummy_object_pair.first);
    }
  }
  // Update the actor's state.
  ActorTableDataT new_actor_data = actor_entry->second.GetTableData();
  new_actor_data.state = new_state;
  if (was_local) {
    // If the actor was local, immediately update the state in actor registry.
    // So if we receive any actor tasks before we receive GCS notification,
    // these tasks can be correctly routed to the `MethodsWaitingForActorCreation` queue,
    // instead of being assigned to the dead actor.
    HandleActorStateTransition(actor_id, ActorRegistration(new_actor_data));
  }
  ray::gcs::ActorTable::WriteCallback failure_callback = nullptr;
  if (was_local) {
    failure_callback = [](gcs::AsyncGcsClient *client, const ActorID &id,
                          const ActorTableDataT &data) {
      // If the disconnected actor was local, only this node will try to update actor
      // state. So the update shouldn't fail.
      RAY_LOG(FATAL) << "Failed to update state for actor " << id;
    };
  }
  PublishActorStateTransition(actor_id, new_actor_data, failure_callback);
}

void NodeManager::ProcessGetTaskMessage(
    const std::shared_ptr<LocalClientConnection> &client) {
  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
  RAY_CHECK(worker);
  // If the worker was assigned a task, mark it as finished.
  if (!worker->GetAssignedTaskId().is_nil()) {
    FinishAssignedTask(*worker);
  }
  // Return the worker to the idle pool.
  worker_pool_.PushWorker(std::move(worker));
  // Local resource availability changed: invoke scheduling policy for local node.
  const ClientID &local_client_id = gcs_client_->client_table().GetLocalClientId();
  cluster_resource_map_[local_client_id].SetLoadResources(
      local_queues_.GetResourceLoad());
  // Call task dispatch to assign work to the new worker.
  DispatchTasks(local_queues_.GetReadyTasksWithResources());
}

void NodeManager::ProcessDisconnectClientMessage(
    const std::shared_ptr<LocalClientConnection> &client, bool intentional_disconnect) {
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
    }
  }
  RAY_CHECK(!(is_worker && is_driver));

  // If the client has any blocked tasks, mark them as unblocked. In
  // particular, we are no longer waiting for their dependencies.
  if (worker) {
    if (is_worker && worker->IsDead()) {
      // Don't need to unblock the client if it's a worker and is already dead.
      // Because in this case, its task is already cleaned up.
      RAY_LOG(DEBUG) << "Skip unblocking worker because it's already dead.";
    } else {
      while (!worker->GetBlockedTaskIds().empty()) {
        // NOTE(swang): HandleTaskUnblocked will modify the worker, so it is
        // not safe to pass in the iterator directly.
        const TaskID task_id = *worker->GetBlockedTaskIds().begin();
        HandleTaskUnblocked(client, task_id);
      }
    }
  }

  if (is_worker) {
    // The client is a worker.
    if (worker->IsDead()) {
      // If the worker was killed by us because the driver exited,
      // treat it as intentionally disconnected.
      intentional_disconnect = true;
    }

    const ActorID &actor_id = worker->GetActorId();
    if (!actor_id.is_nil()) {
      // If the worker was an actor, update actor state, reconstruct the actor if needed,
      // and clean up actor's tasks if the actor is permanently dead.
      HandleDisconnectedActor(actor_id, true, intentional_disconnect);
    }

    const TaskID &task_id = worker->GetAssignedTaskId();
    // If the worker was running a task, clean up the task and push an error to
    // the driver, unless the worker is already dead.
    if (!task_id.is_nil() && !worker->IsDead()) {
      // If the worker was an actor, the task was already cleaned up in
      // `HandleDisconnectedActor`.
      if (actor_id.is_nil()) {
        const Task &task = local_queues_.RemoveTask(task_id);
        TreatTaskAsFailed(task, ErrorType::WORKER_DIED);
      }

      if (!intentional_disconnect) {
        // Push the error to driver.
        const DriverID &driver_id = worker->GetAssignedDriverId();
        // TODO(rkn): Define this constant somewhere else.
        std::string type = "worker_died";
        std::ostringstream error_message;
        error_message << "A worker died or was killed while executing task " << task_id
                      << ".";
        RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(
            driver_id, type, error_message.str(), current_time_ms()));
      }
    }

    // Remove the dead client from the pool and stop listening for messages.
    worker_pool_.DisconnectWorker(worker);

    const ClientID &client_id = gcs_client_->client_table().GetLocalClientId();

    // Return the resources that were being used by this worker.
    auto const &task_resources = worker->GetTaskResourceIds();
    local_available_resources_.Release(task_resources);
    cluster_resource_map_[client_id].Release(task_resources.ToResourceSet());
    worker->ResetTaskResourceIds();

    auto const &lifetime_resources = worker->GetLifetimeResourceIds();
    local_available_resources_.Release(lifetime_resources);
    cluster_resource_map_[client_id].Release(lifetime_resources.ToResourceSet());
    worker->ResetLifetimeResourceIds();

    RAY_LOG(DEBUG) << "Worker (pid=" << worker->Pid() << ") is disconnected. "
                   << "driver_id: " << worker->GetAssignedDriverId();

    // Since some resources may have been released, we can try to dispatch more tasks.
    DispatchTasks(local_queues_.GetReadyTasksWithResources());
  } else if (is_driver) {
    // The client is a driver.
    RAY_CHECK_OK(
        gcs_client_->driver_table().AppendDriverData(DriverID(client->GetClientId()),
                                                     /*is_dead=*/true));
    auto driver_id = worker->GetAssignedTaskId();
    RAY_CHECK(!driver_id.is_nil());
    local_queues_.RemoveDriverTaskId(driver_id);
    worker_pool_.DisconnectDriver(worker);

    RAY_LOG(DEBUG) << "Driver (pid=" << worker->Pid() << ") is disconnected. "
                   << "driver_id: " << worker->GetAssignedDriverId();
  }

  // TODO(rkn): Tell the object manager that this client has disconnected so
  // that it can clean up the wait requests for this client. Currently I think
  // these can be leaked.
}

void NodeManager::ProcessSubmitTaskMessage(const uint8_t *message_data) {
  // Read the task submitted by the client.
  auto message = flatbuffers::GetRoot<protocol::SubmitTaskRequest>(message_data);
  TaskExecutionSpecification task_execution_spec(
      from_flatbuf<ObjectID>(*message->execution_dependencies()));
  TaskSpecification task_spec(*message->task_spec());
  Task task(task_execution_spec, task_spec);
  // Submit the task to the raylet. Since the task was submitted
  // locally, there is no uncommitted lineage.
  SubmitTask(task, Lineage());
}

void NodeManager::ProcessFetchOrReconstructMessage(
    const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::FetchOrReconstruct>(message_data);
  std::vector<ObjectID> required_object_ids;
  for (size_t i = 0; i < message->object_ids()->size(); ++i) {
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
    HandleTaskBlocked(client, required_object_ids, task_id);
  }
}

void NodeManager::ProcessWaitRequestMessage(
    const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data) {
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
  bool client_blocked = !required_object_ids.empty();
  if (client_blocked) {
    HandleTaskBlocked(client, required_object_ids, current_task_id);
  }

  ray::Status status = object_manager_.Wait(
      object_ids, wait_ms, num_required_objects, wait_local,
      [this, client_blocked, client, current_task_id](std::vector<ObjectID> found,
                                                      std::vector<ObjectID> remaining) {
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
          if (client_blocked) {
            HandleTaskUnblocked(client, current_task_id);
          }
        } else {
          // We failed to write to the client, so disconnect the client.
          RAY_LOG(WARNING)
              << "Failed to send WaitReply to client, so disconnecting client";
          // We failed to send the reply to the client, so disconnect the worker.
          ProcessDisconnectClientMessage(client);
        }
      });
  RAY_CHECK_OK(status);
}

void NodeManager::ProcessPushErrorRequestMessage(const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::PushErrorRequest>(message_data);

  DriverID driver_id = from_flatbuf<DriverID>(*message->driver_id());
  auto const &type = string_from_flatbuf(*message->type());
  auto const &error_message = string_from_flatbuf(*message->error_message());
  double timestamp = message->timestamp();

  RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(driver_id, type,
                                                            error_message, timestamp));
}

void NodeManager::ProcessPrepareActorCheckpointRequest(
    const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data) {
  auto message =
      flatbuffers::GetRoot<protocol::PrepareActorCheckpointRequest>(message_data);
  ActorID actor_id = from_flatbuf<ActorID>(*message->actor_id());
  RAY_LOG(DEBUG) << "Preparing checkpoint for actor " << actor_id;
  const auto &actor_entry = actor_registry_.find(actor_id);
  RAY_CHECK(actor_entry != actor_registry_.end());

  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
  RAY_CHECK(worker && worker->GetActorId() == actor_id);

  // Find the task that is running on this actor.
  const auto task_id = worker->GetAssignedTaskId();
  const Task &task = local_queues_.GetTaskOfState(task_id, TaskState::RUNNING);
  // Generate checkpoint id and data.
  ActorCheckpointID checkpoint_id = ActorCheckpointID::from_random();
  auto checkpoint_data =
      actor_entry->second.GenerateCheckpointData(actor_entry->first, task);

  // Write checkpoint data to GCS.
  RAY_CHECK_OK(gcs_client_->actor_checkpoint_table().Add(
      DriverID::nil(), checkpoint_id, checkpoint_data,
      [worker, actor_id, this](ray::gcs::AsyncGcsClient *client,
                               const ActorCheckpointID &checkpoint_id,
                               const ActorCheckpointDataT &data) {
        RAY_LOG(DEBUG) << "Checkpoint " << checkpoint_id << " saved for actor "
                       << worker->GetActorId();
        // Save this actor-to-checkpoint mapping, and remove old checkpoints associated
        // with this actor.
        RAY_CHECK_OK(gcs_client_->actor_checkpoint_id_table().AddCheckpointId(
            DriverID::nil(), actor_id, checkpoint_id));
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

void NodeManager::ProcessNewNodeManager(TcpClientConnection &node_manager_client) {
  node_manager_client.ProcessMessages();
}

void NodeManager::ProcessNodeManagerMessage(TcpClientConnection &node_manager_client,
                                            int64_t message_type,
                                            const uint8_t *message_data) {
  const auto message_type_value = static_cast<protocol::MessageType>(message_type);
  RAY_LOG(DEBUG) << "[NodeManager] Message "
                 << protocol::EnumNameMessageType(message_type_value) << "("
                 << message_type << ") from node manager";
  switch (message_type_value) {
  case protocol::MessageType::ConnectClient: {
    auto message = flatbuffers::GetRoot<protocol::ConnectClient>(message_data);
    auto client_id = from_flatbuf<ClientID>(*message->client_id());
    node_manager_client.SetClientID(client_id);
  } break;
  case protocol::MessageType::ForwardTaskRequest: {
    auto message = flatbuffers::GetRoot<protocol::ForwardTaskRequest>(message_data);
    TaskID task_id = from_flatbuf<TaskID>(*message->task_id());

    Lineage uncommitted_lineage(*message);
    const Task &task = uncommitted_lineage.GetEntry(task_id)->TaskData();
    RAY_LOG(DEBUG) << "Received forwarded task " << task.GetTaskSpecification().TaskId()
                   << " on node " << gcs_client_->client_table().GetLocalClientId()
                   << " spillback=" << task.GetTaskExecutionSpec().NumForwards();
    SubmitTask(task, uncommitted_lineage, /* forwarded = */ true);
  } break;
  case protocol::MessageType::DisconnectClient: {
    // TODO(rkn): We need to do some cleanup here.
    RAY_LOG(DEBUG) << "Received disconnect message from remote node manager. "
                   << "We need to do some cleanup here.";
    // Do not process any more messages from this node manager.
    return;
  } break;
  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }
  node_manager_client.ProcessMessages();
}

void NodeManager::ScheduleTasks(
    std::unordered_map<ClientID, SchedulingResources> &resource_map) {
  const ClientID &local_client_id = gcs_client_->client_table().GetLocalClientId();

  // If the resource map contains the local raylet, update load before calling policy.
  if (resource_map.count(local_client_id) > 0) {
    resource_map[local_client_id].SetLoadResources(local_queues_.GetResourceLoad());
  }
  // Invoke the scheduling policy.
  auto policy_decision = scheduling_policy_.Schedule(resource_map, local_client_id);

#ifndef NDEBUG
  RAY_LOG(DEBUG) << "[NM ScheduleTasks] policy decision:";
  for (const auto &task_client_pair : policy_decision) {
    TaskID task_id = task_client_pair.first;
    ClientID client_id = task_client_pair.second;
    RAY_LOG(DEBUG) << task_id << " --> " << client_id;
  }
#endif

  // Extract decision for this raylet.
  std::unordered_set<TaskID> local_task_ids;
  // Iterate over (taskid, clientid) pairs, extract tasks assigned to the local node.
  for (const auto &task_client_pair : policy_decision) {
    const TaskID &task_id = task_client_pair.first;
    const ClientID &client_id = task_client_pair.second;
    if (client_id == local_client_id) {
      local_task_ids.insert(task_id);
    } else {
      // TODO(atumanov): need a better interface for task exit on forward.
      // (See design_docs/task_states.rst for the state transition diagram.)
      const auto task = local_queues_.RemoveTask(task_id);
      // Attempt to forward the task. If this fails to forward the task,
      // the task will be resubmit locally.
      ForwardTaskOrResubmit(task, client_id);
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
          << "The task with ID " << task.GetTaskSpecification().TaskId()
          << " is infeasible and cannot currently be executed. It requires "
          << task.GetTaskSpecification().GetRequiredResources().ToString()
          << " for execution and "
          << task.GetTaskSpecification().GetRequiredPlacementResources().ToString()
          << " for placement. Check the client table to view node resources.";
      RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(
          task.GetTaskSpecification().DriverId(), type, error_message.str(),
          current_time_ms()));
    }
    // Assert that this placeable task is not feasible locally (necessary but not
    // sufficient).
    RAY_CHECK(!task.GetTaskSpecification().GetRequiredPlacementResources().IsSubset(
        cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()]
            .GetTotalResources()));
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
                 << EnumNameErrorType(error_type) << ".";
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
  const std::string meta = std::to_string(static_cast<int>(error_type));
  for (int64_t i = 0; i < num_returns; i++) {
    const auto object_id = spec.ReturnId(i).to_plasma_id();
    arrow::Status status = store_client_.CreateAndSeal(object_id, "", meta);
    if (!status.ok() && !status.IsPlasmaObjectExists()) {
      // If we failed to save the error code, log a warning and push an error message
      // to the driver.
      std::ostringstream stream;
      stream << "An plasma error (" << status.ToString() << ") occurred while saving"
             << " error code to object " << object_id << ". Anyone who's getting this"
             << " object may hang forever.";
      std::string error_message = stream.str();
      RAY_LOG(WARNING) << error_message;
      RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(
          task.GetTaskSpecification().DriverId(), "task", error_message,
          current_time_ms()));
    }
  }
  // A task failing is equivalent to assigning and finishing the task, so clean
  // up any leftover state as for any task dispatched and removed from the
  // local queue.
  lineage_cache_.AddReadyTask(task);
  task_dependency_manager_.TaskCanceled(spec.TaskId());
  // Notify the task dependency manager that we no longer need this task's
  // object dependencies. TODO(swang): Ideally, we would check the return value
  // here. However, we don't know at this point if the task was in the WAITING
  // or READY queue before, in which case we would not have been subscribed to
  // its dependencies.
  task_dependency_manager_.UnsubscribeDependencies(spec.TaskId());
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
    const ObjectID object_id = spec.ReturnId(i);
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
  RAY_LOG(DEBUG) << "Submitting task: task_id=" << task_id
                 << ", actor_id=" << spec.ActorId()
                 << ", actor_creation_id=" << spec.ActorCreationId()
                 << ", actor_handle_id=" << spec.ActorHandleId()
                 << ", actor_counter=" << spec.ActorCounter()
                 << ", task_descriptor=" << spec.FunctionDescriptorString() << " on node "
                 << gcs_client_->client_table().GetLocalClientId();

  if (local_queues_.HasTask(task_id)) {
    RAY_LOG(WARNING) << "Submitted task " << task_id
                     << " is already queued and will not be reconstructed. This is most "
                        "likely due to spurious reconstruction.";
    return;
  }

  // Add the task and its uncommitted lineage to the lineage cache.
  if (!lineage_cache_.AddWaitingTask(task, uncommitted_lineage)) {
    RAY_LOG(WARNING)
        << "Task " << task_id
        << " already in lineage cache. This is most likely due to reconstruction.";
  }

  if (spec.IsActorTask()) {
    // Check whether we know the location of the actor.
    const auto actor_entry = actor_registry_.find(spec.ActorId());
    bool seen = actor_entry != actor_registry_.end();
    // If we have already seen this actor and this actor is not being reconstructed,
    // its location is known.
    bool location_known =
        seen && actor_entry->second.GetState() != ActorState::RECONSTRUCTING;
    if (location_known) {
      if (actor_entry->second.GetState() == ActorState::DEAD) {
        // If this actor is dead, either because the actor process is dead
        // or because its residing node is dead, treat this task as failed.
        TreatTaskAsFailed(task, ErrorType::ACTOR_DIED);
      } else {
        // If this actor is alive, check whether this actor is local.
        auto node_manager_id = actor_entry->second.GetNodeManagerId();
        if (node_manager_id == gcs_client_->client_table().GetLocalClientId()) {
          // The actor is local.
          int64_t expected_task_counter = GetExpectedTaskCounter(
              actor_registry_, spec.ActorId(), spec.ActorHandleId());
          if (spec.ActorCounter() < expected_task_counter) {
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
        auto lookup_callback = [this](gcs::AsyncGcsClient *client,
                                      const ActorID &actor_id,
                                      const std::vector<ActorTableDataT> &data) {
          if (!data.empty()) {
            // The actor has been created. We only need the last entry, because
            // it represents the latest state of this actor.
            HandleActorStateTransition(actor_id, ActorRegistration(data.back()));
          }
        };
        RAY_CHECK_OK(gcs_client_->actor_table().Lookup(DriverID::nil(), spec.ActorId(),
                                                       lookup_callback));
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
      // UnsubscribeDependencies.
      task_dependency_manager_.SubscribeDependencies(spec.TaskId(),
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

void NodeManager::HandleTaskBlocked(const std::shared_ptr<LocalClientConnection> &client,
                                    const std::vector<ObjectID> &required_object_ids,
                                    const TaskID &current_task_id) {
  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
  if (worker) {
    // The client is a worker. If the worker is not already blocked and the
    // blocked task matches the one assigned to the worker, then mark the
    // worker as blocked. This temporarily releases any resources that the
    // worker holds while it is blocked.
    if (!worker->IsBlocked() && current_task_id == worker->GetAssignedTaskId()) {
      const auto task = local_queues_.RemoveTask(current_task_id);
      local_queues_.QueueTasks({task}, TaskState::RUNNING);
      // Get the CPU resources required by the running task.
      const auto required_resources = task.GetTaskSpecification().GetRequiredResources();
      const ResourceSet cpu_resources = required_resources.GetNumCpus();

      // Release the CPU resources.
      auto const cpu_resource_ids = worker->ReleaseTaskCpuResources();
      local_available_resources_.Release(cpu_resource_ids);
      cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Release(
          cpu_resources);
      worker->MarkBlocked();

      // Try dispatching tasks since we may have released some resources.
      DispatchTasks(local_queues_.GetReadyTasksWithResources());
    }
  } else {
    // The client is a driver. Drivers do not hold resources, so we simply mark
    // the task as blocked.
    worker = worker_pool_.GetRegisteredDriver(client);
  }

  RAY_CHECK(worker);
  // Mark the task as blocked.
  worker->AddBlockedTaskId(current_task_id);
  if (local_queues_.GetBlockedTaskIds().count(current_task_id) == 0) {
    local_queues_.AddBlockedTaskId(current_task_id);
  }

  // Subscribe to the objects required by the ray.get. These objects will
  // be fetched and/or reconstructed as necessary, until the objects become
  // local or are unsubscribed.
  task_dependency_manager_.SubscribeDependencies(current_task_id, required_object_ids);
}

void NodeManager::HandleTaskUnblocked(
    const std::shared_ptr<LocalClientConnection> &client, const TaskID &current_task_id) {
  std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);

  // TODO(swang): Because the object dependencies are tracked in the task
  // dependency manager, we could actually remove this message entirely and
  // instead unblock the worker once all the objects become available.
  if (worker) {
    // The client is a worker. If the worker is not already unblocked and the
    // unblocked task matches the one assigned to the worker, then mark the
    // worker as unblocked. This returns the temporarily released resources to
    // the worker.
    if (worker->IsBlocked() && current_task_id == worker->GetAssignedTaskId()) {
      // (See design_docs/task_states.rst for the state transition diagram.)
      const auto task = local_queues_.RemoveTask(current_task_id);
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
        cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Acquire(
            cpu_resources);
      } else {
        // In this case, we simply don't reacquire the CPU resources for the worker.
        // The worker can keep running and when the task finishes, it will simply
        // not have any CPU resources to release.
        RAY_LOG(WARNING)
            << "Resources oversubscribed: "
            << cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()]
                   .GetAvailableResources()
                   .ToString();
      }
      worker->MarkUnblocked();
    }
  } else {
    // The client is a driver. Drivers do not hold resources, so we simply
    // mark the driver as unblocked.
    worker = worker_pool_.GetRegisteredDriver(client);
  }

  RAY_CHECK(worker);
  // If the task was previously blocked, then stop waiting for its dependencies
  // and mark the task as unblocked.
  worker->RemoveBlockedTaskId(current_task_id);
  // Unsubscribe to the objects. Any fetch or reconstruction operations to
  // make the objects local are canceled.
  RAY_CHECK(task_dependency_manager_.UnsubscribeDependencies(current_task_id));
  local_queues_.RemoveBlockedTaskId(current_task_id);
}

void NodeManager::EnqueuePlaceableTask(const Task &task) {
  // TODO(atumanov): add task lookup hashmap and change EnqueuePlaceableTask to take
  // a vector of TaskIDs. Trigger MoveTask internally.
  // Subscribe to the task's dependencies.
  bool args_ready = task_dependency_manager_.SubscribeDependencies(
      task.GetTaskSpecification().TaskId(), task.GetDependencies());
  // Enqueue the task. If all dependencies are available, then the task is queued
  // in the READY state, else the WAITING state.
  // (See design_docs/task_states.rst for the state transition diagram.)
  if (args_ready) {
    local_queues_.QueueTasks({task}, TaskState::READY);
    DispatchTasks(MakeTasksWithResources({task}));
  } else {
    local_queues_.QueueTasks({task}, TaskState::WAITING);
  }
  // Mark the task as pending. Once the task has finished execution, or once it
  // has been forwarded to another node, the task must be marked as canceled in
  // the TaskDependencyManager.
  task_dependency_manager_.TaskPending(task);
}

bool NodeManager::AssignTask(const Task &task) {
  const TaskSpecification &spec = task.GetTaskSpecification();

  // If this is an actor task, check that the new task has the correct counter.
  if (spec.IsActorTask()) {
    // An actor task should only be ready to be assigned if it matches the
    // expected task counter.
    int64_t expected_task_counter =
        GetExpectedTaskCounter(actor_registry_, spec.ActorId(), spec.ActorHandleId());
    RAY_CHECK(spec.ActorCounter() == expected_task_counter)
        << "Expected actor counter: " << expected_task_counter << ", task "
        << spec.TaskId() << " has: " << spec.ActorCounter();
  }

  // Try to get an idle worker that can execute this task.
  std::shared_ptr<Worker> worker = worker_pool_.PopWorker(spec);
  if (worker == nullptr) {
    // There are no workers that can execute this task.
    if (!spec.IsActorTask()) {
      // There are no more non-actor workers available to execute this task.
      // Start a new worker.
      worker_pool_.StartWorkerProcess(spec.GetLanguage());
      // Push an error message to the user if the worker pool tells us that it is
      // getting too big.
      const std::string warning_message = worker_pool_.WarningAboutSize();
      if (warning_message != "") {
        RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(
            DriverID::nil(), "worker_pool_large", warning_message, current_time_ms()));
      }
    }
    // We couldn't assign this task, as no worker available.
    return false;
  }

  RAY_LOG(DEBUG) << "Assigning task " << spec.TaskId() << " to worker with pid "
                 << worker->Pid();
  flatbuffers::FlatBufferBuilder fbb;

  // Resource accounting: acquire resources for the assigned task.
  auto acquired_resources =
      local_available_resources_.Acquire(spec.GetRequiredResources());
  const auto &my_client_id = gcs_client_->client_table().GetLocalClientId();
  cluster_resource_map_[my_client_id].Acquire(spec.GetRequiredResources());

  if (spec.IsActorCreationTask()) {
    // Check that we are not placing an actor creation task on a node with 0 CPUs.
    RAY_CHECK(cluster_resource_map_[my_client_id].GetTotalResources().GetResourceMap().at(
                  kCPU_ResourceLabel) != 0);
    worker->SetLifetimeResourceIds(acquired_resources);
  } else {
    worker->SetTaskResourceIds(acquired_resources);
  }

  ResourceIdSet resource_id_set =
      worker->GetTaskResourceIds().Plus(worker->GetLifetimeResourceIds());
  auto resource_id_set_flatbuf = resource_id_set.ToFlatbuf(fbb);

  auto message = protocol::CreateGetTaskReply(fbb, spec.ToFlatbuffer(fbb),
                                              fbb.CreateVector(resource_id_set_flatbuf));
  fbb.Finish(message);
  // Give the callback a copy of the task so it can modify it.
  Task assigned_task(task);
  worker->Connection()->WriteMessageAsync(
      static_cast<int64_t>(protocol::MessageType::ExecuteTask), fbb.GetSize(),
      fbb.GetBufferPointer(), [this, worker, assigned_task](ray::Status status) mutable {
        if (status.ok()) {
          auto spec = assigned_task.GetTaskSpecification();
          // We successfully assigned the task to the worker.
          worker->AssignTaskId(spec.TaskId());
          worker->AssignDriverId(spec.DriverId());
          // Actor tasks require extra accounting to track the actor's state.
          if (spec.IsActorTask()) {
            auto actor_entry = actor_registry_.find(spec.ActorId());
            RAY_CHECK(actor_entry != actor_registry_.end());
            // Process any new actor handles that were created since the
            // previous task on this handle was executed. The first task
            // submitted on a new actor handle will depend on the dummy object
            // returned by the previous task, so the dependency will not be
            // released until this first task is submitted.
            for (auto &new_handle_id : spec.NewActorHandles()) {
              // Get the execution dependency for the first task submitted on the new
              // actor handle. Since the new actor handle was created after this task
              // began and before this task finished, it must have the same execution
              // dependency.
              const auto &execution_dependencies =
                  assigned_task.GetTaskExecutionSpec().ExecutionDependencies();
              // TODO(swang): We expect this task to have exactly 1 execution dependency,
              // the dummy object returned by the previous actor task. However, this
              // leaks information about the TaskExecutionSpecification implementation.
              RAY_CHECK(execution_dependencies.size() == 1);
              const ObjectID &execution_dependency = execution_dependencies.front();
              // Add the new handle and give it a reference to the finished task's
              // execution dependency.
              actor_entry->second.AddHandle(new_handle_id, execution_dependency);
            }

            // If the task was an actor task, then record this execution to
            // guarantee consistency in the case of reconstruction.
            auto execution_dependency = actor_entry->second.GetExecutionDependency();
            // The execution dependency is initialized to the actor creation task's
            // return value, and is subsequently updated to the assigned tasks'
            // return values, so it should never be nil.
            RAY_CHECK(!execution_dependency.is_nil());
            // Update the task's execution dependencies to reflect the actual
            // execution order, to support deterministic reconstruction.
            // NOTE(swang): The update of an actor task's execution dependencies is
            // performed asynchronously. This means that if this node manager dies,
            // we may lose updates that are in flight to the task table. We only
            // guarantee deterministic reconstruction ordering for tasks whose
            // updates are reflected in the task table.
            // (SetExecutionDependencies takes a non-const so copy task in a
            //  on-const variable.)
            assigned_task.SetExecutionDependencies({execution_dependency});
          } else {
            RAY_CHECK(spec.NewActorHandles().empty());
          }

          // We started running the task, so the task is ready to write to GCS.
          if (!lineage_cache_.AddReadyTask(assigned_task)) {
            RAY_LOG(WARNING) << "Task " << spec.TaskId() << " already in lineage cache."
                             << " This is most likely due to reconstruction.";
          }
          // Mark the task as running.
          // (See design_docs/task_states.rst for the state transition diagram.)
          local_queues_.QueueTasks({assigned_task}, TaskState::RUNNING);
          // Notify the task dependency manager that we no longer need this task's
          // object dependencies.
          RAY_CHECK(task_dependency_manager_.UnsubscribeDependencies(spec.TaskId()));
        } else {
          RAY_LOG(WARNING) << "Failed to send task to worker, disconnecting client";
          // We failed to send the task to the worker, so disconnect the worker.
          ProcessDisconnectClientMessage(worker->Connection());
          // Queue this task for future assignment. We need to do this since
          // DispatchTasks() removed it from the ready queue. The task will be
          // assigned to a worker once one becomes available.
          // (See design_docs/task_states.rst for the state transition diagram.)
          local_queues_.QueueTasks({assigned_task}, TaskState::READY);
          DispatchTasks(MakeTasksWithResources({assigned_task}));
        }
      });

  // We assigned this task to a worker.
  // (Note this means that we sent the task to the worker. The assignment
  //  might still fail if the worker fails in the meantime, for instance.)
  return true;
}

void NodeManager::FinishAssignedTask(Worker &worker) {
  TaskID task_id = worker.GetAssignedTaskId();
  RAY_LOG(DEBUG) << "Finished task " << task_id;

  // (See design_docs/task_states.rst for the state transition diagram.)
  const auto task = local_queues_.RemoveTask(task_id);

  // Release task's resources. The worker's lifetime resources are still held.
  auto const &task_resources = worker.GetTaskResourceIds();
  local_available_resources_.Release(task_resources);
  cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Release(
      task_resources.ToResourceSet());
  worker.ResetTaskResourceIds();

  // If this was an actor or actor creation task, handle the actor's new state.
  if (task.GetTaskSpecification().IsActorCreationTask() ||
      task.GetTaskSpecification().IsActorTask()) {
    FinishAssignedActorTask(worker, task);
  }

  // Notify the task dependency manager that this task has finished execution.
  task_dependency_manager_.TaskCanceled(task_id);

  // Unset the worker's assigned task.
  worker.AssignTaskId(TaskID::nil());
  // Unset the worker's assigned driver Id if this is not an actor.
  if (!task.GetTaskSpecification().IsActorCreationTask() &&
      !task.GetTaskSpecification().IsActorTask()) {
    worker.AssignDriverId(DriverID::nil());
  }
}

ActorTableDataT NodeManager::CreateActorTableDataFromCreationTask(const Task &task) {
  RAY_CHECK(task.GetTaskSpecification().IsActorCreationTask());
  auto actor_id = task.GetTaskSpecification().ActorCreationId();
  auto actor_entry = actor_registry_.find(actor_id);
  ActorTableDataT new_actor_data;
  // TODO(swang): If this is an actor that was reconstructed, and previous
  // actor notifications were delayed, then this node may not have an entry for
  // the actor in actor_regisry_. Then, the fields for the number of
  // reconstructions will be wrong.
  if (actor_entry == actor_registry_.end()) {
    // Set all of the static fields for the actor. These fields will not
    // change even if the actor fails or is reconstructed.
    new_actor_data.actor_id = actor_id.binary();
    new_actor_data.actor_creation_dummy_object_id =
        task.GetTaskSpecification().ActorDummyObject().binary();
    new_actor_data.driver_id = task.GetTaskSpecification().DriverId().binary();
    new_actor_data.max_reconstructions =
        task.GetTaskSpecification().MaxActorReconstructions();
    // This is the first time that the actor has been created, so the number
    // of remaining reconstructions is the max.
    new_actor_data.remaining_reconstructions =
        task.GetTaskSpecification().MaxActorReconstructions();
  } else {
    // If we've already seen this actor, it means that this actor was reconstructed.
    // Thus, its previous state must be RECONSTRUCTING.
    RAY_CHECK(actor_entry->second.GetState() == ActorState::RECONSTRUCTING);
    // Copy the static fields from the current actor entry.
    new_actor_data = actor_entry->second.GetTableData();
    // We are reconstructing the actor, so subtract its
    // remaining_reconstructions by 1.
    new_actor_data.remaining_reconstructions--;
  }

  // Set the new fields for the actor's state to indicate that the actor is
  // now alive on this node manager.
  new_actor_data.node_manager_id =
      gcs_client_->client_table().GetLocalClientId().binary();
  new_actor_data.state = ActorState::ALIVE;
  return new_actor_data;
}

void NodeManager::FinishAssignedActorTask(Worker &worker, const Task &task) {
  ActorID actor_id;
  ActorHandleID actor_handle_id;
  bool resumed_from_checkpoint = false;
  if (task.GetTaskSpecification().IsActorCreationTask()) {
    actor_id = task.GetTaskSpecification().ActorCreationId();
    actor_handle_id = ActorHandleID::nil();
    if (checkpoint_id_to_restore_.count(actor_id) > 0) {
      resumed_from_checkpoint = true;
    }
  } else {
    actor_id = task.GetTaskSpecification().ActorId();
    actor_handle_id = task.GetTaskSpecification().ActorHandleId();
  }

  if (task.GetTaskSpecification().IsActorCreationTask()) {
    // This was an actor creation task. Convert the worker to an actor.
    worker.AssignActorId(actor_id);
    // Notify the other node managers that the actor has been created.
    const auto new_actor_data = CreateActorTableDataFromCreationTask(task);
    if (resumed_from_checkpoint) {
      // This actor was resumed from a checkpoint. In this case, we first look
      // up the checkpoint in GCS and use it to restore the actor registration
      // and frontier.
      const auto checkpoint_id = checkpoint_id_to_restore_[actor_id];
      checkpoint_id_to_restore_.erase(actor_id);
      RAY_LOG(DEBUG) << "Looking up checkpoint " << checkpoint_id << " for actor "
                     << actor_id;
      RAY_CHECK_OK(gcs_client_->actor_checkpoint_table().Lookup(
          DriverID::nil(), checkpoint_id,
          [this, actor_id, new_actor_data](ray::gcs::AsyncGcsClient *client,
                                           const UniqueID &checkpoint_id,
                                           const ActorCheckpointDataT &checkpoint_data) {
            RAY_LOG(INFO) << "Restoring registration for actor " << actor_id
                          << " from checkpoint " << checkpoint_id;
            ActorRegistration actor_registration =
                ActorRegistration(new_actor_data, checkpoint_data);
            // Mark the unreleased dummy objects in the checkpoint frontier as local.
            for (const auto &entry : actor_registration.GetDummyObjects()) {
              HandleObjectLocal(entry.first);
            }
            HandleActorStateTransition(actor_id, std::move(actor_registration));
            PublishActorStateTransition(
                actor_id, new_actor_data,
                /*failure_callback=*/
                [](gcs::AsyncGcsClient *client, const ActorID &id,
                   const ActorTableDataT &data) {
                  // Only one node at a time should succeed at creating the actor.
                  RAY_LOG(FATAL) << "Failed to update state to ALIVE for actor " << id;
                });
          },
          [actor_id](ray::gcs::AsyncGcsClient *client, const UniqueID &checkpoint_id) {
            RAY_LOG(FATAL) << "Couldn't find checkpoint " << checkpoint_id
                           << " for actor " << actor_id << " in GCS.";
          }));
    } else {
      // The actor did not resume from a checkpoint. Immediately notify the
      // other node managers that the actor has been created.
      HandleActorStateTransition(actor_id, ActorRegistration(new_actor_data));
      PublishActorStateTransition(
          actor_id, new_actor_data,
          /*failure_callback=*/
          [](gcs::AsyncGcsClient *client, const ActorID &id,
             const ActorTableDataT &data) {
            // Only one node at a time should succeed at creating the actor.
            RAY_LOG(FATAL) << "Failed to update state to ALIVE for actor " << id;
          });
    }
  }

  if (!resumed_from_checkpoint) {
    // The actor was not resumed from a checkpoint. We extend the actor's
    // frontier as usual since there is no frontier to restore.
    auto actor_entry = actor_registry_.find(actor_id);
    RAY_CHECK(actor_entry != actor_registry_.end());
    // Extend the actor's frontier to include the executed task.
    const auto dummy_object = task.GetTaskSpecification().ActorDummyObject();
    const ObjectID object_to_release =
        actor_entry->second.ExtendFrontier(actor_handle_id, dummy_object);
    if (!object_to_release.is_nil()) {
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
    HandleObjectLocal(dummy_object);
  }
}

void NodeManager::HandleTaskReconstruction(const TaskID &task_id) {
  // Retrieve the task spec in order to re-execute the task.
  RAY_CHECK_OK(gcs_client_->raylet_task_table().Lookup(
      DriverID::nil(), task_id,
      /*success_callback=*/
      [this](ray::gcs::AsyncGcsClient *client, const TaskID &task_id,
             const ray::protocol::TaskT &task_data) {
        // The task was in the GCS task table. Use the stored task spec to
        // re-execute the task.
        const Task task(task_data);
        ResubmitTask(task);
      },
      /*failure_callback=*/
      [this](ray::gcs::AsyncGcsClient *client, const TaskID &task_id) {
        // The task was not in the GCS task table. It must therefore be in the
        // lineage cache.
        RAY_CHECK(lineage_cache_.ContainsTask(task_id))
            << "Task metadata not found in either GCS or lineage cache. It may have been "
               "evicted "
            << "by the redis LRU configuration. Consider increasing the memory "
               "allocation via "
            << "ray.init(redis_max_memory=<max_memory_bytes>).";
        // Use a copy of the cached task spec to re-execute the task.
        const Task task = lineage_cache_.GetTaskOrDie(task_id);
        ResubmitTask(task);
      }));
}

void NodeManager::ResubmitTask(const Task &task) {
  RAY_LOG(DEBUG) << "Attempting to resubmit task "
                 << task.GetTaskSpecification().TaskId();

  // Actors should only be recreated if the first initialization failed or if
  // the most recent instance of the actor failed.
  if (task.GetTaskSpecification().IsActorCreationTask()) {
    const auto &actor_id = task.GetTaskSpecification().ActorCreationId();
    const auto it = actor_registry_.find(actor_id);
    if (it != actor_registry_.end() && it->second.GetState() == ActorState::ALIVE) {
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
    RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(
        task.GetTaskSpecification().DriverId(), type, error_message.str(),
        current_time_ms()));
    return;
  }

  RAY_LOG(INFO) << "Resubmitting task " << task.GetTaskSpecification().TaskId()
                << " on client " << gcs_client_->client_table().GetLocalClientId();
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
                 << " on " << gcs_client_->client_table().GetLocalClientId()
                 << ready_task_ids.size() << " tasks ready";
  // Transition the tasks whose dependencies are now fulfilled to the ready state.
  if (ready_task_ids.size() > 0) {
    std::unordered_set<TaskID> ready_task_id_set(ready_task_ids.begin(),
                                                 ready_task_ids.end());

    // First filter out the tasks that should not be moved to READY.
    local_queues_.FilterState(ready_task_id_set, TaskState::BLOCKED);
    local_queues_.FilterState(ready_task_id_set, TaskState::DRIVER);
    local_queues_.FilterState(ready_task_id_set, TaskState::WAITING_FOR_ACTOR_CREATION);

    // Make sure that the remaining tasks are all WAITING.
    auto ready_task_id_set_copy = ready_task_id_set;
    local_queues_.FilterState(ready_task_id_set_copy, TaskState::WAITING);
    RAY_CHECK(ready_task_id_set_copy.empty());

    // Queue and dispatch the tasks that are ready to run (i.e., WAITING).
    auto ready_tasks = local_queues_.RemoveTasks(ready_task_id_set);
    local_queues_.QueueTasks(ready_tasks, TaskState::READY);
    DispatchTasks(MakeTasksWithResources(ready_tasks));
  }
}

void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is no longer local.
  const auto waiting_task_ids = task_dependency_manager_.HandleObjectMissing(object_id);
  RAY_LOG(DEBUG) << "Object missing " << object_id << ", "
                 << " on " << gcs_client_->client_table().GetLocalClientId()
                 << waiting_task_ids.size() << " tasks waiting";
  // Transition any tasks that were in the runnable state and are dependent on
  // this object to the waiting state.
  if (!waiting_task_ids.empty()) {
    std::unordered_set<TaskID> waiting_task_id_set(waiting_task_ids.begin(),
                                                   waiting_task_ids.end());
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
  ForwardTask(task, node_manager_id, [this, task, node_manager_id](ray::Status error) {
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
          RayConfig::instance().node_manager_forward_task_retry_timeout_milliseconds());
      retry_timer->expires_from_now(retry_duration);
      retry_timer->async_wait(
          [this, task, task_id, retry_timer](const boost::system::error_code &error) {
            // Timer killing will receive the boost::asio::error::operation_aborted,
            // we only handle the timeout event.
            RAY_CHECK(!error);
            RAY_LOG(INFO) << "Resubmitting task " << task_id
                          << " because ForwardTask failed.";
            SubmitTask(task, Lineage());
          });
      // Remove the task from the lineage cache. The task will get added back
      // once it is resubmitted.
      lineage_cache_.RemoveWaitingTask(task_id);
    } else {
      // The task is not for an actor and may therefore be placed on another
      // node immediately. Send it to the scheduling policy to be placed again.
      local_queues_.QueueTasks({task}, TaskState::PLACEABLE);
      ScheduleTasks(cluster_resource_map_);
    }
  });
}

void NodeManager::ForwardTask(const Task &task, const ClientID &node_id,
                              const std::function<void(const ray::Status &)> &on_error) {
  const auto &spec = task.GetTaskSpecification();
  auto task_id = spec.TaskId();

  // Get and serialize the task's unforwarded, uncommitted lineage.
  Lineage uncommitted_lineage;
  if (lineage_cache_.ContainsTask(task_id)) {
    uncommitted_lineage = lineage_cache_.GetUncommittedLineageOrDie(task_id, node_id);
  } else {
    // TODO: We expected the lineage to be in cache, but it was evicted (#3813).
    // This is a bug but is not fatal to the application.
    RAY_DCHECK(false) << "No lineage cache entry found for task " << task_id;
    uncommitted_lineage.SetEntry(task, GcsStatus::NONE);
  }
  auto entry = uncommitted_lineage.GetEntryMutable(task_id);
  Task &lineage_cache_entry_task = entry->TaskDataMutable();

  // Increment forward count for the forwarded task.
  lineage_cache_entry_task.IncrementNumForwards();

  flatbuffers::FlatBufferBuilder fbb;
  auto request = uncommitted_lineage.ToFlatbuffer(fbb, task_id);
  fbb.Finish(request);

  RAY_LOG(DEBUG) << "Forwarding task " << task_id << " from "
                 << gcs_client_->client_table().GetLocalClientId() << " to " << node_id
                 << " spillback="
                 << lineage_cache_entry_task.GetTaskExecutionSpec().NumForwards();

  // Lookup remote server connection for this node_id and use it to send the request.
  auto it = remote_server_connections_.find(node_id);
  if (it == remote_server_connections_.end()) {
    // TODO(atumanov): caller must handle failure to ensure tasks are not lost.
    RAY_LOG(INFO) << "No NodeManager connection found for GCS client id " << node_id;
    on_error(ray::Status::IOError("NodeManager connection not found"));
    return;
  }

  auto &server_conn = it->second;
  server_conn->WriteMessageAsync(
      static_cast<int64_t>(protocol::MessageType::ForwardTaskRequest), fbb.GetSize(),
      fbb.GetBufferPointer(),
      [this, on_error, task_id, node_id, spec](ray::Status status) {
        if (status.ok()) {
          // If we were able to forward the task, remove the forwarded task from the
          // lineage cache since the receiving node is now responsible for writing
          // the task to the GCS.
          if (!lineage_cache_.RemoveWaitingTask(task_id)) {
            RAY_LOG(WARNING) << "Task " << task_id << " already removed from the lineage"
                             << " cache. This is most likely due to reconstruction.";
          } else {
            // Mark as forwarded so that the task and its lineage is not
            // re-forwarded in the future to the receiving node.
            lineage_cache_.MarkTaskAsForwarded(task_id, node_id);
          }

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
            for (int i = 0; i < spec.NumArgs(); ++i) {
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
          on_error(status);
        }
      });
}

void NodeManager::DumpDebugState() const {
  std::fstream fs;
  fs.open(temp_dir_ + "/debug_state.txt", std::fstream::out | std::fstream::trunc);
  fs << DebugString();
  fs.close();
}

std::string NodeManager::DebugString() const {
  std::stringstream result;
  uint64_t now_ms = current_time_ms();
  result << "NodeManager:";
  result << "\nInitialConfigResources: " << initial_config_.resource_config.ToString();
  result << "\nClusterResources:";
  for (auto &pair : cluster_resource_map_) {
    result << "\n" << pair.first.hex() << ": " << pair.second.DebugString();
  }
  result << "\n" << object_manager_.DebugString();
  result << "\n" << gcs_client_->DebugString();
  result << "\n" << worker_pool_.DebugString();
  result << "\n" << local_queues_.DebugString();
  result << "\n" << reconstruction_policy_.DebugString();
  result << "\n" << task_dependency_manager_.DebugString();
  result << "\n" << lineage_cache_.DebugString();
  result << "\nActorRegistry:";

  auto statistical_data = GetActorStatisticalData(actor_registry_);
  result << "\n- num live actors: " << statistical_data.live_actors;
  result << "\n- num reconstructing actors: " << statistical_data.reconstructing_actors;
  result << "\n- num dead actors: " << statistical_data.dead_actors;
  result << "\n- max num handles: " << statistical_data.max_num_handles;

  result << "\nRemoteConnections:";
  for (auto &pair : remote_server_connections_) {
    result << "\n" << pair.first.hex() << ": " << pair.second->DebugString();
  }
  result << "\nDebugString() time ms: " << (current_time_ms() - now_ms);
  return result.str();
}

void NodeManager::RecordMetrics() const {
  if (stats::StatsConfig::instance().IsStatsDisabled()) {
    return;
  }

  // Record available resources of this node.
  const auto &available_resources =
      cluster_resource_map_.at(client_id_).GetAvailableResources().GetResourceMap();
  for (const auto &pair : available_resources) {
    stats::LocalAvailableResource().Record(pair.second,
                                           {{stats::ResourceNameKey, pair.first}});
  }
  // Record total resources of this node.
  const auto &total_resources =
      cluster_resource_map_.at(client_id_).GetTotalResources().GetResourceMap();
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
