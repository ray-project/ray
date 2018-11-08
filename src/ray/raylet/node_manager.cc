#include "ray/raylet/node_manager.h"

#include "ray/common/common_protocol.h"
#include "ray/id.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace {

#define RAY_CHECK_ENUM(x, y) \
  static_assert(static_cast<int>(x) == static_cast<int>(y), "protocol mismatch")

/// A helper function to determine whether a given actor task has already been executed
/// according to the given actor registry. Returns true if the task is a duplicate.
bool CheckDuplicateActorTask(
    const std::unordered_map<ray::ActorID, ray::raylet::ActorRegistration>
        &actor_registry,
    const ray::raylet::TaskSpecification &spec) {
  auto actor_entry = actor_registry.find(spec.ActorId());
  RAY_CHECK(actor_entry != actor_registry.end());
  const auto &frontier = actor_entry->second.GetFrontier();
  int64_t expected_task_counter = 0;
  auto frontier_entry = frontier.find(spec.ActorHandleId());
  if (frontier_entry != frontier.end()) {
    expected_task_counter = frontier_entry->second.task_counter;
  }
  if (spec.ActorCounter() < expected_task_counter) {
    // The assigned task counter is less than expected. The actor has already
    // executed past this task, so do not assign the task again.
    RAY_LOG(WARNING) << "A task was resubmitted, so we are ignoring it. This "
                     << "should only happen during reconstruction.";
    return true;
  }
  RAY_CHECK(spec.ActorCounter() == expected_task_counter)
      << "Expected actor counter: " << expected_task_counter
      << ", got: " << spec.ActorCounter();
  return false;
};

}  // namespace

namespace ray {

namespace raylet {

NodeManager::NodeManager(boost::asio::io_service &io_service,
                         const NodeManagerConfig &config, ObjectManager &object_manager,
                         std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    : io_service_(io_service),
      object_manager_(object_manager),
      gcs_client_(gcs_client),
      heartbeat_timer_(io_service),
      heartbeat_period_(std::chrono::milliseconds(config.heartbeat_period_ms)),
      object_manager_profile_timer_(io_service),
      local_resources_(config.resource_config),
      local_available_resources_(config.resource_config),
      worker_pool_(config.num_initial_workers, config.num_workers_per_process,
                   config.maximum_startup_concurrency, config.worker_commands),
      local_queues_(SchedulingQueue()),
      scheduling_policy_(local_queues_),
      reconstruction_policy_(
          io_service_,
          [this](const TaskID &task_id) { HandleTaskReconstruction(task_id); },
          RayConfig::instance().initial_reconstruction_timeout_milliseconds(),
          gcs_client_->client_table().GetLocalClientId(), gcs_client->task_lease_table(),
          std::make_shared<ObjectDirectory>(io_service, gcs_client),
          gcs_client_->task_reconstruction_log()),
      task_dependency_manager_(
          object_manager, reconstruction_policy_, io_service,
          gcs_client_->client_table().GetLocalClientId(),
          RayConfig::instance().initial_reconstruction_timeout_milliseconds(),
          gcs_client->task_lease_table()),
      lineage_cache_(gcs_client_->client_table().GetLocalClientId(),
                     gcs_client->raylet_task_table(), gcs_client->raylet_task_table(),
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

  ARROW_CHECK_OK(store_client_.Connect(config.store_socket_name.c_str(), "", 0));
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
      JobID::nil(), gcs_client_->client_table().GetLocalClientId(),
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
      JobID::nil(), gcs_client_->client_table().GetLocalClientId(),
      task_lease_notification_callback, task_lease_empty_callback, nullptr));

  // Register a callback for actor creation notifications.
  auto actor_creation_callback = [this](
      gcs::AsyncGcsClient *client, const ActorID &actor_id,
      const std::vector<ActorTableDataT> &data) { HandleActorCreation(actor_id, data); };

  RAY_RETURN_NOT_OK(gcs_client_->actor_table().Subscribe(
      UniqueID::nil(), UniqueID::nil(), actor_creation_callback, nullptr));

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

  // Subscribe to node manager heartbeats.
  const auto heartbeat_added = [this](gcs::AsyncGcsClient *client, const ClientID &id,
                                      const HeartbeatTableDataT &heartbeat_data) {
    HeartbeatAdded(client, id, heartbeat_data);
  };
  RAY_RETURN_NOT_OK(gcs_client_->heartbeat_table().Subscribe(
      UniqueID::nil(), UniqueID::nil(), heartbeat_added, nullptr,
      [](gcs::AsyncGcsClient *client) {
        RAY_LOG(DEBUG) << "heartbeat table subscription done callback called.";
      }));

  // Subscribe to driver table updates.
  const auto driver_table_handler = [this](
      gcs::AsyncGcsClient *client, const ClientID &client_id,
      const std::vector<DriverTableDataT> &driver_data) {
    HandleDriverTableUpdate(client_id, driver_data);
  };
  RAY_RETURN_NOT_OK(gcs_client_->driver_table().Subscribe(JobID::nil(), UniqueID::nil(),
                                                          driver_table_handler, nullptr));

  // Start sending heartbeats to the GCS.
  last_heartbeat_at_ms_ = current_time_ms();
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
    const ClientID &id, const std::vector<DriverTableDataT> &driver_data) {
  for (const auto &entry : driver_data) {
    RAY_LOG(DEBUG) << "HandleDriverTableUpdate " << UniqueID::from_binary(entry.driver_id)
                   << " " << entry.is_dead;
    if (entry.is_dead) {
      auto driver_id = UniqueID::from_binary(entry.driver_id);
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

  RAY_LOG(DEBUG) << "[Heartbeat] sending heartbeat.";
  auto &heartbeat_table = gcs_client_->heartbeat_table();
  auto heartbeat_data = std::make_shared<HeartbeatTableDataT>();
  const auto &my_client_id = gcs_client_->client_table().GetLocalClientId();
  SchedulingResources &local_resources = cluster_resource_map_[my_client_id];
  heartbeat_data->client_id = my_client_id.binary();
  // TODO(atumanov): modify the heartbeat table protocol to use the ResourceSet directly.
  // TODO(atumanov): implement a ResourceSet const_iterator.
  RAY_LOG(DEBUG) << "[Heartbeat] resources available: "
                 << local_resources.GetAvailableResources().ToString();
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
      UniqueID::nil(), gcs_client_->client_table().GetLocalClientId(), heartbeat_data,
      [](ray::gcs::AsyncGcsClient *client, const ClientID &id,
         const HeartbeatTableDataT &data) {
        RAY_LOG(DEBUG) << "[HEARTBEAT] heartbeat sent callback";
      });

  if (!status.ok()) {
    RAY_LOG(INFO) << "heartbeat failed: string " << status.ToString() << status.message();
    RAY_LOG(INFO) << "is redis error: " << status.IsRedisError();
  }
  RAY_CHECK_OK(status);

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

  RAY_LOG(DEBUG) << "[ClientAdded] received callback from client id " << client_id;
  if (client_id == gcs_client_->client_table().GetLocalClientId()) {
    // We got a notification for ourselves, so we are connected to the GCS now.
    // Save this NodeManager's resource information in the cluster resource map.
    cluster_resource_map_[client_id] = local_resources_;
    return;
  }

  // TODO(atumanov): make remote client lookup O(1)
  if (std::find(remote_clients_.begin(), remote_clients_.end(), client_id) ==
      remote_clients_.end()) {
    RAY_LOG(DEBUG) << "a new client: " << client_id;
    remote_clients_.push_back(client_id);
  } else {
    // NodeManager connection to this client was already established.
    RAY_LOG(DEBUG) << "received a new client connection that already exists: "
                   << client_id;
    return;
  }

  // Establish a new NodeManager connection to this GCS client.
  RAY_LOG(DEBUG) << "[ClientAdded] Trying to connect to client " << client_id << " at "
                 << client_data.node_manager_address << ":"
                 << client_data.node_manager_port;

  boost::asio::ip::tcp::socket socket(io_service_);
  auto status =
      TcpConnect(socket, client_data.node_manager_address, client_data.node_manager_port);
  // A disconnected client has 2 entries in the client table (one for being
  // inserted and one for being removed). When a new raylet starts, ClientAdded
  // will be called with the disconnected client's first entry, which will cause
  // IOError and "Connection refused".
  if (!status.ok()) {
    RAY_LOG(WARNING) << "Failed to connect to client " << client_id
                     << " in ClientAdded. TcpConnect returned status: "
                     << status.ToString() << ". This may be caused by "
                     << "trying to connect to a node manager that has failed.";
    return;
  }

  // The client is connected.
  auto server_conn = TcpServerConnection::Create(std::move(socket));
  remote_server_connections_.emplace(client_id, std::move(server_conn));

  ResourceSet resources_total(client_data.resources_total_label,
                              client_data.resources_total_capacity);
  cluster_resource_map_.emplace(client_id, SchedulingResources(resources_total));
}

void NodeManager::ClientRemoved(const ClientTableDataT &client_data) {
  // TODO(swang): If we receive a notification for our own death, clean up and
  // exit immediately.
  const ClientID client_id = ClientID::from_binary(client_data.client_id);
  RAY_LOG(DEBUG) << "[ClientRemoved] received callback from client id " << client_id;

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
  remote_server_connections_.erase(client_id);
}

void NodeManager::HeartbeatAdded(gcs::AsyncGcsClient *client, const ClientID &client_id,
                                 const HeartbeatTableDataT &heartbeat_data) {
  RAY_LOG(DEBUG) << "[HeartbeatAdded]: received heartbeat from client id " << client_id;
  const ClientID &local_client_id = gcs_client_->client_table().GetLocalClientId();
  if (client_id == local_client_id) {
    // Skip heartbeats from self.
    return;
  }
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
  RAY_LOG(DEBUG) << "[HeartbeatAdded]: received load: " << remote_load.ToString();
  remote_resources.SetAvailableResources(std::move(remote_available));
  // Extract the load information and save it locally.
  remote_resources.SetLoadResources(std::move(remote_load));

  auto decision = scheduling_policy_.SpillOver(remote_resources);
  // Extract decision for this local scheduler.
  std::unordered_set<TaskID> local_task_ids;
  for (const auto &task_id : decision) {
    // (See design_docs/task_states.rst for the state transition diagram.)
    const auto task = local_queues_.RemoveTask(task_id);
    // Since we are spilling back from the ready and waiting queues, we need
    // to unsubscribe the dependencies.
    task_dependency_manager_.UnsubscribeDependencies(task_id);
    // Attempt to forward the task. If this fails to forward the task,
    // the task will be resubmit locally.
    ForwardTaskOrResubmit(task, client_id);
  }
}

void NodeManager::HandleActorCreation(const ActorID &actor_id,
                                      const std::vector<ActorTableDataT> &data) {
  RAY_LOG(DEBUG) << "Actor creation notification received: " << actor_id;

  // TODO(swang): In presence of failures, data may have size > 1, since the
  // actor will have been created multiple times. In that case, we should
  // only consider the last entry as valid. All previous entries should have
  // a dead node_manager_id.
  RAY_CHECK(data.size() == 1);

  // Register the new actor.
  ActorRegistration actor_registration(data.back());
  ClientID received_node_manager_id = actor_registration.GetNodeManagerId();
  // Extend the frontier to include the actor creation task. NOTE(swang): The
  // creator of the actor is always assigned nil as the actor handle ID.
  actor_registration.ExtendFrontier(ActorHandleID::nil(),
                                    actor_registration.GetActorCreationDependency());
  auto inserted = actor_registry_.emplace(actor_id, std::move(actor_registration));
  if (!inserted.second) {
    // If we weren't able to insert the actor's location, check that the
    // existing entry is the same as the new one.
    // TODO(swang): This is not true in the case of failures.
    RAY_CHECK(received_node_manager_id == inserted.first->second.GetNodeManagerId())
        << "Actor scheduled on " << inserted.first->second.GetNodeManagerId()
        << ", but received notification for " << received_node_manager_id;
  } else {
    // The actor's location is now known. Dequeue any methods that were
    // submitted before the actor's location was known.
    // (See design_docs/task_states.rst for the state transition diagram.)
    const auto &methods = local_queues_.GetMethodsWaitingForActorCreation();
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
      // The task's uncommitted lineage was already added to the local lineage
      // cache upon the initial submission, so it's okay to resubmit it with an
      // empty lineage this time.
      SubmitTask(method, Lineage());
    }
  }
}

void NodeManager::CleanUpTasksForDeadActor(const ActorID &actor_id) {
  auto tasks_to_remove = local_queues_.GetTaskIdsForActor(actor_id);
  auto removed_tasks = local_queues_.RemoveTasks(tasks_to_remove);

  for (auto const &task : removed_tasks) {
    const TaskSpecification &spec = task.GetTaskSpecification();
    TreatTaskAsFailed(spec);
    task_dependency_manager_.TaskCanceled(spec.TaskId());
  }
}

void NodeManager::CleanUpTasksForDeadDriver(const DriverID &driver_id) {
  auto tasks_to_remove = local_queues_.GetTaskIdsForDriver(driver_id);
  local_queues_.RemoveTasks(tasks_to_remove);

  task_dependency_manager_.RemoveTasksAndRelatedObjects(tasks_to_remove);
}

void NodeManager::ProcessNewClient(LocalClientConnection &client) {
  // The new client is a worker, so begin listening for messages.
  client.ProcessMessages();
}

void NodeManager::DispatchTasks() {
  // Work with a copy of scheduled tasks.
  // (See design_docs/task_states.rst for the state transition diagram.)
  auto ready_tasks = local_queues_.GetReadyTasks();
  // Return if there are no tasks to schedule.
  if (ready_tasks.empty()) {
    return;
  }

  for (const auto &task : ready_tasks) {
    const auto &task_resources = task.GetTaskSpecification().GetRequiredResources();
    if (!local_available_resources_.Contains(task_resources)) {
      // Not enough local resources for this task right now, skip this task.
      // TODO(rkn): We should always skip node managers that have 0 CPUs.
      continue;
    }
    // We have enough resources for this task. Assign task.
    // TODO(atumanov): perform the task state/queue transition inside AssignTask.
    // (See design_docs/task_states.rst for the state transition diagram.)
    auto dispatched_task = local_queues_.RemoveTask(task.GetTaskSpecification().TaskId());
    AssignTask(dispatched_task);
  }
}

void NodeManager::ProcessClientMessage(
    const std::shared_ptr<LocalClientConnection> &client, int64_t message_type,
    const uint8_t *message_data) {
  RAY_LOG(DEBUG) << "Message of type " << message_type;

  auto registered_worker = worker_pool_.GetRegisteredWorker(client);
  auto message_type_value = static_cast<protocol::MessageType>(message_type);
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
    ProcessDisconnectClientMessage(client, /* push_warning = */ false);
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
    HandleTaskUnblocked(client, from_flatbuf(*message->task_id()));
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
    std::vector<ObjectID> object_ids = from_flatbuf(*message->object_ids());
    object_manager_.FreeObjects(object_ids, message->local_only());
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
  client->SetClientID(from_flatbuf(*message->client_id()));
  auto worker =
      std::make_shared<Worker>(message->worker_pid(), message->language(), client);
  if (message->is_worker()) {
    // Register the new worker.
    worker_pool_.RegisterWorker(std::move(worker));
    DispatchTasks();
  } else {
    // Register the new driver. Note that here the driver_id in RegisterClientRequest
    // message is actually the ID of the driver task, while client_id represents the
    // real driver ID, which can associate all the tasks/actors for a given driver,
    // which is set to the worker ID.
    const JobID driver_task_id = from_flatbuf(*message->driver_id());
    worker->AssignTaskId(driver_task_id);
    worker->AssignDriverId(from_flatbuf(*message->client_id()));
    worker_pool_.RegisterDriver(std::move(worker));
    local_queues_.AddDriverTaskId(driver_task_id);
  }
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
  DispatchTasks();
}

void NodeManager::ProcessDisconnectClientMessage(
    const std::shared_ptr<LocalClientConnection> &client, bool push_warning) {
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
    while (!worker->GetBlockedTaskIds().empty()) {
      // NOTE(swang): HandleTaskUnblocked will modify the worker, so it is
      // not safe to pass in the iterator directly.
      const TaskID task_id = *worker->GetBlockedTaskIds().begin();
      HandleTaskUnblocked(client, task_id);
    }
  }

  // Remove the dead client from the pool and stop listening for messages.
  if (is_worker) {
    // The client is a worker. Handle the case where the worker is killed
    // while executing a task. Clean up the assigned task's resources, push
    // an error to the driver.
    // (See design_docs/task_states.rst for the state transition diagram.)
    const TaskID &task_id = worker->GetAssignedTaskId();
    if (!task_id.is_nil() && !worker->IsDead()) {
      // If the worker was killed intentionally, e.g., when the driver that created
      // the task that this worker is currently executing exits, the task for this
      // worker has already been removed from queue, so the following are skipped.
      task_dependency_manager_.TaskCanceled(task_id);
      const Task &task = local_queues_.RemoveTask(task_id);
      const TaskSpecification &spec = task.GetTaskSpecification();
      // Handle the task failure in order to raise an exception in the
      // application.
      TreatTaskAsFailed(spec);

      const JobID &job_id = worker->GetAssignedDriverId();

      if (push_warning) {
        // TODO(rkn): Define this constant somewhere else.
        std::string type = "worker_died";
        std::ostringstream error_message;
        error_message << "A worker died or was killed while executing task " << task_id
                      << ".";
        RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(
            job_id, type, error_message.str(), current_time_ms()));
      }
    }

    worker_pool_.DisconnectWorker(worker);

    // If the worker was an actor, add it to the list of dead actors.
    const ActorID &actor_id = worker->GetActorId();
    if (!actor_id.is_nil()) {
      // TODO(rkn): Consider broadcasting a message to all of the other
      // node managers so that they can mark the actor as dead.
      RAY_LOG(DEBUG) << "The actor with ID " << actor_id << " died.";
      auto actor_entry = actor_registry_.find(actor_id);
      RAY_CHECK(actor_entry != actor_registry_.end());
      actor_entry->second.MarkDead();
      // For dead actors, if there are remaining tasks for this actor, we
      // should handle them.
      CleanUpTasksForDeadActor(actor_id);
    }

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
    DispatchTasks();
  } else if (is_driver) {
    // The client is a driver.
    RAY_CHECK_OK(gcs_client_->driver_table().AppendDriverData(client->GetClientID(),
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
      from_flatbuf(*message->execution_dependencies()));
  TaskSpecification task_spec(*message->task_spec());
  Task task(task_execution_spec, task_spec);
  // Submit the task to the local scheduler. Since the task was submitted
  // locally, there is no uncommitted lineage.
  SubmitTask(task, Lineage());
}

void NodeManager::ProcessFetchOrReconstructMessage(
    const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data) {
  auto message = flatbuffers::GetRoot<protocol::FetchOrReconstruct>(message_data);
  std::vector<ObjectID> required_object_ids;
  for (size_t i = 0; i < message->object_ids()->size(); ++i) {
    ObjectID object_id = from_flatbuf(*message->object_ids()->Get(i));
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
    const TaskID task_id = from_flatbuf(*message->task_id());
    HandleTaskBlocked(client, required_object_ids, task_id);
  }
}

void NodeManager::ProcessWaitRequestMessage(
    const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data) {
  // Read the data.
  auto message = flatbuffers::GetRoot<protocol::WaitRequest>(message_data);
  std::vector<ObjectID> object_ids = from_flatbuf(*message->object_ids());
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

  const TaskID &current_task_id = from_flatbuf(*message->task_id());
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

  JobID job_id = from_flatbuf(*message->job_id());
  auto const &type = string_from_flatbuf(*message->type());
  auto const &error_message = string_from_flatbuf(*message->error_message());
  double timestamp = message->timestamp();

  RAY_CHECK_OK(gcs_client_->error_table().PushErrorToDriver(job_id, type, error_message,
                                                            timestamp));
}

void NodeManager::ProcessNewNodeManager(TcpClientConnection &node_manager_client) {
  node_manager_client.ProcessMessages();
}

void NodeManager::ProcessNodeManagerMessage(TcpClientConnection &node_manager_client,
                                            int64_t message_type,
                                            const uint8_t *message_data) {
  switch (static_cast<protocol::MessageType>(message_type)) {
  case protocol::MessageType::ForwardTaskRequest: {
    auto message = flatbuffers::GetRoot<protocol::ForwardTaskRequest>(message_data);
    TaskID task_id = from_flatbuf(*message->task_id());

    Lineage uncommitted_lineage(*message);
    const Task &task = uncommitted_lineage.GetEntry(task_id)->TaskData();
    RAY_LOG(DEBUG) << "got task " << task.GetTaskSpecification().TaskId()
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

  // Extract decision for this local scheduler.
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
  for (const auto &task : local_queues_.GetPlaceableTasks()) {
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
  RAY_CHECK(local_queues_.GetPlaceableTasks().size() == 0);
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

void NodeManager::TreatTaskAsFailed(const TaskSpecification &spec) {
  RAY_LOG(DEBUG) << "Treating task " << spec.TaskId() << " as failed.";
  // Loop over the return IDs (except the dummy ID) and store a fake object in
  // the object store.
  int64_t num_returns = spec.NumReturns();
  if (spec.IsActorTask()) {
    // TODO(rkn): We subtract 1 to avoid the dummy ID. However, this leaks
    // information about the TaskSpecification implementation.
    num_returns -= 1;
  }
  for (int64_t i = 0; i < num_returns; i++) {
    const ObjectID object_id = spec.ReturnId(i);

    std::shared_ptr<Buffer> data;
    // TODO(ekl): this writes an invalid arrow object, which is sufficient to
    // signal that the worker failed, but it would be nice to return more
    // detailed failure metadata in the future.
    arrow::Status status =
        store_client_.Create(object_id.to_plasma_id(), 1, NULL, 0, &data);
    if (!status.IsPlasmaObjectExists()) {
      // TODO(rkn): We probably don't want this checks. E.g., if the object
      // store is full, we don't want to kill the raylet.
      ARROW_CHECK_OK(status);
      ARROW_CHECK_OK(store_client_.Seal(object_id.to_plasma_id()));
    }
  }
}

void NodeManager::SubmitTask(const Task &task, const Lineage &uncommitted_lineage,
                             bool forwarded) {
  const TaskID &task_id = task.GetTaskSpecification().TaskId();
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

  const TaskSpecification &spec = task.GetTaskSpecification();
  if (spec.IsActorTask()) {
    // Check whether we know the location of the actor.
    const auto actor_entry = actor_registry_.find(spec.ActorId());
    if (actor_entry != actor_registry_.end()) {
      // We have a known location for the actor.
      auto node_manager_id = actor_entry->second.GetNodeManagerId();
      if (node_manager_id == gcs_client_->client_table().GetLocalClientId()) {
        // The actor is local. Check if the actor is still alive.
        if (!actor_entry->second.IsAlive()) {
          // Handle the fact that this actor is dead.
          TreatTaskAsFailed(spec);
        } else {
          // Queue the task for local execution, bypassing placement.
          EnqueuePlaceableTask(task);
        }
      } else {
        // The actor is remote. Forward the task to the node manager that owns
        // the actor.
        if (gcs_client_->client_table().IsRemoved(node_manager_id)) {
          // The remote node manager is dead, so handle the fact that this actor
          // is also dead.
          TreatTaskAsFailed(spec);
        } else {
          // Attempt to forward the task. If this fails to forward the task,
          // the task will be resubmit locally.
          ForwardTaskOrResubmit(task, node_manager_id);
        }
      }
    } else {
      // We do not have a registered location for the object, so either the
      // actor has not yet been created or we missed the notification for the
      // actor creation because this node joined the cluster after the actor
      // was already created. Look up the actor's registered location in case
      // we missed the creation notification.
      // NOTE(swang): This codepath needs to be tested in a cluster setting.
      auto lookup_callback = [this](gcs::AsyncGcsClient *client, const ActorID &actor_id,
                                    const std::vector<ActorTableDataT> &data) {
        if (!data.empty()) {
          // The actor has been created.
          HandleActorCreation(actor_id, data);
        } else {
          // The actor has not yet been created.
          // TODO(swang): Set a timer for reconstructing the actor creation
          // task.
        }
      };
      RAY_CHECK_OK(gcs_client_->actor_table().Lookup(JobID::nil(), spec.ActorId(),
                                                     lookup_callback));
      // Keep the task queued until we discover the actor's location.
      // (See design_docs/task_states.rst for the state transition diagram.)
      local_queues_.QueueMethodsWaitingForActorCreation({task});
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
      local_queues_.QueuePlaceableTasks({task});
      ScheduleTasks(cluster_resource_map_);
      DispatchTasks();
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
      local_queues_.QueueRunningTasks({task});
      // Get the CPU resources required by the running task.
      const auto required_resources = task.GetTaskSpecification().GetRequiredResources();
      double required_cpus = required_resources.GetNumCpus();
      const std::unordered_map<std::string, double> cpu_resources = {
          {kCPU_ResourceLabel, required_cpus}};

      // Release the CPU resources.
      auto const cpu_resource_ids = worker->ReleaseTaskCpuResources();
      local_available_resources_.Release(cpu_resource_ids);
      RAY_CHECK(
          cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Release(
              ResourceSet(cpu_resources)));
      worker->MarkBlocked();

      // Try dispatching tasks since we may have released some resources.
      DispatchTasks();
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
      local_queues_.QueueRunningTasks({task});
      // Get the CPU resources required by the running task.
      const auto required_resources = task.GetTaskSpecification().GetRequiredResources();
      double required_cpus = required_resources.GetNumCpus();
      const ResourceSet cpu_resources(
          std::unordered_map<std::string, double>({{kCPU_ResourceLabel, required_cpus}}));

      // Check if we can reacquire the CPU resources.
      bool oversubscribed = !local_available_resources_.Contains(cpu_resources);

      if (!oversubscribed) {
        // Reacquire the CPU resources for the worker. Note that care needs to be
        // taken if the user is using the specific CPU IDs since the IDs that we
        // reacquire here may be different from the ones that the task started with.
        auto const resource_ids = local_available_resources_.Acquire(cpu_resources);
        worker->AcquireTaskCpuResources(resource_ids);
        RAY_CHECK(
            cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Acquire(
                cpu_resources));
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
  task_dependency_manager_.UnsubscribeDependencies(current_task_id);
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
    local_queues_.QueueReadyTasks({task});
    // Try to dispatch the newly ready task.
    DispatchTasks();
  } else {
    local_queues_.QueueWaitingTasks({task});
  }
  // Mark the task as pending. Once the task has finished execution, or once it
  // has been forwarded to another node, the task must be marked as canceled in
  // the TaskDependencyManager.
  task_dependency_manager_.TaskPending(task);
}

void NodeManager::AssignTask(Task &task) {
  const TaskSpecification &spec = task.GetTaskSpecification();

  // If this is an actor task, check that the new task has the correct counter.
  if (spec.IsActorTask()) {
    if (CheckDuplicateActorTask(actor_registry_, spec)) {
      // Drop tasks that have already been executed.
      return;
    }
  }

  // Try to get an idle worker that can execute this task.
  std::shared_ptr<Worker> worker = worker_pool_.PopWorker(spec);
  if (worker == nullptr) {
    // There are no workers that can execute this task.
    if (!spec.IsActorTask()) {
      // There are no more non-actor workers available to execute this task.
      // Start a new worker.
      worker_pool_.StartWorkerProcess(spec.GetLanguage());
    }
    // Queue this task for future assignment. The task will be assigned to a
    // worker once one becomes available.
    // (See design_docs/task_states.rst for the state transition diagram.)
    local_queues_.QueueReadyTasks(std::vector<Task>({task}));
    return;
  }

  RAY_LOG(DEBUG) << "Assigning task to worker with pid " << worker->Pid();
  flatbuffers::FlatBufferBuilder fbb;

  // Resource accounting: acquire resources for the assigned task.
  auto acquired_resources =
      local_available_resources_.Acquire(spec.GetRequiredResources());
  const auto &my_client_id = gcs_client_->client_table().GetLocalClientId();
  RAY_CHECK(cluster_resource_map_[my_client_id].Acquire(spec.GetRequiredResources()));

  if (spec.IsActorCreationTask()) {
    // Check that we are not placing an actor creation task on a node with 0 CPUs.
    RAY_CHECK(cluster_resource_map_[my_client_id].GetTotalResources().GetNumCpus() != 0);
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
  worker->Connection()->WriteMessageAsync(
      static_cast<int64_t>(protocol::MessageType::ExecuteTask), fbb.GetSize(),
      fbb.GetBufferPointer(), [this, worker, task](ray::Status status) mutable {
        if (status.ok()) {
          auto spec = task.GetTaskSpecification();
          // We successfully assigned the task to the worker.
          worker->AssignTaskId(spec.TaskId());
          worker->AssignDriverId(spec.DriverId());
          // If the task was an actor task, then record this execution to guarantee
          // consistency in the case of reconstruction.
          if (spec.IsActorTask()) {
            auto actor_entry = actor_registry_.find(spec.ActorId());
            RAY_CHECK(actor_entry != actor_registry_.end());
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
            task.SetExecutionDependencies({execution_dependency});
            // Extend the frontier to include the executing task.
            actor_entry->second.ExtendFrontier(spec.ActorHandleId(),
                                               spec.ActorDummyObject());
          }
          // We started running the task, so the task is ready to write to GCS.
          if (!lineage_cache_.AddReadyTask(task)) {
            RAY_LOG(WARNING) << "Task " << spec.TaskId() << " already in lineage cache. "
                                                            "This is most likely due to "
                                                            "reconstruction.";
          }
          // Mark the task as running.
          // (See design_docs/task_states.rst for the state transition diagram.)
          local_queues_.QueueRunningTasks(std::vector<Task>({task}));
          // Notify the task dependency manager that we no longer need this task's
          // object dependencies.
          task_dependency_manager_.UnsubscribeDependencies(spec.TaskId());
        } else {
          RAY_LOG(WARNING) << "Failed to send task to worker, disconnecting client";
          // We failed to send the task to the worker, so disconnect the worker.
          ProcessDisconnectClientMessage(worker->Connection());
          // Queue this task for future assignment. The task will be assigned to a
          // worker once one becomes available.
          // (See design_docs/task_states.rst for the state transition diagram.)
          local_queues_.QueueReadyTasks(std::vector<Task>({task}));
          DispatchTasks();
        }
      });
}

void NodeManager::FinishAssignedTask(Worker &worker) {
  TaskID task_id = worker.GetAssignedTaskId();
  RAY_LOG(DEBUG) << "Finished task " << task_id;

  // (See design_docs/task_states.rst for the state transition diagram.)
  const auto task = local_queues_.RemoveTask(task_id);

  if (task.GetTaskSpecification().IsActorCreationTask()) {
    // If this was an actor creation task, then convert the worker to an actor.
    auto actor_id = task.GetTaskSpecification().ActorCreationId();
    worker.AssignActorId(actor_id);
    const auto driver_id = task.GetTaskSpecification().DriverId();

    // Publish the actor creation event to all other nodes so that methods for
    // the actor will be forwarded directly to this node.
    auto actor_notification = std::make_shared<ActorTableDataT>();
    actor_notification->actor_id = actor_id.binary();
    actor_notification->actor_creation_dummy_object_id =
        task.GetTaskSpecification().ActorDummyObject().binary();
    actor_notification->driver_id = driver_id.binary();
    actor_notification->node_manager_id =
        gcs_client_->client_table().GetLocalClientId().binary();

    RAY_LOG(DEBUG) << "Publishing actor creation: " << actor_id
                   << " driver_id: " << driver_id;
    RAY_CHECK_OK(gcs_client_->actor_table().Append(JobID::nil(), actor_id,
                                                   actor_notification, nullptr));

    // Resources required by an actor creation task are acquired for the
    // lifetime of the actor, so we do not release any resources here.
  } else {
    // Release task's resources.
    local_available_resources_.Release(worker.GetTaskResourceIds());
    worker.ResetTaskResourceIds();

    RAY_CHECK(
        cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Release(
            task.GetTaskSpecification().GetRequiredResources()));
  }

  // If the finished task was an actor task, mark the returned dummy object as
  // locally available. This is not added to the object table, so the update
  // will be invisible to both the local object manager and the other nodes.
  // NOTE(swang): These objects are never cleaned up. We should consider
  // removing the objects, e.g., when an actor is terminated.
  if (task.GetTaskSpecification().IsActorCreationTask() ||
      task.GetTaskSpecification().IsActorTask()) {
    auto dummy_object = task.GetTaskSpecification().ActorDummyObject();
    HandleObjectLocal(dummy_object);
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

void NodeManager::HandleTaskReconstruction(const TaskID &task_id) {
  RAY_LOG(INFO) << "Reconstructing task " << task_id << " on client "
                << gcs_client_->client_table().GetLocalClientId();
  // Retrieve the task spec in order to re-execute the task.
  RAY_CHECK_OK(gcs_client_->raylet_task_table().Lookup(
      JobID::nil(), task_id,
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
        RAY_CHECK(lineage_cache_.ContainsTask(task_id));
        // Use a copy of the cached task spec to re-execute the task.
        const Task task = lineage_cache_.GetTask(task_id);
        ResubmitTask(task);

      }));
}

void NodeManager::ResubmitTask(const Task &task) {
  // Actor reconstruction is turned off by default right now. If this is an
  // actor task, treat the task as failed and do not resubmit it.
  if (task.GetTaskSpecification().IsActorTask()) {
    TreatTaskAsFailed(task.GetTaskSpecification());
    return;
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

  // The task may be reconstructed. Submit it with an empty lineage, since any
  // uncommitted lineage must already be in the lineage cache. At this point,
  // the task should not yet exist in the local scheduling queue. If it does,
  // then this is a spurious reconstruction.
  SubmitTask(task, Lineage());
}

void NodeManager::HandleObjectLocal(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is local.
  const auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(object_id);
  // Transition the tasks whose dependencies are now fulfilled to the ready state.
  if (ready_task_ids.size() > 0) {
    std::unordered_set<TaskID> ready_task_id_set(ready_task_ids.begin(),
                                                 ready_task_ids.end());
    // Transition tasks from waiting to scheduled.
    // (See design_docs/task_states.rst for the state transition diagram.)
    local_queues_.MoveTasks(ready_task_id_set, TaskState::WAITING, TaskState::READY);
    // New ready tasks appeared in the queue, try to dispatch them.
    DispatchTasks();

    // Check that remaining tasks that could not be transitioned are blocked
    // workers or drivers.
    local_queues_.FilterState(ready_task_id_set, TaskState::BLOCKED);
    local_queues_.FilterState(ready_task_id_set, TaskState::DRIVER);
    RAY_CHECK(ready_task_id_set.empty());
  }
}

void NodeManager::HandleObjectMissing(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is no longer local.
  const auto waiting_task_ids = task_dependency_manager_.HandleObjectMissing(object_id);
  // Transition any tasks that were in the runnable state and are dependent on
  // this object to the waiting state.
  if (!waiting_task_ids.empty()) {
    // Transition the tasks back to the waiting state. They will be made
    // runnable once the deleted object becomes available again.
    std::unordered_set<TaskID> waiting_task_id_set(waiting_task_ids.begin(),
                                                   waiting_task_ids.end());
    local_queues_.MoveTasks(waiting_task_id_set, TaskState::READY, TaskState::WAITING);

    // Check that remaining tasks that could not be transitioned are running
    // workers or drivers, now blocked in a get.
    local_queues_.FilterState(waiting_task_id_set, TaskState::RUNNING);
    local_queues_.FilterState(waiting_task_id_set, TaskState::DRIVER);
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
            RAY_LOG(DEBUG) << "Resubmitting task " << task_id
                           << " because ForwardTask failed.";
            SubmitTask(task, Lineage());
          });
      // Remove the task from the lineage cache. The task will get added back
      // once it is resubmitted.
      lineage_cache_.RemoveWaitingTask(task_id);
    } else {
      // The task is not for an actor and may therefore be placed on another
      // node immediately. Send it to the scheduling policy to be placed again.
      local_queues_.QueuePlaceableTasks({task});
      ScheduleTasks(cluster_resource_map_);
      DispatchTasks();
    }
  });
}

void NodeManager::ForwardTask(const Task &task, const ClientID &node_id,
                              const std::function<void(const ray::Status &)> &on_error) {
  const auto &spec = task.GetTaskSpecification();
  auto task_id = spec.TaskId();

  // Get and serialize the task's unforwarded, uncommitted lineage.
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id, node_id);
  Task &lineage_cache_entry_task =
      uncommitted_lineage.GetEntryMutable(task_id)->TaskDataMutable();

  // Increment forward count for the forwarded task.
  lineage_cache_entry_task.IncrementNumForwards();

  flatbuffers::FlatBufferBuilder fbb;
  auto request = uncommitted_lineage.ToFlatbuffer(fbb, task_id);
  fbb.Finish(request);

  RAY_LOG(DEBUG) << "Forwarding task " << task_id << " to " << node_id << " spillback="
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
            RAY_LOG(WARNING) << "Task " << task_id << " already removed from the lineage "
                                                      "cache. This is most likely due to "
                                                      "reconstruction.";
          }
          // Mark as forwarded so that the task and its lineage is not re-forwarded
          // in the future to the receiving node.
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

}  // namespace raylet

}  // namespace ray
