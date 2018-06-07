#include "ray/raylet/node_manager.h"

#include "common_protocol.h"
#include "local_scheduler/format/local_scheduler_generated.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace {

namespace local_scheduler_protocol = ray::local_scheduler::protocol;

#define RAY_CHECK_ENUM(x, y) \
  static_assert(static_cast<int>(x) == static_cast<int>(y), "protocol mismatch")

// Check consistency between client and server protocol.
RAY_CHECK_ENUM(protocol::MessageType::SubmitTask,
               local_scheduler_protocol::MessageType::SubmitTask);
RAY_CHECK_ENUM(protocol::MessageType::TaskDone,
               local_scheduler_protocol::MessageType::TaskDone);
RAY_CHECK_ENUM(protocol::MessageType::EventLogMessage,
               local_scheduler_protocol::MessageType::EventLogMessage);
RAY_CHECK_ENUM(protocol::MessageType::RegisterClientRequest,
               local_scheduler_protocol::MessageType::RegisterClientRequest);
RAY_CHECK_ENUM(protocol::MessageType::RegisterClientReply,
               local_scheduler_protocol::MessageType::RegisterClientReply);
RAY_CHECK_ENUM(protocol::MessageType::DisconnectClient,
               local_scheduler_protocol::MessageType::DisconnectClient);
RAY_CHECK_ENUM(protocol::MessageType::GetTask,
               local_scheduler_protocol::MessageType::GetTask);
RAY_CHECK_ENUM(protocol::MessageType::ExecuteTask,
               local_scheduler_protocol::MessageType::ExecuteTask);
RAY_CHECK_ENUM(protocol::MessageType::ReconstructObject,
               local_scheduler_protocol::MessageType::ReconstructObject);
RAY_CHECK_ENUM(protocol::MessageType::NotifyUnblocked,
               local_scheduler_protocol::MessageType::NotifyUnblocked);
RAY_CHECK_ENUM(protocol::MessageType::PutObject,
               local_scheduler_protocol::MessageType::PutObject);
RAY_CHECK_ENUM(protocol::MessageType::GetActorFrontierRequest,
               local_scheduler_protocol::MessageType::GetActorFrontierRequest);
RAY_CHECK_ENUM(protocol::MessageType::GetActorFrontierReply,
               local_scheduler_protocol::MessageType::GetActorFrontierReply);
RAY_CHECK_ENUM(protocol::MessageType::SetActorFrontier,
               local_scheduler_protocol::MessageType::SetActorFrontier);

/// A helper function to determine whether a given actor task has already been executed
/// according to the given actor registry. Returns true if the task is a duplicate.
bool CheckDuplicateActorTask(
    const std::unordered_map<ActorID, ray::raylet::ActorRegistration> &actor_registry,
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
      heartbeat_period_ms_(config.heartbeat_period_ms),
      local_resources_(config.resource_config),
      worker_pool_(config.num_initial_workers,
                   static_cast<int>(config.resource_config.GetNumCpus()),
                   config.worker_command),
      local_queues_(SchedulingQueue()),
      scheduling_policy_(local_queues_),
      reconstruction_policy_([this](const TaskID &task_id) { ResubmitTask(task_id); }),
      task_dependency_manager_(object_manager),
      lineage_cache_(gcs_client_->client_table().GetLocalClientId(),
                     gcs_client->raylet_task_table(), gcs_client->raylet_task_table(),
                     config.max_lineage_size),
      remote_clients_(),
      remote_server_connections_(),
      actor_registry_() {
  RAY_CHECK(heartbeat_period_ms_ > 0);
  // Initialize the resource map with own cluster resource configuration.
  ClientID local_client_id = gcs_client_->client_table().GetLocalClientId();
  cluster_resource_map_.emplace(local_client_id,
                                SchedulingResources(config.resource_config));

  RAY_CHECK_OK(object_manager_.SubscribeObjAdded([this](const ObjectInfoT &object_info) {
    ObjectID object_id = ObjectID::from_binary(object_info.object_id);
    HandleObjectLocal(object_id);
  }));
  RAY_CHECK_OK(object_manager_.SubscribeObjDeleted(
      [this](const ObjectID &object_id) { HandleObjectMissing(object_id); }));
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
      task_committed_callback, nullptr));

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

  // Subscribe to node manager heartbeats.
  const auto heartbeat_added = [this](gcs::AsyncGcsClient *client, const ClientID &id,
                                      const HeartbeatTableDataT &heartbeat_data) {
    HeartbeatAdded(client, id, heartbeat_data);
  };
  RAY_RETURN_NOT_OK(gcs_client_->heartbeat_table().Subscribe(
      UniqueID::nil(), UniqueID::nil(), heartbeat_added, [](gcs::AsyncGcsClient *client) {
        RAY_LOG(DEBUG) << "heartbeat table subscription done callback called.";
      }));

  // Start sending heartbeats to the GCS.
  Heartbeat();

  return ray::Status::OK();
}

void NodeManager::Heartbeat() {
  RAY_LOG(DEBUG) << "[Heartbeat] sending heartbeat.";
  auto &heartbeat_table = gcs_client_->heartbeat_table();
  auto heartbeat_data = std::make_shared<HeartbeatTableDataT>();
  auto client_id = gcs_client_->client_table().GetLocalClientId();
  const SchedulingResources &local_resources = cluster_resource_map_[client_id];
  heartbeat_data->client_id = client_id.hex();
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
  auto heartbeat_period = boost::posix_time::milliseconds(heartbeat_period_ms_);
  heartbeat_timer_.expires_from_now(heartbeat_period);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    Heartbeat();
  });
}

void NodeManager::ClientAdded(const ClientTableDataT &client_data) {
  ClientID client_id = ClientID::from_binary(client_data.client_id);
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

  ResourceSet resources_total(client_data.resources_total_label,
                              client_data.resources_total_capacity);
  this->cluster_resource_map_.emplace(client_id, SchedulingResources(resources_total));

  // Establish a new NodeManager connection to this GCS client.
  auto client_info = gcs_client_->client_table().GetClient(client_id);
  RAY_LOG(DEBUG) << "[ClientAdded] CONNECTING TO: "
                 << " " << client_info.node_manager_address << " "
                 << client_info.node_manager_port;

  boost::asio::ip::tcp::socket socket(io_service_);
  RAY_CHECK_OK(TcpConnect(socket, client_info.node_manager_address,
                          client_info.node_manager_port));
  auto server_conn = TcpServerConnection(std::move(socket));
  remote_server_connections_.emplace(client_id, std::move(server_conn));
}

void NodeManager::HeartbeatAdded(gcs::AsyncGcsClient *client, const ClientID &client_id,
                                 const HeartbeatTableDataT &heartbeat_data) {
  RAY_LOG(DEBUG) << "[HeartbeatAdded]: received heartbeat from client id " << client_id;
  if (client_id == gcs_client_->client_table().GetLocalClientId()) {
    // Skip heartbeats from self.
    return;
  }
  // Locate the client id in remote client table and update available resources based on
  // the received heartbeat information.
  auto it = this->cluster_resource_map_.find(client_id);
  if (it == cluster_resource_map_.end()) {
    // Haven't received the client registration for this client yet, skip this heartbeat.
    RAY_LOG(INFO) << "[HeartbeatAdded]: received heartbeat from unknown client id "
                  << client_id;
    return;
  }
  SchedulingResources &resources = it->second;
  ResourceSet heartbeat_resource_available(heartbeat_data.resources_available_label,
                                           heartbeat_data.resources_available_capacity);
  resources.SetAvailableResources(
      ResourceSet(heartbeat_data.resources_available_label,
                  heartbeat_data.resources_available_capacity));
  RAY_CHECK(this->cluster_resource_map_[client_id].GetAvailableResources() ==
            heartbeat_resource_available);
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
    const auto &methods = local_queues_.GetUncreatedActorMethods();
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
      lineage_cache_.RemoveWaitingTask(method.GetTaskSpecification().TaskId());
      // The task's uncommitted lineage was already added to the local lineage
      // cache upon the initial submission, so it's okay to resubmit it with an
      // empty lineage this time.
      SubmitTask(method, Lineage());
    }
  }
}

void NodeManager::ProcessNewClient(LocalClientConnection &client) {
  // The new client is a worker, so begin listening for messages.
  client.ProcessMessages();
}

void NodeManager::DispatchTasks() {
  // Work with a copy of scheduled tasks.
  auto scheduled_tasks = local_queues_.GetScheduledTasks();
  // Return if there are no tasks to schedule.
  if (scheduled_tasks.empty()) {
    return;
  }
  const ClientID &my_client_id = gcs_client_->client_table().GetLocalClientId();

  for (const auto &task : scheduled_tasks) {
    const auto &local_resources =
        cluster_resource_map_[my_client_id].GetAvailableResources();
    const auto &task_resources = task.GetTaskSpecification().GetRequiredResources();
    if (!task_resources.IsSubset(local_resources)) {
      // Not enough local resources for this task right now, skip this task.
      continue;
    }
    // We have enough resources for this task. Assign task.
    // TODO(atumanov): perform the task state/queue transition inside AssignTask.
    auto dispatched_task =
        local_queues_.RemoveTasks({task.GetTaskSpecification().TaskId()});
    AssignTask(dispatched_task.front());
  }
}

void NodeManager::ProcessClientMessage(
    const std::shared_ptr<LocalClientConnection> &client, int64_t message_type,
    const uint8_t *message_data) {
  RAY_LOG(DEBUG) << "Message of type " << message_type;

  switch (static_cast<protocol::MessageType>(message_type)) {
  case protocol::MessageType::RegisterClientRequest: {
    auto message = flatbuffers::GetRoot<protocol::RegisterClientRequest>(message_data);
    if (message->is_worker()) {
      // Create a new worker from the registration request.
      auto worker = std::make_shared<Worker>(message->worker_pid(), client);
      // Register the new worker.
      worker_pool_.RegisterWorker(std::move(worker));
    }
  } break;
  case protocol::MessageType::GetTask: {
    std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    RAY_CHECK(worker);
    // If the worker was assigned a task, mark it as finished.
    if (!worker->GetAssignedTaskId().is_nil()) {
      FinishAssignedTask(*worker);
    }
    // Return the worker to the idle pool.
    worker_pool_.PushWorker(std::move(worker));
    // Call task dispatch to assign work to the new worker.
    DispatchTasks();

  } break;
  case protocol::MessageType::DisconnectClient: {
    // Remove the dead worker from the pool and stop listening for messages.
    const std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    if (worker) {
      // TODO(swang): Handle the case where the worker is killed while
      // executing a task. Clean up the assigned task's resources, return an
      // error to the driver.
      // RAY_CHECK(worker->GetAssignedTaskId().is_nil())
      //    << "Worker died while executing task: " << worker->GetAssignedTaskId();
      worker_pool_.DisconnectWorker(worker);
    }
    return;
  } break;
  case protocol::MessageType::SubmitTask: {
    // Read the task submitted by the client.
    auto message = flatbuffers::GetRoot<protocol::SubmitTaskRequest>(message_data);
    TaskExecutionSpecification task_execution_spec(
        from_flatbuf(*message->execution_dependencies()));
    TaskSpecification task_spec(*message->task_spec());
    Task task(task_execution_spec, task_spec);
    // Submit the task to the local scheduler. Since the task was submitted
    // locally, there is no uncommitted lineage.
    SubmitTask(task, Lineage());
  } break;
  case protocol::MessageType::ReconstructObject: {
    // TODO(hme): handle multiple object ids.
    auto message = flatbuffers::GetRoot<protocol::ReconstructObject>(message_data);
    ObjectID object_id = from_flatbuf(*message->object_id());
    RAY_LOG(DEBUG) << "reconstructing object " << object_id;
    if (!task_dependency_manager_.CheckObjectLocal(object_id)) {
      // TODO(swang): Instead of calling Pull on the object directly, record the
      // fact that the blocked task is dependent on this object_id in the task
      // dependency manager.
      RAY_CHECK_OK(object_manager_.Pull(object_id));
    }

    // If the blocked client is a worker, and the worker isn't already blocked,
    // then release any CPU resources that it acquired for its assigned task
    // while it is blocked. The resources will be acquired again once the
    // worker is unblocked.
    std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    if (worker && !worker->IsBlocked()) {
      RAY_CHECK(!worker->GetAssignedTaskId().is_nil());
      auto tasks = local_queues_.RemoveTasks({worker->GetAssignedTaskId()});
      const auto &task = tasks.front();
      // Get the CPU resources required by the running task.
      const auto required_resources = task.GetTaskSpecification().GetRequiredResources();
      double required_cpus = required_resources.GetNumCpus();
      const std::unordered_map<std::string, double> cpu_resources = {
          {kCPU_ResourceLabel, required_cpus}};
      // Release the CPU resources.
      RAY_CHECK(
          cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Release(
              ResourceSet(cpu_resources)));
      // Mark the task as blocked.
      local_queues_.QueueBlockedTasks(tasks);
      worker->MarkBlocked();

      // Try to dispatch more tasks since the blocked worker released some
      // resources.
      DispatchTasks();
    }
  } break;
  case protocol::MessageType::NotifyUnblocked: {
    std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    // Re-acquire the CPU resources for the task that was assigned to the
    // unblocked worker.
    if (worker) {
      RAY_CHECK(worker->IsBlocked());
      RAY_CHECK(!worker->GetAssignedTaskId().is_nil());

      auto tasks = local_queues_.RemoveTasks({worker->GetAssignedTaskId()});
      const auto &task = tasks.front();
      // Get the CPU resources required by the running task.
      const auto required_resources = task.GetTaskSpecification().GetRequiredResources();
      double required_cpus = required_resources.GetNumCpus();
      const std::unordered_map<std::string, double> cpu_resources = {
          {kCPU_ResourceLabel, required_cpus}};
      // Acquire the CPU resources.
      bool oversubscribed =
          !cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Acquire(
              ResourceSet(cpu_resources));
      if (oversubscribed) {
        const SchedulingResources &local_resources =
            cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()];
        RAY_LOG(WARNING) << "Resources oversubscribed: "
                         << local_resources.GetAvailableResources().ToString();
      }
      // Mark the task as running again.
      local_queues_.QueueRunningTasks(tasks);
      worker->MarkUnblocked();
    }
  } break;
  case protocol::MessageType::WaitRequest: {
    // Read the data.
    auto message = flatbuffers::GetRoot<protocol::WaitRequest>(message_data);
    std::vector<ObjectID> object_ids = from_flatbuf(*message->object_ids());
    int64_t wait_ms = message->timeout();
    uint64_t num_required_objects = static_cast<uint64_t>(message->num_ready_objects());
    bool wait_local = message->wait_local();

    ray::Status status = object_manager_.Wait(
        object_ids, wait_ms, num_required_objects, wait_local,
        [client](std::vector<ObjectID> found, std::vector<ObjectID> remaining) {
          // Write the data.
          flatbuffers::FlatBufferBuilder fbb;
          flatbuffers::Offset<protocol::WaitReply> wait_reply = protocol::CreateWaitReply(
              fbb, to_flatbuf(fbb, found), to_flatbuf(fbb, remaining));
          fbb.Finish(wait_reply);
          RAY_CHECK_OK(
              client->WriteMessage(static_cast<int64_t>(protocol::MessageType::WaitReply),
                                   fbb.GetSize(), fbb.GetBufferPointer()));
        });
    RAY_CHECK_OK(status);
  } break;

  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  // Listen for more messages.
  client->ProcessMessages();
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
                   << " spillback=" << task.GetTaskExecutionSpecReadonly().NumForwards();
    SubmitTask(task, uncommitted_lineage);
  } break;
  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }
  node_manager_client.ProcessMessages();
}

void NodeManager::ScheduleTasks() {
  // This method performs the transition of tasks from PENDING to SCHEDULED.
  auto policy_decision = scheduling_policy_.Schedule(
      cluster_resource_map_, gcs_client_->client_table().GetLocalClientId(),
      remote_clients_);
  RAY_LOG(DEBUG) << "[NM ScheduleTasks] policy decision:";
  for (const auto &pair : policy_decision) {
    TaskID task_id = pair.first;
    ClientID client_id = pair.second;
    RAY_LOG(DEBUG) << task_id << " --> " << client_id;
  }

  // Extract decision for this local scheduler.
  std::unordered_set<TaskID> local_task_ids;
  // Iterate over (taskid, clientid) pairs, extract tasks assigned to the local node.
  for (const auto &task_schedule : policy_decision) {
    TaskID task_id = task_schedule.first;
    ClientID client_id = task_schedule.second;
    if (client_id == gcs_client_->client_table().GetLocalClientId()) {
      local_task_ids.insert(task_id);
    } else {
      auto tasks = local_queues_.RemoveTasks({task_id});
      RAY_CHECK(1 == tasks.size());
      Task &task = tasks.front();
      // TODO(swang): Handle forward task failure.
      RAY_CHECK_OK(ForwardTask(task, client_id));
    }
    // Notify the task dependency manager that we no longer need this task's
    // object dependencies.
    // NOTE(swang): For local tasks, the scheduled task's dependencies may get
    // evicted before it can be assigned to a worker.
    task_dependency_manager_.UnsubscribeDependencies(task_id);
  }

  // Transition locally scheduled tasks to SCHEDULED and dispatch scheduled tasks.
  if (local_task_ids.size() > 0) {
    std::vector<Task> tasks = local_queues_.RemoveTasks(local_task_ids);
    local_queues_.QueueScheduledTasks(tasks);
    DispatchTasks();
  }
}

void NodeManager::SubmitTask(const Task &task, const Lineage &uncommitted_lineage) {
  // Add the task and its uncommitted lineage to the lineage cache.
  lineage_cache_.AddWaitingTask(task, uncommitted_lineage);
  // Mark the task as pending. Once the task has finished execution, or once it
  // has been forwarded to another node, the task must be marked as canceled in
  // the TaskDependencyManager.
  task_dependency_manager_.TaskPending(task);

  const TaskSpecification &spec = task.GetTaskSpecification();
  if (spec.IsActorTask()) {
    // Check whether we know the location of the actor.
    const auto actor_entry = actor_registry_.find(spec.ActorId());
    if (actor_entry != actor_registry_.end()) {
      // We have a known location for the actor.
      auto node_manager_id = actor_entry->second.GetNodeManagerId();
      if (node_manager_id == gcs_client_->client_table().GetLocalClientId()) {
        // The actor is local. Queue the task for local execution.
        QueueTask(task);
      } else {
        // The actor is remote. Forward the task to the node manager that owns
        // the actor.
        // TODO(swang): Handle forward task failure.
        RAY_CHECK_OK(ForwardTask(task, node_manager_id));
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
      local_queues_.QueueUncreatedActorMethods({task});
    }
  } else {
    // This is a non-actor task. Queue the task for local execution.
    QueueTask(task);
  }
}

void NodeManager::HandleRemoteDependencyRequired(const ObjectID &dependency_id) {
  // Try to fetch the object from the object manager.
  RAY_CHECK_OK(object_manager_.Pull(dependency_id));
  // TODO(swang): Request reconstruction of the object, possibly after a
  // timeout.
}

void NodeManager::HandleRemoteDependencyCanceled(const ObjectID &dependency_id) {
  // Cancel the fetch request from the object manager.
  RAY_CHECK_OK(object_manager_.Cancel(dependency_id));
  // TODO(swang): Cancel reconstruction of the object.
}

void NodeManager::QueueTask(const Task &task) {
  // Subscribe to the task's dependencies.
  bool ready = task_dependency_manager_.SubscribeDependencies(
      task.GetTaskSpecification().TaskId(), task.GetDependencies());
  // Queue the task. If all dependencies are available, then the task is queued
  // in the READY state, else the WAITING.
  if (ready) {
    local_queues_.QueueReadyTasks({task});
    // Try to schedule the newly ready task.
    ScheduleTasks();
  } else {
    local_queues_.QueueWaitingTasks({task});
  }
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
  std::shared_ptr<Worker> worker = worker_pool_.PopWorker(spec.ActorId());
  if (worker == nullptr) {
    // There are no workers that can execute this task.
    if (!spec.IsActorTask()) {
      // There are no more non-actor workers available to execute this task.
      // Start a new worker.
      worker_pool_.StartWorker();
    }
    // Queue this task for future assignment. The task will be assigned to a
    // worker once one becomes available.
    local_queues_.QueueScheduledTasks(std::vector<Task>({task}));
    return;
  }

  RAY_LOG(DEBUG) << "Assigning task to worker with pid " << worker->Pid();
  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateGetTaskReply(fbb, spec.ToFlatbuffer(fbb),
                                              fbb.CreateVector(std::vector<int>()));
  fbb.Finish(message);
  auto status = worker->Connection()->WriteMessage(
      static_cast<int64_t>(protocol::MessageType::ExecuteTask), fbb.GetSize(),
      fbb.GetBufferPointer());
  if (status.ok()) {
    // Resource accounting: acquire resources for the assigned task.
    const ClientID &my_client_id = gcs_client_->client_table().GetLocalClientId();
    RAY_CHECK(
        this->cluster_resource_map_[my_client_id].Acquire(spec.GetRequiredResources()));

    // We successfully assigned the task to the worker.
    worker->AssignTaskId(spec.TaskId());
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
      TaskExecutionSpecification &mutable_spec = task.GetTaskExecutionSpec();
      mutable_spec.SetExecutionDependencies({execution_dependency});
      // Extend the frontier to include the executing task.
      actor_entry->second.ExtendFrontier(spec.ActorHandleId(), spec.ActorDummyObject());
    }
    // We started running the task, so the task is ready to write to GCS.
    lineage_cache_.AddReadyTask(task);
    // Mark the task as running.
    local_queues_.QueueRunningTasks(std::vector<Task>({task}));
  } else {
    RAY_LOG(WARNING) << "Failed to send task to worker, disconnecting client";
    // We failed to send the task to the worker, so disconnect the worker.
    ProcessClientMessage(worker->Connection(),
                         static_cast<int64_t>(protocol::MessageType::DisconnectClient),
                         NULL);
    // Queue this task for future assignment. The task will be assigned to a
    // worker once one becomes available.
    local_queues_.QueueScheduledTasks(std::vector<Task>({task}));
  }
}

void NodeManager::FinishAssignedTask(Worker &worker) {
  TaskID task_id = worker.GetAssignedTaskId();
  RAY_LOG(DEBUG) << "Finished task " << task_id;
  auto tasks = local_queues_.RemoveTasks({task_id});
  auto task = *tasks.begin();

  if (task.GetTaskSpecification().IsActorCreationTask()) {
    // If this was an actor creation task, then convert the worker to an actor.
    auto actor_id = task.GetTaskSpecification().ActorCreationId();
    worker.AssignActorId(actor_id);

    // Publish the actor creation event to all other nodes so that methods for
    // the actor will be forwarded directly to this node.
    auto actor_notification = std::make_shared<ActorTableDataT>();
    actor_notification->actor_id = actor_id.binary();
    actor_notification->actor_creation_dummy_object_id =
        task.GetTaskSpecification().ActorDummyObject().binary();
    // TODO(swang): The driver ID.
    actor_notification->driver_id = JobID::nil().binary();
    actor_notification->node_manager_id =
        gcs_client_->client_table().GetLocalClientId().binary();
    RAY_LOG(DEBUG) << "Publishing actor creation: " << actor_id;
    RAY_CHECK_OK(gcs_client_->actor_table().Append(JobID::nil(), actor_id,
                                                   actor_notification, nullptr));

    // Resources required by an actor creation task are acquired for the
    // lifetime of the actor, so we do not release any resources here.
  } else {
    // Release task's resources.
    RAY_CHECK(this->cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()]
                  .Release(task.GetTaskSpecification().GetRequiredResources()));
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
}

void NodeManager::ResubmitTask(const TaskID &task_id) {
  throw std::runtime_error("Method not implemented");
}

void NodeManager::HandleObjectLocal(const ObjectID &object_id) {
  // Notify the task dependency manager that this object is local.
  const auto ready_task_ids = task_dependency_manager_.HandleObjectLocal(object_id);
  // Transition the tasks whose dependencies are now fulfilled to the ready
  // state.
  if (ready_task_ids.size() > 0) {
    std::unordered_set<TaskID> ready_task_id_set(ready_task_ids.begin(),
                                                 ready_task_ids.end());
    auto ready_tasks = local_queues_.RemoveTasks(ready_task_id_set);
    local_queues_.QueueReadyTasks(std::vector<Task>(ready_tasks));
    // Schedule the newly ready tasks.
    ScheduleTasks();
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
    auto waiting_tasks = local_queues_.RemoveTasks(waiting_task_id_set);
    local_queues_.QueueWaitingTasks(std::vector<Task>(waiting_tasks));
  }
}

ray::Status NodeManager::ForwardTask(const Task &task, const ClientID &node_id) {
  const auto &spec = task.GetTaskSpecification();
  auto task_id = spec.TaskId();

  // Get and serialize the task's uncommitted lineage.
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id);
  Task &lineage_cache_entry_task =
      uncommitted_lineage.GetEntryMutable(task_id)->TaskDataMutable();
  // Increment forward count for the forwarded task.
  lineage_cache_entry_task.GetTaskExecutionSpec().IncrementNumForwards();

  flatbuffers::FlatBufferBuilder fbb;
  auto request = uncommitted_lineage.ToFlatbuffer(fbb, task_id);
  fbb.Finish(request);

  RAY_LOG(DEBUG) << "Forwarding task " << task_id << " to " << node_id << " spillback="
                 << lineage_cache_entry_task.GetTaskExecutionSpec().NumForwards();

  auto client_info = gcs_client_->client_table().GetClient(node_id);

  // Lookup remote server connection for this node_id and use it to send the request.
  auto it = remote_server_connections_.find(node_id);
  if (it == remote_server_connections_.end()) {
    // TODO(atumanov): caller must handle failure to ensure tasks are not lost.
    RAY_LOG(INFO) << "No NodeManager connection found for GCS client id " << node_id;
    return ray::Status::IOError("NodeManager connection not found");
  }

  auto &server_conn = it->second;
  auto status = server_conn.WriteMessage(
      static_cast<int64_t>(protocol::MessageType::ForwardTaskRequest), fbb.GetSize(),
      fbb.GetBufferPointer());
  if (status.ok()) {
    // If we were able to forward the task, remove the forwarded task from the
    // lineage cache since the receiving node is now responsible for writing
    // the task to the GCS.
    lineage_cache_.RemoveWaitingTask(task_id);
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
            RAY_CHECK_OK(object_manager_.Push(argument_id, node_id));
          }
        }
      }
    }
  } else {
    // TODO(atumanov): caller must handle ForwardTask failure to ensure tasks are not
    // lost.
    RAY_LOG(FATAL) << "[NodeManager][ForwardTask] failed to forward task " << task_id
                   << " to node " << node_id;
  }
  return status;
}

}  // namespace raylet

}  // namespace ray
