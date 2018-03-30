#include "ray/raylet/node_manager.h"

#include "common_protocol.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

namespace raylet {

NodeManager::NodeManager(boost::asio::io_service &io_service,
                         const NodeManagerConfig &config, ObjectManager &object_manager,
                         std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    : io_service_(io_service),
      heartbeat_timer_(io_service),
      heartbeat_period_ms_(config.heartbeat_period_ms),
      local_resources_(config.resource_config),
      worker_pool_(config.num_initial_workers, config.worker_command),
      local_queues_(SchedulingQueue()),
      scheduling_policy_(local_queues_),
      reconstruction_policy_([this](const TaskID &task_id) { ResubmitTask(task_id); }),
      task_dependency_manager_(
          object_manager,
          // reconstruction_policy_,
          [this](const TaskID &task_id) { HandleWaitingTaskReady(task_id); }),
      lineage_cache_(gcs_client->raylet_task_table()),
      gcs_client_(gcs_client),
      remote_clients_(),
      remote_server_connections_(),
      object_manager_(object_manager) {
  RAY_CHECK(heartbeat_period_ms_ > 0);
  // Initialize the resource map with own cluster resource configuration.
  ClientID local_client_id = gcs_client_->client_table().GetLocalClientId();
  cluster_resource_map_.emplace(local_client_id,
                                SchedulingResources(config.resource_config));
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
         std::shared_ptr<HeartbeatTableDataT> data) {
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

void NodeManager::ClientAdded(gcs::AsyncGcsClient *client, const UniqueID &id,
                              const ClientTableDataT &client_data) {
  ClientID client_id = ClientID::from_binary(client_data.client_id);
  RAY_LOG(DEBUG) << "[ClientAdded] received callback from client id " << client_id.hex();
  if (client_id == gcs_client_->client_table().GetLocalClientId()) {
    // We got a notification for ourselves, so we are connected to the GCS now.
    // Save this NodeManager's resource information in the cluster resource map.
    cluster_resource_map_[client_id] = local_resources_;
    // Start sending heartbeats to the GCS.
    Heartbeat();
    // Subscribe to heartbeats.
    const auto heartbeat_added = [this](gcs::AsyncGcsClient *client, const ClientID &id,
                                        const HeartbeatTableDataT &heartbeat_data) {
      this->HeartbeatAdded(client, id, heartbeat_data);
    };
    ray::Status status = client->heartbeat_table().Subscribe(
        UniqueID::nil(), UniqueID::nil(), heartbeat_added,
        [this](gcs::AsyncGcsClient *client) {
          RAY_LOG(DEBUG) << "heartbeat table subscription done callback called.";
        });
    RAY_CHECK_OK(status);
    return;
  }

  // TODO(atumanov): make remote client lookup O(1)
  if (std::find(remote_clients_.begin(), remote_clients_.end(), client_id) ==
      remote_clients_.end()) {
    RAY_LOG(DEBUG) << "a new client: " << client_id.hex();
    remote_clients_.push_back(client_id);
  } else {
    // NodeManager connection to this client was already established.
    RAY_LOG(DEBUG) << "received a new client connection that already exists: "
                   << client_id.hex();
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
  RAY_LOG(DEBUG) << "[HeartbeatAdded]: received heartbeat from client id "
                 << client_id.hex();
  if (client_id == gcs_client_->client_table().GetLocalClientId()) {
    // Skip heartbeats from self.
    return;
  }
  // Locate the client id in remote client table and update available resources based on
  // the received heartbeat information.
  if (this->cluster_resource_map_.count(client_id) == 0) {
    // Haven't received the client registration for this client yet, skip this heartbeat.
    RAY_LOG(INFO) << "[HeartbeatAdded]: received heartbeat from unknown client id "
                  << client_id.hex();
    return;
  }
  SchedulingResources &resources = this->cluster_resource_map_[client_id];
  ResourceSet heartbeat_resource_available(heartbeat_data.resources_available_label,
                                           heartbeat_data.resources_available_capacity);
  resources.SetAvailableResources(
      ResourceSet(heartbeat_data.resources_available_label,
                  heartbeat_data.resources_available_capacity));
  RAY_CHECK(this->cluster_resource_map_[client_id].GetAvailableResources() ==
            heartbeat_resource_available);
}

void NodeManager::ProcessNewClient(std::shared_ptr<LocalClientConnection> client) {
  // The new client is a worker, so begin listening for messages.
  client->ProcessMessages();
}

void NodeManager::ProcessClientMessage(std::shared_ptr<LocalClientConnection> client,
                                       int64_t message_type,
                                       const uint8_t *message_data) {
  RAY_LOG(DEBUG) << "Message of type " << message_type;

  switch (message_type) {
  case protocol::MessageType_RegisterClientRequest: {
    auto message = flatbuffers::GetRoot<protocol::RegisterClientRequest>(message_data);
    if (message->is_worker()) {
      // Create a new worker from the registration request.
      std::shared_ptr<Worker> worker(new Worker(message->worker_pid(), client));
      // Register the new worker.
      worker_pool_.RegisterWorker(std::move(worker));
    }
  } break;
  case protocol::MessageType_GetTask: {
    const std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    RAY_CHECK(worker);
    // If the worker was assigned a task, mark it as finished.
    if (!worker->GetAssignedTaskId().is_nil()) {
      FinishTask(worker->GetAssignedTaskId());
    }
    // Return the worker to the idle pool.
    worker_pool_.PushWorker(worker);
    auto scheduled_tasks = local_queues_.GetScheduledTasks();
    if (!scheduled_tasks.empty()) {
      const TaskID &scheduled_task_id =
          scheduled_tasks.front().GetTaskSpecification().TaskId();
      auto scheduled_tasks = local_queues_.RemoveTasks({scheduled_task_id});
      AssignTask(scheduled_tasks.front());
    }
  } break;
  case protocol::MessageType_DisconnectClient: {
    // Remove the dead worker from the pool and stop listening for messages.
    const std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    if (worker) {
      if (!worker->GetAssignedTaskId().is_nil()) {
        // TODO(swang): Clean up any tasks that were assigned to the worker.
        // Release any resources that may be held by this worker.
        FinishTask(worker->GetAssignedTaskId());
      }
      worker_pool_.DisconnectWorker(worker);
    }
    return;
  } break;
  case protocol::MessageType_SubmitTask: {
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
  case protocol::MessageType_ReconstructObject: {
    // TODO(hme): handle multiple object ids.
    auto message = flatbuffers::GetRoot<protocol::ReconstructObject>(message_data);
    ObjectID object_id = from_flatbuf(*message->object_id());
    RAY_LOG(DEBUG) << "reconstructing object " << object_id.hex();
    RAY_CHECK_OK(object_manager_.Pull(object_id));
  } break;

  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  // Listen for more messages.
  client->ProcessMessages();
}

void NodeManager::ProcessNewNodeManager(
    std::shared_ptr<TcpClientConnection> node_manager_client) {
  node_manager_client->ProcessMessages();
}

void NodeManager::ProcessNodeManagerMessage(
    std::shared_ptr<TcpClientConnection> node_manager_client, int64_t message_type,
    const uint8_t *message_data) {
  switch (message_type) {
  case protocol::MessageType_ForwardTaskRequest: {
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
  node_manager_client->ProcessMessages();
}

void NodeManager::HandleWaitingTaskReady(const TaskID &task_id) {
  auto ready_tasks = local_queues_.RemoveTasks({task_id});
  local_queues_.QueueReadyTasks(std::vector<Task>(ready_tasks));
  // Schedule the newly ready tasks if possible.
  ScheduleTasks();
}

void NodeManager::ScheduleTasks() {
  auto policy_decision = scheduling_policy_.Schedule(
      cluster_resource_map_, gcs_client_->client_table().GetLocalClientId(),
      remote_clients_);
  RAY_LOG(DEBUG) << "[NM ScheduleTasks] policy decision:";
  for (const auto &pair : policy_decision) {
    TaskID task_id = pair.first;
    ClientID client_id = pair.second;
    RAY_LOG(DEBUG) << task_id.hex() << " --> " << client_id.hex();
  }

  // Extract decision for this local scheduler.
  std::unordered_set<TaskID, UniqueIDHasher> local_task_ids;
  // Iterate over (taskid, clientid) pairs, extract tasks to run on the local client.
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
      // TODO(swang): Unsubscribe this task in the task dependency manager.
      RAY_CHECK_OK(ForwardTask(task, client_id));
    }
  }

  // Assign the tasks to workers.
  std::vector<Task> tasks = local_queues_.RemoveTasks(local_task_ids);
  for (auto &task : tasks) {
    AssignTask(task);
  }
}

void NodeManager::SubmitTask(const Task &task, const Lineage &uncommitted_lineage) {
  // Add the task and its uncommitted lineage to the lineage cache.
  lineage_cache_.AddWaitingTask(task, uncommitted_lineage);
  // Queue the task according to the availability of its arguments.
  if (task_dependency_manager_.TaskReady(task)) {
    local_queues_.QueueReadyTasks(std::vector<Task>({task}));
    ScheduleTasks();
  } else {
    local_queues_.QueueWaitingTasks(std::vector<Task>({task}));
    task_dependency_manager_.SubscribeTaskReady(task);
  }
}

void NodeManager::AssignTask(const Task &task) {
  // Resource accounting: acquire resources for the scheduled task.
  const ClientID &my_client_id = gcs_client_->client_table().GetLocalClientId();
  RAY_CHECK(this->cluster_resource_map_[my_client_id].Acquire(
      task.GetTaskSpecification().GetRequiredResources()));

  if (worker_pool_.PoolSize() == 0) {
    worker_pool_.StartWorker();
    // Queue this task for future assignment. The task will be assigned to a
    // worker once one becomes available.
    local_queues_.QueueScheduledTasks(std::vector<Task>({task}));
    return;
  }

  const TaskSpecification &spec = task.GetTaskSpecification();
  std::shared_ptr<Worker> worker = worker_pool_.PopWorker();
  RAY_LOG(DEBUG) << "Assigning task to worker with pid " << worker->Pid();

  worker->AssignTaskId(spec.TaskId());
  local_queues_.QueueRunningTasks(std::vector<Task>({task}));

  flatbuffers::FlatBufferBuilder fbb;
  auto message = protocol::CreateGetTaskReply(fbb, spec.ToFlatbuffer(fbb),
                                              fbb.CreateVector(std::vector<int>()));
  fbb.Finish(message);
  auto status = worker->Connection()->WriteMessage(protocol::MessageType_ExecuteTask,
                                                   fbb.GetSize(), fbb.GetBufferPointer());
  if (status.ok()) {
    // We started running the task, so the task is ready to write to GCS.
    lineage_cache_.AddReadyTask(task);
  } else {
    // We failed to send the task to the worker, so disconnect the worker. The
    // task will get queued again during cleanup.
    ProcessClientMessage(worker->Connection(), protocol::MessageType_DisconnectClient,
                         NULL);
  }
}

void NodeManager::FinishTask(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Finished task " << task_id.hex();
  auto tasks = local_queues_.RemoveTasks({task_id});
  RAY_CHECK(tasks.size() == 1);
  auto task = *tasks.begin();

  // Resource accounting: release task's resources.
  RAY_CHECK(
      this->cluster_resource_map_[gcs_client_->client_table().GetLocalClientId()].Release(
          task.GetTaskSpecification().GetRequiredResources()));
}

void NodeManager::ResubmitTask(const TaskID &task_id) {
  throw std::runtime_error("Method not implemented");
}

ray::Status NodeManager::ForwardTask(Task &task, const ClientID &node_id) {
  auto task_id = task.GetTaskSpecification().TaskId();

  // Get and serialize the task's uncommitted lineage.
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id);
  Task &lineage_cache_entry_task =
      uncommitted_lineage.GetEntryMutable(task_id)->TaskDataMutable();
  // Increment forward count for the forwarded task.
  lineage_cache_entry_task.GetTaskExecutionSpec().IncrementNumForwards();

  flatbuffers::FlatBufferBuilder fbb;
  auto request = uncommitted_lineage.ToFlatbuffer(fbb, task_id);
  fbb.Finish(request);

  RAY_LOG(DEBUG) << "Forwarding task " << task_id.hex() << " to " << node_id.hex()
                 << " spillback="
                 << lineage_cache_entry_task.GetTaskExecutionSpec().NumForwards();

  auto client_info = gcs_client_->client_table().GetClient(node_id);

  // Lookup remote server connection for this node_id and use it to send the request.
  if (remote_server_connections_.count(node_id) == 0) {
    // TODO(atumanov): caller must handle failure to ensure tasks are not lost.
    RAY_LOG(INFO) << "No NodeManager connection found for GCS client id "
                  << node_id.hex();
    return ray::Status::IOError("NodeManager connection not found");
  }

  auto &server_conn = remote_server_connections_.at(node_id);
  auto status = server_conn.WriteMessage(protocol::MessageType_ForwardTaskRequest,
                                         fbb.GetSize(), fbb.GetBufferPointer());
  if (status.ok()) {
    // If we were able to forward the task, remove the forwarded task from the
    // lineage cache since the receiving node is now responsible for writing
    // the task to the GCS.
    lineage_cache_.RemoveWaitingTask(task_id);
  } else {
    // TODO(atumanov): caller must handle ForwardTask failure to ensure tasks are not
    // lost.
    RAY_LOG(FATAL) << "[NodeManager][ForwardTask] failed to forward task "
                   << task_id.hex() << " to node " << node_id.hex();
  }
  return status;
}

}  // namespace raylet

}  // namespace ray
