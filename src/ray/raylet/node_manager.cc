#include "ray/raylet/node_manager.h"

#include "common_protocol.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

namespace raylet {

NodeManager::NodeManager(boost::asio::io_service &io_service,
                         const NodeManagerConfig &config, ObjectManager &object_manager,
                         LineageCache &lineage_cache,
                         std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
    : io_service_(io_service),
      local_resources_(config.resource_config),
      worker_pool_(config.num_initial_workers, config.worker_command),
      local_queues_(SchedulingQueue()),
      scheduling_policy_(local_queues_),
      reconstruction_policy_([this](const TaskID &task_id) { ResubmitTask(task_id); }),
      task_dependency_manager_(
          object_manager,
          // reconstruction_policy_,
          [this](const TaskID &task_id) { HandleWaitingTaskReady(task_id); }),
      lineage_cache_(lineage_cache),
      gcs_client_(gcs_client),
      remote_clients_(),
      remote_server_connections_(),
      object_manager_(object_manager) {

  // Initialize the resource map with own cluster resource configuration.
  ClientID local_client_id = gcs_client_->client_table().GetLocalClientId();
  cluster_resource_map_.emplace(local_client_id, SchedulingResources(config.resource_config));
}

void NodeManager::ClientAdded(gcs::AsyncGcsClient *client,
                      const UniqueID &id,
                      std::shared_ptr<ClientTableDataT> data) {
  ClientID client_id = ClientID::from_binary(data->client_id);
  RAY_LOG(DEBUG) << "[ClientAdded] received callback from client id " << client_id.hex();
  if (client_id == gcs_client_->client_table().GetLocalClientId()) {
    return;
  }
  // TODO(atumanov): make remote client lookup O(1)
  if (std::find(remote_clients_.begin(), remote_clients_.end(), client_id) == remote_clients_.end()) {
    RAY_LOG(INFO) << "a new client: " << client_id.hex();
    remote_clients_.push_back(client_id);
  } else {
    // NodeManager connection to this client was already established.
    RAY_LOG(DEBUG) << "received a new client connection that already exists: " << client_id.hex();
    return;
  }

  ResourceSet resources_total;
  for (uint i=0; i < data->resources_total_label.size(); i++) {
    resources_total.AddResource(data->resources_total_label[i],
                                data->resources_total_capacity[i]);
  }
  this->cluster_resource_map_.emplace(client_id, SchedulingResources(resources_total));

  // Establish a new NodeManager connection to this GCS client.
  auto client_info = gcs_client_->client_table().GetClient(client_id);
  RAY_LOG(DEBUG) <<"[ClientAdded] CONNECTING TO: "
                 << " " << client_info.node_manager_address.c_str()
                 << " " << client_info.node_manager_port;

  boost::asio::ip::tcp::socket socket(io_service_);
  RAY_CHECK_OK(TcpConnect(socket, client_info.node_manager_address,
                          client_info.node_manager_port));
  auto server_conn = TcpServerConnection(std::move(socket));
  remote_server_connections_.emplace(client_id, std::move(server_conn));
}

void NodeManager::HeartbeatHandler(gcs::AsyncGcsClient *client, const ClientID &id,
                                   std::shared_ptr<ClientTableDataT> data) {
  RAY_LOG(INFO) << "received heartbeat from " << id.hex();
  if (id == gcs_client_->client_table().GetLocalClientId()) {
    return;
  }
  // TODO(atumanov): Locate the client id in remote client table and save heartbeat information.
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
    case MessageType_RegisterClientRequest: {
      auto message = flatbuffers::GetRoot<RegisterClientRequest>(message_data);
      if (message->is_worker()) {
        // Create a new worker from the registration request.
        std::shared_ptr<Worker> worker(new Worker(message->worker_pid(), client));
        // Register the new worker.
        worker_pool_.RegisterWorker(std::move(worker));
      }

      // Build the reply to the worker's registration request. TODO(swang): This
      // is legacy code and should be removed once actor creation tasks are
      // implemented.
      flatbuffers::FlatBufferBuilder fbb;
      auto reply = CreateRegisterClientReply(fbb, fbb.CreateVector(std::vector<int>()));
      fbb.Finish(reply);
      // Reply to the worker's registration request, then listen for more
      // messages.
      auto status = client->WriteMessage(MessageType_RegisterClientReply, fbb.GetSize(),
                                         fbb.GetBufferPointer());
      if (!status.ok()) {
        const std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
        worker_pool_.DisconnectWorker(worker);
      }
    }
      break;
    case MessageType_GetTask: {
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
    }
      break;
    case MessageType_DisconnectClient: {
      // Remove the dead worker from the pool and stop listening for messages.
      const std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
      if (worker) {
        // TODO(swang): Clean up any tasks that were assigned to the worker.
        worker_pool_.DisconnectWorker(worker);
      }
      return;
    }
      break;
    case MessageType_SubmitTask: {
      // Read the task submitted by the client.
      auto message = flatbuffers::GetRoot<SubmitTaskRequest>(message_data);
      TaskExecutionSpecification task_execution_spec(
          from_flatbuf(*message->execution_dependencies()));
      TaskSpecification task_spec(*message->task_spec());
      Task task(task_execution_spec, task_spec);
      // Submit the task to the local scheduler. Since the task was submitted
      // locally, there is no uncommitted lineage.
      SubmitTask(task, Lineage());
      // Listen for more messages.
    }
      break;
    case MessageType_ReconstructObject: {
      // TODO(hme): handle multiple object ids.
      auto message = flatbuffers::GetRoot<ReconstructObject>(message_data);
      ObjectID object_id = from_flatbuf(*message->object_id());
      RAY_CHECK_OK(object_manager_.Pull(object_id));
    } break;
    default:RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }

  RAY_CHECK(message_type != MessageType_DisconnectClient);
  client->ProcessMessages();
}

void NodeManager::ProcessNewNodeManager(
    std::shared_ptr<TcpClientConnection> node_manager_client) {
  RAY_LOG(INFO) << "a new node manager connected!";
  node_manager_client->ProcessMessages();
}

void NodeManager::ProcessNodeManagerMessage(
    std::shared_ptr<TcpClientConnection> node_manager_client, int64_t message_type,
    const uint8_t *message_data) {
  switch (message_type) {
  case MessageType_ForwardTaskRequest: {
    RAY_LOG(INFO) << "ForwardTaskRequest received";
    auto message = flatbuffers::GetRoot<ForwardTaskRequest>(message_data);
    TaskID task_id = from_flatbuf(*message->task_id());

    Lineage uncommitted_lineage(*message);
    const Task &task = uncommitted_lineage.GetEntry(task_id)->TaskData();
    RAY_LOG(INFO) << "got task " << task.GetTaskSpecification().TaskId()
                  << "with numforwards=" << task.GetTaskExecutionSpecReadonly().NumForwards();
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
      cluster_resource_map_, gcs_client_->client_table().GetLocalClientId(), remote_clients_);
  RAY_LOG(INFO) << "[NM ScheduleTasks] policy decision:";
  for (const auto & pair: policy_decision) {
    TaskID task_id = pair.first;
    ClientID client_id = pair.second;
    RAY_LOG(INFO) << task_id.hex() << " --> " << client_id.hex();
  }

  // Extract decision for this local scheduler.
  std::unordered_set<TaskID, UniqueIDHasher> local_task_ids;
  //std::unordered_set<TaskID, UniqueIDHasher> remote_task_ids;
  // Iterate over (taskid, clientid) pairs, extract tasks to run on the local client.
  for (const auto &task_schedule : policy_decision) {
    TaskID task_id = task_schedule.first;
    ClientID client_id = task_schedule.second;
    if (client_id == gcs_client_->client_table().GetLocalClientId()) {
      local_task_ids.insert(task_id);
    } else {
      //remote_task_ids.insert(task_id);
      auto tasks = local_queues_.RemoveTasks({task_id});
      RAY_CHECK(1 == tasks.size());
      Task &task = tasks.front();
      // TODO(swang): Handle forward task failure.
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
  if (worker_pool_.PoolSize() == 0) {
    // Start a new worker.
    worker_pool_.StartWorker();
    // Queue this task for future assignment. The task will be assigned to a
    // worker once one becomes available.
    local_queues_.QueueScheduledTasks(std::vector<Task>({task}));
    // TODO(swang): Acquire resources here or when a worker becomes available?
    return;
  }

  const TaskSpecification &spec = task.GetTaskSpecification();
  std::shared_ptr<Worker> worker = worker_pool_.PopWorker();
  RAY_LOG(DEBUG) << "Assigning task to worker with pid " << worker->Pid();

  // TODO(swang): Acquire resources for the task.
  // local_resources_.Acquire(task.GetTaskSpecification().GetRequiredResources());

  worker->AssignTaskId(spec.TaskId());
  local_queues_.QueueRunningTasks(std::vector<Task>({task}));

  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreateGetTaskReply(fbb, spec.ToFlatbuffer(fbb),
                                    fbb.CreateVector(std::vector<int>()));
  fbb.Finish(message);
  auto status = worker->Connection()->WriteMessage(MessageType_ExecuteTask, fbb.GetSize(),
                                                   fbb.GetBufferPointer());
  if (status.ok()) {
    // We started running the task, so the task is ready to write to GCS.
    lineage_cache_.AddReadyTask(task);
  } else {
    // We failed to send the task to the worker, so disconnect the worker. The
    // task will get queued again during cleanup.
    ProcessClientMessage(worker->Connection(), MessageType_DisconnectClient, NULL);
  }
}

void NodeManager::FinishTask(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Finished task " << task_id.hex();
  auto tasks = local_queues_.RemoveTasks({task_id});
  RAY_CHECK(tasks.size() == 1);
  auto task = *tasks.begin();

  // TODO(swang): Release resources that were held for the task.
}

void NodeManager::ResubmitTask(const TaskID &task_id) {
  throw std::runtime_error("Method not implemented");
}

ray::Status NodeManager::ForwardTask(Task &task, const ClientID &node_id) {
  auto task_id = task.GetTaskSpecification().TaskId();


  // Get and serialize the task's uncommitted lineage.
  auto uncommitted_lineage = lineage_cache_.GetUncommittedLineage(task_id);
  Task &lineage_cache_entry_task = uncommitted_lineage.GetEntryMutable(task_id)->TaskDataMutable();
  // Increment forward count for the forwarded task.
  lineage_cache_entry_task.GetTaskExecutionSpec().IncrementNumForwards();
  RAY_LOG(INFO) << "AFTER: " << lineage_cache_entry_task.GetTaskExecutionSpec().NumForwards();

  RAY_LOG(INFO) << "[FORWARDTASK] " << uncommitted_lineage.GetEntry(task_id)->TaskData().GetTaskExecutionSpecReadonly().NumForwards();
  flatbuffers::FlatBufferBuilder fbb;
  auto request = uncommitted_lineage.ToFlatbuffer(fbb, task_id);
  fbb.Finish(request);

  RAY_LOG(INFO) << "Forwarding task " << task_id.hex() << " to " << node_id.hex()
                << " with numforwards=" << lineage_cache_entry_task.GetTaskExecutionSpec().NumForwards();

  auto client_info = gcs_client_->client_table().GetClient(node_id);

  // Lookup remote server connection for this node_id and use it to send the request.
  if (remote_server_connections_.count(node_id) == 0) {
    // TODO(atumanov): caller must handle failure to ensure tasks are not lost.
    RAY_LOG(INFO) << "No NodeManager connection found for GCS client id " << node_id.hex();
    return ray::Status::IOError("NodeManager connection not found");
  }

  auto &server_conn = remote_server_connections_.at(node_id);
  auto status = server_conn.WriteMessage(MessageType_ForwardTaskRequest, fbb.GetSize(),
                                  fbb.GetBufferPointer());
  if (status.ok()) {
    // If we were able to forward the task, remove the forwarded task from the
    // lineage cache since the receiving node is now responsible for writing
    // the task to the GCS.
    lineage_cache_.RemoveWaitingTask(task_id);
  } else {
    // TODO(atumanov): caller must handle ForwardTask failure to ensure tasks are not lost.
    RAY_LOG(FATAL) << "[NodeManager][ForwardTask] failed to forward task " << task_id.hex()
                   << " to node " << node_id.hex();
  }
  return status;
}

} // namespace raylet

}  // namespace ray
