#include "ray/raylet/node_manager.h"

#include "common_protocol.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

NodeManager::NodeManager(const std::string &socket_name,
                         const ResourceSet &resource_config,
                         ObjectManager &object_manager)
    : local_resources_(resource_config),
      worker_pool_(WorkerPool(0)),
      local_queues_(SchedulingQueue()),
      scheduling_policy_(local_queues_),
      reconstruction_policy_([this](const TaskID &task_id) { ResubmitTask(task_id); }),
      task_dependency_manager_(
          object_manager,
          // reconstruction_policy_,
          [this](const TaskID &task_id) { HandleWaitingTaskReady(task_id); }) {
  //// TODO(atumanov): need to add the self-knowledge of ClientID, using nill().
  // cluster_resource_map_[ClientID::nil()] = local_resources_;
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
    client->WriteMessage(MessageType_RegisterClientReply, fbb.GetSize(),
                         fbb.GetBufferPointer());
  } break;
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
  } break;
  case MessageType_DisconnectClient: {
    // Remove the dead worker from the pool and stop listening for messages.
    const std::shared_ptr<Worker> worker = worker_pool_.GetRegisteredWorker(client);
    if (worker) {
      worker_pool_.DisconnectWorker(worker);
    }
  } break;
  case MessageType_SubmitTask: {
    // Read the task submitted by the client.
    auto message = flatbuffers::GetRoot<SubmitTaskRequest>(message_data);
    TaskExecutionSpecification task_execution_spec(
        from_flatbuf(*message->execution_dependencies()));
    TaskSpecification task_spec(*message->task_spec());
    Task task(task_execution_spec, task_spec);
    // Submit the task to the local scheduler.
    SubmitTask(task);
    // Listen for more messages.
    client->ProcessMessages();
  } break;
  default:
    RAY_LOG(FATAL) << "Received unexpected message type " << message_type;
  }
}

void NodeManager::HandleWaitingTaskReady(const TaskID &task_id) {
  auto ready_tasks = local_queues_.RemoveTasks({task_id});
  local_queues_.QueueReadyTasks(std::vector<Task>(ready_tasks));
  // Schedule the newly ready tasks if possible.
  ScheduleTasks();
}

void NodeManager::ScheduleTasks() {
  // Ask policy for scheduling decision.
  // TODO(alexey): Give the policy all cluster resources instead of just the
  // local one.
  std::unordered_map<ClientID, SchedulingResources, UniqueIDHasher> cluster_resource_map;
  cluster_resource_map[ClientID::nil()] = local_resources_;
  const auto &policy_decision = scheduling_policy_.Schedule(cluster_resource_map);
  // Extract decision for this local scheduler.
  // TODO(alexey): Check for this node's own client ID, not for nil.
  std::unordered_set<TaskID, UniqueIDHasher> task_ids;
  for (auto &task_schedule : policy_decision) {
    if (task_schedule.second.is_nil()) {
      task_ids.insert(task_schedule.first);
    }
  }

  // Assign the tasks to workers.
  std::vector<Task> tasks = local_queues_.RemoveTasks(task_ids);
  for (auto &task : tasks) {
    AssignTask(task);
  }
}

void NodeManager::SubmitTask(const Task &task) {
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

  std::shared_ptr<Worker> worker = worker_pool_.PopWorker();
  RAY_LOG(DEBUG) << "Assigning task to worker with pid " << worker->Pid();

  // TODO(swang): Acquire resources for the task.
  // local_resources_.Acquire(task.GetTaskSpecification().GetRequiredResources());

  flatbuffers::FlatBufferBuilder fbb;
  const TaskSpecification &spec = task.GetTaskSpecification();
  auto message = CreateGetTaskReply(fbb, spec.ToFlatbuffer(fbb),
                                    fbb.CreateVector(std::vector<int>()));
  fbb.Finish(message);
  worker->Connection()->WriteMessage(MessageType_ExecuteTask, fbb.GetSize(),
                                     fbb.GetBufferPointer());
  worker->AssignTaskId(spec.TaskId());
  local_queues_.QueueRunningTasks(std::vector<Task>({task}));
}

void NodeManager::FinishTask(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Finished task " << task_id.hex();
  local_queues_.RemoveTasks({task_id});
  // TODO(swang): Release resources that were held for the task.
}

void NodeManager::ResubmitTask(const TaskID &task_id) {
  throw std::runtime_error("Method not implemented");
}

}  // namespace ray
