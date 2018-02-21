#include "node_manager.h"

#include <iostream>

#include <boost/bind.hpp>

#include "common.h"
#include "common_protocol.h"
#include "format/nm_generated.h"
#include "Task.h"
#include "TaskSpecification.h"

using namespace std;
namespace ray {

NodeServer::NodeServer(boost::asio::io_service& io_service,
                       const std::string &socket_name,
                       const ResourceSet &resource_config)
    : acceptor_(io_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(io_service),
      local_resources_(resource_config),
      worker_pool_(WorkerPool(0)),
      local_queues_(LsQueue()),
      sched_policy_(local_queues_) {

  // TODO(atumanov): need to add the self-knowledge of DBClientID, using nill().
  cluster_resource_map_[DBClientID::nil()] = local_resources_;

  // Start listening for clients.
  doAccept();
}

void NodeServer::doAccept() {
  acceptor_.async_accept(socket_,
      boost::bind(&NodeServer::handleAccept, this, boost::asio::placeholders::error)
      );
}

void NodeServer::handleAccept(const boost::system::error_code& error) {
  if (!error) {
    // Accept a new client.
    auto new_connection = ClientConnection::Create(*this, std::move(socket_));
    new_connection->ProcessMessages();
  }
  // We're ready to accept another client.
  doAccept();
}

void NodeServer::ProcessClientMessage(shared_ptr<ClientConnection> client, int64_t message_type, const uint8_t *message_data) {
  LOG_INFO("Message of type %" PRId64, message_type);

  switch (message_type) {
  case MessageType_RegisterClientRequest: {
    auto message = flatbuffers::GetRoot<RegisterClientRequest>(message_data);
    // Create a new worker from the registration request.
    Worker worker(message->worker_pid(), client);
    // Add the new worker to the pool.
    worker_pool_.AddWorker(std::move(worker));

    // Build the reply to the worker's registration request. TODO(swang): This
    // is legacy code and should be removed once actor creation tasks are
    // implemented.
    flatbuffers::FlatBufferBuilder fbb;
    auto reply =
        CreateRegisterClientReply(fbb, fbb.CreateVector(std::vector<int>()));
    fbb.Finish(reply);
    // Reply to the worker's registration request, then listen for more
    // messages.
    client->WriteMessage(MessageType_RegisterClientReply, fbb.GetSize(), fbb.GetBufferPointer());
  } break;
  case MessageType_DisconnectClient: {
    // Remove the dead worker from the pool and stop listening for messages.
    worker_pool_.RemoveWorker(client);
  } break;
  case MessageType_SubmitTask: {
    // Read the task submitted by the client.
    auto message = flatbuffers::GetRoot<SubmitTaskRequest>(message_data);
    TaskExecutionSpecification task_execution_spec(from_flatbuf(*message->execution_dependencies()));
    TaskSpecification task_spec(*message->task_spec());
    Task task(task_execution_spec, task_spec);
    // Submit the task to the local scheduler.
    submitTask(task);
  } break;
  default:
    CHECK(0);
  }
}

void NodeServer::submitTask(Task& task) {
  local_queues_.QueueReadyTasks(std::vector<Task>({task}));
  // - Ask policy for scheduling decision.

  const auto &sched_policy_decision = sched_policy_.Schedule(cluster_resource_map_);
  // Extract decision for this local scheduler.
  TaskID task_schedule = sched_policy_decision.at(DBClientID::nil());

  // TODO(alexey): Get the task IDs to schedule from the policy.
  std::unordered_set<TaskID, UniqueIDHasher> task_ids;
  task_ids.insert(task_schedule);

  // Assign the tasks to a worker.
  std::vector<Task> tasks = local_queues_.RemoveTasks(task_ids);
  for (auto &task : tasks) {
    assignTask(task);
  }
}

void NodeServer::assignTask(Task& task) {
  if (worker_pool_.PoolSize() == 0) {
    // TODO(swang): Start a new worker and queue this task for future
    // assignment.
    return;
  }

  Worker worker = worker_pool_.PopWorker();
  LOG_INFO("Assigning task to worker with pid %d", worker.Pid());

  // TODO(swang): Acquire resources for the task.

  flatbuffers::FlatBufferBuilder fbb;
  const TaskSpecification &spec = task.GetTaskSpecification();
  auto message =
      CreateGetTaskReply(fbb, spec.ToFlatbuffer(fbb),
                         fbb.CreateVector(std::vector<int>()));
  fbb.Finish(message);
  worker.Connection()->WriteMessage(MessageType_ExecuteTask, fbb.GetSize(), fbb.GetBufferPointer());
}

} // end namespace ray
