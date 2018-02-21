#include "node_manager.h"

#include <iostream>

#include <boost/bind.hpp>

#include "common.h"
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
      local_resources_(resource_config) {

//  // Initialize local queues
//  local_queues_ = LsQueue();
//  // Initialize the scheduling policy
//  sched_policy_ = LsPolicy(local_queues_);
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
    auto new_connection = ClientConnection::Create(*this, std::move(socket_), local_resources_.GetWorkerPool());
    new_connection->ProcessMessages();
  }
  // We're ready to accept another client.
  doAccept();
}

void NodeServer::SubmitTask(Task& task) {
  // TODO(swang): Do something with the task.
  // - Ask policy for scheduling decision.
  // - Queue the task.

  // Assign the task to a worker.
  assignTask(task);
}

void NodeServer::assignTask(Task& task) {
  if (local_resources_.GetWorkerPool().PoolSize() == 0) {
    // TODO(swang): Start a new worker and queue this task for future
    // assignment.
    return;
  }

  Worker worker = local_resources_.GetWorkerPool().PopWorker();
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
