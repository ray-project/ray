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
  flatbuffers::FlatBufferBuilder fbb;
  TaskSpecification spec = task.GetTaskSpecification();
  auto message =
      CreateGetTaskReply(fbb, fbb.CreateString(spec.Data(), spec.Size()),
                         fbb.CreateVector(std::vector<int>()));
  fbb.Finish(message);
  LOG_INFO("fbb with size %d", fbb.GetSize());
}

} // end namespace ray

#ifndef NODE_MANAGER_TEST
int main(int argc, char *argv[]) {
  CHECK(argc == 2);

  boost::asio::io_service io_service;
  std::unordered_map<std::string, double> static_resource_conf;
  static_resource_conf = {{"num_cpus", 1}, {"num_gpus", 1}};
  ray::ResourceSet resource_config(static_resource_conf);

  // Initialize the node manager.
  ray::NodeServer server(io_service, std::string(argv[1]), resource_config);
  io_service.run();
  return 0;
}
#endif
