#ifndef RAY_NODE_MANAGER_H
#define RAY_NODE_MANAGER_H

#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "Worker.h"
#include "WorkerPool.h"
#include "LsResources.h"

using namespace std;
namespace ray {

class NodeServer;

class NodeServer {
 public:
  /// Create a node manager server and listen for new clients.
  NodeServer(boost::asio::io_service& io_service,
             const std::string &socket_name,
             const ResourceSet &resource_config);

 private:
  /// Accept a client connection.
  void doAccept();
  /// Handle an accepted client connection.
  void handleAccept(const boost::system::error_code& error);

  /// The list of active clients.
  std::list<std::unique_ptr<ClientConnection>> clients_;
  /// An acceptor for new clients.
  boost::asio::local::stream_protocol::acceptor acceptor_;
  /// The socket to listen on for new clients.
  boost::asio::local::stream_protocol::socket socket_;
  WorkerPool worker_pool_;
  // TODO(atumanov): LsResources should replace/provide worker pool.
  LsResources local_resources_;
};

} // end namespace ray

#endif  // RAY_NODE_MANAGER_H
