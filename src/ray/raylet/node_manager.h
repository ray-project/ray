#ifndef RAY_NODE_MANAGER_H
#define RAY_NODE_MANAGER_H

#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "local_scheduler.h"
#include "object_manager.h"
#include "LsResources.h"

using namespace std;
namespace ray {

class Task;
class LocalScheduler;

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

  /// An acceptor for new clients.
  boost::asio::local::stream_protocol::acceptor acceptor_;
  /// The socket to listen on for new clients.
  boost::asio::local::stream_protocol::socket socket_;

  // TODO(swang): Object directory.
  // TODO(swang): GCS client.
  // TODO(swang): Lineage cache.
  // Manages client requests for object transfers and availability.
  ObjectManager object_manager_;
  // Manages client requests for task submission and execution.
  LocalScheduler local_scheduler_;
};

} // end namespace ray

#endif  // RAY_NODE_MANAGER_H
