#ifndef RAY_NODE_MANAGER_H
#define RAY_NODE_MANAGER_H

#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

#include "local_scheduler.h"
#include "ray/om/object_manager.h"
#include "LsResources.h"

namespace ray {

class Task;
class LocalScheduler;

class NodeServer {
 public:
  /// Create a node manager server and listen for new clients.
  NodeServer(boost::asio::io_service& io_service,
             const std::string &socket_name,
             const ResourceSet &resource_config,
             const OMConfig &om_config,
             shared_ptr<ray::GcsClient> gcs_client,
             shared_ptr<ray::ObjectDirectory> od);

  ObjectManager &GetObjectManager();
  void Terminate();

 private:
  /// Register GCS client.
  ClientID RegisterGcs();
  /// Accept a client connection.
  void DoAccept();
  /// Handle an accepted client connection.
  void HandleAccept(const boost::system::error_code &error);
  /// Accept a tcp client connection.
  void DoAcceptTcp();
  /// Handle an accepted tcp client connection.
  void HandleAcceptTcp(TCPClientConnection::pointer new_connection,
                       const boost::system::error_code& error);

  /// An acceptor for new clients.
  boost::asio::local::stream_protocol::acceptor acceptor_;
  /// The socket to listen on for new clients.
  boost::asio::local::stream_protocol::socket socket_;
  /// An acceptor for new tcp clients.
  boost::asio::ip::tcp::acceptor tcp_acceptor_;
  /// The socket to listen on for new tcp clients.
  boost::asio::ip::tcp::socket tcp_socket_;

  // TODO(swang): Object directory.
  // TODO(swang): GCS client.
  // TODO(swang): Lineage cache.
  // Manages client requests for object transfers and availability.
  ObjectManager object_manager_;
  // Manages client requests for task submission and execution.
  LocalScheduler local_scheduler_;
  shared_ptr<ray::GcsClient> gcs_client_;
};

} // end namespace ray

#endif  // RAY_NODE_MANAGER_H
