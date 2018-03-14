#ifndef RAY_RAYLET_RAYLET_H
#define RAY_RAYLET_RAYLET_H

#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

// clang-format off
#include "ray/raylet/node_manager.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/scheduling_resources.h"
// clang-format on

namespace ray {

namespace raylet {

class Task;
class NodeManager;

// TODO(swang): Rename class and source files to Raylet.
class Raylet {
 public:
  /// Create a node manager server and listen for new clients.
  ///
  /// \param io_service The event loop to run the server on.
  /// \param socket_name The Unix domain socket to listen on for local clients.
  /// \param resource_config The initial set of resources to start the local
  /// scheduler with.
  /// \param object_manager_config Configuration to initialize the object
  /// manager.
  /// \param gcs_client A client connection to the GCS.
  Raylet(boost::asio::io_service &io_service, const std::string &socket_name,
         const ResourceSet &resource_config,
         const ObjectManagerConfig &object_manager_config,
         std::shared_ptr<gcs::AsyncGcsClient> gcs_client);

  /// Destroy the NodeServer.
  ~Raylet();

  // TODO(melih): Get rid of this method.
  ObjectManager &GetObjectManager();

 private:
  /// Register GCS client.
  ClientID RegisterGcs(boost::asio::io_service &io_service);
  /// Accept a client connection.
  void DoAccept();
  /// Handle an accepted client connection.
  void HandleAccept(const boost::system::error_code &error);
  /// Accept a tcp client connection.
  void DoAcceptTcp();
  /// Handle an accepted tcp client connection.
  void HandleAcceptTcp(TCPClientConnection::pointer new_connection,
                       const boost::system::error_code &error);
  void DoAcceptNodeManager();
  void HandleAcceptNodeManager(const boost::system::error_code &error);

  /// An acceptor for new clients.
  boost::asio::local::stream_protocol::acceptor acceptor_;
  /// The socket to listen on for new clients.
  boost::asio::local::stream_protocol::socket socket_;
  /// An acceptor for new tcp clients.
  boost::asio::ip::tcp::acceptor tcp_acceptor_;
  /// The socket to listen on for new tcp clients.
  boost::asio::ip::tcp::socket tcp_socket_;
  /// An acceptor for new tcp clients.
  boost::asio::ip::tcp::acceptor node_manager_acceptor_;
  /// The socket to listen on for new tcp clients.
  boost::asio::ip::tcp::socket node_manager_socket_;

  /// A client connection to the GCS.
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  // TODO(swang): Lineage cache.
  LineageCache lineage_cache_;
  /// Manages client requests for object transfers and availability.
  ObjectManager object_manager_;
  /// Manages client requests for task submission and execution.
  NodeManager node_manager_;
};

} // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RAYLET_H
