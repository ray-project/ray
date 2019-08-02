#ifndef RAY_RAYLET_RAYLET_H
#define RAY_RAYLET_RAYLET_H

#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

// clang-format off
#include "ray/raylet/node_manager.h"
#include "ray/object_manager/object_manager.h"
#include "ray/common/task/scheduling_resources.h"
// clang-format on

namespace ray {

namespace raylet {

using rpc::ClientTableData;

class NodeManager;

class Raylet {
 public:
  /// Create a node manager server and listen for new clients.
  ///
  /// \param main_service The event loop to run the server on.
  /// \param object_manager_service The asio io_service tied to the object manager.
  /// \param socket_name The Unix domain socket to listen on for local clients.
  /// \param node_ip_address The IP address of this node.
  /// \param redis_address The IP address of the redis instance we are connecting to.
  /// \param redis_port The port of the redis instance we are connecting to.
  /// \param redis_password The password of the redis instance we are connecting to.
  /// \param node_manager_config Configuration to initialize the node manager.
  /// scheduler with.
  /// \param object_manager_config Configuration to initialize the object
  /// manager.
  /// \param gcs_client A client connection to the GCS.
  Raylet(boost::asio::io_service &main_service, const std::string &socket_name,
         const std::string &node_ip_address, const std::string &redis_address,
         int redis_port, const std::string &redis_password,
         const NodeManagerConfig &node_manager_config,
         const ObjectManagerConfig &object_manager_config,
         std::shared_ptr<gcs::RedisGcsClient> gcs_client);

  /// Destroy the NodeServer.
  ~Raylet();

 private:
  /// Register GCS client.
  ray::Status RegisterGcs(const std::string &node_ip_address,
                          const std::string &raylet_socket_name,
                          const std::string &object_store_socket_name,
                          const std::string &redis_address, int redis_port,
                          const std::string &redis_password,
                          boost::asio::io_service &io_service, const NodeManagerConfig &);

  ray::Status RegisterPeriodicTimer(boost::asio::io_service &io_service);
  /// Accept a client connection.
  void DoAccept();
  /// Handle an accepted client connection.
  void HandleAccept(const boost::system::error_code &error);

  friend class TestObjectManagerIntegration;

  /// A client connection to the GCS.
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;
  /// The object table. This is shared between the object manager and node
  /// manager.
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
  /// Manages client requests for object transfers and availability.
  ObjectManager object_manager_;
  /// Manages client requests for task submission and execution.
  NodeManager node_manager_;
  /// The name of the socket this raylet listens on.
  std::string socket_name_;

  /// An acceptor for new clients.
  boost::asio::local::stream_protocol::acceptor acceptor_;
  /// The socket to listen on for new clients.
  boost::asio::local::stream_protocol::socket socket_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RAYLET_H
