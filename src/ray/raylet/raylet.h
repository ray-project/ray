// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

using rpc::GcsNodeInfo;

class NodeManager;

class Raylet {
 public:
  /// Create a raylet server and listen for local clients.
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
         std::shared_ptr<gcs::GcsClient> gcs_client);

  /// Start this raylet.
  void Start();

  /// Stop this raylet.
  void Stop();

  /// Destroy the NodeServer.
  ~Raylet();

 private:
  /// Register GCS client.
  ray::Status RegisterGcs();

  /// Accept a client connection.
  void DoAccept();
  /// Handle an accepted client connection.
  void HandleAccept(const boost::system::error_code &error);

  friend class TestObjectManagerIntegration;

  /// ID of this node.
  ClientID self_node_id_;
  /// Information of this node.
  GcsNodeInfo self_node_info_;

  /// A client connection to the GCS.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
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
  boost::asio::basic_socket_acceptor<local_stream_protocol> acceptor_;
  /// The socket to listen on for new clients.
  local_stream_socket socket_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_RAYLET_H
