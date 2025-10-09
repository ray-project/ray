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

#pragma once

#include <boost/asio.hpp>
#include <memory>
#include <string>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/node_manager.h"

namespace ray::raylet {

using rpc::GcsNodeInfo;
using rpc::NodeSnapshot;

class NodeManager;

class Raylet {
 public:
  /// Create a raylet server and listen for local clients.
  ///
  /// \param main_service The event loop to run the server on.
  /// \param object_manager_service The asio io_service tied to the object manager.
  /// \param socket_name The Unix domain socket to listen on for local clients.
  /// \param node_ip_address The IP address of this node.
  /// \param node_manager_config Configuration to initialize the node manager.
  /// scheduler with.
  /// \param object_manager_config Configuration to initialize the object
  /// manager.
  /// \param gcs_client A client connection to the GCS.
  /// \param metrics_export_port A port at which metrics are exposed to.
  /// \param is_head_node Whether this node is the head node.
  Raylet(instrumented_io_context &main_service,
         const NodeID &self_node_id,
         const std::string &socket_name,
         const std::string &node_ip_address,
         const std::string &node_name,
         const NodeManagerConfig &node_manager_config,
         const ObjectManagerConfig &object_manager_config,
         gcs::GcsClient &gcs_client,
         int metrics_export_port,
         bool is_head_node,
         NodeManager &node_manager);

  /// Start this raylet.
  void Start();

  /// Stop this raylet.
  void Stop();

  /// Unregister this raylet from the GCS.
  ///
  /// \param node_death_info The death information regarding why to unregister self.
  /// \param unregister_done_callback The callback to call when the unregistration is
  /// done.
  void UnregisterSelf(const rpc::NodeDeathInfo &node_death_info,
                      std::function<void()> unregister_done_callback);

  /// Destroy the NodeServer.
  ~Raylet();

  NodeID GetNodeId() const { return self_node_id_; }

  NodeManager &node_manager() { return node_manager_; }

 private:
  /// Register GCS client.
  void RegisterGcs();

  /// Accept a client connection.
  void DoAccept();
  /// Handle an accepted client connection.
  void HandleAccept(const boost::system::error_code &error);

  friend class TestObjectManagerIntegration;

  /// ID of this node.
  NodeID self_node_id_;
  /// Information of this node.
  GcsNodeInfo self_node_info_;

  /// A client connection to the GCS.
  gcs::GcsClient &gcs_client_;
  /// Manages client requests for task submission and execution.
  NodeManager &node_manager_;
  /// The name of the socket this raylet listens on.
  std::string socket_name_;

  /// An acceptor for new clients.
  boost::asio::basic_socket_acceptor<local_stream_protocol> acceptor_;
  /// The socket to listen on for new clients.
  local_stream_socket socket_;
};

}  // namespace ray::raylet
