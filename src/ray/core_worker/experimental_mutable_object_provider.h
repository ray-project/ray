// Copyright 2024 The Ray Authors.
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

#include "ray/core_worker/common.h"
#include "ray/core_worker/experimental_mutable_object_manager.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace core {
namespace experimental {

// This class coordinates the transfer of mutable objects between different nodes. It
// handles mutable objects that are received from remote nodes, and it also observes local
// mutable objects and pushes them to remote nodes as needed.
class MutableObjectProvider {
 public:
  typedef std::function<std::shared_ptr<MutableObjectReaderInterface>(
      const NodeID &node_id)>
      RayletFactory;

  MutableObjectProvider(std::shared_ptr<plasma::PlasmaClientInterface> plasma,
                        const RayletFactory &factory);

  ~MutableObjectProvider();

  std::unique_ptr<rpc::ClientCallManager> &client_call_manager() {
    return client_call_manager_;
  }

  /// Registers a writer channel for `object_id` on this node. On each write to this
  /// channel, the write will be sent via RPC to node `node_id`.
  /// \param[in] object_id The ID of the object.
  /// \param[in] node_id The ID of the node to write to.
  void RegisterWriterChannel(const ObjectID &object_id, const NodeID &node_id);

  /// Registers a reader channel for `object_id` on this node.
  /// \param[in] object_id The ID of the object.
  void RegisterReaderChannel(const ObjectID &object_id);

  /// RPC callback for when a writer pushes a mutable object over the network to a reader
  /// on this node.
  void HandlePushMutableObject(const rpc::PushMutableObjectRequest &request,
                               rpc::PushMutableObjectReply *reply);

 private:
  /// Listens for local changes to `object_id` and sends the changes to remote nodes via
  /// the network.
  void PollWriterClosure(const ObjectID &object_id,
                         std::shared_ptr<MutableObjectReaderInterface> reader);

  // Kicks off `io_service_`.
  void RunIOService();

  // The plasma store.
  std::shared_ptr<plasma::PlasmaClientInterface> plasma_;

  // Object manager for the mutable objects.
  std::unique_ptr<ray::experimental::MutableObjectManager> object_manager_;

  // Creates a function for each object. This object waits for changes on the object and
  // then sends those changes to a remote node via RPC.
  std::function<std::shared_ptr<MutableObjectReaderInterface>(const NodeID &node_id)>
      raylet_client_factory_;
  // Context in which the application looks for local changes to mutable objects and sends
  // the changes to remote nodes via the network.
  instrumented_io_context io_service_;
  // Manages RPCs for inter-node communication of mutable objects.
  boost::asio::io_service::work io_work_;
  // Manages outgoing RPCs that send mutable object changes to remote nodes.
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  // Thread that waits for local mutable object changes and then sends the changes to
  // remote nodes via the network.
  std::thread io_thread_;
};

}  // namespace experimental
}  // namespace core
}  // namespace ray
