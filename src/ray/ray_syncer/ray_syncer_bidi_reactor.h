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

#include <gtest/gtest_prod.h>

#include <memory>
#include <string>
#include <utility>

#include "ray/ray_syncer/common.h"
#include "src/ray/protobuf/ray_syncer.grpc.pb.h"

namespace ray::syncer {

using ray::rpc::syncer::CommandsSyncMessage;
using ray::rpc::syncer::MessageType;
using ray::rpc::syncer::RaySyncMessage;
using ray::rpc::syncer::ResourceViewSyncMessage;

/// This is the base class for the bidi-streaming call and defined the method
/// needed. A bidi-stream for ray syncer needs to support pushing message and
/// disconnect from the remote node.
/// For the implementation, in the constructor, it needs to connect to the remote
/// node and it needs to implement the communication between the two nodes.
///
/// Please refer to https://github.com/grpc/proposal/blob/master/L67-cpp-callback-api.md
/// for the callback API
///
// clang-format off
/// For the server side:
///                                     grpc end (error or request)
///                       +---------------------------------------------------------------+
///                       |                                                               v
/// +------------+      +-------------+  canceled by client            +----------+     +--------+     +--------+
/// | StartRead  | <--> | OnReadDone  | -----------------------------> | OnCancel | --> | Finish | --> | OnDone |
/// +------------+      +-------------+                                +----------+     +--------+     +--------+
///                                     canceled by client               ^                ^
///                       +----------------------------------------------+                |
///                       |                                                               |
/// +------------+      +-------------+  grpc end (error or request)                      |
/// | StartWrite | <--> | OnWriteDone | --------------------------------------------------+
/// +------------+      +-------------+
///
///
/// For the client side:
/// +------------+      +-------------+       +------------+  gRPC error or ALL incoming data read   +--------+
/// | StartCall  | ---> |  StartRead  | <---> | OnReadDone | --------------------------------------> | OnDone |
/// +------------+      +-------------+       +------------+                                         +--------+
///   |                                                                                                   ^
///   |                                                                                                   |
///   v                                                                                                   |
/// +------------+      +-------------+  gRPC error or disconnected                                       |
/// | StartWrite | <--> | OnWriteDone | ------------------------------------------------------------------+
/// +------------+      +-------------+
// clang-format on
class RaySyncerBidiReactor {
 public:
  explicit RaySyncerBidiReactor(std::string remote_node_id)
      : remote_node_id_(std::move(remote_node_id)) {}

  virtual ~RaySyncerBidiReactor() = default;

  /// Push a message to the sending queue to be sent later. Some message
  /// might be dropped if the module think the target node has already got the
  /// information. Usually it'll happen when the message has the source node id
  /// as the target or the message is sent from the remote node.
  ///
  /// \param message The message to be sent.
  ///
  /// \return true if push to queue successfully.
  virtual bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message) = 0;

  /// Return the remote node id of this connection.
  const std::string &GetRemoteNodeID() const { return remote_node_id_; }

  /// Disconnect will terminate the communication between local and remote node.
  /// It also needs to do proper cleanup.
  void Disconnect() {
    if (!*disconnected_) {
      *disconnected_ = true;
      DoDisconnect();
    }
  };

  /// Set rpc completion callback, which is called after rpc read finishes.
  /// This function is expected to call only once, repeated invocations will check fail.
  void SetRpcCompletionCallbackForOnce(RpcCompletionCallback on_rpc_completion) {
    RAY_CHECK(on_rpc_completion);
    RAY_CHECK(!on_rpc_completion_);
    on_rpc_completion_ = std::move(on_rpc_completion);
  }

  /// Return true if it's disconnected.
  std::shared_ptr<bool> IsDisconnected() const { return disconnected_; }

  // Node id which is communicating with the current reactor.
  std::string remote_node_id_;

 protected:
  /// Sync message observer, which is a callback on received message response.
  RpcCompletionCallback on_rpc_completion_;

 private:
  virtual void DoDisconnect() = 0;
  std::shared_ptr<bool> disconnected_ = std::make_shared<bool>(false);

  FRIEND_TEST(SyncerReactorTest, TestReactorFailure);
};

}  // namespace ray::syncer
