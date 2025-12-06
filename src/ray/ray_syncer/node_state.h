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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "ray/ray_syncer/common.h"
#include "src/ray/protobuf/ray_syncer.grpc.pb.h"

namespace ray::syncer {

// Forward declaration.
struct ReporterInterface;
struct ReceiverInterface;

using ray::rpc::syncer::RaySyncMessage;
using ray::rpc::syncer::ResourceViewSyncMessage;

/// NodeState keeps track of the modules in the local nodes.
/// It contains the local components for receiving and reporting.
/// It also keeps the raw messages receivers got.
class NodeState {
 public:
  /// Constructor of NodeState.
  NodeState() = default;

  /// Set the local component for resource view synchronization.
  ///
  /// \param reporter The reporter is defined to be the local module which wants to
  /// broadcast its internal status to the whole cluster. When it's null, it means there
  /// is no reporter in the local node. This is the place where messages are generated.
  /// \param receiver The receiver is defined to be the module which eventually
  /// will have the view of the cluster. It's the place where received messages are
  /// consumed.
  ///
  /// \return true if set successfully.
  bool SetComponent(const ReporterInterface *reporter, ReceiverInterface *receiver);

  /// Get the snapshot of the resource view for a newer version.
  ///
  /// \return If a snapshot is taken, return the message, otherwise std::nullopt.
  std::optional<RaySyncMessage> CreateSyncMessage();

  /// Consume a message. Receiver will consume this message if it doesn't have
  /// this message.
  ///
  /// \param message The message received.
  ///
  /// \return true if the local node doesn't have message with newer version.
  bool ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message);

  /// Return the cluster view of this local node.
  const absl::flat_hash_map<std::string, std::shared_ptr<const RaySyncMessage>>
      &GetClusterView() const {
    return cluster_view_;
  }

  /// Remove a node from the cluster view.
  bool RemoveNode(const std::string &node_id);

 private:
  /// Reporter for the local node (generates sync messages).
  const ReporterInterface *reporter_ = nullptr;
  /// Receiver for the local node (consumes sync messages from other nodes).
  ReceiverInterface *receiver_ = nullptr;

  /// This field records the version of the sync message that has been taken.
  int64_t sync_message_version_taken_ = -1;

  /// Keep track of the latest messages received from each node.
  /// Use shared pointer for easier liveness management since these messages might be
  /// sending via rpc.
  absl::flat_hash_map<std::string, std::shared_ptr<const RaySyncMessage>> cluster_view_;
};

}  // namespace ray::syncer
