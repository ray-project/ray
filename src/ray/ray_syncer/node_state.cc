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

#include "ray/ray_syncer/node_state.h"

#include <string>

#include "ray/common/id.h"
#include "ray/ray_syncer/ray_syncer.h"

namespace ray::syncer {

NodeState::NodeState() { sync_message_versions_taken_.fill(-1); }

bool NodeState::SetComponent(MessageType message_type,
                             const ReporterInterface *reporter,
                             ReceiverInterface *receiver) {
  if (message_type < static_cast<MessageType>(kComponentArraySize) &&
      reporters_[message_type] == nullptr && receivers_[message_type] == nullptr) {
    reporters_[message_type] = reporter;
    receivers_[message_type] = receiver;
    return true;
  }
  RAY_LOG(FATAL) << "Fail to set components, message_type:" << message_type
                 << ", reporter:" << reporter << ", receiver:" << receiver;
  return false;
}

std::optional<RaySyncMessage> NodeState::CreateSyncMessage(MessageType message_type) {
  if (reporters_[message_type] == nullptr) {
    return std::nullopt;
  }
  auto message = reporters_[message_type]->CreateSyncMessage(
      sync_message_versions_taken_[message_type], message_type);
  if (message != std::nullopt) {
    sync_message_versions_taken_[message_type] = message->version();
    RAY_LOG(DEBUG) << "Sync message taken: message_type:" << message_type
                   << ", version:" << message->version()
                   << ", node:" << NodeID::FromBinary(message->node_id());
  }
  return message;
}

bool NodeState::RemoveNode(const std::string &node_id) {
  return cluster_view_.erase(node_id) != 0;
}

bool NodeState::ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()][message->message_type()];

  RAY_LOG(DEBUG) << "ConsumeSyncMessage: local_version="
                 << (current ? current->version() : -1)
                 << " message_version=" << message->version()
                 << ", message_from=" << NodeID::FromBinary(message->node_id());
  // Check whether newer version of this message has been received.
  if (current && current->version() >= message->version()) {
    return false;
  }

  current = message;
  auto receiver = receivers_[message->message_type()];
  if (receiver != nullptr) {
    RAY_LOG(DEBUG).WithField(NodeID::FromBinary(message->node_id()))
        << "Consume message from node";
    receiver->ConsumeSyncMessage(message);
  }
  return true;
}

}  // namespace ray::syncer
