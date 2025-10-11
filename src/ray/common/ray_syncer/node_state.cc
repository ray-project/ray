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

#include "ray/common/ray_syncer/node_state.h"

#include <string>

#include "ray/common/id.h"
#include "ray/common/ray_syncer/ray_syncer.h"

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

std::optional<MergedRaySyncMessage> NodeState::CreateMergedSyncMessage(
    MessageType message_type) {
  if (reporters_[message_type] == nullptr) {
    return std::nullopt;
  }
  auto inner_message = reporters_[message_type]->CreateInnerSyncMessage(
      sync_message_versions_taken_[message_type], message_type);
  if (inner_message != std::nullopt) {
    sync_message_versions_taken_[message_type] = inner_message->version();
    RAY_LOG(DEBUG) << "Sync message taken: message_type:" << message_type
                   << ", version:" << inner_message->version()
                   << ", node:" << NodeID::FromBinary(inner_message->node_id());
    MergedRaySyncMessage message;
    auto batched_message = message.mutable_batched_messages();
    (*batched_message)[NodeID::FromBinary(inner_message->node_id()).Hex()] =
        std::move(*inner_message);
    return message;
  } else {
    return std::nullopt;
  }
}

bool NodeState::RemoveNode(const std::string &node_id) {
  return cluster_view_.erase(node_id) != 0;
}

bool NodeState::ConsumeMergedSyncMessage(std::shared_ptr<MergedRaySyncMessage> message) {
  auto *mutable_batched_messages = message->mutable_batched_messages();

  int64_t consumed_messages = 0;
  // Iterate through the map, update local state and remove stale messages in one pass
  for (auto it = mutable_batched_messages->begin();
       it != mutable_batched_messages->end();) {
    const auto &inner_message = it->second;
    auto &inner_current =
        cluster_view_[inner_message.node_id()][inner_message.message_type()];

    if (!inner_current || inner_current->version() < inner_message.version()) {
      RAY_LOG(DEBUG) << "ConsumeInnerSyncMessage: local_version="
                     << (inner_current ? inner_current->version() : -1)
                     << " message_version=" << inner_message.version()
                     << ", message_from=" << NodeID::FromBinary(inner_message.node_id())
                     << ", message_type=" << inner_message.message_type();
      // Update the current message to the newer one
      inner_current = std::make_shared<const InnerRaySyncMessage>(inner_message);
      ++consumed_messages;
      auto receiver = receivers_[inner_message.message_type()];
      if (receiver != nullptr) {
        RAY_LOG(DEBUG) << "Consume message from: "
                       << NodeID::FromBinary(inner_message.node_id());
        receiver->ConsumeInnerSyncMessage(inner_current);
      }
      ++it;  // Keep this message, move to next
    } else {
      RAY_LOG(DEBUG) << "Skip to consume inner message from: "
                     << NodeID::FromBinary(inner_message.node_id())
                     << " because the inner message version " << inner_message.version()
                     << " is older than the local version "
                     << (inner_current ? inner_current->version() : -1);
      // Remove stale message and move to next
      it = mutable_batched_messages->erase(it);
    }
  }

  if (consumed_messages == 0) {
    RAY_LOG(DEBUG) << "Skip to consume batched message because all messages are older "
                      "than the local version";
    return false;
  }

  return true;
}

}  // namespace ray::syncer
