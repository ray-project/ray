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

bool NodeState::SetComponent(const ReporterInterface *reporter,
                             ReceiverInterface *receiver) {
  if (reporter_ == nullptr && receiver_ == nullptr) {
    reporter_ = reporter;
    receiver_ = receiver;
    return true;
  }
  RAY_LOG(FATAL) << "Fail to set components, reporter:" << reporter
                 << ", receiver:" << receiver;
  return false;
}

std::optional<RaySyncMessage> NodeState::CreateSyncMessage() {
  if (reporter_ == nullptr) {
    return std::nullopt;
  }
  auto message = reporter_->CreateSyncMessage(sync_message_version_taken_);
  if (message != std::nullopt) {
    sync_message_version_taken_ = message->version();
    RAY_LOG(DEBUG) << "Sync message taken: version:" << message->version()
                   << ", node:" << NodeID::FromBinary(message->node_id());
  }
  return message;
}

bool NodeState::RemoveNode(const std::string &node_id) {
  return cluster_view_.erase(node_id) != 0;
}

bool NodeState::ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()];

  RAY_LOG(DEBUG) << "ConsumeSyncMessage: local_version="
                 << (current ? current->version() : -1)
                 << " message_version=" << message->version()
                 << ", message_from=" << NodeID::FromBinary(message->node_id());

  // Check whether newer version of this message has been received.
  if (current && current->version() >= message->version()) {
    RAY_LOG(INFO) << "Dropping sync message with stale version. latest version: "
                  << current->version()
                  << ", dropped message version: " << message->version();
    return false;
  }

  current = message;
  if (receiver_ != nullptr) {
    RAY_LOG(DEBUG).WithField(NodeID::FromBinary(message->node_id()))
        << "Consume message from node";
    receiver_->ConsumeSyncMessage(message);
  }
  return true;
}

}  // namespace ray::syncer
