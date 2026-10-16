// Copyright The Ray Authors.
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

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "ray/ray_syncer/ray_syncer.h"
#include "ray/ray_syncer/ray_syncer_bidi_reactor_base.h"

namespace ray {
namespace syncer {

// Reporter whose CreateSyncMessage behavior is injected via a callback. The
// callback receives the current_version argument.
class FakeReporterInterface : public ReporterInterface {
 public:
  // Invoked with the `current_version` argument; returns std::nullopt if unset.
  std::function<std::optional<RaySyncMessage>(int64_t current_version)>
      create_sync_message_fn;

  std::optional<RaySyncMessage> CreateSyncMessage(
      int64_t current_version, MessageType message_type) const override {
    if (create_sync_message_fn) {
      return create_sync_message_fn(current_version);
    }
    return std::nullopt;
  }
};

// Receiver whose ConsumeSyncMessages behavior is injected via a callback.
class FakeReceiverInterface : public ReceiverInterface {
 public:
  std::function<void(MessageType message_type,
                     std::vector<std::shared_ptr<const RaySyncMessage>> messages)>
      consume_sync_messages_fn;

  void ConsumeSyncMessages(
      MessageType message_type,
      std::vector<std::shared_ptr<const RaySyncMessage>> messages) override {
    if (consume_sync_messages_fn) {
      consume_sync_messages_fn(message_type, std::move(messages));
    }
  }
};

// Reactor base that no-ops DoDisconnect so the real sending/dedup logic can be
// exercised without a live gRPC stream.
template <typename T>
class FakeRaySyncerBidiReactorBase : public RaySyncerBidiReactorBase<T> {
 public:
  using RaySyncerBidiReactorBase<T>::RaySyncerBidiReactorBase;

  void DoDisconnect() override {}
};

}  // namespace syncer
}  // namespace ray
