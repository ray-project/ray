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

#include <atomic>
#include <optional>
#include <string>

#include "ray/ray_syncer/common.h"
#include "ray/ray_syncer/ray_syncer_bidi_reactor.h"
#include "ray/ray_syncer/ray_syncer_bidi_reactor_base.h"
#include "ray/rpc/authentication/authentication_token.h"

namespace ray::syncer {

using ServerBidiReactor = grpc::ServerBidiReactor<RaySyncMessage, RaySyncMessage>;

/// Reactor for gRPC server side. It defines the server's specific behavior for a
/// streaming call.
class RayServerBidiReactor : public RaySyncerBidiReactorBase<ServerBidiReactor> {
 public:
  RayServerBidiReactor(
      grpc::CallbackServerContext *server_context,
      instrumented_io_context &io_context,
      const std::string &local_node_id,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(RaySyncerBidiReactor *, bool)> cleanup_cb,
      const std::optional<ray::rpc::AuthenticationToken> &auth_token);

  ~RayServerBidiReactor() override = default;

  bool IsFinished() const { return finished_.load(); }

 private:
  void DoDisconnect() override;
  void OnCancel() override;
  void OnDone() override;

  void Finish(grpc::Status status) {
    finished_.store(true);
    ServerBidiReactor::Finish(status);
  }

  /// Cleanup callback when the call ends.
  const std::function<void(RaySyncerBidiReactor *, bool)> cleanup_cb_;

  /// grpc callback context
  grpc::CallbackServerContext *server_context_;

  /// Authentication token for validation, will be empty if token authentication is
  /// disabled
  std::optional<ray::rpc::AuthenticationToken> auth_token_;

  /// Track if Finish() has been called to avoid using a reactor that is terminating
  std::atomic<bool> finished_{false};

  FRIEND_TEST(SyncerReactorTest, TestReactorFailure);
};

}  // namespace ray::syncer
