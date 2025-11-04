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

#include "ray/ray_syncer/ray_syncer_client.h"

#include <memory>
#include <string>
#include <utility>

#include "ray/rpc/authentication/authentication_token_loader.h"

namespace ray::syncer {

RayClientBidiReactor::RayClientBidiReactor(
    const std::string &remote_node_id,
    const std::string &local_node_id,
    instrumented_io_context &io_context,
    std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
    std::function<void(RaySyncerBidiReactor *, bool)> cleanup_cb,
    std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub)
    : RaySyncerBidiReactorBase<ClientBidiReactor>(
          io_context, remote_node_id, std::move(message_processor)),
      cleanup_cb_(std::move(cleanup_cb)),
      stub_(std::move(stub)) {
  client_context_.AddMetadata("node_id", NodeID::FromBinary(local_node_id).Hex());
  // Add authentication token if token authentication is enabled
  auto auth_token = ray::rpc::AuthenticationTokenLoader::instance().GetToken();
  if (auth_token.has_value() && !auth_token->empty()) {
    auth_token->SetMetadata(client_context_);
  }
  stub_->async()->StartSync(&client_context_, this);
  // Prevent this call from being terminated.
  // Check https://github.com/grpc/proposal/blob/master/L67-cpp-callback-api.md
  // for details.
  AddHold();
  StartPull();
}

void RayClientBidiReactor::OnDone(const grpc::Status &status) {
  io_context_.dispatch(
      [this, status]() {
        cleanup_cb_(this, !status.ok());
        delete this;
      },
      "");
}

void RayClientBidiReactor::DoDisconnect() {
  io_context_.dispatch(
      [this]() {
        StartWritesDone();
        // Free the hold to allow OnDone being called.
        RemoveHold();
      },
      "");
}

}  // namespace ray::syncer
