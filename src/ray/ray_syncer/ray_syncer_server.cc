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

#include "ray/ray_syncer/ray_syncer_server.h"

#include <string>
#include <utility>

#include "ray/common/constants.h"

namespace ray::syncer {

namespace {

std::string GetNodeIDFromServerContext(grpc::CallbackServerContext *server_context) {
  const auto &metadata = server_context->client_metadata();
  auto iter = metadata.find("node_id");
  RAY_CHECK(iter != metadata.end());
  return NodeID::FromHex(std::string(iter->second.begin(), iter->second.end())).Binary();
}

}  // namespace

RayServerBidiReactor::RayServerBidiReactor(
    grpc::CallbackServerContext *server_context,
    instrumented_io_context &io_context,
    const std::string &local_node_id,
    std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
    std::function<void(RaySyncerBidiReactor *, bool)> cleanup_cb,
    const std::optional<ray::rpc::AuthenticationToken> &auth_token)
    : RaySyncerBidiReactorBase<ServerBidiReactor>(
          io_context,
          GetNodeIDFromServerContext(server_context),
          std::move(message_processor)),
      cleanup_cb_(std::move(cleanup_cb)),
      server_context_(server_context),
      auth_token_(auth_token) {
  if (auth_token_.has_value() && !auth_token_->empty()) {
    // Validate authentication token
    const auto &metadata = server_context->client_metadata();
    auto it = metadata.find(kAuthTokenKey);
    if (it == metadata.end()) {
      RAY_LOG(WARNING) << "Missing authorization header in syncer connection from node "
                       << NodeID::FromBinary(GetRemoteNodeID());
      Finish(grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                          "Missing authorization header"));
      return;
    }

    const std::string_view header(it->second.data(), it->second.length());
    ray::rpc::AuthenticationToken provided_token =
        ray::rpc::AuthenticationToken::FromMetadata(header);

    if (!auth_token_->Equals(provided_token)) {
      RAY_LOG(WARNING) << "Invalid bearer token in syncer connection from node "
                       << NodeID::FromBinary(GetRemoteNodeID());
      Finish(grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Invalid bearer token"));
      return;
    }
  }

  // Send the local node id to the remote
  server_context_->AddInitialMetadata("node_id", NodeID::FromBinary(local_node_id).Hex());
  StartSendInitialMetadata();

  // Start pulling from remote
  StartPull();
}

void RayServerBidiReactor::DoDisconnect() {
  io_context_.dispatch([this]() { Finish(grpc::Status::OK); }, "");
}

void RayServerBidiReactor::OnCancel() {
  io_context_.dispatch([this]() { Disconnect(); }, "");
}

void RayServerBidiReactor::OnDone() {
  io_context_.dispatch(
      [this, cleanup_cb = cleanup_cb_, remote_node_id = GetRemoteNodeID()]() {
        cleanup_cb(this, false);
        delete this;
      },
      "");
}

}  // namespace ray::syncer
