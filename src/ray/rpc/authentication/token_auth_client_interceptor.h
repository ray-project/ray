// Copyright 2025 The Ray Authors.
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

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "ray/common/constants.h"
#include "ray/rpc/authentication/authentication_token.h"

namespace ray {
namespace rpc {

/// gRPC MetadataCredentialsPlugin that adds Ray authentication tokens to outgoing RPCs.
///
/// The token is cached at plugin creation time and ToAuthorizationHeaderValue() is
/// called on each RPC to generate the header.
class RayTokenAuthPlugin : public grpc::MetadataCredentialsPlugin {
 public:
  /// \param token The authentication token to use for all RPCs on this channel.
  explicit RayTokenAuthPlugin(std::optional<AuthenticationToken> token)
      : token_(std::move(token)) {}

  grpc::Status GetMetadata(grpc::string_ref service_url,
                           grpc::string_ref method_name,
                           const grpc::AuthContext &channel_auth_context,
                           std::multimap<grpc::string, grpc::string> *metadata) override {
    if (token_.has_value() && !token_->empty()) {
      metadata->insert(
          std::make_pair(kAuthTokenKey, token_->ToAuthorizationHeaderValue()));
    }
    return grpc::Status::OK;
  }

  bool IsBlocking() const override { return false; }

  grpc::string DebugString() override { return "RayTokenAuthPlugin"; }

 private:
  std::optional<AuthenticationToken> token_;
};

/// Creates call credentials with the Ray token auth plugin.
/// Returns nullptr if token auth is not enabled.
std::shared_ptr<grpc::CallCredentials> CreateTokenAuthCallCredentials();

}  // namespace rpc
}  // namespace ray
