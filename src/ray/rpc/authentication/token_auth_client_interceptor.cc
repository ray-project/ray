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

#include "ray/rpc/authentication/token_auth_client_interceptor.h"

#include <map>
#include <memory>
#include <utility>

#include "ray/rpc/authentication/authentication_token_loader.h"

namespace ray {
namespace rpc {

std::shared_ptr<grpc::CallCredentials> CreateTokenAuthCallCredentials() {
  auto token = AuthenticationTokenLoader::instance().GetToken();
  if (!token.has_value() || token->empty()) {
    return nullptr;
  }
  return grpc::MetadataCredentialsFromPlugin(
      std::make_unique<RayTokenAuthPlugin>(std::move(token)));
}

}  // namespace rpc
}  // namespace ray
