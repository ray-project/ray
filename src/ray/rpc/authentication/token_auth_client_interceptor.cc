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

#include <grpcpp/support/client_interceptor.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/constants.h"
#include "ray/rpc/authentication/authentication_token_loader.h"

namespace ray {
namespace rpc {

void RayTokenAuthClientInterceptor::Intercept(
    grpc::experimental::InterceptorBatchMethods *methods) {
  if (methods->QueryInterceptionHookPoint(
          grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
    auto token = AuthenticationTokenLoader::instance().GetToken();

    // If token is present and non-empty, add it to the metadata
    if (token.has_value() && !token->empty()) {
      // Get the metadata map and add the authorization header
      auto *metadata = methods->GetSendInitialMetadata();
      metadata->insert(
          std::make_pair(kAuthTokenKey, token->ToAuthorizationHeaderValue()));
    }
  }
  methods->Proceed();
}

grpc::experimental::Interceptor *
RayTokenAuthClientInterceptorFactory::CreateClientInterceptor(
    grpc::experimental::ClientRpcInfo *info) {
  return new RayTokenAuthClientInterceptor();
}

std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
CreateTokenAuthInterceptorFactories() {
  std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
      interceptor_factories;
  interceptor_factories.push_back(
      std::make_unique<RayTokenAuthClientInterceptorFactory>());
  return interceptor_factories;
}

}  // namespace rpc
}  // namespace ray
