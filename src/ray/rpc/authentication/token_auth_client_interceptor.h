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

#include <grpcpp/support/client_interceptor.h>

#include <memory>
#include <vector>

namespace ray {
namespace rpc {

/// Client interceptor that automatically adds Ray authentication tokens to outgoing RPCs.
/// The token is loaded from AuthenticationTokenLoader and added as a Bearer token
/// in the "authorization" metadata key.
class RayTokenAuthClientInterceptor : public grpc::experimental::Interceptor {
 public:
  void Intercept(grpc::experimental::InterceptorBatchMethods *methods) override;
};

/// Factory for creating RayTokenAuthClientInterceptor instances
class RayTokenAuthClientInterceptorFactory
    : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  grpc::experimental::Interceptor *CreateClientInterceptor(
      grpc::experimental::ClientRpcInfo *info) override;
};

/// Creates a vector of interceptor factories for token authentication.
/// This should be used when creating gRPC channels with token auth enabled.
std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
CreateTokenAuthInterceptorFactories();

}  // namespace rpc
}  // namespace ray
