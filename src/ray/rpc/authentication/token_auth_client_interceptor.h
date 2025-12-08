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
#include <mutex>
#include <string>
#include <vector>

namespace ray {
namespace rpc {

/// Client interceptor that automatically adds Ray authentication tokens to outgoing RPCs.
/// The token is loaded from AuthenticationTokenLoader and added as a Bearer token
/// in the "authorization" metadata key.
///
/// For performance, the interceptor receives a pointer to a cached authorization header
/// string from its factory, avoiding per-RPC token copying and string allocation.
class RayTokenAuthClientInterceptor : public grpc::experimental::Interceptor {
 public:
  /// \param cached_header Pointer to the factory's cached "Bearer <token>" string.
  ///        Must remain valid for the lifetime of this interceptor.
  explicit RayTokenAuthClientInterceptor(const std::string *cached_header)
      : cached_header_(cached_header) {}

  void Intercept(grpc::experimental::InterceptorBatchMethods *methods) override;

 private:
  const std::string *cached_header_;
};

/// Factory for creating RayTokenAuthClientInterceptor instances.
/// Caches the authorization header string on first interceptor creation to avoid
/// per-RPC overhead of token loading, copying, and string allocation.
class RayTokenAuthClientInterceptorFactory
    : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  grpc::experimental::Interceptor *CreateClientInterceptor(
      grpc::experimental::ClientRpcInfo *info) override;

 private:
  std::string cached_header_;  // Cached "Bearer <token>" string
  std::once_flag init_flag_;   // Ensures thread-safe one-time initialization
};

/// Creates a vector of interceptor factories for token authentication.
/// This should be used when creating gRPC channels with token auth enabled.
std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
CreateTokenAuthInterceptorFactories();

}  // namespace rpc
}  // namespace ray
