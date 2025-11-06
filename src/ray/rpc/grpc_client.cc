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

#include "ray/rpc/grpc_client.h"

#include <grpcpp/support/client_interceptor.h>

#include "ray/common/constants.h"
#include "ray/rpc/authentication/authentication_mode.h"
#include "ray/rpc/authentication/authentication_token_loader.h"

namespace ray {
namespace rpc {

namespace {

/// Client interceptor that automatically adds Ray authentication tokens to outgoing RPCs.
/// The token is loaded from AuthenticationTokenLoader and added as a Bearer token
/// in the "authorization" metadata key.
class RayTokenAuthClientInterceptor : public grpc::experimental::Interceptor {
 public:
  void Intercept(grpc::experimental::InterceptorBatchMethods *methods) override {
    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      auto token = AuthenticationTokenLoader::instance().GetToken();

      // If token is present and non-empty, add it to the metadata
      if (token.has_value() && !token->empty()) {
        // Get the metadata map and add the authorization header
        auto *metadata = methods->GetSendInitialMetadata();
        metadata->insert(std::make_pair(kAuthTokenKey,
                                        token->ToAuthorizationHeaderValue()));
      }
    }
    methods->Proceed();
  }
};

/// Factory for creating RayAuthClientInterceptor instances
class RayTokenAuthClientInterceptorFactory
    : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  grpc::experimental::Interceptor *CreateClientInterceptor(
      grpc::experimental::ClientRpcInfo *info) override {
    return new RayTokenAuthClientInterceptor();
  }
};

}  // namespace

std::shared_ptr<grpc::Channel> BuildChannel(
    const std::string &address,
    int port,
    std::optional<grpc::ChannelArguments> arguments) {
  // Set up channel arguments with default values if not provided
  if (!arguments.has_value()) {
    arguments = grpc::ChannelArguments();
  }

  arguments->SetInt(GRPC_ARG_ENABLE_HTTP_PROXY,
                    ::RayConfig::instance().grpc_enable_http_proxy() ? 1 : 0);
  arguments->SetMaxSendMessageSize(::RayConfig::instance().max_grpc_message_size());
  arguments->SetMaxReceiveMessageSize(::RayConfig::instance().max_grpc_message_size());
  arguments->SetInt(GRPC_ARG_HTTP2_WRITE_BUFFER_SIZE,
                    ::RayConfig::instance().grpc_stream_buffer_size());

  // Step 1: Get transport credentials (TLS or insecure)
  std::shared_ptr<grpc::ChannelCredentials> channel_creds;

  if (::RayConfig::instance().USE_TLS()) {
    std::string server_cert_file = std::string(::RayConfig::instance().TLS_SERVER_CERT());
    std::string server_key_file = std::string(::RayConfig::instance().TLS_SERVER_KEY());
    std::string root_cert_file = std::string(::RayConfig::instance().TLS_CA_CERT());
    std::string server_cert_chain = ReadCert(server_cert_file);
    std::string private_key = ReadCert(server_key_file);
    std::string cacert = ReadCert(root_cert_file);

    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = cacert;
    ssl_opts.pem_private_key = private_key;
    ssl_opts.pem_cert_chain = server_cert_chain;
    channel_creds = grpc::SslCredentials(ssl_opts);
  } else {
    channel_creds = grpc::InsecureChannelCredentials();
  }

  // Step 2: Create channel with interceptors if token auth is enabled
  std::string target_address = BuildAddress(address, port);

  if (GetAuthenticationMode() == AuthenticationMode::TOKEN) {
    // Create channel with auth interceptor
    std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
        interceptor_factories;
    interceptor_factories.push_back(
        std::make_unique<RayTokenAuthClientInterceptorFactory>());

    return grpc::experimental::CreateCustomChannelWithInterceptors(
        target_address, channel_creds, *arguments, std::move(interceptor_factories));
  } else {
    // Create channel without interceptors
    return grpc::CreateCustomChannel(target_address, channel_creds, *arguments);
  }
}

}  // namespace rpc
}  // namespace ray
