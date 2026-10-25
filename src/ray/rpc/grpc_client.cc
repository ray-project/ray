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

#include <memory>
#include <string>

#include "ray/rpc/authentication/authentication_mode.h"
#include "ray/rpc/authentication/token_auth_client_interceptor.h"

namespace ray {
namespace rpc {

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
    return grpc::experimental::CreateCustomChannelWithInterceptors(
        target_address, channel_creds, *arguments, CreateTokenAuthInterceptorFactories());
  } else {
    // Create channel without interceptors
    return grpc::CreateCustomChannel(target_address, channel_creds, *arguments);
  }
}

}  // namespace rpc
}  // namespace ray
