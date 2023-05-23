// Copyright 2023 The Ray Authors.
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

#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {
namespace rpc {
grpc::ChannelArguments GetGcsRpcClientArguments() {
  grpc::ChannelArguments arguments = CreateDefaultChannelArguments();
  arguments.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS,
                   ::RayConfig::instance().gcs_grpc_max_reconnect_backoff_ms());
  arguments.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS,
                   ::RayConfig::instance().gcs_grpc_min_reconnect_backoff_ms());
  arguments.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS,
                   ::RayConfig::instance().gcs_grpc_initial_reconnect_backoff_ms());
  return arguments;
}

std::shared_ptr<grpc::Channel> GcsRpcClient::GetDefaultChannel(const std::string &address,
                                                               int port) {
  static std::shared_ptr<grpc::Channel> channel_;
  static std::mutex mu_;

  static std::string address_;
  static int port_;
  std::lock_guard<std::mutex> guard(mu_);
  // Don't reuse channel if proxy or tls is set
  if (::RayConfig::instance().grpc_enable_http_proxy() ||
      ::RayConfig::instance().USE_TLS()) {
    return BuildChannel(address, port, GetGcsRpcClientArguments());
  }

  if (channel_ == nullptr) {
    address_ = address;
    port_ = port;
    channel_ = BuildChannel(address, port, GetGcsRpcClientArguments());
  }

  if (address_ == address && port_ == port) {
    return channel_;
  } else {
    RAY_LOG(WARNING) << "Generate a new GCS channel: " << address << ":" << port
                     << ". Potentially it will increase GCS socket numbers";
    return BuildChannel(address, port, GetGcsRpcClientArguments());
  }
}

}  // namespace rpc
}  // namespace ray
