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
  static std::mutex mu_;
  static std::shared_ptr<grpc::Channel> channel_;
  static std::string address_;
  static int port_ = 0;

  // Don't reuse channel if proxy or tls is set
  // TODO: Reuse the channel even it's tls.
  // Right now, if we do this, python/ray/serve/tests/test_grpc.py
  // will fail.
  if (::RayConfig::instance().grpc_enable_http_proxy() ||
      ::RayConfig::instance().USE_TLS()) {
    return BuildChannel(address, port, GetGcsRpcClientArguments());
  }

  std::lock_guard<std::mutex> guard(mu_);
  if (channel_ == nullptr || (address_ != address || port_ != port)) {
    address_ = address;
    port_ = port;

    // This condition shouldn't happen in most cases. It could only happen when
    // ray driver wanted to talk with different GCS.
    // - This mostly happens in testing, where the test main process is the driver.
    //   It calls ray.init and then ray.shutdown and later ray.init with a different
    //   GCS address.
    // - Potentially it can also happen in the user's driver where there are two
    //   ray clusters and the user ray.init and ray.shutdown and then tries to
    //   connect to a different GCS.
    if (channel_ != nullptr) {
      RAY_LOG(WARNING) << "Generate a new GCS channel: " << address << ":" << port
                       << ". Potentially it will increase GCS socket numbers."
                       << " This could only happen in testing or in the same driver "
                       << " it tries to connect to different GCS clusters.";
    }
    channel_ = BuildChannel(address, port, GetGcsRpcClientArguments());
  }

  return channel_;
}

}  // namespace rpc
}  // namespace ray
