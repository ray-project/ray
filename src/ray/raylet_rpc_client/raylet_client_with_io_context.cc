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

#include "ray/raylet_rpc_client/raylet_client_with_io_context.h"

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"

namespace ray {
namespace rpc {

RayletClientWithIoContext::RayletClientWithIoContext(const std::string &ip_address,
                                                     int port) {
  // Connect to the raylet on a singleton io service with a dedicated thread.
  // This is to avoid creating multiple threads for multiple clients in python.
  static InstrumentedIOContextWithThread io_context("raylet_client_io_service");
  instrumented_io_context &io_service = io_context.GetIoService();
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(
      io_service, /*record_stats=*/false, ip_address);
  auto raylet_unavailable_timeout_callback = []() {
    RAY_LOG(WARNING)
        << "Raylet is unavailable for "
        << ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_max_s() << "s";
  };
  rpc::Address rpc_address;
  rpc_address.set_ip_address(ip_address);
  rpc_address.set_port(port);
  raylet_client_ = std::make_unique<rpc::RayletClient>(
      rpc_address, *client_call_manager_, std::move(raylet_unavailable_timeout_callback));
}

void RayletClientWithIoContext::GetWorkerPIDs(
    const rpc::OptionalItemCallback<std::vector<int32_t>> &callback, int64_t timeout_ms) {
  raylet_client_->GetWorkerPIDs(callback, timeout_ms);
}

}  // namespace rpc
}  // namespace ray
