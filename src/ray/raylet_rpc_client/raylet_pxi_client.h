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

#include <boost/asio.hpp>
#include <map>
#include <string>
#include <thread>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/node_manager.grpc.pb.h"

namespace ray {
namespace rpc {

/// A minimal, self-contained RPC client for PXI/Cython bindings.
/// - Owns its IO context and thread.
/// - Provides a synchronous, Cython-friendly ResizeLocalResourceInstances.
class RayletPXIClient {
 public:
  /// A simple C++ struct for Cython-friendly return value.
  struct ResizeResult {
    /// 0 means OK; non-zero indicates RPC or server-side error.
    int status_code = 0;
    /// Human-readable message for non-OK status.
    std::string message = std::string();
    /// Updated total resources returned by the server.
    std::map<std::string, double> total_resources = {};
  };

  /// Construct a client connected to the target host:port.
  /// Spawns background thread to run the IO context.
  RayletPXIClient(const std::string &host, int port);

  ~RayletPXIClient();

  /// Synchronously resize local resource instances on the target raylet.
  /// Returns a Cython-friendly struct with status and a plain map.
  ResizeResult ResizeLocalResourceInstances(
      const std::map<std::string, double> &resources);

 private:
  /// Event loop for client callbacks.
  instrumented_io_context io_service_;
  /// Manager for gRPC calls (owns polling threads).
  ClientCallManager client_call_manager_;
  /// Underlying gRPC client for NodeManagerService.
  GrpcClient<NodeManagerService> grpc_client_;
  /// Background thread running io_service_.
  std::thread io_thread_;
  /// Guard to keep io_service_ alive.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
};

}  // namespace rpc
}  // namespace ray
