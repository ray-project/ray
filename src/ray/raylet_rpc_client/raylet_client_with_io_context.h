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

#include "ray/raylet_rpc_client/raylet_client.h"
#include "ray/rpc/grpc_client.h"

namespace ray {
namespace rpc {

/// Raylet client with io context is provided for python (e.g. ReporterAgent) to
/// communicate with raylet. It creates and manages a separate thread to run the grpc
/// event loop
class RayletClientWithIoContext {
 public:
  /// Connect to the raylet. Only used for cython wrapper `CRayletClientWithIoContext`
  /// new io service and new thread will be created inside.
  ///
  /// \param ip_address The IP address of raylet.
  /// \param port The port of raylet.
  RayletClientWithIoContext(const std::string &ip_address, int port);

  /// Get the worker pids from raylet.
  /// \param callback The callback to set the worker pids.
  /// \param timeout_ms The timeout in milliseconds.
  void GetWorkerPIDs(const rpc::OptionalItemCallback<std::vector<int32_t>> &callback,
                     int64_t timeout_ms);

 private:
  /// client call manager is created inside the raylet client, it should be kept active
  /// during the whole lifetime of client.
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::unique_ptr<rpc::RayletClient> raylet_client_;
};

}  // namespace rpc
}  // namespace ray
