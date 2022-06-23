// Copyright 2017 The Ray Authors.
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

#include <grpcpp/grpcpp.h>

#include <thread>

#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/reporter.grpc.pb.h"
#include "src/ray/protobuf/reporter.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class MetricsAgentClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the metrics agent server.
  /// \param[in] port Port of the metrics agent server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  MetricsAgentClient(const std::string &address,
                     const int port,
                     ClientCallManager &client_call_manager) {
    RAY_LOG(DEBUG) << "Initiate the metrics client of address:" << address
                   << " port:" << port;
    grpc_client_ =
        std::make_unique<GrpcClient<ReporterService>>(address, port, client_call_manager);
  };

  /// Report metrics to metrics agent.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  VOID_RPC_CLIENT_METHOD(ReporterService,
                         ReportMetrics,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  /// Report open census protobuf metrics to metrics agent.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  VOID_RPC_CLIENT_METHOD(ReporterService,
                         ReportOCMetrics,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<ReporterService>> grpc_client_;
};

}  // namespace rpc
}  // namespace ray
