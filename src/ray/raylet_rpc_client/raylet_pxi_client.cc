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

#include "ray/raylet_rpc_client/raylet_pxi_client.h"

#include <future>
#include <memory>

namespace ray {
namespace rpc {

RayletPXIClient::RayletPXIClient(const std::string &host, int port)
    : io_service_(/*emit_metrics=*/false,
                  /*running_on_single_thread=*/true),
      client_call_manager_(io_service_,
                           /*record_stats=*/false,
                           /*local_address=*/""),
      grpc_client_(host, port, client_call_manager_),
      work_guard_(io_service_.get_executor()) {
  io_thread_ = std::thread([this]() { io_service_.run(); });
}

RayletPXIClient::~RayletPXIClient() {
  io_service_.stop();
  if (io_thread_.joinable()) {
    io_thread_.join();
  }
}

RayletPXIClient::ResizeResult RayletPXIClient::ResizeLocalResourceInstances(
    const std::map<std::string, double> &resources) {
  rpc::ResizeLocalResourceInstancesRequest request;
  for (const auto &kv : resources) {
    (*request.mutable_resources())[kv.first] = kv.second;
  }

  auto promise = std::make_shared<std::promise<ResizeResult>>();

  auto callback = [promise](const Status &status,
                            rpc::ResizeLocalResourceInstancesReply &&reply) {
    ResizeResult result;
    if (!status.ok()) {
      result.status_code = static_cast<int>(status.code());
      result.message = status.ToString();
    } else {
      for (const auto &it : reply.total_resources()) {
        result.total_resources[it.first] = it.second;
      }
    }
    promise->set_value(std::move(result));
  };

  auto *grpc_client_ptr = &grpc_client_;
  INVOKE_RPC_CALL(NodeManagerService,
                  ResizeLocalResourceInstances,
                  request,
                  callback,
                  grpc_client_ptr,
                  /*method_timeout_ms*/ -1);

  return promise->get_future().get();
}

}  // namespace rpc
}  // namespace ray
