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

#include "ray/raylet_rpc_client/threaded_raylet_client.h"

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

ThreadedRayletClient::ThreadedRayletClient(const std::string &ip_address, int port)
    : RayletClient(), ip_address_(ip_address), port_(port) {
  // Connect to the raylet on a singleton io service with a dedicated thread.
  // This is to avoid creating multiple threads for multiple clients in python.
  ConnectOnSingletonIoContext();
}

void ThreadedRayletClient::ConnectOnSingletonIoContext() {
  static InstrumentedIOContextWithThread io_context("raylet_client_io_service");
  instrumented_io_context &io_service = io_context.GetIoService();
  Connect(io_service);
}

void ThreadedRayletClient::Connect(instrumented_io_context &io_service) {
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(
      io_service, /*record_stats=*/false, ip_address_);
  grpc_client_ = std::make_unique<rpc::GrpcClient<rpc::NodeManagerService>>(
      ip_address_, port_, *client_call_manager_);
  auto raylet_unavailable_timeout_callback = []() {
    RAY_LOG(WARNING) << "Raylet is unavailable for "
                     << ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_s()
                     << "s";
  };
  retryable_grpc_client_ = rpc::RetryableGrpcClient::Create(
      grpc_client_->Channel(),
      client_call_manager_->GetMainService(),
      /*max_pending_requests_bytes=*/
      std::numeric_limits<uint64_t>::max(),
      /*check_channel_status_interval_milliseconds=*/
      ::RayConfig::instance().grpc_client_check_connection_status_interval_milliseconds(),
      /*server_unavailable_timeout_seconds=*/
      ::RayConfig::instance().raylet_rpc_server_reconnect_timeout_s(),
      /*server_unavailable_timeout_callback=*/
      raylet_unavailable_timeout_callback,
      /*server_name=*/
      std::string("Raylet ") + ip_address_);
}

Status ThreadedRayletClient::GetWorkerPIDs(
    std::shared_ptr<std::vector<int32_t>> worker_pids, int64_t timeout_ms) {
  rpc::GetWorkerPIDsRequest request;
  auto promise = std::make_shared<std::promise<Status>>();
  std::weak_ptr<std::promise<Status>> weak_promise = promise;
  std::weak_ptr<std::vector<int32_t>> weak_worker_pids = worker_pids;
  auto future = promise->get_future();
  auto callback = [weak_promise, weak_worker_pids](const Status &status,
                                                   rpc::GetWorkerPIDsReply &&reply) {
    auto p = weak_promise.lock();
    auto workers = weak_worker_pids.lock();
    if (p != nullptr && workers != nullptr) {
      if (status.ok()) {
        *workers = std::vector<int32_t>(reply.pids().begin(), reply.pids().end());
      }
      p->set_value(status);
    }
  };
  INVOKE_RETRYABLE_RPC_CALL(retryable_grpc_client_,
                            NodeManagerService,
                            GetWorkerPIDs,
                            request,
                            callback,
                            grpc_client_,
                            timeout_ms);
  if (future.wait_for(std::chrono::milliseconds(timeout_ms)) ==
      std::future_status::timeout) {
    return Status::TimedOut("Timed out getting worker PIDs from raylet");
  }
  return future.get();
}

}  // namespace rpc
}  // namespace ray
