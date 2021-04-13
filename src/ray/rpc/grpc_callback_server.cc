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

#include "ray/rpc/grpc_callback_server.h"

#include <grpcpp/impl/service_type.h>

#include <boost/asio/detail/socket_holder.hpp>

#include "ray/common/ray_config.h"
#include "ray/util/util.h"

namespace ray {
namespace rpc {

GrpcCallbackServer::GrpcCallbackServer(std::string name, const uint32_t port,
                                       int64_t shutdown_deadline_ms)
    : name_(std::move(name)), port_(port), shutdown_deadline_ms_(shutdown_deadline_ms) {}

void GrpcCallbackServer::Run() {
  uint32_t specified_port = port_;
  std::string server_address("0.0.0.0:" + std::to_string(port_));
  grpc::ServerBuilder builder;
  // Disable the SO_REUSEPORT option. We don't need it in ray. If the option is enabled
  // (default behavior in grpc), we may see multiple workers listen on the same port and
  // the requests sent to this port may be handled by any of the workers.
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
  builder.AddChannelArgument(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH,
                             RayConfig::instance().max_grpc_message_size());
  builder.AddChannelArgument(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH,
                             RayConfig::instance().max_grpc_message_size());
  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
  // Register all the services to this server.
  if (services_.empty()) {
    RAY_LOG(WARNING) << "No service is found when start grpc server " << name_;
  }
  for (auto &entry : services_) {
    builder.RegisterService(&entry.get());
  }
  // Build and start server.
  server_ = builder.BuildAndStart();

  RAY_CHECK(server_)
      << "Failed to start the grpc server. The specified port is " << specified_port
      << ". This means that Ray's core components will not be able to function "
      << "correctly. If the server startup error message is `Address already in use`, "
      << "it indicates the server fails to start because the port is already used by "
      << "other processes (such as --node-manager-port, --object-manager-port, "
      << "--gcs-server-port, and ports between --min-worker-port, --max-worker-port). "
      << "Try running lsof -i :" << specified_port
      << " to check if there are other processes listening to the port.";
  RAY_CHECK(port_ > 0);
  RAY_LOG(INFO) << name_ << " server started, listening on port " << port_ << ".";

  // Set the server as running.
  is_closed_ = false;
}

void GrpcCallbackServer::RegisterService(grpc::Service &service) {
  services_.emplace_back(service);
}

}  // namespace rpc
}  // namespace ray
