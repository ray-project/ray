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

#include "ray/rpc/grpc_server.h"

#include <grpcpp/impl/service_type.h>

#include <boost/asio/detail/socket_holder.hpp>

#include "ray/common/ray_config.h"
#include "ray/rpc/grpc_server.h"
#include "ray/stats/metric.h"
#include "ray/util/util.h"

DEFINE_stats(grpc_server_req_latency_ms, "Request latency in grpc server", ("Method"), (),
             ray::stats::GAUGE);
DEFINE_stats(grpc_server_req_new, "New request number in grpc server", ("Method"), (),
             ray::stats::COUNT);
DEFINE_stats(grpc_server_req_handling, "Request number are handling in grpc server",
             ("Method"), (), ray::stats::COUNT);
DEFINE_stats(grpc_server_req_finished, "Finished request number in grpc server",
             ("Method"), (), ray::stats::COUNT);

namespace ray {
namespace rpc {

GrpcServer::GrpcServer(std::string name, const uint32_t port, int num_threads,
                       int64_t keepalive_time_ms)
    : name_(std::move(name)),
      port_(port),
      is_closed_(true),
      num_threads_(num_threads),
      keepalive_time_ms_(keepalive_time_ms) {
  cqs_.resize(num_threads_);
}

void GrpcServer::Run() {
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
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, keepalive_time_ms_);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                             RayConfig::instance().grpc_keepalive_timeout_ms());
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 0);

  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
  // Register all the services to this server.
  if (services_.empty()) {
    RAY_LOG(WARNING) << "No service is found when start grpc server " << name_;
  }
  for (auto &entry : services_) {
    builder.RegisterService(&entry.get());
  }
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  for (int i = 0; i < num_threads_; i++) {
    cqs_[i] = builder.AddCompletionQueue();
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

  // Create calls for all the server call factories.
  for (auto &entry : server_call_factories_) {
    for (int i = 0; i < num_threads_; i++) {
      // Create a buffer of 100 calls for each RPC handler.
      // TODO(edoakes): a small buffer should be fine and seems to have better
      // performance, but we don't currently handle backpressure on the client.
      int buffer_size = 100;
      if (entry->GetMaxActiveRPCs() != -1) {
        buffer_size = entry->GetMaxActiveRPCs();
      }
      for (int j = 0; j < buffer_size; j++) {
        entry->CreateCall();
      }
    }
  }
  // Start threads that polls incoming requests.
  for (int i = 0; i < num_threads_; i++) {
    polling_threads_.emplace_back(&GrpcServer::PollEventsFromCompletionQueue, this, i);
  }
  // Set the server as running.
  is_closed_ = false;
}

void GrpcServer::RegisterService(GrpcService &service) {
  services_.emplace_back(service.GetGrpcService());

  for (int i = 0; i < num_threads_; i++) {
    service.InitServerCallFactories(cqs_[i], &server_call_factories_);
  }
}

void GrpcServer::PollEventsFromCompletionQueue(int index) {
  SetThreadName("server.poll" + std::to_string(index));
  void *tag;
  bool ok;

  // Keep reading events from the `CompletionQueue` until it's shutdown.
  while (cqs_[index]->Next(&tag, &ok)) {
    auto *server_call = static_cast<ServerCall *>(tag);
    bool delete_call = false;
    if (ok) {
      switch (server_call->GetState()) {
      case ServerCallState::PENDING:
        // We've received a new incoming request. Now this call object is used to
        // track this request.
        server_call->SetState(ServerCallState::PROCESSING);
        server_call->HandleRequest();
        break;
      case ServerCallState::SENDING_REPLY:
        // GRPC has sent reply successfully, invoking the callback.
        server_call->OnReplySent();
        // The rpc call has finished and can be deleted now.
        delete_call = true;
        break;
      default:
        RAY_LOG(FATAL) << "Shouldn't reach here.";
        break;
      }
    } else {
      // `ok == false` will occur in two situations:
      // First, the server has been shut down, the server call's status is PENDING
      // Second, server has sent reply to client and failed, the server call's status is
      // SENDING_REPLY
      if (server_call->GetState() == ServerCallState::SENDING_REPLY) {
        server_call->OnReplyFailed();
      }
      delete_call = true;
    }
    if (delete_call) {
      if (ok && server_call->GetServerCallFactory().GetMaxActiveRPCs() != -1) {
        // Create a new `ServerCall` to accept the next incoming request.
        server_call->GetServerCallFactory().CreateCall();
      }
      delete server_call;
    }
  }
}

}  // namespace rpc
}  // namespace ray
