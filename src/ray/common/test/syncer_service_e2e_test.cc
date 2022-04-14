// Copyright 2022 The Ray Authors.
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

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <cstdlib>
#include <ctime>
#include <iostream>

#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/ray_syncer/ray_syncer.h"
using namespace std;
using namespace ray::syncer;

class LocalNode : public ReporterInterface {
 public:
  LocalNode(instrumented_io_context &io_context, ray::NodeID node_id)
      : node_id_(node_id), timer_(io_context) {
    timer_.RunFnPeriodically(
        [this]() {
          auto v = static_cast<double>(std::rand()) / RAND_MAX;
          if (v < 0.3) {
            int old_state = state_;
            state_ += std::rand() % 10;
            ++version_;
            RAY_LOG(INFO) << node_id_ << " change from (" << old_state
                          << ", v:" << (version_ - 1) << ") to (" << state_
                          << ", v:" << version_ << ")";
          }
        },
        1000);
  }

  std::optional<RaySyncMessage> Snapshot(int64_t current_version,
                                         RayComponentId) const override {
    if (current_version > version_) {
      return std::nullopt;
    }
    ray::rpc::syncer::RaySyncMessage msg;
    msg.set_component_id(ray::rpc::syncer::RayComponentId::RESOURCE_MANAGER);
    msg.set_version(version_);
    msg.set_sync_message(
        std::string(reinterpret_cast<const char *>(&state_), sizeof(state_)));
    msg.set_node_id(node_id_.Binary());
    return msg;
  }

 private:
  int64_t version_ = 0;
  int state_ = 0;
  ray::NodeID node_id_;
  ray::PeriodicalRunner timer_;
};

class RemoteNodes : public ReceiverInterface {
 public:
  RemoteNodes() {}
  void Update(std::shared_ptr<const ray::rpc::syncer::RaySyncMessage> msg) override {
    auto version = msg->version();
    int state = *reinterpret_cast<const int *>(msg->sync_message().data());
    auto iter = infos_.find(msg->node_id());
    if (iter == infos_.end() || iter->second.second < version) {
      RAY_LOG(INFO) << "Update node " << ray::NodeID::FromBinary(msg->node_id()).Hex()
                    << " to (" << state << ", v:" << version << ")";
      infos_[msg->node_id()] = std::make_pair(state, version);
    }
  }

 private:
  absl::flat_hash_map<std::string, std::pair<int, int>> infos_;
};

int main(int argc, char *argv[]) {
  std::srand(std::time(nullptr));
  instrumented_io_context io_context;
  RAY_CHECK(argc == 3) << "./test_syncer_service server_port leader_port";
  auto node_id = ray::NodeID::FromRandom();
  auto server_port = std::string(argv[1]);
  auto leader_port = std::string(argv[2]);
  auto local_node = std::make_unique<LocalNode>(io_context, node_id);
  auto remote_node = std::make_unique<RemoteNodes>();
  RaySyncer syncer(io_context, node_id.Binary());
  // RPC related field
  grpc::ServerBuilder builder;
  std::unique_ptr<RaySyncerService> service;
  std::unique_ptr<grpc::Server> server;
  std::shared_ptr<grpc::Channel> channel;
  syncer.Register(ray::rpc::syncer::RayComponentId::RESOURCE_MANAGER,
                  local_node.get(),
                  remote_node.get());
  if (server_port != ".") {
    RAY_LOG(INFO) << "Start server on port " << server_port;
    auto server_address = "0.0.0.0:" + server_port;
    service = std::make_unique<RaySyncerService>(syncer);
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
  }
  if (leader_port != ".") {
    grpc::ChannelArguments argument;
    // Disable http proxy since it disrupts local connections. TODO(ekl) we should make
    // this configurable, or selectively set it for known local connections only.
    argument.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    argument.SetMaxSendMessageSize(::RayConfig::instance().max_grpc_message_size());
    argument.SetMaxReceiveMessageSize(::RayConfig::instance().max_grpc_message_size());

    channel = grpc::CreateCustomChannel(
        "localhost:" + leader_port, grpc::InsecureChannelCredentials(), argument);

    syncer.Connect(channel);
  }

  boost::asio::io_context::work work(io_context);
  io_context.run();

  return 0;
}
