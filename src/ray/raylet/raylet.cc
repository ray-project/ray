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

#include "ray/raylet/raylet.h"

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <iostream>

#include "ray/common/status.h"
#include "ray/util/util.h"

namespace {

const std::vector<std::string> GenerateEnumNames(const char *const *enum_names_ptr,
                                                 int start_index, int end_index) {
  std::vector<std::string> enum_names;
  for (int i = 0; i < start_index; ++i) {
    enum_names.push_back("EmptyMessageType");
  }
  size_t i = 0;
  while (true) {
    const char *name = enum_names_ptr[i];
    if (name == nullptr) {
      break;
    }
    enum_names.push_back(name);
    i++;
  }
  RAY_CHECK(static_cast<size_t>(end_index) == enum_names.size() - 1)
      << "Message Type mismatch!";
  return enum_names;
}

static const std::vector<std::string> node_manager_message_enum =
    GenerateEnumNames(ray::protocol::EnumNamesMessageType(),
                      static_cast<int>(ray::protocol::MessageType::MIN),
                      static_cast<int>(ray::protocol::MessageType::MAX));
}  // namespace

namespace ray {

namespace raylet {

Raylet::Raylet(instrumented_io_context &main_service, const std::string &socket_name,
               const std::string &node_ip_address,
               const NodeManagerConfig &node_manager_config,
               const ObjectManagerConfig &object_manager_config,
               std::shared_ptr<gcs::GcsClient> gcs_client, int metrics_export_port)
    : main_service_(main_service),
      self_node_id_(
          !RayConfig::instance().OVERRIDE_NODE_ID_FOR_TESTING().empty()
              ? NodeID::FromHex(RayConfig::instance().OVERRIDE_NODE_ID_FOR_TESTING())
              : NodeID::FromRandom()),
      gcs_client_(gcs_client),
      node_manager_(main_service, self_node_id_, node_manager_config,
                    object_manager_config, gcs_client_),
      socket_name_(socket_name),
      acceptor_(main_service, ParseUrlEndpoint(socket_name)),
      socket_(main_service) {
  self_node_info_.set_node_id(self_node_id_.Binary());
  self_node_info_.set_state(GcsNodeInfo::ALIVE);
  self_node_info_.set_node_manager_address(node_ip_address);
  self_node_info_.set_raylet_socket_name(socket_name);
  self_node_info_.set_object_store_socket_name(object_manager_config.store_socket_name);
  self_node_info_.set_object_manager_port(node_manager_.GetObjectManagerPort());
  self_node_info_.set_node_manager_port(node_manager_.GetServerPort());
  self_node_info_.set_node_manager_hostname(boost::asio::ip::host_name());
  self_node_info_.set_metrics_export_port(metrics_export_port);
  const auto &resource_map = node_manager_config.resource_config.GetResourceMap();
  self_node_info_.mutable_resources_total()->insert(resource_map.begin(),
                                                    resource_map.end());
}

Raylet::~Raylet() {}

void Raylet::Start() {
  RAY_CHECK_OK(RegisterGcs());

  // Start listening for clients.
  DoAccept();
}

void Raylet::Stop() {
  RAY_CHECK_OK(gcs_client_->Nodes().DrainSelf());
  node_manager_.Stop();
  acceptor_.close();
}

ray::Status Raylet::RegisterGcs() {
  auto register_callback = [this](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Raylet of id, " << self_node_id_
                  << " started. Raylet consists of node_manager and object_manager."
                  << " node_manager address: " << self_node_info_.node_manager_address()
                  << ":" << self_node_info_.node_manager_port()
                  << " object_manager address: " << self_node_info_.node_manager_address()
                  << ":" << self_node_info_.object_manager_port()
                  << " hostname: " << self_node_info_.node_manager_address();

    // Add resource information.
    const NodeManagerConfig &node_manager_config = node_manager_.GetInitialConfig();
    std::unordered_map<std::string, std::shared_ptr<rpc::ResourceTableData>> resources;
    for (const auto &resource_pair :
         node_manager_config.resource_config.GetResourceMap()) {
      auto resource = std::make_shared<rpc::ResourceTableData>();
      resource->set_resource_capacity(resource_pair.second);
      resources.emplace(resource_pair.first, resource);
    }
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncUpdateResources(self_node_id_,
                                                                   resources, nullptr));

    RAY_CHECK_OK(node_manager_.RegisterGcs());
  };

  RAY_RETURN_NOT_OK(
      gcs_client_->Nodes().RegisterSelf(self_node_info_, register_callback));
  return Status::OK();
}

void Raylet::DoAccept() {
  acceptor_.async_accept(socket_, boost::bind(&Raylet::HandleAccept, this,
                                              boost::asio::placeholders::error));
}

void Raylet::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    // TODO: typedef these handlers.
    ClientHandler client_handler = [this](ClientConnection &client) {
      node_manager_.ProcessNewClient(client);
    };
    MessageHandler message_handler = [this](std::shared_ptr<ClientConnection> client,
                                            int64_t message_type,
                                            const std::vector<uint8_t> &message) {
      node_manager_.ProcessClientMessage(client, message_type, message.data());
    };
    flatbuffers::FlatBufferBuilder fbb;
    protocol::DisconnectClientBuilder builder(fbb);
    builder.add_disconnect_type(static_cast<int>(rpc::WorkerExitType::SYSTEM_ERROR_EXIT));
    fbb.Finish(builder.Finish());
    std::vector<uint8_t> message_data(fbb.GetBufferPointer(),
                                      fbb.GetBufferPointer() + fbb.GetSize());
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = ClientConnection::Create(
        client_handler, message_handler, std::move(socket_), "worker",
        node_manager_message_enum,
        static_cast<int64_t>(protocol::MessageType::DisconnectClient), message_data);
  }
  // We're ready to accept another client.
  DoAccept();
}

}  // namespace raylet

}  // namespace ray
