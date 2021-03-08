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
#include <boost/bind.hpp>
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

Raylet::Raylet(boost::asio::io_service &main_service, const std::string &socket_name,
               const std::string &node_ip_address, const std::string &redis_address,
               int redis_port, const std::string &redis_password,
               const NodeManagerConfig &node_manager_config,
               const ObjectManagerConfig &object_manager_config,
               std::shared_ptr<gcs::GcsClient> gcs_client, int metrics_export_port)
    : main_service_(main_service),
      self_node_id_(NodeID::FromRandom()),
      gcs_client_(gcs_client),
      object_directory_(
          RayConfig::instance().ownership_based_object_directory_enabled()
              ? std::dynamic_pointer_cast<ObjectDirectoryInterface>(
                    std::make_shared<OwnershipBasedObjectDirectory>(
                        main_service, gcs_client_,

                        [this](const ObjectID &obj_id) {
                          rpc::ObjectReference ref;
                          ref.set_object_id(obj_id.Binary());
                          node_manager_.MarkObjectsAsFailed(
                              ErrorType::OBJECT_UNRECONSTRUCTABLE, {ref}, JobID::Nil());
                        }

                        ))
              : std::dynamic_pointer_cast<ObjectDirectoryInterface>(
                    std::make_shared<ObjectDirectory>(main_service, gcs_client_))),
      object_manager_(
          main_service, self_node_id_, object_manager_config, object_directory_,
          [this](const ObjectID &object_id, const std::string &object_url,
                 const NodeID &node_id,
                 std::function<void(const ray::Status &)> callback) {
            node_manager_.GetLocalObjectManager().AsyncRestoreSpilledObject(
                object_id, object_url, node_id, callback);
          },
          [this]() {
            // This callback is called from the plasma store thread.
            // NOTE: It means the local object manager should be thread-safe.
            main_service_.post([this]() {
              node_manager_.GetLocalObjectManager().SpillObjectUptoMaxThroughput();
            });
            return node_manager_.GetLocalObjectManager().IsSpillingInProgress();
          },
          [this]() {
            // Post on the node manager's event loop since this
            // callback is called from the plasma store thread.
            // This will help keep node manager lock-less.
            main_service_.post([this]() { node_manager_.TriggerGlobalGC(); });
          }),
      node_manager_(main_service, self_node_id_, node_manager_config, object_manager_,
                    gcs_client_, object_directory_,
                    [this](const ObjectID &object_id) {
                      // It is used by local_object_store.
                      return object_manager_.IsPlasmaObjectSpillable(object_id);
                    }),
      socket_name_(socket_name),
      acceptor_(main_service, ParseUrlEndpoint(socket_name)),
      socket_(main_service) {
  self_node_info_.set_node_id(self_node_id_.Binary());
  self_node_info_.set_state(GcsNodeInfo::ALIVE);
  self_node_info_.set_node_manager_address(node_ip_address);
  self_node_info_.set_raylet_socket_name(socket_name);
  self_node_info_.set_object_store_socket_name(object_manager_config.store_socket_name);
  self_node_info_.set_object_manager_port(object_manager_.GetServerPort());
  self_node_info_.set_node_manager_port(node_manager_.GetServerPort());
  self_node_info_.set_node_manager_hostname(boost::asio::ip::host_name());
  self_node_info_.set_metrics_export_port(metrics_export_port);
}

Raylet::~Raylet() {}

void Raylet::Start() {
  RAY_CHECK_OK(RegisterGcs());

  // Start listening for clients.
  DoAccept();
}

void Raylet::Stop() {
  object_manager_.Stop();
  RAY_CHECK_OK(gcs_client_->Nodes().UnregisterSelf());
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
    fbb.Finish(protocol::CreateDisconnectClient(fbb));
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
