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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/scheduling/resource_set.h"
#include "ray/common/status.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"
#include "ray/object_manager/object_manager.h"
#include "ray/object_manager/ownership_object_directory.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/util/network_util.h"
#include "ray/util/time.h"

namespace {

const std::vector<std::string> GenerateEnumNames(const char *const *enum_names_ptr,
                                                 int start_index,
                                                 int end_index) {
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

Raylet::Raylet(instrumented_io_context &main_service,
               const NodeID &self_node_id,
               const std::string &socket_name,
               const std::string &node_ip_address,
               const std::string &node_name,
               const NodeManagerConfig &node_manager_config,
               const ObjectManagerConfig &object_manager_config,
               gcs::GcsClient &gcs_client,
               int metrics_export_port,
               bool is_head_node,
               NodeManager &node_manager)
    : self_node_id_(self_node_id),
      gcs_client_(gcs_client),
      node_manager_(node_manager),
      socket_name_(socket_name),
      acceptor_(main_service, ParseUrlEndpoint(socket_name)),
      socket_(main_service) {
  SetCloseOnExec(acceptor_);
  self_node_info_.set_node_id(self_node_id_.Binary());
  self_node_info_.set_state(GcsNodeInfo::ALIVE);
  self_node_info_.set_node_manager_address(node_ip_address);
  self_node_info_.set_node_name(node_name);
  self_node_info_.set_raylet_socket_name(socket_name);
  self_node_info_.set_object_store_socket_name(object_manager_config.store_socket_name);
  self_node_info_.set_object_manager_port(node_manager_.GetObjectManagerPort());
  self_node_info_.set_node_manager_port(node_manager_.GetServerPort());
  self_node_info_.set_node_manager_hostname(boost::asio::ip::host_name());
  self_node_info_.set_metrics_export_port(metrics_export_port);
  self_node_info_.set_runtime_env_agent_port(node_manager_config.runtime_env_agent_port);
  self_node_info_.mutable_state_snapshot()->set_state(NodeSnapshot::ACTIVE);
  auto resource_map = node_manager_config.resource_config.GetResourceMap();
  self_node_info_.mutable_resources_total()->insert(resource_map.begin(),
                                                    resource_map.end());
  self_node_info_.set_start_time_ms(current_sys_time_ms());
  self_node_info_.set_is_head_node(is_head_node);
  self_node_info_.mutable_labels()->insert(node_manager_config.labels.begin(),
                                           node_manager_config.labels.end());

  // Setting up autoscaler related fields from ENV
  auto instance_id = std::getenv(kNodeCloudInstanceIdEnv);
  self_node_info_.set_instance_id(instance_id ? instance_id : "");
  auto cloud_node_type_name = std::getenv(kNodeTypeNameEnv);
  self_node_info_.set_node_type_name(cloud_node_type_name ? cloud_node_type_name : "");
  auto instance_type_name = std::getenv(kNodeCloudInstanceTypeNameEnv);
  self_node_info_.set_instance_type_name(instance_type_name ? instance_type_name : "");
}

Raylet::~Raylet() {}

void Raylet::Start() {
  RegisterGcs();

  // Start listening for clients.
  DoAccept();
}

void Raylet::UnregisterSelf(const rpc::NodeDeathInfo &node_death_info,
                            std::function<void()> unregister_done_callback) {
  gcs_client_.Nodes().UnregisterSelf(node_death_info, unregister_done_callback);
}

void Raylet::Stop() {
  node_manager_.Stop();
  acceptor_.close();
}

void Raylet::RegisterGcs() {
  auto register_callback = [this](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Raylet of id, " << self_node_id_
                  << " started. Raylet consists of node_manager and object_manager."
                  << " node_manager address: "
                  << BuildAddress(self_node_info_.node_manager_address(),
                                  self_node_info_.node_manager_port())
                  << " object_manager address: "
                  << BuildAddress(self_node_info_.node_manager_address(),
                                  self_node_info_.object_manager_port())
                  << " hostname: " << self_node_info_.node_manager_hostname();
    node_manager_.RegisterGcs();
  };

  RAY_CHECK_OK(gcs_client_.Nodes().RegisterSelf(self_node_info_, register_callback));
}

void Raylet::DoAccept() {
  acceptor_.async_accept(
      socket_,
      boost::bind(&Raylet::HandleAccept, this, boost::asio::placeholders::error));
}

void Raylet::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    ConnectionErrorHandler error_handler =
        [this](const std::shared_ptr<ClientConnection> &client,
               const boost::system::error_code &err) {
          node_manager_.HandleClientConnectionError(client, err);
        };

    MessageHandler message_handler = [this](
                                         const std::shared_ptr<ClientConnection> &client,
                                         int64_t message_type,
                                         const std::vector<uint8_t> &message) {
      node_manager_.ProcessClientMessage(client, message_type, message.data());
    };

    // Accept a new local client and dispatch it to the node manager.
    auto conn = ClientConnection::Create(message_handler,
                                         error_handler,
                                         std::move(socket_),
                                         "worker",
                                         node_manager_message_enum);

    // Begin processing messages. The message handler above is expected to call this to
    // continue processing messages.
    conn->ProcessMessages();
  } else {
    RAY_LOG(ERROR) << "Raylet failed to accept new connection: " << error.message();
    if (error == boost::asio::error::operation_aborted) {
      // The server is being destroyed. Don't continue accepting connections.
      return;
    }
  };

  // We're ready to accept another client.
  DoAccept();
}

}  // namespace raylet

}  // namespace ray
