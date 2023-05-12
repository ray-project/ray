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

#include "ray/raylet/dashboard_agent_manager.h"

#include <thread>

#include "ray/common/ray_config.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace ray {
namespace raylet {

void DashboardAgentManager::HandleRegisterAgent(
    rpc::RegisterAgentRequest request,
    rpc::RegisterAgentReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  reported_agent_ip_address_ = request.agent_ip_address();
  reported_agent_port_ = request.agent_port();
  reported_agent_id_ = request.agent_id();
  // TODO(SongGuyang): We should remove this after we find better port resolution.
  // Note: `agent_port_` should be 0 if the grpc port of agent is in conflict.
  if (reported_agent_port_ != 0) {
    RAY_LOG(INFO) << "Handle register dashboard agent, ip: " << reported_agent_ip_address_
                  << ", port: " << reported_agent_port_ << ", id: " << reported_agent_id_;
  } else {
    RAY_LOG(WARNING) << "The GRPC port of the dashboard agent is invalid (0), ip: "
                     << reported_agent_ip_address_ << ", id: " << reported_agent_id_
                     << ". The agent client in the raylet has been disabled.";
  }
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

}  // namespace raylet
}  // namespace ray
