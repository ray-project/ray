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

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_client.h"
#include "ray/rpc/agent_manager/agent_manager_client.h"
#include "ray/rpc/agent_manager/agent_manager_server.h"
#include "ray/rpc/job/job_client.h"
#include "ray/rpc/job/job_server.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace raylet {

typedef std::function<std::unique_ptr<rpc::JobClient>(const std::string &ip_address,
                                                      int port)>
    JobClientFactoryFn;

typedef std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                                   uint32_t delay_ms)>
    DelayExecutorFn;

class AgentManager : public rpc::AgentManagerServiceHandler {
 public:
  struct Options {
    const NodeID node_id;
    std::vector<std::string> agent_commands;
  };

  explicit AgentManager(Options options, std::shared_ptr<gcs::GcsClient> gcs_client,
                        JobClientFactoryFn job_client_factory,
                        DelayExecutorFn delay_executor)
      : options_(std::move(options)),
        gcs_client_(std::move(gcs_client)),
        job_client_factory_(std::move(job_client_factory)),
        delay_executor_(std::move(delay_executor)) {
    StartAgent();
  }

  void HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void InitializeJobEnv(std::shared_ptr<rpc::JobTableData> job_data, bool start_driver);

 private:
  void StartAgent();

 private:
  Options options_;
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  std::unique_ptr<rpc::JobClient> job_client_;
  JobClientFactoryFn job_client_factory_;
  DelayExecutorFn delay_executor_;

  pid_t agent_pid_ = 0;
  int agent_port_ = 0;
  std::string agent_ip_address_;
};

class DefaultAgentManagerServiceHandler : public rpc::AgentManagerServiceHandler {
 public:
  explicit DefaultAgentManagerServiceHandler(std::unique_ptr<AgentManager> &delegate)
      : delegate_(delegate) {}

  void HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleRegisterAgent(request, reply, send_reply_callback);
  }

 private:
  std::unique_ptr<AgentManager> &delegate_;
};

}  // namespace raylet
}  // namespace ray
