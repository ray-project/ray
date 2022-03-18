// Copyright 2021 The Ray Authors.
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

namespace ray {
namespace raylet {

class MockAgentManager : public AgentManager {
 public:
  MOCK_METHOD(void,
              HandleRegisterAgent,
              (const rpc::RegisterAgentRequest &request,
               rpc::RegisterAgentReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              GetOrCreateRuntimeEnv,
              (const JobID &job_id,
               const std::string &serialized_runtime_env,
               const rpc::RuntimeEnvConfig &runtime_env_config,
               CreateRuntimeEnvCallback callback),
              (override));
  MOCK_METHOD(void,
              DeleteRuntimeEnv,
              (const std::string &serialized_runtime_env,
               DeleteRuntimeEnvCallback callback),
              (override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockDefaultAgentManagerServiceHandler : public DefaultAgentManagerServiceHandler {
 public:
  MOCK_METHOD(void,
              HandleRegisterAgent,
              (const rpc::RegisterAgentRequest &request,
               rpc::RegisterAgentReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace raylet
}  // namespace ray
