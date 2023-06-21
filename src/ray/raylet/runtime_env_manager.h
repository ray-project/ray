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

#include "ray/rpc/runtime_env/runtime_env_server.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace raylet {

class RuntimeEnvManager : public rpc::RuntimeEnvServiceHandler {
 public:
  explicit RuntimeEnvManager() {}

  virtual void GetOrCreateRuntimeEnv(rpc::GetOrCreateRuntimeEnvRequest request,
                                     rpc::GetOrCreateRuntimeEnvReply *reply,
                                     rpc::SendReplyCallback send_reply_callback);

  virtual void DeleteRuntimeEnvIfPossible(rpc::DeleteRuntimeEnvIfPossibleRequest request,
                                          rpc::DeleteRuntimeEnvIfPossibleReply *reply,
                                          rpc::SendReplyCallback send_reply_callback);

  virtual void GetRuntimeEnvsInfo(rpc::GetRuntimeEnvsInfoRequest request,
                                  rpc::GetRuntimeEnvsInfoReply *reply,
                                  rpc::SendReplyCallback send_reply_callback);
  
};

}  // namespace raylet
}  // namespace ray