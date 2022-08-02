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
#include <memory>

#include "ray/common/runtime_env_manager.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
namespace ray {
namespace gcs {

typedef std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                                   uint32_t delay_ms)>
    DelayExecutorFn;

class RuntimeEnvHandler : public rpc::RuntimeEnvHandler {
 public:
  RuntimeEnvHandler(instrumented_io_context &io_service,
                    RuntimeEnvManager &runtime_env_manager,
                    DelayExecutorFn delay_executor)
      : runtime_env_manager_(runtime_env_manager),
        delay_executor_(std::move(delay_executor)) {}

  void HandlePinRuntimeEnvURI(const rpc::PinRuntimeEnvURIRequest &request,
                              rpc::PinRuntimeEnvURIReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

 private:
  ray::RuntimeEnvManager &runtime_env_manager_;
  DelayExecutorFn delay_executor_;
};

}  // namespace gcs
}  // namespace ray
