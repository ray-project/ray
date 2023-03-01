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

#include "ray/gcs/gcs_server/runtime_env_handler.h"

namespace ray {
namespace gcs {

void RuntimeEnvHandler::HandlePinRuntimeEnvURI(
    rpc::PinRuntimeEnvURIRequest request,
    rpc::PinRuntimeEnvURIReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Received PinRuntimeEnvURI request: " << request.DebugString();
  // Use a random ID to hold the temporary reference URI.
  std::string hex_id(12, 0);
  FillRandom(&hex_id);

  runtime_env_manager_.AddURIReference(hex_id, request.uri());

  delay_executor_(
      [this, hex_id, request] {
        runtime_env_manager_.RemoveURIReference(hex_id);
        RAY_LOG(DEBUG) << "Removed temporary URI reference for ID " << hex_id
                       << "with URI:" << request.uri();
      },
      /* expiration_ms= */ request.expiration_s() * 1000);

  // The `request` object will be destroyed when the reply is sent, so this
  // must be called after the delay executor is set up.
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}
}  // namespace gcs
}  // namespace ray
