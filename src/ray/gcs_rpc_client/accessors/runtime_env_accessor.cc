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

#include "ray/gcs_rpc_client/accessors/runtime_env_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"

namespace ray {
namespace gcs {

RuntimeEnvAccessor::RuntimeEnvAccessor(GcsClientContext *context) : context_(context) {}

Status RuntimeEnvAccessor::PinRuntimeEnvUri(const std::string &uri,
                                            int expiration_s,
                                            int64_t timeout_ms) {
  rpc::PinRuntimeEnvURIRequest request;
  request.set_uri(uri);
  request.set_expiration_s(expiration_s);
  rpc::PinRuntimeEnvURIReply reply;
  return context_->GetGcsRpcClient().SyncPinRuntimeEnvURI(
      std::move(request), &reply, timeout_ms);
}

}  // namespace gcs
}  // namespace ray
