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

#include "ray/gcs_rpc_client/accessors/runtime_env_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// \class RuntimeEnvAccessor
/// Implementation of RuntimeEnvAccessorInterface.
class RuntimeEnvAccessor : public RuntimeEnvAccessorInterface {
 public:
  RuntimeEnvAccessor() = default;
  explicit RuntimeEnvAccessor(GcsClientContext *context);
  virtual ~RuntimeEnvAccessor() = default;

  /// Pin runtime environment URI.
  virtual Status PinRuntimeEnvUri(const std::string &uri,
                                  int expiration_s,
                                  int64_t timeout_ms = -1) override;

 private:
  // GCS client implementation.
  GcsClientContext *context_ = nullptr;
};

}  // namespace gcs
}  // namespace ray
