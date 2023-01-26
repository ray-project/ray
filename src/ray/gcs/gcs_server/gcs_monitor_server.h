// Copyright 2023 The Ray Authors.
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

#include "src/ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// GcsMonitorServer is a shim responsible for providing a compatible interface between
/// GCS and `monitor.py`
class GcsMonitorServer : public rpc::MonitorServiceHandler {
 public:
  explicit GcsMonitorServer();

  void HandleGetRayVersion(rpc::GetRayVersionRequest request,
                           rpc::GetRayVersionReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;
};
}  // namespace gcs
}  // namespace ray
