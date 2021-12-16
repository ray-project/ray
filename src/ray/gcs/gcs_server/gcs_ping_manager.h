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

#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

/// This implementation class of `PingHandler`.
class GcsPingManager : public rpc::PingHandler {
 public:
  explicit GcsPingManager() {}

  void HandlePing(const rpc::PingRequest &request, rpc::PingReply *reply,
                                rpc::SendReplyCallback send_reply_callback);
};

}  // namespace gcs
}  // namespace ray
