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

#ifndef RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H
#define RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ObjectInfoHandler`.
class DefaultObjectInfoHandler : public rpc::ObjectInfoHandler {
 public:
  explicit DefaultObjectInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleGetObjectLocations(const GetObjectLocationsRequest &request,
                                GetObjectLocationsReply *reply,
                                SendReplyCallback send_reply_callback) override;

  void HandleAddObjectLocation(const AddObjectLocationRequest &request,
                               AddObjectLocationReply *reply,
                               SendReplyCallback send_reply_callback) override;

  void HandleRemoveObjectLocation(const RemoveObjectLocationRequest &request,
                                  RemoveObjectLocationReply *reply,
                                  SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_OBJECT_INFO_HANDLER_IMPL_H
