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

#ifndef RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H

#include "gcs_actor_manager.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {

namespace rpc {
/// This implementation class of `ActorInfoHandler`.
class DefaultActorInfoHandler : public rpc::ActorInfoHandler {
 public:
  explicit DefaultActorInfoHandler(gcs::RedisGcsClient &gcs_client,
                                   gcs::GcsActorManager &gcs_actor_manager)
      : gcs_client_(gcs_client), gcs_actor_manager_(gcs_actor_manager) {}

  void HandleCreateActor(const CreateActorRequest &request, CreateActorReply *reply,
                         SendReplyCallback send_reply_callback) override;

  void HandleGetActorInfo(const GetActorInfoRequest &request, GetActorInfoReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleRegisterActorInfo(const RegisterActorInfoRequest &request,
                               RegisterActorInfoReply *reply,
                               SendReplyCallback send_reply_callback) override;

  void HandleUpdateActorInfo(const UpdateActorInfoRequest &request,
                             UpdateActorInfoReply *reply,
                             SendReplyCallback send_reply_callback) override;

  void HandleAddActorCheckpoint(const AddActorCheckpointRequest &request,
                                AddActorCheckpointReply *reply,
                                SendReplyCallback send_reply_callback) override;

  void HandleGetActorCheckpoint(const GetActorCheckpointRequest &request,
                                GetActorCheckpointReply *reply,
                                SendReplyCallback send_reply_callback) override;

  void HandleGetActorCheckpointID(const GetActorCheckpointIDRequest &request,
                                  GetActorCheckpointIDReply *reply,
                                  SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
  gcs::GcsActorManager &gcs_actor_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
