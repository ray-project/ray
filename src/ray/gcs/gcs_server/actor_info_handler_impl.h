#ifndef RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ActorInfoHandler`.
class DefaultActorInfoHandler : public rpc::ActorInfoHandler {
 public:
  explicit DefaultActorInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleGetActor(const GetActorRequest &request, GetActorReply *reply,
                      SendReplyCallback send_reply_callback) override;

  void HandleRegisterActor(const RegisterActorRequest &request, RegisterActorReply *reply,
                           SendReplyCallback send_reply_callback) override;

  void HandleUpdateActor(const UpdateActorRequest &request, UpdateActorReply *reply,
                         SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ACTOR_INFO_HANDLER_IMPL_H
