#ifndef RAY_GCS_GCS_SERVER_ACTOR_INFO_HANDLER_IMPL_H
#define RAY_GCS_GCS_SERVER_ACTOR_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

class DefaultActorInfoHandler : public rpc::ActorInfoHandler {
 public:
  explicit DefaultActorInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleAsyncGet(const ActorAsyncGetRequest &request, ActorAsyncGetReply *reply,
                      SendReplyCallback send_reply_callback) override;

  void HandleAsyncRegister(const ActorAsyncRegisterRequest &request,
                           ActorAsyncRegisterReply *reply,
                           SendReplyCallback send_reply_callback) override;

  void HandleAsyncUpdate(const ActorAsyncUpdateRequest &request,
                         ActorAsyncUpdateReply *reply,
                         SendReplyCallback send_reply_callback) override;

  uint64_t GetAsyncGetCount() { return metrics_[ASYNC_GET]; }

  uint64_t GetAsyncRegisterCount() { return metrics_[ASYNC_REGISTER]; }

  uint64_t GetAsyncUpdateCount() { return metrics_[ASYNC_UPDATE]; }

 private:
  gcs::RedisGcsClient &gcs_client_;

  enum { ASYNC_GET = 0, ASYNC_REGISTER = 1, ASYNC_UPDATE = 2, COMMAND_TYPE_END = 3 };
  /// metrics
  uint64_t metrics_[COMMAND_TYPE_END] = {0};
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_GCS_SERVER_ACTOR_INFO_HANDLER_IMPL_H