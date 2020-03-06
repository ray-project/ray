#ifndef RAY_GCS_STATS_HANDLER_IMPL_H
#define RAY_GCS_STATS_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `StatsHandler`.
class DefaultStatsHandler : public rpc::StatsHandler {
 public:
  explicit DefaultStatsHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleAddProfileData(const AddProfileDataRequest &request,
                            AddProfileDataReply *reply,
                            SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_STATS_HANDLER_IMPL_H
