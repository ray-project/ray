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
