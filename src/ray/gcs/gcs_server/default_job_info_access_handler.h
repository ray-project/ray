#pragma once
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

class DefaultJobInfoAccessHandler : public rpc::JobInfoAccessHandler {
 public:
  void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                    SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                             MarkJobFinishedReply *reply,
                             SendReplyCallback send_reply_callback) override;
};

}  // namespace rpc
}  // namespace ray
