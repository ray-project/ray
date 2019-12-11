#pragma once
#include "ray/rpc/gcs_server/job_info_access_server.h"

namespace ray {
namespace rpc {

class DefaultJobInfoAccessHandler : public rpc::JobInfoAccessHandler {
 public:
  void HandleAddJob(const GcsJobInfo &request, AddJobReply *reply,
                    SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFinished(const FinishedJob &request, MarkJobFinishedReply *reply,
                             SendReplyCallback send_reply_callback) override;
};

}  // namespace rpc
}  // namespace ray
