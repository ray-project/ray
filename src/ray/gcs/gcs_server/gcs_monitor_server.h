#pragma once

#include "src/ray/protobuf/monitor.pb.h"
#include "src/ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// GcsMonitorServer is a shim responsible for providing a compatible interface between GCS and `monitor.py`
class GcsMonitorServer : public rpc::MonitorServiceHandler {
 public:
  explicit GcsMonitorServer();

  void HandleGetRayVersion(rpc::GetRayVersionRequest request,
                           rpc::GetRayVersionReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;
};
}  // namespace gcs
}  // namespace ray
