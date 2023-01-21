#pragma once

#include "src/ray/protobuf/monitor.pb.h"
#include "src/ray/rpc/gcs_server/gcs_rpc_server.h"


namespace ray {
namespace gcs {

  /// GcsNodeManager is responsible for managing and monitoring nodes as well as handing
  /// node and resource related rpc requests.
  /// This class is not thread-safe.
class GcsMonitorServer : public ray::rpc::MonitorServiceHandler {
  public:

  explicit GcsMonitorServer();

  void HandleGetRayVersion(rpc::GetRayVersionRequest request,
                           rpc::GetRayVersionReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;
};
}
}
