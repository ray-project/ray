#pragma once

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {
class GcsVirtualClusterManager : public rpc::VirtualClusterInfoHandler {
 public:
  GcsVirtualClusterManager(instrumented_io_context &io_context);

  ~GcsVirtualClusterManager() = default;

  void HandleCreateVirtualCluster(rpc::CreateVirtualClusterRequest request,
                                  rpc::CreateVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveVirtualCluster(rpc::RemoveVirtualClusterRequest request,
                                  rpc::RemoveVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

 private:
  instrumented_io_context &io_context_;
};
}  // namespace gcs
}  // namespace ray
