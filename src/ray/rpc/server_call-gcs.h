#pragma once

#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

template <>
void ServerCallImpl<NodeInfoGcsService, RegisterClientRequest, RegisterClientReply>::
    HandleRequest();

}  // namespace rpc
}  // namespace ray
