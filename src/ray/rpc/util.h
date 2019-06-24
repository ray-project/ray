#ifndef RAY_RPC_UTIL_H
#define RAY_RPC_UTIL_H

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"

namespace ray {
namespace rpc {

/// Helper function that converts a ray status to gRPC status.
inline grpc::Status RayStatusToGrpcStatus(const Status &ray_status) {
  if (ray_status.ok()) {
    return grpc::Status::OK;
  } else {
    // TODO(hchen): Use more specific error code.
    return grpc::Status(grpc::StatusCode::UNKNOWN, ray_status.message());
  }
}

/// Helper function that converts a gRPC status to ray status.
inline Status GrpcStatusToRayStatus(const grpc::Status &grpc_status) {
  if (grpc_status.ok()) {
    return Status::OK();
  } else {
    return Status::IOError(grpc_status.error_message());
  }
}

}  // namespace rpc
}  // namespace ray

#endif
