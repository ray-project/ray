#ifndef RAY_RPC_UTIL_H
#define RAY_RPC_UTIL_H

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"

namespace ray {

inline ::grpc::Status RayStatusToGrpcStatus(const Status &ray_status) {
  if (ray_status.ok()) {
    return ::grpc::Status::OK;
  } else {
    return ::grpc::Status(::grpc::StatusCode::UNKNOWN, ray_status.message());
  }
}

inline Status GrpcStatusToRayStatus(const ::grpc::Status &grpc_status) {
  if (grpc_status.ok()) {
    return Status::OK();
  } else {
    return Status::IOError(grpc_status.error_message());
  }
}

}  // namespace ray

#endif
