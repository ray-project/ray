#ifndef RAY_RPC_UTIL_H
#define RAY_RPC_UTIL_H

#include <google/protobuf/map.h>
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

template <class K, class V>
inline std::unordered_map<K, V> MapFromProtobuf(::google::protobuf::Map<K, V> pb_map) {
  return std::unordered_map<K, V>(pb_map.begin(), pb_map.end());
}

template <class T>
inline std::vector<T> VectorFromProtobuf(
    const ::google::protobuf::RepeatedPtrField<T> &pb_repeated) {
  return std::vector<T>(pb_repeated.begin(), pb_repeated.end());
}

template <class T>
inline std::vector<T> VectorFromProtobuf(
    const ::google::protobuf::RepeatedField<T> &pb_repeated) {
  return std::vector<T>(pb_repeated.begin(), pb_repeated.end());
}

template <class ID>
inline std::vector<ID> IdVectorFromProtobuf(
    const ::google::protobuf::RepeatedPtrField<::std::string> &pb_repeated) {
  auto str_vec = VectorFromProtobuf(pb_repeated);
  std::vector<ID> ret(str_vec.size());
  std::transform(str_vec.begin(), str_vec.end(), std::back_inserter(ret),
                 &ID::FromBinary);
  return ret;
}

}  // namespace rpc
}  // namespace ray

#endif
