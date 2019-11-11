#ifndef RAY_COMMON_GRPC_UTIL_H
#define RAY_COMMON_GRPC_UTIL_H

#include <google/protobuf/map.h>
#include <google/protobuf/repeated_field.h>
#include <grpcpp/grpcpp.h>

#include "status.h"

namespace ray {

/// Wrap a protobuf message.
template <class Message>
class MessageWrapper {
 public:
  /// Construct an empty message wrapper. This should not be used directly.
  MessageWrapper() : message_(std::make_shared<Message>()) {}

  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit MessageWrapper(const Message message)
      : message_(std::make_shared<Message>(std::move(message))) {}

  /// Construct from a protobuf message shared_ptr.
  ///
  /// \param message The protobuf message.
  explicit MessageWrapper(std::shared_ptr<Message> message) : message_(message) {}

  /// Construct from protobuf-serialized binary.
  ///
  /// \param serialized_binary Protobuf-serialized binary.
  explicit MessageWrapper(const std::string &serialized_binary)
      : message_(std::make_shared<Message>()) {
    message_->ParseFromString(serialized_binary);
  }

  /// Get const reference of the protobuf message.
  const Message &GetMessage() const { return *message_; }

  /// Get reference of the protobuf message.
  Message &GetMutableMessage() const { return *message_; }

  /// Serialize the message to a string.
  const std::string Serialize() const { return message_->SerializeAsString(); }

 protected:
  /// The wrapped message.
  std::shared_ptr<Message> message_;
};

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

/// Converts a Protobuf `RepeatedPtrField` to a vector.
template <class T>
inline std::vector<T> VectorFromProtobuf(
    const ::google::protobuf::RepeatedPtrField<T> &pb_repeated) {
  return std::vector<T>(pb_repeated.begin(), pb_repeated.end());
}

/// Converts a Protobuf `RepeatedField` to a vector.
template <class T>
inline std::vector<T> VectorFromProtobuf(
    const ::google::protobuf::RepeatedField<T> &pb_repeated) {
  return std::vector<T>(pb_repeated.begin(), pb_repeated.end());
}

/// Converts a Protobuf `RepeatedField` to a vector of IDs.
template <class ID>
inline std::vector<ID> IdVectorFromProtobuf(
    const ::google::protobuf::RepeatedPtrField<::std::string> &pb_repeated) {
  auto str_vec = VectorFromProtobuf(pb_repeated);
  std::vector<ID> ret;
  std::transform(str_vec.begin(), str_vec.end(), std::back_inserter(ret),
                 &ID::FromBinary);
  return ret;
}

/// Converts a Protobuf map to a `unordered_map`.
template <class K, class V>
inline std::unordered_map<K, V> MapFromProtobuf(::google::protobuf::Map<K, V> pb_map) {
  return std::unordered_map<K, V>(pb_map.begin(), pb_map.end());
}

}  // namespace ray

#endif
