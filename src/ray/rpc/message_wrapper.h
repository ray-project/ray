#ifndef RAY_RPC_WRAPPER_H
#define RAY_RPC_WRAPPER_H

#include <memory>

namespace ray {

namespace rpc {

/// Wrap a protobuf message.
template <class Message>
class MessageWrapper {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit MessageWrapper(const Message message) : message_(std::move(message)) {}

  /// Construct from protobuf-serialized binary.
  ///
  /// \param serialized_binary Protobuf-serialized binary.
  explicit MessageWrapper(const std::string &serialized_binary) {
    message_.ParseFromString(serialized_binary);
  }

  /// Get reference of the protobuf message.
  const Message &GetMessage() const { return message_; }

  /// Serialize the message to a string.
  const std::string Serialize() const { return message_.SerializeAsString(); }

 protected:
  /// The wrapped message.
  Message message_;
};

}  // namespace rpc

}  // namespace ray

#endif  // RAY_RPC_WRAPPER_H
