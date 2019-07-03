#ifndef RAY_RPC_WRAPPER_H
#define RAY_RPC_WRAPPER_H

#include <memory>

namespace ray {

namespace rpc {

template <class Message>
class MessageWrapper {
 public:
  explicit MessageWrapper(const Message message)
      : message_(std::move(message)) {
  }

  explicit MessageWrapper(const std::string &serialized_binary) {
    message_.ParseFromString(serialized_binary);
  }

  const Message &GetMessage() const {
    return message_;
  }

  const std::string Serialize() const {
    return message_.SerializeAsString();
  }

 protected:
  Message message_;
};

}  // namespace rpc

}  // namespace ray

#endif  // RAY_RPC_WRAPPER_H
