#ifndef RAY_RPC_WRAPPER_H
#define RAY_RPC_WRAPPER_H

#include <memory>
#include <string>

namespace ray {

namespace rpc {

template <class Message>
class MessageWrapper {
 public:
  explicit MessageWrapper(Message &message) : message_(&message) {}

  explicit MessageWrapper(std::unique_ptr<Message> message)
      : message_unique_ptr_(std::move(message)), message_(message_unique_ptr_.get()) {}

  MessageWrapper(const MessageWrapper<Message> &from)
      : MessageWrapper(std::unique_ptr<Message>(new Message(from.GetMessage()))) {}

  const Message &GetMessage() const { return *message_; }

  const std::string Serialize() const {
    std::string ret;
    message_->SerializeToString(&ret);
    return ret;
  }

 protected:
  std::unique_ptr<Message> message_unique_ptr_;
  Message *message_;
};

template <class Message>
class ConstMessageWrapper {
 public:
  explicit ConstMessageWrapper(const Message &message) : message_(&message) {}

  explicit ConstMessageWrapper(std::unique_ptr<const Message> message)
  : message_unique_ptr_(std::move(message)), message_(message_unique_ptr_.get()) {}

  ConstMessageWrapper(const ConstMessageWrapper<Message> &from)
      : ConstMessageWrapper(std::unique_ptr<Message>(new Message(from.GetMessage()))) {}

  const Message &GetMessage() const { return *message_; }

  const std::string Serialize() const {
    std::string ret;
    message_->SerializeToString(&ret);
    return ret;
  }

 protected:
  std::unique_ptr<const Message> message_unique_ptr_;
  const Message *message_;
};

}  // namespace rpc

}  // namespace ray

#endif  // RAY_RPC_WRAPPER_H
