#ifndef RAY_RPC_WRAPPER_H
#define RAY_RPC_WRAPPER_H

#include <memory>

namespace ray {

namespace rpc {

template <class Message>
class MessageWrapper {
 public:
  explicit MessageWrapper(Message &message) : message_(&message) {
    RAY_CHECK(message_ != nullptr);
  }

  explicit MessageWrapper(std::unique_ptr<Message> message)
      : message_unique_ptr(std::move(message)), message_(message_unique_ptr.get()) {
    RAY_CHECK(message_ != nullptr);
  }

  explicit MessageWrapper(const std::string &serialized_binary) {
    message_unique_ptr->reset(new Message);
    message_ = message_unique_ptr.get();
    message_->ParseFromString(serialized_binary);
  }

  MessageWrapper(const MessageWrapper<Message> &from)
      : MessageWrapper(std::unique_ptr<Message>(new Message(from.GetMessage()))) {}

  const Message &GetMessage() const { return *message_; }

  const std::string Serialize() const {
    std::string ret;
    message_->SerializeToString(&ret);
    return ret;
  }

 protected:
  std::unique_ptr<Message> message_unique_ptr;
  Message *message_;
};

template <class Message>
class ConstMessageWrapper {
 public:
  explicit ConstMessageWrapper(const Message &message) : message_(&message) {
    RAY_CHECK(message_ != nullptr);
  }

  explicit ConstMessageWrapper(std::unique_ptr<const Message> message)
  : message_unique_ptr(std::move(message)), message_(message_unique_ptr.get()) {
    RAY_CHECK(message_ != nullptr);
  }

  explicit ConstMessageWrapper(const std::string &serialized_binary) {
    auto message = new Message();
    RAY_CHECK(message->ParseFromString(serialized_binary));
    message_unique_ptr.reset(message);
    message_ = message_unique_ptr.get();
  }

  ConstMessageWrapper(const ConstMessageWrapper<Message> &from)
      : ConstMessageWrapper(std::unique_ptr<Message>(new Message(from.GetMessage()))) {}

  const Message &GetMessage() const { return *message_; }

  const std::string Serialize() const {
    return message_->SerializeAsString();
  }

 protected:
  std::unique_ptr<const Message> message_unique_ptr;
  const Message *message_;
};

}  // namespace rpc

}  // namespace ray

#endif  // RAY_RPC_WRAPPER_H
