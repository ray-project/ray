#include "message/message.h"

#include <cstring>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

StreamingMessage::StreamingMessage(std::shared_ptr<uint8_t> &payload_data,
                                   uint32_t payload_size, uint64_t msg_id,
                                   StreamingMessageType message_type)
    : payload_(payload_data),
      payload_size_(payload_size),
      message_type_(message_type),
      message_id_(msg_id) {}

StreamingMessage::StreamingMessage(std::shared_ptr<uint8_t> &&payload_data,
                                   uint32_t payload_size, uint64_t msg_id,
                                   StreamingMessageType message_type)
    : payload_(payload_data),
      payload_size_(payload_size),
      message_type_(message_type),
      message_id_(msg_id) {}

StreamingMessage::StreamingMessage(const uint8_t *payload_data, uint32_t payload_size,
                                   uint64_t msg_id, StreamingMessageType message_type)
    : payload_size_(payload_size), message_type_(message_type), message_id_(msg_id) {
  payload_.reset(new uint8_t[payload_size], std::default_delete<uint8_t[]>());
  std::memcpy(payload_.get(), payload_data, payload_size);
}

StreamingMessage::StreamingMessage(const StreamingMessage &msg) {
  payload_size_ = msg.payload_size_;
  payload_ = msg.payload_;
  message_id_ = msg.message_id_;
  message_type_ = msg.message_type_;
}

StreamingMessagePtr StreamingMessage::FromBytes(const uint8_t *bytes,
                                                bool verifer_check) {
  uint32_t byte_offset = 0;
  uint32_t data_size = *reinterpret_cast<const uint32_t *>(bytes + byte_offset);
  byte_offset += sizeof(data_size);

  uint64_t msg_id = *reinterpret_cast<const uint64_t *>(bytes + byte_offset);
  byte_offset += sizeof(msg_id);

  StreamingMessageType msg_type =
      *reinterpret_cast<const StreamingMessageType *>(bytes + byte_offset);
  byte_offset += sizeof(msg_type);

  auto buf = new uint8_t[data_size];
  std::memcpy(buf, bytes + byte_offset, data_size);
  auto data_ptr = std::shared_ptr<uint8_t>(buf, std::default_delete<uint8_t[]>());
  return std::make_shared<StreamingMessage>(data_ptr, data_size, msg_id, msg_type);
}

void StreamingMessage::ToBytes(uint8_t *serlizable_data) {
  uint32_t byte_offset = 0;
  std::memcpy(serlizable_data + byte_offset, reinterpret_cast<char *>(&payload_size_),
              sizeof(payload_size_));
  byte_offset += sizeof(payload_size_);

  std::memcpy(serlizable_data + byte_offset, reinterpret_cast<char *>(&message_id_),
              sizeof(message_id_));
  byte_offset += sizeof(message_id_);

  std::memcpy(serlizable_data + byte_offset, reinterpret_cast<char *>(&message_type_),
              sizeof(message_type_));
  byte_offset += sizeof(message_type_);

  std::memcpy(serlizable_data + byte_offset, reinterpret_cast<char *>(payload_.get()),
              payload_size_);

  byte_offset += payload_size_;

  STREAMING_CHECK(byte_offset == this->ClassBytesSize());
}

bool StreamingMessage::operator==(const StreamingMessage &message) const {
  return PayloadSize() == message.PayloadSize() &&
         GetMessageId() == message.GetMessageId() &&
         GetMessageType() == message.GetMessageType() &&
         !std::memcmp(Payload(), message.Payload(), PayloadSize());
}

std::ostream &operator<<(std::ostream &os, const StreamingMessage &message) {
  os << "{"
     << " message_type_: " << static_cast<int>(message.GetMessageType())
     << " message_id_: " << message.GetMessageId()
     << " payload_size_: " << message.payload_size_
     << " payload_: " << (void *)message.payload_.get() << "}";
  return os;
}

}  // namespace streaming
}  // namespace ray
