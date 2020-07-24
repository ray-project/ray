#include "message/message.h"

#include <cstring>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

StreamingMessage::StreamingMessage(std::shared_ptr<uint8_t> &data, uint32_t data_size,
                                   uint64_t seq_id, StreamingMessageType message_type)
    : message_data_(data),
      data_size_(data_size),
      message_type_(message_type),
      message_id_(seq_id) {}

StreamingMessage::StreamingMessage(std::shared_ptr<uint8_t> &&data, uint32_t data_size,
                                   uint64_t seq_id, StreamingMessageType message_type)
    : message_data_(data),
      data_size_(data_size),
      message_type_(message_type),
      message_id_(seq_id) {}

StreamingMessage::StreamingMessage(const uint8_t *data, uint32_t data_size,
                                   uint64_t seq_id, StreamingMessageType message_type)
    : data_size_(data_size), message_type_(message_type), message_id_(seq_id) {
  message_data_.reset(new uint8_t[data_size], std::default_delete<uint8_t[]>());
  std::memcpy(message_data_.get(), data, data_size_);
}

StreamingMessage::StreamingMessage(const StreamingMessage &msg) {
  data_size_ = msg.data_size_;
  message_data_ = msg.message_data_;
  message_id_ = msg.message_id_;
  message_type_ = msg.message_type_;
}

StreamingMessagePtr StreamingMessage::FromBytes(const uint8_t *bytes,
                                                bool verifer_check) {
  uint32_t byte_offset = 0;
  uint32_t data_size = *reinterpret_cast<const uint32_t *>(bytes + byte_offset);
  byte_offset += sizeof(data_size);

  uint64_t seq_id = *reinterpret_cast<const uint64_t *>(bytes + byte_offset);
  byte_offset += sizeof(seq_id);

  StreamingMessageType msg_type =
      *reinterpret_cast<const StreamingMessageType *>(bytes + byte_offset);
  byte_offset += sizeof(msg_type);

  auto buf = new uint8_t[data_size];
  std::memcpy(buf, bytes + byte_offset, data_size);
  auto data_ptr = std::shared_ptr<uint8_t>(buf, std::default_delete<uint8_t[]>());
  return std::make_shared<StreamingMessage>(data_ptr, data_size, seq_id, msg_type);
}

void StreamingMessage::ToBytes(uint8_t *serlizable_data) {
  uint32_t byte_offset = 0;
  std::memcpy(serlizable_data + byte_offset, reinterpret_cast<char *>(&data_size_),
              sizeof(data_size_));
  byte_offset += sizeof(data_size_);

  std::memcpy(serlizable_data + byte_offset, reinterpret_cast<char *>(&message_id_),
              sizeof(message_id_));
  byte_offset += sizeof(message_id_);

  std::memcpy(serlizable_data + byte_offset, reinterpret_cast<char *>(&message_type_),
              sizeof(message_type_));
  byte_offset += sizeof(message_type_);

  std::memcpy(serlizable_data + byte_offset,
              reinterpret_cast<char *>(message_data_.get()), data_size_);

  byte_offset += data_size_;

  STREAMING_CHECK(byte_offset == this->ClassBytesSize());
}

bool StreamingMessage::operator==(const StreamingMessage &message) const {
  return GetDataSize() == message.GetDataSize() &&
         GetMessageSeqId() == message.GetMessageSeqId() &&
         GetMessageType() == message.GetMessageType() &&
         !std::memcmp(RawData(), message.RawData(), data_size_);
}

}  // namespace streaming
}  // namespace ray
