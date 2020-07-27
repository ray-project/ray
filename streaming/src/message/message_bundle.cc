#include "message/message_bundle.h"

#include <cstring>
#include <string>

#include "config/streaming_config.h"
#include "ray/common/status.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {
StreamingMessageBundle::StreamingMessageBundle(uint64_t last_offset_seq_id,
                                               uint64_t message_bundle_ts)
    : StreamingMessageBundleMeta(message_bundle_ts, last_offset_seq_id, 0,
                                 StreamingMessageBundleType::Empty) {
  this->raw_bundle_size_ = 0;
}

StreamingMessageBundleMeta::StreamingMessageBundleMeta(const uint8_t *bytes) {
  std::memcpy(GetFirstMemberAddress(), bytes,
              kMessageBundleMetaHeaderSize - sizeof(uint32_t));
}

StreamingMessageBundleMeta::StreamingMessageBundleMeta(
    const uint64_t message_bundle_ts, const uint64_t last_offset_seq_id,
    const uint32_t message_list_size, const StreamingMessageBundleType bundle_type)
    : message_bundle_ts_(message_bundle_ts),
      last_message_id_(last_offset_seq_id),
      message_list_size_(message_list_size),
      bundle_type_(bundle_type) {
  STREAMING_CHECK(message_list_size <= StreamingConfig::MESSAGE_BUNDLE_MAX_SIZE);
}

void StreamingMessageBundleMeta::ToBytes(uint8_t *bytes) {
  uint32_t magicNum = StreamingMessageBundleMeta::StreamingMessageBundleMagicNum;
  std::memcpy(bytes, reinterpret_cast<const uint8_t *>(&magicNum), sizeof(uint32_t));
  std::memcpy(bytes + sizeof(uint32_t), GetFirstMemberAddress(),
              kMessageBundleMetaHeaderSize - sizeof(uint32_t));
}

StreamingMessageBundleMetaPtr StreamingMessageBundleMeta::FromBytes(const uint8_t *bytes,
                                                                    bool check) {
  STREAMING_CHECK(bytes);

  uint32_t byte_offset = 0;
  STREAMING_CHECK(CheckBundleMagicNum(bytes));
  byte_offset += sizeof(uint32_t);

  auto result = std::make_shared<StreamingMessageBundleMeta>(bytes + byte_offset);
  STREAMING_CHECK(result->GetMessageListSize() <=
                  StreamingConfig::MESSAGE_BUNDLE_MAX_SIZE);
  return result;
}

bool StreamingMessageBundleMeta::operator==(StreamingMessageBundleMeta &meta) const {
  return this->message_list_size_ == meta.GetMessageListSize() &&
         this->message_bundle_ts_ == meta.GetMessageBundleTs() &&
         this->bundle_type_ == meta.GetBundleType() &&
         this->last_message_id_ == meta.GetLastMessageId();
}

bool StreamingMessageBundleMeta::operator==(StreamingMessageBundleMeta *meta) const {
  return operator==(*meta);
}

StreamingMessageBundleMeta::StreamingMessageBundleMeta()
    : bundle_type_(StreamingMessageBundleType::Empty) {}

StreamingMessageBundle::StreamingMessageBundle(
    std::list<StreamingMessagePtr> &&message_list, uint64_t message_ts,
    uint64_t last_offset_seq_id, StreamingMessageBundleType bundle_type,
    uint32_t raw_data_size)
    : StreamingMessageBundleMeta(message_ts, last_offset_seq_id, message_list.size(),
                                 bundle_type),
      raw_bundle_size_(raw_data_size),
      message_list_(message_list) {
  if (bundle_type_ != StreamingMessageBundleType::Empty) {
    if (!raw_bundle_size_) {
      raw_bundle_size_ = std::accumulate(
          message_list_.begin(), message_list_.end(), 0,
          [](uint32_t x, StreamingMessagePtr &y) { return x + y->ClassBytesSize(); });
    }
  }
}

StreamingMessageBundle::StreamingMessageBundle(
    std::list<StreamingMessagePtr> &message_list, uint64_t message_ts,
    uint64_t last_offset_seq_id, StreamingMessageBundleType bundle_type,
    uint32_t raw_data_size)
    : StreamingMessageBundle(std::list<StreamingMessagePtr>(message_list), message_ts,
                             last_offset_seq_id, bundle_type, raw_data_size) {}

StreamingMessageBundle::StreamingMessageBundle(StreamingMessageBundle &bundle) {
  message_bundle_ts_ = bundle.message_bundle_ts_;
  message_list_size_ = bundle.message_list_size_;
  raw_bundle_size_ = bundle.raw_bundle_size_;
  bundle_type_ = bundle.bundle_type_;
  last_message_id_ = bundle.last_message_id_;
  message_list_ = bundle.message_list_;
}

void StreamingMessageBundle::ToBytes(uint8_t *bytes) {
  uint32_t byte_offset = 0;
  StreamingMessageBundleMeta::ToBytes(bytes + byte_offset);

  byte_offset += StreamingMessageBundleMeta::ClassBytesSize();

  std::memcpy(bytes + byte_offset, reinterpret_cast<char *>(&raw_bundle_size_),
              sizeof(uint32_t));
  byte_offset += sizeof(uint32_t);

  if (raw_bundle_size_ > 0) {
    ConvertMessageListToRawData(message_list_, raw_bundle_size_, bytes + byte_offset);
  }
}

StreamingMessageBundlePtr StreamingMessageBundle::FromBytes(const uint8_t *bytes,
                                                            bool verifer_check) {
  uint32_t byte_offset = 0;
  StreamingMessageBundleMetaPtr meta_ptr =
      StreamingMessageBundleMeta::FromBytes(bytes + byte_offset);
  byte_offset += meta_ptr->ClassBytesSize();

  uint32_t raw_data_size = *reinterpret_cast<const uint32_t *>(bytes + byte_offset);
  byte_offset += sizeof(uint32_t);

  std::list<StreamingMessagePtr> message_list;
  // only message bundle own raw data
  if (meta_ptr->GetBundleType() != StreamingMessageBundleType::Empty) {
    GetMessageListFromRawData(bytes + byte_offset, raw_data_size,
                              meta_ptr->GetMessageListSize(), message_list);
    byte_offset += raw_data_size;
  }
  auto result = std::make_shared<StreamingMessageBundle>(
      message_list, meta_ptr->GetMessageBundleTs(), meta_ptr->GetLastMessageId(),
      meta_ptr->GetBundleType());
  STREAMING_CHECK(byte_offset == result->ClassBytesSize());
  return result;
}

void StreamingMessageBundle::GetMessageListFromRawData(
    const uint8_t *bytes, uint32_t byte_size, uint32_t message_list_size,
    std::list<StreamingMessagePtr> &message_list) {
  uint32_t byte_offset = 0;
  // only message bundle own raw data
  for (size_t i = 0; i < message_list_size; ++i) {
    StreamingMessagePtr item = StreamingMessage::FromBytes(bytes + byte_offset);
    message_list.push_back(item);
    byte_offset += item->ClassBytesSize();
  }
  STREAMING_CHECK(byte_offset == byte_size);
}

void StreamingMessageBundle::GetMessageList(
    std::list<StreamingMessagePtr> &message_list) {
  message_list = message_list_;
}

void StreamingMessageBundle::ConvertMessageListToRawData(
    const std::list<StreamingMessagePtr> &message_list, uint32_t raw_data_size,
    uint8_t *raw_data) {
  uint32_t byte_offset = 0;
  for (auto &message : message_list) {
    message->ToBytes(raw_data + byte_offset);
    byte_offset += message->ClassBytesSize();
  }
  STREAMING_CHECK(byte_offset == raw_data_size);
}

bool StreamingMessageBundle::operator==(StreamingMessageBundle &bundle) const {
  if (!(StreamingMessageBundleMeta::operator==(&bundle) &&
        this->GetRawBundleSize() == bundle.GetRawBundleSize() &&
        this->GetMessageListSize() == bundle.GetMessageListSize())) {
    return false;
  }
  auto it1 = message_list_.begin();
  auto it2 = bundle.message_list_.begin();
  while (it1 != message_list_.end() && it2 != bundle.message_list_.end()) {
    if (!((*it1).get()->operator==(*(*it2).get()))) {
      return false;
    }
    it1++;
    it2++;
  }
  return true;
}

bool StreamingMessageBundle::operator==(StreamingMessageBundle *bundle) const {
  return this->operator==(*bundle);
}
}  // namespace streaming
}  // namespace ray
