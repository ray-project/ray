#include "queue/queue_item.h"

namespace ray {
namespace streaming {

QueueItem::QueueItem(uint64_t seq_id, uint8_t *data, uint32_t data_size,
                     uint64_t timestamp, bool raw)
    : seq_id_(seq_id),
      timestamp_(timestamp),
      raw_(raw),
      /*COPY*/ buffer_(std::make_shared<LocalMemoryBuffer>(data, data_size, true)) {}

QueueItem::QueueItem(const QueueItem &&item) {
  buffer_ = item.buffer_;
  seq_id_ = item.seq_id_;
  timestamp_ = item.timestamp_;
  raw_ = item.raw_;
}

QueueItem::QueueItem(const QueueItem &item) {
  buffer_ = item.buffer_;
  seq_id_ = item.seq_id_;
  timestamp_ = item.timestamp_;
  raw_ = item.raw_;
}

QueueItem &QueueItem::operator=(const QueueItem &item) {
  buffer_ = item.buffer_;
  seq_id_ = item.seq_id_;
  timestamp_ = item.timestamp_;
  raw_ = item.raw_;
  return *this;
}

QueueItem::~QueueItem() = default;

uint64_t QueueItem::SeqId() { return seq_id_; }

bool QueueItem::IsRaw() { return raw_; }

uint64_t QueueItem::TimeStamp() { return timestamp_; }

size_t QueueItem::DataSize() { return buffer_->Size(); }

std::shared_ptr<LocalMemoryBuffer> QueueItem::Buffer() { return buffer_; }

uint64_t QueueItem::MaxMsgId() {
  if (raw_) {
    return 0;
  }
  auto message_bundle = StreamingMessageBundleMeta::FromBytes(buffer_->Data());
  return message_bundle->GetLastMessageId();
}

InvalidQueueItem::InvalidQueueItem() : QueueItem(QUEUE_INVALID_SEQ_ID, data_, 1, 0) {}

}  // namespace streaming
}  // namespace ray
