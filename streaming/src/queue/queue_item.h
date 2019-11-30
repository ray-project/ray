#ifndef _STREAMING_QUEUE_ITEM_H_
#define _STREAMING_QUEUE_ITEM_H_
#include <iterator>
#include <list>
#include <thread>
#include <vector>

#include "ray/common/id.h"

#include "message/message_bundle.h"
#include "message.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

using ray::ObjectID;
const uint64_t QUEUE_INVALID_SEQ_ID = std::numeric_limits<uint64_t>::max();

class QueueItem {
 public:
  QueueItem(uint64_t seq_id, uint8_t *data, uint32_t data_size, uint64_t timestamp,
            bool raw = false)
      : seq_id_(seq_id),
        timestamp_(timestamp),
        raw_(raw),
        /*COPY*/ buffer_(std::make_shared<LocalMemoryBuffer>(data, data_size, true)) {
  }

  QueueItem(uint64_t seq_id, std::shared_ptr<LocalMemoryBuffer> buffer,
            uint64_t timestamp, bool raw = false)
      : seq_id_(seq_id), timestamp_(timestamp), raw_(raw), buffer_(buffer) {}

  QueueItem(std::shared_ptr<DataMessage> data_msg)
      : seq_id_(data_msg->SeqId()),
        raw_(data_msg->IsRaw()),
        buffer_(data_msg->Buffer()) {}

  QueueItem(const QueueItem &&item) {
    buffer_ = item.buffer_;
    seq_id_ = item.seq_id_;
    timestamp_ = item.timestamp_;
    raw_ = item.raw_;
  }

  QueueItem(const QueueItem &item) {
    buffer_ = item.buffer_;
    seq_id_ = item.seq_id_;
    timestamp_ = item.timestamp_;
    raw_ = item.raw_;
  }

  QueueItem &operator=(const QueueItem &item) {
    buffer_ = item.buffer_;
    seq_id_ = item.seq_id_;
    timestamp_ = item.timestamp_;
    raw_ = item.raw_;
    return *this;
  }

  virtual ~QueueItem() = default;

  uint64_t SeqId() { return seq_id_; }

  bool IsRaw() { return raw_; }

  /// Parse this QueueItem content to get last message id.
  /// Should not be called too frequently.
  /// TODO: Add msg_id_ member variable and set it when PushQueueItem
  uint64_t MaxMsgId() {
    if (raw_) {
      return 0;
    }
    auto message_bundle = StreamingMessageBundleMeta::FromBytes(buffer_->Data());
    return message_bundle->GetLastMessageId();
  }

  uint64_t TimeStamp() { return timestamp_; }

  size_t DataSize() { return buffer_->Size(); }

  std::shared_ptr<LocalMemoryBuffer> Buffer() { return buffer_; }

 protected:
  uint64_t seq_id_;
  uint64_t timestamp_;
  bool raw_;

  std::shared_ptr<LocalMemoryBuffer> buffer_;
};

class NullQueueItem : public QueueItem {
 public:
  NullQueueItem() : QueueItem(QUEUE_INVALID_SEQ_ID, data_, 1, 0) {}
 private:
  uint8_t data_[1];
};
typedef std::shared_ptr<QueueItem> QueueItemPtr;

}  // namespace streaming
}  // namespace ray
#endif
