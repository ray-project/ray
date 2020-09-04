#pragma once

#include <iterator>
#include <list>
#include <thread>
#include <vector>

#include "message/message_bundle.h"
#include "queue/message.h"
#include "ray/common/id.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

using ray::ObjectID;
const uint64_t QUEUE_INVALID_SEQ_ID = std::numeric_limits<uint64_t>::max();
const uint64_t QUEUE_INITIAL_SEQ_ID = 1;

/// QueueItem is the element stored in `Queue`. Actually, when DataWriter pushes a message
/// bundle into a queue, the bundle is packed into one QueueItem, so a one-to-one
/// relationship exists between message bundle and QueueItem. Meanwhile, the QueueItem is
/// also the minimum unit to send through direct actor call. Each QueueItem holds a
/// LocalMemoryBuffer shared_ptr, which will be sent out by Transport.
class QueueItem {
 public:
  QueueItem() = default;
  /// Construct a QueueItem object.
  /// \param[in] seq_id the sequential id assigned by DataWriter for a message bundle and
  /// QueueItem.
  /// \param[in] data the data buffer to be stored in this QueueItem.
  /// \param[in] data_size the data size in bytes.
  /// \param[in] timestamp the time when this QueueItem created.
  /// \param[in] raw whether the data content is raw bytes, only used in some tests.
  QueueItem(uint64_t seq_id, uint8_t *data, uint32_t data_size, uint64_t timestamp,
            uint64_t msg_id_start, uint64_t msg_id_end, bool raw = false)
      : seq_id_(seq_id),
        msg_id_start_(msg_id_start),
        msg_id_end_(msg_id_end),
        timestamp_(timestamp),
        raw_(raw),
        /*COPY*/ buffer_(std::make_shared<LocalMemoryBuffer>(data, data_size, true)) {}

  QueueItem(uint64_t seq_id, std::shared_ptr<LocalMemoryBuffer> buffer,
            uint64_t timestamp, uint64_t msg_id_start, uint64_t msg_id_end,
            bool raw = false)
      : seq_id_(seq_id),
        msg_id_start_(msg_id_start),
        msg_id_end_(msg_id_end),
        timestamp_(timestamp),
        raw_(raw),
        buffer_(buffer) {}

  QueueItem(std::shared_ptr<DataMessage> data_msg)
      : seq_id_(data_msg->SeqId()),
        msg_id_start_(data_msg->MsgIdStart()),
        msg_id_end_(data_msg->MsgIdEnd()),
        raw_(data_msg->IsRaw()),
        buffer_(data_msg->Buffer()) {}

  QueueItem(const QueueItem &&item) {
    buffer_ = item.buffer_;
    seq_id_ = item.seq_id_;
    msg_id_start_ = item.msg_id_start_;
    msg_id_end_ = item.msg_id_end_;
    timestamp_ = item.timestamp_;
    raw_ = item.raw_;
  }

  QueueItem(const QueueItem &item) {
    buffer_ = item.buffer_;
    seq_id_ = item.seq_id_;
    msg_id_start_ = item.msg_id_start_;
    msg_id_end_ = item.msg_id_end_;
    timestamp_ = item.timestamp_;
    raw_ = item.raw_;
  }

  QueueItem &operator=(const QueueItem &item) {
    buffer_ = item.buffer_;
    seq_id_ = item.seq_id_;
    msg_id_start_ = item.msg_id_start_;
    msg_id_end_ = item.msg_id_end_;
    timestamp_ = item.timestamp_;
    raw_ = item.raw_;
    return *this;
  }

  virtual ~QueueItem() = default;

  inline uint64_t SeqId() { return seq_id_; }
  inline uint64_t MsgIdStart() { return msg_id_start_; }
  inline uint64_t MsgIdEnd() { return msg_id_end_; }
  inline bool InItem(uint64_t msg_id) {
    return msg_id >= msg_id_start_ && msg_id <= msg_id_end_;
  }
  inline bool IsRaw() { return raw_; }
  inline uint64_t TimeStamp() { return timestamp_; }
  inline size_t DataSize() { return buffer_->Size(); }
  inline std::shared_ptr<LocalMemoryBuffer> Buffer() { return buffer_; }

  /// Get max message id in this item.
  /// \return max message id.
  uint64_t MaxMsgId() {
    if (raw_) {
      return 0;
    }
    auto message_bundle = StreamingMessageBundleMeta::FromBytes(buffer_->Data());
    return message_bundle->GetLastMessageId();
  }

 protected:
  uint64_t seq_id_;
  uint64_t msg_id_start_;
  uint64_t msg_id_end_;
  uint64_t timestamp_;
  bool raw_;

  std::shared_ptr<LocalMemoryBuffer> buffer_;
};

class InvalidQueueItem : public QueueItem {
 public:
  InvalidQueueItem()
      : QueueItem(QUEUE_INVALID_SEQ_ID, data_, 1, 0, QUEUE_INVALID_SEQ_ID,
                  QUEUE_INVALID_SEQ_ID) {}

 private:
  uint8_t data_[1];
};
typedef std::shared_ptr<QueueItem> QueueItemPtr;

}  // namespace streaming
}  // namespace ray
