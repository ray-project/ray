#ifndef _STREAMING_QUEUE_ITEM_H_
#define _STREAMING_QUEUE_ITEM_H_
#include <iterator>
#include <list>
#include <thread>
#include <vector>

#include "ray/common/id.h"

#include "message.h"
#include "message/message_bundle.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

using ray::ObjectID;
const uint64_t QUEUE_INVALID_SEQ_ID = std::numeric_limits<uint64_t>::max();

/// QueueItem is the element stored in `Queue`. Actually, when DataWriter pushes a message
/// bundle into a queue, the bundle is packed into one QueueItem, so a one-to-one
/// relationship exists between message bundle and QueueItem. Meanwhile, the QueueItem is
/// also the minimum unit to send through direct actor call. Each QueueItem holds a
/// LocalMemoryBuffer shared_ptr, which will be sent out by Transport.
class QueueItem {
 public:
  /// Construct a QueueItem object.
  /// \param[in] seq_id the sequential id assigned by DataWriter for a message bundle and
  /// QueueItem.
  /// \param[in] data the data buffer to be stored in this QueueItem.
  /// \param[in] data_size the data size in bytes.
  /// \param[in] timestamp the time when this QueueItem created.
  /// \param[in] raw whether the data content is raw bytes, only used in some tests.
  QueueItem(uint64_t seq_id, uint8_t *data, uint32_t data_size, uint64_t timestamp,
            bool raw = false);

  QueueItem(uint64_t seq_id, std::shared_ptr<LocalMemoryBuffer> buffer,
            uint64_t timestamp, bool raw = false)
      : seq_id_(seq_id), timestamp_(timestamp), raw_(raw), buffer_(buffer) {}

  QueueItem(std::shared_ptr<DataMessage> data_msg)
      : seq_id_(data_msg->SeqId()),
        raw_(data_msg->IsRaw()),
        buffer_(data_msg->Buffer()) {}

  QueueItem(const QueueItem &&item);

  QueueItem(const QueueItem &item);

  QueueItem &operator=(const QueueItem &item);

  virtual ~QueueItem();

  uint64_t SeqId();
  bool IsRaw();
  uint64_t TimeStamp();
  size_t DataSize();
  std::shared_ptr<LocalMemoryBuffer> Buffer();

  /// Get max message id in this item.
  /// \return max message id.
  uint64_t MaxMsgId();

 protected:
  uint64_t seq_id_;
  uint64_t timestamp_;
  bool raw_;

  std::shared_ptr<LocalMemoryBuffer> buffer_;
};

class InvalidQueueItem : public QueueItem {
 public:
  InvalidQueueItem();

 private:
  uint8_t data_[1];
};
typedef std::shared_ptr<QueueItem> QueueItemPtr;

}  // namespace streaming
}  // namespace ray
#endif
