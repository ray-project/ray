#include "queue/queue_item.h"

namespace ray {
namespace streaming {

QueueItem::QueueItem(uint64_t seq_id, uint8_t *data, uint32_t data_size,
                     uint64_t timestamp, bool raw)
    : seq_id_(seq_id),
      timestamp_(timestamp),
      raw_(raw),
      /*COPY*/ buffer_(std::make_shared<LocalMemoryBuffer>(data, data_size, true)) {}

uint64_t QueueItem::MaxMsgId() {
  if (raw_) {
    return 0;
  }
  auto message_bundle = StreamingMessageBundleMeta::FromBytes(buffer_->Data());
  return message_bundle->GetLastMessageId();
}

}  // namespace streaming
}  // namespace ray
