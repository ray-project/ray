#include "ring_buffer.h"

#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

StreamingRingBuffer::StreamingRingBuffer(size_t buf_size,
                                         StreamingRingBufferType buffer_type) {
  switch (buffer_type) {
  case StreamingRingBufferType::SPSC:
    message_buffer_ =
        std::make_shared<RingBufferImplLockFree<StreamingMessagePtr>>(buf_size);
    break;
  case StreamingRingBufferType::SPSC_LOCK:
  default:
    message_buffer_ =
        std::make_shared<RingBufferImplThreadSafe<StreamingMessagePtr>>(buf_size);
  }
}

bool StreamingRingBuffer::Push(const StreamingMessagePtr &msg) {
  message_buffer_->Push(msg);
  return true;
}

StreamingMessagePtr &StreamingRingBuffer::Front() {
  STREAMING_CHECK(!message_buffer_->Empty());
  return message_buffer_->Front();
}

void StreamingRingBuffer::Pop() {
  STREAMING_CHECK(!message_buffer_->Empty());
  message_buffer_->Pop();
}

bool StreamingRingBuffer::IsFull() const { return message_buffer_->Full(); }

bool StreamingRingBuffer::IsEmpty() const { return message_buffer_->Empty(); }

size_t StreamingRingBuffer::Size() const { return message_buffer_->Size(); }

size_t StreamingRingBuffer::Capacity() const { return message_buffer_->Capacity(); }

size_t StreamingRingBuffer::GetTransientBufferSize() {
  return transient_buffer_.GetTransientBufferSize();
};

void StreamingRingBuffer::SetTransientBufferSize(uint32_t new_transient_buffer_size) {
  return transient_buffer_.SetTransientBufferSize(new_transient_buffer_size);
}

size_t StreamingRingBuffer::GetMaxTransientBufferSize() const {
  return transient_buffer_.GetMaxTransientBufferSize();
}

const uint8_t *StreamingRingBuffer::GetTransientBuffer() const {
  return transient_buffer_.GetTransientBuffer();
}

uint8_t *StreamingRingBuffer::GetTransientBufferMutable() const {
  return transient_buffer_.GetTransientBufferMutable();
}

void StreamingRingBuffer::ReallocTransientBuffer(uint32_t size) {
  transient_buffer_.ReallocTransientBuffer(size);
}

bool StreamingRingBuffer::IsTransientAvaliable() {
  return transient_buffer_.IsTransientAvaliable();
}

void StreamingRingBuffer::FreeTransientBuffer(bool is_force) {
  transient_buffer_.FreeTransientBuffer(is_force);
}

}  // namespace streaming
}  // namespace ray
