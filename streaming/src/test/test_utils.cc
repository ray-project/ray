#include "test_utils.h"

namespace ray {
namespace streaming {
namespace util {

ray::streaming::Buffer ToMessageBuffer(StreamingWriter *writer, const ray::ObjectID &qid,
                                       uint8_t *data, uint32_t size) {
  ray::streaming::Buffer buffer;
  auto buffer_pool = writer->GetBufferPool(qid);
  STREAMING_CHECK(buffer_pool->GetBuffer(size, &buffer) == StreamingStatus::OK);
  while (buffer.Size() < size) {
    STREAMING_LOG(INFO) << "can't get enough buffer to write, need " << size
                        << ", wait 1 ms, pool usage " << buffer_pool->PrintUsage();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    STREAMING_CHECK(buffer_pool->GetBuffer(size, &buffer) == StreamingStatus::OK);
  }
  std::memcpy(buffer.Data(), data, size);
  return ray::streaming::Buffer(buffer.Data(), size);
}

StreamingMessagePtr MakeMessagePtr(
    const uint8_t *data, uint32_t size, uint64_t seq_id,
    StreamingMessageType msg_type = StreamingMessageType::Message) {
  auto buf = new uint8_t[size];
  std::memcpy(buf, data, size);
  auto data_ptr = std::shared_ptr<uint8_t>(buf, std::default_delete<uint8_t[]>());
  return std::make_shared<StreamingMessage>(data_ptr, size, seq_id, msg_type);
}

}  // namespace util
}  // namespace streaming
}  // namespace ray
