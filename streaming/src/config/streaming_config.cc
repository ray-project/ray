#include <unistd.h>

#include "streaming_config.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_EMPTY_MESSAGE_TIME_INTERVAL = 20;
// Time to force clean if barrier in queue, default 0ms
const uint32_t StreamingConfig::MESSAGE_BUNDLE_MAX_SIZE = 2048;

void StreamingConfig::FromProto(const uint8_t *data, uint32_t size) {
  proto::StreamingConfig config;
  STREAMING_CHECK(config.ParseFromArray(data, size)) << "Parse streaming conf failed";
  if (!config.job_name().empty()) {
    SetJobName(config.job_name());
  }
  if (!config.task_job_id().empty()) {
    STREAMING_CHECK(config.task_job_id().size() == 2 * JobID::Size());
    SetTaskJobId(config.task_job_id());
  }
  if (!config.worker_name().empty()) {
    SetWorkerName(config.worker_name());
  }
  if (!config.op_name().empty()) {
    SetOpName(config.op_name());
  }
  if (config.role() != proto::OperatorType::UNKNOWN) {
    SetOperatorType(config.role());
  }
  if (config.ring_buffer_capacity() != 0) {
    SetRingBufferCapacity(config.ring_buffer_capacity());
  }
  if (config.empty_message_interval() != 0) {
    SetEmptyMessageTimeInterval(config.empty_message_interval());
  }
}

uint32_t StreamingConfig::GetRingBufferCapacity() const { return ring_buffer_capacity_; }

void StreamingConfig::SetRingBufferCapacity(uint32_t ring_buffer_capacity) {
  StreamingConfig::ring_buffer_capacity_ =
      std::min(ring_buffer_capacity, StreamingConfig::MESSAGE_BUNDLE_MAX_SIZE);
}
}  // namespace streaming
}  // namespace ray
