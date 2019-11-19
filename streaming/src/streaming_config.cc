#include <unistd.h>

#include "streaming_config.h"
#include "streaming_logging.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_STREAMING_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = 20;
// Time to force clean if barrier in queue, default 0ms
const uint32_t StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE = 2048;

uint32_t StreamingConfig::GetStreamingRingBufferCapacity() const {
  return streaming_ring_buffer_capacity;
}

void StreamingConfig::SetStreamingRingBufferCapacity(
    uint32_t streaming_ring_buffer_capacity) {
  StreamingConfig::streaming_ring_buffer_capacity = std::min(
      streaming_ring_buffer_capacity, StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE);
}

uint32_t StreamingConfig::GetStreamingEmptyMessageTimeInterval() const {
  return streaming_empty_message_time_interval;
}

void StreamingConfig::SetStreamingEmptyMessageTimeInterval(
    uint32_t streaming_empty_message_time_interval) {
  StreamingConfig::streaming_empty_message_time_interval =
      streaming_empty_message_time_interval;
}

void StreamingConfig::ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                                     uint32_t value) {
  switch (key) {
  case streaming::fbs::StreamingConfigKey::StreamingEmptyMessageTimeInterval:
    SetStreamingEmptyMessageTimeInterval(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingRingBufferCapacity:
    SetStreamingRingBufferCapacity(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingDefault:
    STREAMING_LOG(INFO) << "skip default key";
    break;
  default:
    STREAMING_LOG(WARNING) << "no such type in uint32";
  }
}
void StreamingConfig::ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                                     const std::string &value) {
  switch (key) {
  case streaming::fbs::StreamingConfigKey::StreamingJobName:
    SetStreamingJobName(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingOpName:
    SetStreamingOpName(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingWorkerName:
    SetStreamingWorkerName(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingDefault:
    STREAMING_LOG(INFO) << "skip default key";
    break;
  default:
    STREAMING_LOG(WARNING) << "no such type in string => " << static_cast<uint32_t>(key);
  }
}

streaming::fbs::StreamingRole StreamingConfig::GetStreamingRole() const {
  return streaming_role;
}

void StreamingConfig::SetStreamingRole(streaming::fbs::StreamingRole streaming_role) {
  StreamingConfig::streaming_role = streaming_role;
}

const std::string &StreamingConfig::GetStreamingJobName() const {
  return streaming_job_name;
}

void StreamingConfig::SetStreamingJobName(const std::string &streaming_job_name) {
  StreamingConfig::streaming_job_name = streaming_job_name;
}

const std::string &StreamingConfig::GetStreamingOpName() const {
  return streaming_op_name;
}

void StreamingConfig::SetStreamingOpName(const std::string &streaming_op_name) {
  StreamingConfig::streaming_op_name = streaming_op_name;
}

const std::string &StreamingConfig::GetStreaming_worker_name() const {
  return streaming_worker_name;
}
void StreamingConfig::SetStreamingWorkerName(const std::string &streaming_worker_name) {
  StreamingConfig::streaming_worker_name = streaming_worker_name;
}

const std::string &StreamingConfig::GetStreamingTaskJobId() const {
  return streaming_task_job_id;
}

void StreamingConfig::SetStreamingTaskJobId(const std::string &streaming_task_job_id) {
  StreamingConfig::streaming_task_job_id = streaming_task_job_id;
}

const std::string &StreamingConfig::GetQueueType() const { return queue_type; }

void StreamingConfig::SetQueueType(const std::string &queue_type) {
  StreamingConfig::queue_type = queue_type;
}
}  // namespace streaming
}  // namespace ray
