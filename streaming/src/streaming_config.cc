#include <unistd.h>

#include "streaming_config.h"
#include "streaming_logging.h"
#include "streaming.pb.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_STREAMING_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = 20;
// Time to force clean if barrier in queue, default 0ms
const uint32_t StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE = 2048;

void StreamingConfig::FromProto(const uint8_t * data, uint32_t size) {
  proto::StreamingConfig config;
  STREAMING_CHECK(config.ParseFromArray(data, size)) << "Parse streaming conf failed";
  if (!config.job_name().empty()) {
    SetStreamingJobName(config.job_name());
  }
  if (!config.task_job_id().empty()) {
    STREAMING_CHECK(config.task_job_id().size() == 2 * JobID::Size());
    SetStreamingTaskJobId(config.task_job_id());
  }
  if (!config.worker_name().empty()) {
    SetStreamingWorkerName(config.worker_name());
  }
  if (!config.op_name().empty()) {
    SetStreamingOpName(config.op_name());
  }
  if (config.role() != proto::StreamingRole::UNKNOWN) {
    SetStreamingRole(config.role());
  }
  if (config.ring_buffer_capacity() != 0) {
    SetStreamingRingBufferCapacity(config.ring_buffer_capacity());
  }
  if (config.empty_message_interval() != 0) {
    SetStreamingEmptyMessageTimeInterval(config.empty_message_interval());
  }
}

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

streaming::proto::StreamingRole StreamingConfig::GetStreamingRole() const {
  return streaming_role;
}

void StreamingConfig::SetStreamingRole(streaming::proto::StreamingRole streaming_role) {
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

}  // namespace streaming
}  // namespace ray
