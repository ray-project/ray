#include <unistd.h>

#include "streaming_config.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_STREAMING_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = 20;
// Time to force clean if barrier in queue, default 0ms
const uint32_t StreamingConfig::STREAMING_MESSAGE_BUNDLE_MAX_SIZE = 2048;

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
  if (config.role() != proto::OperatorType::UNKNOWN) {
    SetOperatorType(config.role());
  }
  if (config.ring_buffer_capacity() != 0) {
    SetStreamingRingBufferCapacity(config.ring_buffer_capacity());
  }
  if (config.empty_message_interval() != 0) {
    SetStreamingEmptyMessageTimeInterval(config.empty_message_interval());
  }
}

uint32_t StreamingConfig::GetStreamingRingBufferCapacity() const {
  return streaming_ring_buffer_capacity_;
}

void StreamingConfig::SetStreamingRingBufferCapacity(
    uint32_t ring_buffer_capacity) {
  StreamingConfig::streaming_ring_buffer_capacity_ = std::min(
      ring_buffer_capacity, StreamingConfig::STREAMING_MESSAGE_BUNDLE_MAX_SIZE);
}

uint32_t StreamingConfig::GetStreamingEmptyMessageTimeInterval() const {
  return streaming_empty_message_time_interval_;
}

void StreamingConfig::SetStreamingEmptyMessageTimeInterval(
    uint32_t empty_message_time_interval) {
  StreamingConfig::streaming_empty_message_time_interval_ =
      empty_message_time_interval;
}

streaming::proto::OperatorType StreamingConfig::GetOperatorType() const {
  return operator_type_;
}

void StreamingConfig::SetOperatorType(streaming::proto::OperatorType type) {
  StreamingConfig::operator_type_ = type;
}

const std::string &StreamingConfig::GetStreamingJobName() const {
  return streaming_job_name_;
}

void StreamingConfig::SetStreamingJobName(const std::string &job_name) {
  StreamingConfig::streaming_job_name_ = job_name;
}

const std::string &StreamingConfig::GetStreamingOpName() const {
  return streaming_op_name_;
}

void StreamingConfig::SetStreamingOpName(const std::string &op_name) {
  StreamingConfig::streaming_op_name_ = op_name;
}

const std::string &StreamingConfig::GetStreaming_worker_name() const {
  return streaming_worker_name_;
}
void StreamingConfig::SetStreamingWorkerName(const std::string &worker_name) {
  StreamingConfig::streaming_worker_name_ = worker_name;
}

const std::string &StreamingConfig::GetStreamingTaskJobId() const {
  return streaming_task_job_id_;
}

void StreamingConfig::SetStreamingTaskJobId(const std::string &task_job_id) {
  StreamingConfig::streaming_task_job_id_ = task_job_id;
}

}  // namespace streaming
}  // namespace ray
