#include <unistd.h>

#include "streaming_config.h"
#include "streaming_logging.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_STREAMING_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_STREAMING_BUFFER_POOL_SIZE = 1024 * 1024;  // 1M
uint32_t StreamingConfig::DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = 20;
// Time to force clean if barrier in queue, default 0ms
const uint32_t StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE = 2048;

uint32_t StreamingConfig::GetStreaming_ring_buffer_capacity() const {
  return streaming_ring_buffer_capacity;
}

void StreamingConfig::SetStreaming_ring_buffer_capacity(
    uint32_t streaming_ring_buffer_capacity) {
  StreamingConfig::streaming_ring_buffer_capacity = std::min(
      streaming_ring_buffer_capacity, StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE);
}

uint32_t StreamingConfig::GetStreaming_buffer_pool_size() const {
  return streaming_buffer_pool_size_;
}

void StreamingConfig::SetStreaming_buffer_pool_size(uint32_t streaming_buffer_pool_size) {
  streaming_buffer_pool_size_ = streaming_buffer_pool_size;
}

uint32_t StreamingConfig::GetStreaming_buffer_pool_min_buffer_size() const {
  return streaming_buffer_pool_min_buffer_size_;
}

void StreamingConfig::SetStreaming_buffer_pool_min_buffer_size(
    uint32_t streaming_buffer_pool_min_buffer_size) {
  streaming_buffer_pool_min_buffer_size_ = streaming_buffer_pool_min_buffer_size;
}

uint32_t StreamingConfig::GetStreaming_empty_message_time_interval() const {
  return streaming_empty_message_time_interval;
}

void StreamingConfig::SetStreaming_empty_message_time_interval(
    uint32_t streaming_empty_message_time_interval) {
  StreamingConfig::streaming_empty_message_time_interval =
      streaming_empty_message_time_interval;
}

void StreamingConfig::ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                                     uint32_t value) {
  switch (key) {
  case streaming::fbs::StreamingConfigKey::StreamingEmptyMessageTimeInterval:
    SetStreaming_empty_message_time_interval(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingRingBufferCapacity:
    SetStreaming_ring_buffer_capacity(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingLogLevel:
    SetStreaming_log_level(value);
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
  case streaming::fbs::StreamingConfigKey::StreamingLogPath:
    SetStreaming_log_path(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingJobName:
    SetStreaming_job_name(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingOpName:
    SetStreaming_op_name(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingWorkerName:
    SetStreaming_worker_name(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingDefault:
    STREAMING_LOG(INFO) << "skip default key";
    break;
  default:
    STREAMING_LOG(WARNING) << "no such type in string => " << static_cast<uint32_t>(key);
  }
}

streaming::fbs::StreamingRole StreamingConfig::GetStreaming_role() const {
  return streaming_role;
}

void StreamingConfig::SetStreaming_role(streaming::fbs::StreamingRole streaming_role) {
  StreamingConfig::streaming_role = streaming_role;
}

const std::string &StreamingConfig::GetStreaming_log_path() const {
  return streaming_log_path;
}

void StreamingConfig::SetStreaming_log_path(const std::string &streaming_log_path) {
  StreamingConfig::streaming_log_path = streaming_log_path;
}
const std::string &StreamingConfig::GetStreaming_job_name() const {
  return streaming_job_name;
}
void StreamingConfig::SetStreaming_job_name(const std::string &streaming_job_name) {
  StreamingConfig::streaming_job_name = streaming_job_name;
}
uint32_t StreamingConfig::GetStreaming_log_level() const { return streaming_log_level; }
void StreamingConfig::SetStreaming_log_level(uint32_t streaming_log_level) {
  StreamingConfig::streaming_log_level = streaming_log_level;
}

const std::string &StreamingConfig::GetStreaming_op_name() const {
  return streaming_op_name;
}

void StreamingConfig::SetStreaming_op_name(const std::string &streaming_op_name) {
  StreamingConfig::streaming_op_name = streaming_op_name;
}

const std::string &StreamingConfig::GetStreaming_raylet_socket_path() const {
  return streaming_raylet_socket_path;
}

void StreamingConfig::SetStreaming_raylet_socket_path(
    const std::string &streaming_raylet_socket_path) {
  StreamingConfig::streaming_raylet_socket_path = streaming_raylet_socket_path;
}
const std::string &StreamingConfig::GetStreaming_worker_name() const {
  return streaming_worker_name;
}
void StreamingConfig::SetStreaming_worker_name(const std::string &streaming_worker_name) {
  StreamingConfig::streaming_worker_name = streaming_worker_name;
}

const std::string &StreamingConfig::GetStreaming_task_job_id() const {
  return streaming_task_job_id;
}

void StreamingConfig::SetStreaming_task_job_id(const std::string &streaming_task_job_id) {
  StreamingConfig::streaming_task_job_id = streaming_task_job_id;
}

const std::string &StreamingConfig::GetPlasma_store_socket_path() const {
  return plasma_store_socket_path;
}
void StreamingConfig::SetPlasma_store_socket_path(
    const std::string &plasma_store_socket_path) {
  StreamingConfig::plasma_store_socket_path = plasma_store_socket_path;
}

}  // namespace streaming
}  // namespace ray
