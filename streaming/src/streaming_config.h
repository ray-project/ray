#ifndef RAY_STREAMING_CONFIG_H
#define RAY_STREAMING_CONFIG_H

#include <cstdint>
#include <string>

#include "format/streaming_generated.h"

namespace ray {
namespace streaming {

class StreamingConfig {
 public:
  static uint64_t TIME_WAIT_UINT;
  static uint32_t DEFAULT_STREAMING_RING_BUFFER_CAPACITY;
  static uint32_t DEFAULT_STREAMING_BUFFER_POOL_SIZE;
  static uint32_t DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;
  static const uint32_t STRAMING_MESSGAE_BUNDLE_MAX_SIZE;
  static uint32_t DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL;

  /* Reference PR : https://github.com/apache/arrow/pull/2522
   * py-module import c++-python extension with static std::string will
   * crash becase of double free, double-linked or corruption (randomly).
   * So replace std::string by enum (uint32).
   */
 private:
  uint32_t streaming_ring_buffer_capacity = DEFAULT_STREAMING_RING_BUFFER_CAPACITY;

  uint32_t streaming_buffer_pool_size_ = DEFAULT_STREAMING_BUFFER_POOL_SIZE;

  uint32_t streaming_buffer_pool_min_buffer_size_ = DEFAULT_STREAMING_BUFFER_POOL_SIZE;

  uint32_t streaming_empty_message_time_interval =
      DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;

  streaming::fbs::StreamingRole streaming_role = streaming::fbs::StreamingRole::Operator;

  std::string streaming_log_path = "/tmp/streaminglogs/";

  std::string streaming_job_name = "DEFAULT_JOB_NAME";

  uint32_t streaming_log_level = 0;

  std::string streaming_op_name = "DEFAULT_OP_NAME";

  // Local mode if raylet_socket_path is empty.
  std::string streaming_raylet_socket_path;

  std::string streaming_worker_name = "DEFAULT_WORKER_NAME";

  std::string streaming_task_job_id = "ffffffff";

  std::string queue_type = "streaming_queue";

 public:
  const std::string &GetStreaming_task_job_id() const;

  void SetStreaming_task_job_id(const std::string &streaming_task_job_id);

  const std::string &GetStreaming_worker_name() const;

  void SetStreaming_worker_name(const std::string &streaming_worker_name);

  const std::string &GetStreaming_raylet_socket_path() const;

  void SetStreaming_raylet_socket_path(const std::string &streaming_raylet_socket_path);

  const std::string &GetStreaming_op_name() const;

  void SetStreaming_op_name(const std::string &streaming_op_name);

  uint32_t GetStreaming_empty_message_time_interval() const;

  void SetStreaming_empty_message_time_interval(
      uint32_t streaming_empty_message_time_interval);

  uint32_t GetStreaming_ring_buffer_capacity() const;

  void SetStreaming_ring_buffer_capacity(uint32_t streaming_ring_buffer_capacity);

  uint32_t GetStreaming_buffer_pool_size() const;

  void SetStreaming_buffer_pool_size(uint32_t streaming_buffer_size);

  uint32_t GetStreaming_buffer_pool_min_buffer_size() const;

  void SetStreaming_buffer_pool_min_buffer_size(
      uint32_t streaming_buffer_min_buffer_size);

  void ReloadProperty(const streaming::fbs::StreamingConfigKey &key, uint32_t value);

  void ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                      const std::string &value);

  streaming::fbs::StreamingRole GetStreaming_role() const;

  void SetStreaming_role(streaming::fbs::StreamingRole streaming_role);

  const std::string &GetStreaming_log_path() const;

  void SetStreaming_log_path(const std::string &streaming_log_path);

  const std::string &GetStreaming_job_name() const;

  void SetStreaming_job_name(const std::string &streaming_job_name);

  uint32_t GetStreaming_log_level() const;

  void SetStreaming_log_level(uint32_t streaming_log_level);
  
  const std::string &GetQueue_type() const;

  void SetQueue_type(const std::string &queue_type);
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_CONFIG_H
