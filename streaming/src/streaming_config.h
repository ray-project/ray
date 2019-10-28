#ifndef RAY_STREAMING_CONFIG_H
#define RAY_STREAMING_CONFIG_H

#include <cstdint>
#include <string>

#include "format/streaming_generated.h"

namespace ray {
namespace streaming {

enum class StreamingStrategy : uint32_t {
  AT_LEAST_ONCE = 1,
  EXACTLY_ONCE = 2,
  EXACTLY_SAME = 3,
  MIN = AT_LEAST_ONCE,
  MAX = EXACTLY_SAME
};

/*!
 * @brief
 * Recreate : reuse local queue object
 * Reconstruct : pull queue items from upstream and reuse it
 * Recreate_and_clear : clear all items if there
 *  are old items in queue object and recreate a new queue
 */
enum class StreamingQueueCreationType : uint32_t {
  RECREATE = 0,
  RECONSTRUCT = 1,
  RECREATE_AND_CLEAR = 2,
  MIN = RECREATE,
  MAX = RECREATE_AND_CLEAR
};

class StreamingMetricsConfig {
 public:
  const std::string GetMetrics_url() const;
  void SetMetrics_url(const std::string &metrics_url_);
  const std::string &GetMetrics_app_name() const;
  void SetMetrics_app_name(const std::string &metrics_app_name_);
  const std::string &GetMetrics_job_name() const;
  void SetMetrics_job_name(const std::string &metrics_job_name_);
  uint32_t GetMetrics_report_interval() const;
  void SetMetrics_report_interval(uint32_t metrics_report_interval);

 private:
  std::string metrics_url_ = "127.0.0.1:24141";
  std::string metrics_app_name_ = "streaming-app";
  std::string metrics_job_name_ = "streaming";
  uint32_t metrics_report_interval_ = 10;
};

class StreamingConfig {
 public:
  static uint64_t TIME_WAIT_UINT;
  static uint32_t DEFAULT_STREAMING_RING_BUFFER_CAPACITY;
  static uint32_t DEFAULT_STREAMING_BUFFER_POOL_SIZE;
  static uint32_t DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;
  static uint32_t DEFAULT_STREAMING_FULL_QUEUE_TIME_INTERVAL;
  static uint32_t DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_TIMEOUT_PER_MB;
  static uint32_t DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES;
  static const uint32_t STRAMING_MESSGAE_BUNDLE_MAX_SIZE;
  static uint32_t DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL;

  /* Reference PR : https://github.com/apache/arrow/pull/2522
   * py-module import c++-python extension with static std::string will
   * crash becase of double free, double-linked or corruption (randomly).
   * So replace std::string by enum (uint32).
   */
 private:
  StreamingStrategy streaming_strategy_ = StreamingStrategy::EXACTLY_ONCE;

  uint32_t streaming_ring_buffer_capacity = DEFAULT_STREAMING_RING_BUFFER_CAPACITY;

  uint32_t streaming_buffer_pool_size_ = DEFAULT_STREAMING_BUFFER_POOL_SIZE;

  uint32_t streaming_buffer_pool_min_buffer_size_ = DEFAULT_STREAMING_BUFFER_POOL_SIZE;

  uint32_t streaming_empty_message_time_interval =
      DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;

  uint32_t streaming_reconstruct_objects_timeout_per_mb =
      DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_TIMEOUT_PER_MB;

  uint32_t streaming_reconstruct_objects_retry_times =
      DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES;

  uint32_t streaming_full_queue_time_interval =
      DEFAULT_STREAMING_FULL_QUEUE_TIME_INTERVAL;

  streaming::fbs::StreamingRole streaming_role = streaming::fbs::StreamingRole::Operator;

  uint64_t streaming_rollback_checkpoint_id = 0;

  std::string streaming_persistence_path =
#ifdef USE_PANGU
      "/zdfs_test/";
#else
      "/tmp/";
#endif

  bool streaming_transfer_event_driven = true;

  uint32_t streaming_event_driven_flowcontrol_interval =
      DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL;

  std::string streaming_log_path = "/tmp/streaminglogs/";

  std::string streaming_job_name = "DEFAULT_JOB_NAME";

  uint32_t streaming_log_level = 0;

  bool streaming_metrics_enable = true;

  std::string streaming_op_name = "DEFAULT_OP_NAME";

  uint32_t streaming_writer_consumed_step = 300;

  uint32_t streaming_reader_consumed_step = 100;

  // Local mode if raylet_socket_path is empty.
  std::string streaming_raylet_socket_path;

  std::string streaming_worker_name = "DEFAULT_WORKER_NAME";

  int64_t streaming_waiting_queue_time_out = 5000;

  // user-define persistence cluster name (pangu cluster or hdfs prefix)
  std::string streaming_persistence_cluster_name = "pangu://localcluster";

  streaming::fbs::StreamingStepUpdater streaming_step_updater =
      streaming::fbs::StreamingStepUpdater::Default;

  std::string streaming_task_job_id = "ffffffff";

  uint32_t streaming_persistence_checkpoint_max_cnt = 1;

  // default unconsumed seq
  streaming::fbs::StreamingFlowControlType streaming_flow_control_type =
      streaming::fbs::StreamingFlowControlType::UnconsumedSeq;

  std::string plasma_store_socket_path = "/tmp/ray/worker/sockets/plasma_store";

 public:
  fbs::StreamingFlowControlType GetStreaming_flow_control_type() const;

  void SetStreaming_transfer_event_driven(bool event_driver) {
    streaming_transfer_event_driven = event_driver;
  }

  bool GetStreaming_transfer_event_driven() { return streaming_transfer_event_driven; }

  void SetStreaming_event_driven_flowcontrol_interval(uint32_t interval) {
    streaming_event_driven_flowcontrol_interval = interval;
  }

  uint32_t GetStreaming_event_driven_flowcontrol_interval() {
    return streaming_event_driven_flowcontrol_interval;
  }

  void SetStreaming_flow_control_type(
      fbs::StreamingFlowControlType streaming_flow_control_type);

  void SetStreaming_flow_control_type(uint32_t streaming_flow_control_type_uint32);

  inline bool IsAtLeastOnce() {
    return StreamingStrategy::AT_LEAST_ONCE == streaming_strategy_;
  }
  inline bool IsExactlyOnce() {
    return StreamingStrategy::EXACTLY_ONCE == streaming_strategy_;
  }
  inline bool IsExactlySame() {
    return StreamingStrategy::EXACTLY_SAME == streaming_strategy_;
  }
  fbs::StreamingStepUpdater GetStreaming_step_updater() const;

  void SetStreaming_step_updater(fbs::StreamingStepUpdater streaming_step_updater);

  void SetStreaming_step_updater(uint32_t streaming_step_updater_uint32);

  const std::string &GetStreaming_task_job_id() const;

  void SetStreaming_task_job_id(const std::string &streaming_task_job_id);

  const std::string &GetStreaming_persistence_cluster_name() const;

  void SetStreaming_persistence_cluster_name(
      const std::string &streaming_persistence_cluster_name);

  int64_t GetStreaming_waiting_queue_time_out() const;

  void SetStreaming_waiting_queue_time_out(int64_t streaming_waiting_queue_time_out);

  uint64_t GetStreaming_rollback_checkpoint_id() const;

  void SetStreaming_rollback_checkpoint_id(uint64_t streaming_rollback_checkpoint_id);

  const std::string &GetStreaming_worker_name() const;

  void SetStreaming_worker_name(const std::string &streaming_worker_name);

  const std::string &GetStreaming_raylet_socket_path() const;

  void SetStreaming_raylet_socket_path(const std::string &streaming_raylet_socket_path);

  uint32_t GetStreaming_writer_consumed_step() const;

  void SetStreaming_writer_consumed_step(uint32_t streaming_writer_consumed_step);

  uint32_t GetStreaming_reader_consumed_step() const;

  void SetStreaming_reader_consumed_step(uint32_t streaming_reader_consumed_step);

  const std::string &GetStreaming_op_name() const;

  void SetStreaming_op_name(const std::string &streaming_op_name);

  bool IsStreaming_metrics_enable() const;

  void SetStreaming_metrics_enable(bool streaming_metrics_enable);

  uint32_t GetStreaming_reconstruct_objects_timeout_per_mb() const;

  void SetStreaming_reconstruct_objects_timeout_per_mb(
      uint32_t streaming_reconstruct_objects_timeout_per_mb);

  uint32_t GetStreaming_reconstruct_objects_retry_times() const;

  void SetStreaming_reconstruct_objects_retry_times(
      uint32_t streaming_reconstruct_objects_retry_times);

  uint32_t GetStreaming_empty_message_time_interval() const;

  void SetStreaming_empty_message_time_interval(
      uint32_t streaming_empty_message_time_interval);

  uint32_t GetStreaming_full_queue_time_interval() const;

  void SetStreaming_full_queue_time_interval(uint32_t streaming_full_queue_time_interval);

  uint32_t GetStreaming_ring_buffer_capacity() const;

  void SetStreaming_ring_buffer_capacity(uint32_t streaming_ring_buffer_capacity);

  uint32_t GetStreaming_buffer_pool_size() const;

  void SetStreaming_buffer_pool_size(uint32_t streaming_buffer_size);

  uint32_t GetStreaming_buffer_pool_min_buffer_size() const;

  void SetStreaming_buffer_pool_min_buffer_size(
      uint32_t streaming_buffer_min_buffer_size);

  StreamingStrategy GetStreaming_strategy_() const;

  void SetStreaming_strategy_(StreamingStrategy streaming_strategy_);

  void ReloadProperty(const streaming::fbs::StreamingConfigKey &key, uint32_t value);

  void ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                      const std::string &value);

  const std::string &GetStreaming_persistence_path() const;

  void SetStreaming_persistence_path(const std::string &streaming_persistence_path);

  streaming::fbs::StreamingRole GetStreaming_role() const;

  void SetStreaming_role(streaming::fbs::StreamingRole streaming_role);

  const std::string &GetStreaming_log_path() const;

  void SetStreaming_log_path(const std::string &streaming_log_path);

  const std::string &GetStreaming_job_name() const;

  void SetStreaming_job_name(const std::string &streaming_job_name);

  uint32_t GetStreaming_log_level() const;

  void SetStreaming_log_level(uint32_t streaming_log_level);

  uint32_t GetStreaming_persistence_checkpoint_max_cnt() const;

  void SetStreaming_persistence_checkpoint_max_cnt(
      uint32_t streaming_persistence_checkpoint_max_cnt);

  const std::string &GetPlasma_store_socket_path() const;

  void SetPlasma_store_socket_path(const std::string &plasma_store_socket_path);
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_CONFIG_H
