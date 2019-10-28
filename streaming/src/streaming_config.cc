#include <unistd.h>

#include "streaming_config.h"
#include "streaming_logging.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_STREAMING_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_STREAMING_BUFFER_POOL_SIZE = 1024 * 1024;  // 1M
uint32_t StreamingConfig::DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = 20;
uint32_t StreamingConfig::DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_TIMEOUT_PER_MB = 100;
uint32_t StreamingConfig::DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES = 2;
// Time to force clean if barrier in queue, default 0ms
uint32_t StreamingConfig::DEFAULT_STREAMING_FULL_QUEUE_TIME_INTERVAL = 0;
const uint32_t StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE = 2048;
uint32_t StreamingConfig::DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL = 2;

StreamingStrategy StreamingConfig::GetStreaming_strategy_() const {
  return streaming_strategy_;
}

void StreamingConfig::SetStreaming_strategy_(StreamingStrategy streaming_strategy_) {
  StreamingConfig::streaming_strategy_ = streaming_strategy_;
}

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

uint32_t StreamingConfig::GetStreaming_reconstruct_objects_timeout_per_mb() const {
  return streaming_reconstruct_objects_timeout_per_mb;
}

void StreamingConfig::SetStreaming_reconstruct_objects_timeout_per_mb(
    uint32_t streaming_reconstruct_objects_timeout_per_mb) {
  StreamingConfig::streaming_reconstruct_objects_timeout_per_mb =
      streaming_reconstruct_objects_timeout_per_mb;
}

uint32_t StreamingConfig::GetStreaming_reconstruct_objects_retry_times() const {
  return streaming_reconstruct_objects_retry_times;
}

void StreamingConfig::SetStreaming_reconstruct_objects_retry_times(
    uint32_t streaming_reconstruct_objects_retry_times) {
  StreamingConfig::streaming_reconstruct_objects_retry_times =
      streaming_reconstruct_objects_retry_times;
}

void StreamingConfig::ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                                     uint32_t value) {
  switch (key) {
  case streaming::fbs::StreamingConfigKey::StreamingEmptyMessageTimeInterval:
    SetStreaming_empty_message_time_interval(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingFullQueueTimeInterval:
    SetStreaming_full_queue_time_interval(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingReconstructObjectsRetryTimes:
    SetStreaming_reconstruct_objects_retry_times(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingRingBufferCapacity:
    SetStreaming_ring_buffer_capacity(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingReconstructObjectsTimeOutPerMb:
    SetStreaming_reconstruct_objects_timeout_per_mb(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingStrategy:
    SetStreaming_strategy_(static_cast<StreamingStrategy>(value));
    break;
  case streaming::fbs::StreamingConfigKey::StreamingLogLevel:
    SetStreaming_log_level(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingWriterConsumedStep:
    SetStreaming_writer_consumed_step(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingReaderConsumedStep:
    SetStreaming_reader_consumed_step(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingReaderConsumedStepUpdater:
    SetStreaming_step_updater(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingFlowControl:
    SetStreaming_flow_control_type(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingBufferPoolSize:
    SetStreaming_buffer_pool_size(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingBufferPoolMinBufferSize:
    SetStreaming_buffer_pool_min_buffer_size(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingDefault:
    STREAMING_LOG(INFO) << "skip default key";
    break;
  default:
    STREAMING_LOG(WARNING) << "no such type in uint32";
  }
}
uint32_t StreamingConfig::GetStreaming_writer_consumed_step() const {
  return streaming_writer_consumed_step;
}

void StreamingConfig::SetStreaming_writer_consumed_step(
    uint32_t streaming_writer_consumed_step) {
  StreamingConfig::streaming_writer_consumed_step = streaming_writer_consumed_step;
}
uint32_t StreamingConfig::GetStreaming_full_queue_time_interval() const {
  return streaming_full_queue_time_interval;
}

void StreamingConfig::SetStreaming_full_queue_time_interval(
    uint32_t streaming_full_queue_time_interval) {
  StreamingConfig::streaming_full_queue_time_interval =
      streaming_full_queue_time_interval;
}
const std::string &StreamingConfig::GetStreaming_persistence_path() const {
  return streaming_persistence_path;
}

void StreamingConfig::SetStreaming_persistence_path(
    const std::string &streaming_persistence_path) {
  StreamingConfig::streaming_persistence_path = streaming_persistence_path;
}

void StreamingConfig::ReloadProperty(const streaming::fbs::StreamingConfigKey &key,
                                     const std::string &value) {
  switch (key) {
  case streaming::fbs::StreamingConfigKey::StreamingPersistencePath:
    SetStreaming_persistence_path(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingLogPath:
    SetStreaming_log_path(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingJobName:
    SetStreaming_job_name(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingOpName:
    SetStreaming_op_name(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingRayletSocketPath:
    SetStreaming_raylet_socket_path(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingWorkerName:
    SetStreaming_worker_name(value);
    break;
  case streaming::fbs::StreamingConfigKey::StreamingPersistenceClusterName:
    SetStreaming_persistence_cluster_name(value);
    break;
  case streaming::fbs::StreamingConfigKey::PlasmaStoreSocketPath:
    SetPlasma_store_socket_path(value);
    STREAMING_LOG(INFO) << "set plasma store socket path " << value;
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

uint64_t StreamingConfig::GetStreaming_rollback_checkpoint_id() const {
  return streaming_rollback_checkpoint_id;
}

void StreamingConfig::SetStreaming_rollback_checkpoint_id(
    uint64_t streaming_rollback_checkpoint_id) {
  StreamingConfig::streaming_rollback_checkpoint_id = streaming_rollback_checkpoint_id;
}

bool StreamingConfig::IsStreaming_metrics_enable() const {
  if (std::getenv("STREAMING_ENABLE_METRICS")) {
    return streaming_metrics_enable;
  } else {
    return false;
  }
}

void StreamingConfig::SetStreaming_metrics_enable(bool streaming_metrics_enable) {
  StreamingConfig::streaming_metrics_enable = streaming_metrics_enable;
}

const std::string &StreamingConfig::GetStreaming_op_name() const {
  return streaming_op_name;
}

void StreamingConfig::SetStreaming_op_name(const std::string &streaming_op_name) {
  StreamingConfig::streaming_op_name = streaming_op_name;
}

uint32_t StreamingConfig::GetStreaming_reader_consumed_step() const {
  return streaming_reader_consumed_step;
}

void StreamingConfig::SetStreaming_reader_consumed_step(
    uint32_t streaming_reader_consumed_step) {
  StreamingConfig::streaming_reader_consumed_step = streaming_reader_consumed_step;
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
int64_t StreamingConfig::GetStreaming_waiting_queue_time_out() const {
  return streaming_waiting_queue_time_out;
}
void StreamingConfig::SetStreaming_waiting_queue_time_out(
    int64_t streaming_waiting_queue_time_out) {
  StreamingConfig::streaming_waiting_queue_time_out = streaming_waiting_queue_time_out;
}
const std::string &StreamingConfig::GetStreaming_persistence_cluster_name() const {
  return streaming_persistence_cluster_name;
}
void StreamingConfig::SetStreaming_persistence_cluster_name(
    const std::string &streaming_persistence_cluster_name) {
  StreamingConfig::streaming_persistence_cluster_name =
      streaming_persistence_cluster_name;
}

fbs::StreamingStepUpdater StreamingConfig::GetStreaming_step_updater() const {
  return streaming_step_updater;
}

void StreamingConfig::SetStreaming_step_updater(
    fbs::StreamingStepUpdater streaming_step_updater) {
  StreamingConfig::streaming_step_updater = streaming_step_updater;
}

void StreamingConfig::SetStreaming_step_updater(uint32_t streaming_step_updater_uint32) {
  StreamingConfig::streaming_step_updater =
      static_cast<streaming::fbs::StreamingStepUpdater>(streaming_step_updater_uint32);
}

fbs::StreamingFlowControlType StreamingConfig::GetStreaming_flow_control_type() const {
  return streaming_flow_control_type;
}

void StreamingConfig::SetStreaming_flow_control_type(
    fbs::StreamingFlowControlType streaming_flow_control_type) {
  StreamingConfig::streaming_flow_control_type = streaming_flow_control_type;
}

void StreamingConfig::SetStreaming_flow_control_type(
    uint32_t streaming_flow_control_type_uint32) {
  StreamingConfig::streaming_flow_control_type =
      static_cast<streaming::fbs::StreamingFlowControlType>(
          streaming_flow_control_type_uint32);
}

const std::string &StreamingConfig::GetStreaming_task_job_id() const {
  return streaming_task_job_id;
}

void StreamingConfig::SetStreaming_task_job_id(const std::string &streaming_task_job_id) {
  StreamingConfig::streaming_task_job_id = streaming_task_job_id;
}

uint32_t StreamingConfig::GetStreaming_persistence_checkpoint_max_cnt() const {
  return streaming_persistence_checkpoint_max_cnt;
}

void StreamingConfig::SetStreaming_persistence_checkpoint_max_cnt(
    uint32_t streaming_persistence_checkpoint_max_cnt) {
  StreamingConfig::streaming_persistence_checkpoint_max_cnt =
      streaming_persistence_checkpoint_max_cnt;
}

const std::string &StreamingConfig::GetPlasma_store_socket_path() const {
  return plasma_store_socket_path;
}
void StreamingConfig::SetPlasma_store_socket_path(
    const std::string &plasma_store_socket_path) {
  StreamingConfig::plasma_store_socket_path = plasma_store_socket_path;
}

const std::string StreamingMetricsConfig::GetMetrics_url() const { return metrics_url_; }

void StreamingMetricsConfig::SetMetrics_url(const std::string &metrics_url_) {
  StreamingMetricsConfig::metrics_url_ = metrics_url_;
}

const std::string &StreamingMetricsConfig::GetMetrics_app_name() const {
  return metrics_app_name_;
};
void StreamingMetricsConfig::SetMetrics_app_name(const std::string &metrics_app_name_) {
  StreamingMetricsConfig::metrics_app_name_ = metrics_app_name_;
}
const std::string &StreamingMetricsConfig::GetMetrics_job_name() const {
  return metrics_job_name_;
}
void StreamingMetricsConfig::SetMetrics_job_name(const std::string &metrics_job_name_) {
  StreamingMetricsConfig::metrics_job_name_ = metrics_job_name_;
}
uint32_t StreamingMetricsConfig::GetMetrics_report_interval() const {
  return metrics_report_interval_;
}
void StreamingMetricsConfig::SetMetrics_report_interval(
    uint32_t metrics_report_interval) {
  StreamingMetricsConfig::metrics_report_interval_ = metrics_report_interval;
}

}  // namespace streaming
}  // namespace ray
