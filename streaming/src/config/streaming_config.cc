#include "config/streaming_config.h"

#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_EMPTY_MESSAGE_TIME_INTERVAL = 20;
// Time to force clean if barrier in queue, default 0ms
const uint32_t StreamingConfig::MESSAGE_BUNDLE_MAX_SIZE = 2048;

#define RESET_IF_INT_CONF(KEY, VALUE) \
  if (0 != VALUE) {                   \
    Set##KEY(VALUE);                  \
  }
#define RESET_IF_STR_CONF(KEY, VALUE) \
  if (!VALUE.empty()) {               \
    Set##KEY(VALUE);                  \
  }
#define RESET_IF_NOT_DEFAULT_CONF(KEY, VALUE, DEFAULT) \
  if (DEFAULT != VALUE) {                              \
    Set##KEY(VALUE);                                   \
  }

void StreamingConfig::FromProto(const uint8_t *data, uint32_t size) {
  proto::StreamingConfig config;
  STREAMING_CHECK(config.ParseFromArray(data, size)) << "Parse streaming conf failed";
  RESET_IF_STR_CONF(JobName, config.job_name())
  RESET_IF_STR_CONF(WorkerName, config.worker_name())
  RESET_IF_STR_CONF(OpName, config.op_name())
  RESET_IF_NOT_DEFAULT_CONF(NodeType, config.role(), proto::NodeType::UNKNOWN)
  RESET_IF_INT_CONF(RingBufferCapacity, config.ring_buffer_capacity())
  RESET_IF_INT_CONF(EmptyMessageTimeInterval, config.empty_message_interval())
  RESET_IF_NOT_DEFAULT_CONF(FlowControlType, config.flow_control_type(),
                            proto::FlowControlType::UNKNOWN_FLOW_CONTROL_TYPE)
  RESET_IF_INT_CONF(WriterConsumedStep, config.writer_consumed_step())
  RESET_IF_INT_CONF(ReaderConsumedStep, config.reader_consumed_step())
  RESET_IF_INT_CONF(EventDrivenFlowControlInterval,
                    config.event_driven_flow_control_interval())
  STREAMING_CHECK(writer_consumed_step_ >= reader_consumed_step_)
      << "Writer consuemd step " << writer_consumed_step_
      << "can not be smaller then reader consumed step " << reader_consumed_step_;
}

uint32_t StreamingConfig::GetRingBufferCapacity() const { return ring_buffer_capacity_; }

void StreamingConfig::SetRingBufferCapacity(uint32_t ring_buffer_capacity) {
  StreamingConfig::ring_buffer_capacity_ =
      std::min(ring_buffer_capacity, StreamingConfig::MESSAGE_BUNDLE_MAX_SIZE);
}
}  // namespace streaming
}  // namespace ray
