#pragma once

#include <cstdint>
#include <string>

#include "protobuf/streaming.pb.h"
#include "ray/common/id.h"

namespace ray {
namespace streaming {

using ReliabilityLevel = proto::ReliabilityLevel;
using StreamingRole = proto::NodeType;

#define DECL_GET_SET_PROPERTY(TYPE, NAME, VALUE) \
  TYPE Get##NAME() const { return VALUE; }       \
  void Set##NAME(TYPE value) { VALUE = value; }

class StreamingConfig {
 public:
  static uint64_t TIME_WAIT_UINT;
  static uint32_t DEFAULT_RING_BUFFER_CAPACITY;
  static uint32_t DEFAULT_EMPTY_MESSAGE_TIME_INTERVAL;
  static const uint32_t MESSAGE_BUNDLE_MAX_SIZE;
  static const uint32_t RESEND_NOTIFY_MAX_INTERVAL;

 private:
  uint32_t ring_buffer_capacity_ = DEFAULT_RING_BUFFER_CAPACITY;

  uint32_t empty_message_time_interval_ = DEFAULT_EMPTY_MESSAGE_TIME_INTERVAL;

  streaming::proto::NodeType node_type_ = streaming::proto::NodeType::TRANSFORM;

  std::string job_name_ = "DEFAULT_JOB_NAME";

  std::string op_name_ = "DEFAULT_OP_NAME";

  std::string worker_name_ = "DEFAULT_WORKER_NAME";

  // Default flow control type is unconsumed sequence flow control. More detail
  // introducation and implemention in ray/streaming/src/flow_control.h.
  streaming::proto::FlowControlType flow_control_type_ =
      streaming::proto::FlowControlType::UnconsumedSeqFlowControl;

  // Default writer and reader consumed step.
  uint32_t writer_consumed_step_ = 1000;
  uint32_t reader_consumed_step_ = 100;

  uint32_t event_driven_flow_control_interval_ = 1;

  ReliabilityLevel streaming_strategy_ = ReliabilityLevel::EXACTLY_ONCE;
  StreamingRole streaming_role = StreamingRole::TRANSFORM;

 public:
  void FromProto(const uint8_t *, uint32_t size);

  inline bool IsAtLeastOnce() const {
    return ReliabilityLevel::AT_LEAST_ONCE == streaming_strategy_;
  }
  inline bool IsExactlyOnce() const {
    return ReliabilityLevel::EXACTLY_ONCE == streaming_strategy_;
  }

  DECL_GET_SET_PROPERTY(const std::string &, WorkerName, worker_name_)
  DECL_GET_SET_PROPERTY(const std::string &, OpName, op_name_)
  DECL_GET_SET_PROPERTY(uint32_t, EmptyMessageTimeInterval, empty_message_time_interval_)
  DECL_GET_SET_PROPERTY(streaming::proto::NodeType, NodeType, node_type_)
  DECL_GET_SET_PROPERTY(const std::string &, JobName, job_name_)
  DECL_GET_SET_PROPERTY(uint32_t, WriterConsumedStep, writer_consumed_step_)
  DECL_GET_SET_PROPERTY(uint32_t, ReaderConsumedStep, reader_consumed_step_)
  DECL_GET_SET_PROPERTY(streaming::proto::FlowControlType, FlowControlType,
                        flow_control_type_)
  DECL_GET_SET_PROPERTY(uint32_t, EventDrivenFlowControlInterval,
                        event_driven_flow_control_interval_)
  DECL_GET_SET_PROPERTY(StreamingRole, StreamingRole, streaming_role)
  DECL_GET_SET_PROPERTY(ReliabilityLevel, ReliabilityLevel, streaming_strategy_)

  uint32_t GetRingBufferCapacity() const;
  /// Note(lingxuan.zlx), RingBufferCapacity's valid range is from 1 to
  /// MESSAGE_BUNDLE_MAX_SIZE, so we don't use DECL_GET_SET_PROPERTY for it.
  void SetRingBufferCapacity(uint32_t ring_buffer_capacity);
};
}  // namespace streaming
}  // namespace ray
