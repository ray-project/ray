#ifndef RAY_STREAMING_FLOW_CONTROL_H
#define RAY_STREAMING_FLOW_CONTROL_H
#include "channel.h"

namespace ray {
namespace streaming {
class ProducerTransfer;
/// We devise a flow control system in queue channel, and that's called flow
/// control by unconsumed seq. Upstream worker will detect consumer statistics via
/// api so it can keep fixed length messages in this process, which makes a
/// continuous datastream in channel or on the transporting way, then downstream
/// can read them from channel immediately.
/// To debug or compare with theses flow control methods, we also support
/// no-flow-control that will do nothing in transporting.
class FlowControl {
 public:
  virtual ~FlowControl() = default;
  virtual bool ShouldFlowControl(ProducerChannelInfo &channel_info) = 0;
};

class NoFlowControl : public FlowControl {
 public:
  bool ShouldFlowControl(ProducerChannelInfo &channel_info) { return false; }
  ~NoFlowControl() = default;
};

class UnconsumedSeqFlowControl : public FlowControl {
 public:
  UnconsumedSeqFlowControl(
      std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map,
      uint32_t step);
  ~UnconsumedSeqFlowControl() = default;
  bool ShouldFlowControl(ProducerChannelInfo &channel_info);

 private:
  /// NOTE(wanxing.wwx) Reference to channel_map_ variable in DataWriter.
  /// Flow-control is checked in FlowControlThread, so channel_map_ is accessed
  /// in multithread situation. Especially, while rescaling, channel_map_ maybe
  /// changed. But for now, FlowControlThread is stopped before rescaling.
  std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> &channel_map_;
  uint32_t consumed_step_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_FLOW_CONTROL_H
