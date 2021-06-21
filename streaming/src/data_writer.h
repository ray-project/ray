#pragma once

#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "channel/channel.h"
#include "config/streaming_config.h"
#include "event_service.h"
#include "flow_control.h"
#include "message/message_bundle.h"
#include "reliability/barrier_helper.h"
#include "reliability_helper.h"
#include "runtime_context.h"

namespace ray {
namespace streaming {
class ReliabilityHelper;

/// DataWriter is designed for data transporting between upstream and downstream.
/// After the user sends the data, it does not immediately send the data to
/// downstream, but caches it in the corresponding memory ring buffer. There is
/// a spearate transfer thread (setup in WriterLoopForward function) to collect
/// the messages from all the ringbuffers, and write them to the corresponding
/// transmission channels, which is backed by StreamingQueue. Actually, the
/// advantage is that the user thread will not be affected by the transmission
/// speed during the data transfer. And also the transfer thread can automatically
/// batch the catched data from memory buffer into a data bundle to reduce
/// transmission overhead. In addtion, when there is no data in the ringbuffer,
/// it will also send an empty bundle, so downstream can know that and process
/// accordingly. It will sleep for a short interval to save cpu if all ring
/// buffers have no data in that moment.
class DataWriter {
 public:
  // For mock writer accessing inner fields.
  friend class MockWriter;

  explicit DataWriter(std::shared_ptr<RuntimeContext> &runtime_context);
  virtual ~DataWriter();

  /// Streaming writer client initialization.
  /// \param queue_id_vec queue id vector
  /// \param init_params some parameters for initializing channels
  /// \param channel_message_id_vec channel seq id is related with message checkpoint
  /// \param queue_size queue size (memory size not length)
  StreamingStatus Init(const std::vector<ObjectID> &channel_ids,
                       const std::vector<ChannelCreationParameter> &init_params,
                       const std::vector<uint64_t> &channel_message_id_vec,
                       const std::vector<uint64_t> &queue_size_vec);

  ///  To increase throughout, we employed an output buffer for message transformation,
  ///  which means we merge a lot of message to a message bundle and no message will be
  ///  pushed into queue directly util daemon thread does this action.
  ///  Additionally, writing will block when buffer ring is full intentionly.
  ///  \param q_id, destination channel id
  ///  \param data, pointer of raw data
  ///  \param data_size, raw data size
  ///  \param message_type
  ///  \return message seq iq
  uint64_t WriteMessageToBufferRing(
      const ObjectID &q_id, uint8_t *data, uint32_t data_size,
      StreamingMessageType message_type = StreamingMessageType::Message);

  /// Send barrier to all channel. note there are user defined data in barrier bundle
  /// \param barrier_id
  /// \param data
  /// \param data_size
  ///
  void BroadcastBarrier(uint64_t barrier_id, const uint8_t *data, uint32_t data_size);

  /// To relieve stress from large source/input data, we define a new function
  /// clear_check_point
  /// in producer/writer class. Worker can invoke this function if and only if
  /// notify_consumed each item
  /// flag is passed in reader/consumer, which means writer's producing became more
  /// rhythmical and reader
  /// can't walk on old way anymore.
  /// \param barrier_id: user-defined numerical checkpoint id
  void ClearCheckpoint(uint64_t barrier_id);

  /// Replay all queue from checkpoint, it's useful under FO
  /// \param result offset vector
  void GetChannelOffset(std::vector<uint64_t> &result);

  void Run();

  void Stop();

  /// Get offset information about channels for checkpoint.
  ///  \param offset_map (return value)
  void GetOffsetInfo(std::unordered_map<ObjectID, ProducerChannelInfo> *&offset_map);

 private:
  bool IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info);

  /// This function handles two scenarios. When there is data in the transient
  /// buffer, the existing data is written into the channel first, otherwise a
  /// certain amount of message is first collected from the buffer and serialized
  /// into the transient buffer, and finally written to the channel.
  /// \\param channel_info
  /// \\param buffer_remain
  StreamingStatus WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                       uint64_t &buffer_remain);

  /// Push empty message when no valid message or bundle was produced each time
  /// interval.
  /// \param channel_info
  StreamingStatus WriteEmptyMessage(ProducerChannelInfo &channel_info);

  /// Flush all data from transient buffer to channel for transporting.
  /// \param channel_info
  StreamingStatus WriteTransientBufferToChannel(ProducerChannelInfo &channel_info);

  bool CollectFromRingBuffer(ProducerChannelInfo &channel_info, uint64_t &buffer_remain);

  StreamingStatus WriteChannelProcess(ProducerChannelInfo &channel_info,
                                      bool *is_empty_message);

  StreamingStatus InitChannel(const ObjectID &q_id, const ChannelCreationParameter &param,
                              uint64_t channel_message_id, uint64_t queue_size);

  /// Write all messages to channel util ringbuffer is empty.
  /// \param channel_info
  bool WriteAllToChannel(ProducerChannelInfo *channel_info);

  /// Trigger an empty message for channel with no valid data.
  /// \param channel_info
  bool SendEmptyToChannel(ProducerChannelInfo *channel_info);

  void EmptyMessageTimerCallback();

  /// Notify channel consumed  refreshing downstream queue stats.
  void RefreshChannelAndNotifyConsumed(ProducerChannelInfo &channel_info);

  /// Notify channel consumed by given offset.
  void NotifyConsumedItem(ProducerChannelInfo &channel_info, uint32_t offset);

  void FlowControlTimer();

  void ClearCheckpointId(ProducerChannelInfo &channel_info, uint64_t seq_id);

 private:
  std::shared_ptr<EventService> event_service_;

  std::shared_ptr<std::thread> empty_message_thread_;

  std::shared_ptr<std::thread> flow_control_thread_;
  // One channel have unique identity.
  std::vector<ObjectID> output_queue_ids_;
  // Flow controller makes a decision when it's should be blocked and avoid
  // unnecessary overflow.
  std::shared_ptr<FlowControl> flow_controller_;

  StreamingBarrierHelper barrier_helper_;
  std::shared_ptr<ReliabilityHelper> reliability_helper_;

  // Make thread-safe between loop thread and user thread.
  // High-level runtime send notification about clear checkpoint if global
  // checkpoint is finished and low-level will auto flush & evict item memory
  // when no more space is available.
  std::atomic_flag notify_flag_ = ATOMIC_FLAG_INIT;

 protected:
  std::unordered_map<ObjectID, ProducerChannelInfo> channel_info_map_;
  /// ProducerChannel is middle broker for data transporting and all downstream
  /// producer channels will be channel_map_.
  std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> channel_map_;
  std::shared_ptr<Config> transfer_config_;
  std::shared_ptr<RuntimeContext> runtime_context_;
};
}  // namespace streaming
}  // namespace ray
