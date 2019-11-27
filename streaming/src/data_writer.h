#ifndef RAY_DATA_WRITER_H
#define RAY_DATA_WRITER_H

#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "channel.h"
#include "config/streaming_config.h"
#include "message/message_bundle.h"
#include "runtime_context.h"

namespace ray {
namespace streaming {

class DataWriter {
 private:
  std::shared_ptr<std::thread> loop_thread_;
  // One channel have unique identity.
  std::vector<ObjectID> output_queue_ids_;

 protected:
  // ProducerTransfer is middle broker for data transporting.
  std::unordered_map<ObjectID, ProducerChannelInfo> channel_info_map_;
  std::unordered_map<ObjectID, std::shared_ptr<ProducerChannel>> channel_map_;
  std::shared_ptr<Config> transfer_config_;
  std::shared_ptr<RuntimeContext> runtime_context_;

 private:
  bool IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info);

  ///  \\param channel_info
  ///  \\param buffer_remain
  ///
  ///  Two conditions in this function:
  ///  1. Send the transient buffer to channel if there is already some data in.
  ///  2. Collecting data from ring buffer, then put them into a bundle and
  ///     serializing it to bytes object in transient buffer. Finally do 1.
  StreamingStatus WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                       uint64_t &buffer_remain);

  ///  Start the loop forward thread for collecting messages from all channels.
  ///  Invoking stack:
  ///  WriterLoopForward
  ///    -- WriteChannelProcess
  ///       -- WriteBufferToChannel
  ///         -- CollectFromRingBuffer
  ///         -- WriteTransientBufferToChannel
  ///    -- WriteEmptyMessage(if WriteChannelProcess return empty state)
  void WriterLoopForward();

  ///  Push empty message when no valid message or bundle was produced each time
  ///  interval.
  ///  \param q_id, queue id
  ///  \param meta_ptr, it's resend empty bundle if meta pointer is non-null.
  StreamingStatus WriteEmptyMessage(ProducerChannelInfo &channel_info);

  /// Flush all data from transient buffer to channel for transporting.
  /// \param channel_info
  StreamingStatus WriteTransientBufferToChannel(ProducerChannelInfo &channel_info);

  bool CollectFromRingBuffer(ProducerChannelInfo &channel_info, uint64_t &buffer_remain);

  StreamingStatus WriteChannelProcess(ProducerChannelInfo &channel_info,
                                      bool *is_empty_message);

  StreamingStatus InitChannel(const ObjectID &q_id, const ActorID &actor_id, uint64_t channel_message_id,
                              uint64_t queue_size);

 public:
  explicit DataWriter(std::shared_ptr<RuntimeContext> &runtime_context);
  virtual ~DataWriter();

  /// Streaming writer client initialization.
  /// \param queue_id_vec queue id vector
  /// \param channel_message_id_vec channel seq id is related with message checkpoint
  /// \param queue_size queue size (memory size not length)

  StreamingStatus Init(const std::vector<ObjectID> &channel_ids,
                       const std::vector<ActorID> &actor_ids,
                       const std::vector<uint64_t> &channel_message_id_vec,
                       const std::vector<uint64_t> &queue_size_vec);

  ///  To increase throughout, we employed an output buffer for message transformation,
  ///  which means we merge a lot of message to a message bundle and no message will be
  ///  pushed into queue directly util daemon thread does this action.
  ///  Additionally, writing will block when buffer ring is full intentionly.
  ///  \param q_id
  ///  \param data
  ///  \param data_size
  ///  \param message_type
  ///  \return message seq iq

  uint64_t WriteMessageToBufferRing(
      const ObjectID &q_id, uint8_t *data, uint32_t data_size,
      StreamingMessageType message_type = StreamingMessageType::Message);

  void Run();

  void Stop();
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_DATA_WRITER_H
