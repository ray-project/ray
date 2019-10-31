#ifndef RAY_STREAMING_WRITER_H
#define RAY_STREAMING_WRITER_H

#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "streaming.h"
#include "streaming_channel.h"
#include "streaming_config.h"
#include "streaming_message_bundle.h"
#include "streaming_transfer.h"

namespace ray {
namespace streaming {

class StreamingWriter : public StreamingCommon {
 private:
  std::shared_ptr<std::thread> loop_thread_;
  // ProducerTransfer is middle broker for data transporting.
  std::shared_ptr<ProducerTransfer> transfer_;

  // One channel have unique identity.
  std::vector<ObjectID> output_queue_ids_;

  // This property is for debug if transfering blocked.
  // The verbose log in timer will report and show which channel is active,
  // which helps us to find some useful channel informations.
  ObjectID last_write_q_id_;
  static constexpr uint32_t kQueueItemMaxBlocks = 10;

 protected:
  std::unordered_map<ObjectID, ProducerChannelInfo> channel_info_map_;
  std::shared_ptr<Config> transfer_config_;

 private:
  bool WriteAllToChannel(ProducerChannelInfo *channel_info);
  bool IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info);

  StreamingStatus WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                       uint64_t &buffer_remain);

  void WriterLoopForward();

  /*!
   * @brief push empty message when no valid message or bundle was produced each time
   * interval
   * @param q_id, queue id
   * @param meta_ptr, it's resend empty bundle if meta pointer is non-null.
   */
  StreamingStatus WriteEmptyMessage(ProducerChannelInfo &channel_info);

  StreamingStatus WriteTransientBufferToChannel(ProducerChannelInfo &channel_info);

  bool CollectFromRingBuffer(ProducerChannelInfo &channel_info, uint64_t &buffer_remain);

  StreamingStatus WriteChannelProcess(ProducerChannelInfo &channel_info,
                                      bool *is_empty_message);

  StreamingStatus InitChannel(const ObjectID &q_id, uint64_t channel_message_id,
                              const std::string &plasma_store_path, uint64_t queue_size);

 public:
  StreamingWriter();
  virtual ~StreamingWriter();

  /*!
   * @brief streaming writer client initialization without raylet/local sheduler
   * @param queue_id_vec queue id vector
   * @param plasma_store_path_vec plasma store path vector, it's bitwise mapping with
   * queueid
   * @param channel_message_id_vec channel seq id is related with message checkpoint
   * @param queue_size queue size (memory size not length)
   * @param abnormal_queues reamaining queue id vector
   */
  StreamingStatus Init(const std::vector<ObjectID> &queue_id_vec,
                       const std::string &plasma_store_path,
                       const std::vector<uint64_t> &channel_message_id_vec,
                       const std::vector<uint64_t> &queue_size_vec);

  /*!
   * @brief To increase throughout, we employed an output buffer for message
   * transformation,
   * which means we merge a lot of message to a message bundle and no message will be
   * pushed
   * into queue directly util daemon thread does this action. Additionally, writing will
   * block
   * when buffer ring is full.
   * @param q_id
   * @param data
   * @param data_size
   * @param message_type
   * @return message seq iq
   */
  uint64_t WriteMessageToBufferRing(
      const ObjectID &q_id, uint8_t *data, uint32_t data_size,
      StreamingMessageType message_type = StreamingMessageType::Message);

  void Run();

  void Stop();
};

class StreamingWriterDirectCall : public StreamingWriter {
 public:
  StreamingWriterDirectCall(CoreWorker *core_worker,
                            const std::vector<ObjectID> &queue_ids,
                            const std::vector<ActorID> &actor_ids,
                            RayFunction async_func, RayFunction sync_func)
      : core_worker_(core_worker) {
    transfer_config_->Set(ConfigEnum::CORE_WORKER,
                          reinterpret_cast<uint64_t>(core_worker_));
    transfer_config_->Set(ConfigEnum::ASYNC_FUNCTION, async_func);
    transfer_config_->Set(ConfigEnum::SYNC_FUNCTION, sync_func);
    for (size_t i = 0; i < queue_ids.size(); ++i) {
      auto &q_id = queue_ids[i];
      channel_info_map_[q_id].actor_id = actor_ids[i];
    }
  }

  virtual ~StreamingWriterDirectCall() { core_worker_ = nullptr; }

 private:
  CoreWorker *core_worker_;
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_WRITER_H
