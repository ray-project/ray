#ifndef RAY_STREAMING_WRITER_H
#define RAY_STREAMING_WRITER_H

#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "buffer_pool.h"
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
  std::unordered_map<ObjectID, ProducerChannelInfo> channel_info_map_;

  // This property is for debug if transfering blocked.
  // The verbose log in timer will report and show which channel is active,
  // which helps us to find some useful channel informations.
  ObjectID last_write_q_id_;
  static constexpr uint32_t kQueueItemMaxBlocks = 10;

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
  StreamingStatus WriteEmptyMessage(
      ProducerChannelInfo &channel_info);

  StreamingStatus WriteTransientBufferToChannel(ProducerChannelInfo &channel_info);

  bool CollectFromRingBuffer(ProducerChannelInfo &channel_info, uint64_t &buffer_remain);

  StreamingStatus WriteChannelProcess(ProducerChannelInfo &channel_info,
                                      bool *is_empty_message);

  StreamingStatus InitChannel(const ObjectID &q_id, uint64_t channel_message_id,
                              const std::string &plasma_store_path, uint64_t queue_size);

 public:
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
   * @brief new writer client in raylet
   * @param queue_id_vec
   * @param plasma_store_path_vec
   * @param channel_seq_id_vec
   * @param raylet_client
   * @param queue_size
   */

  StreamingStatus Init(const std::vector<ObjectID> &queue_id_vec,
                       const std::string &plasma_store_path,
                       const std::vector<uint64_t> &channel_seq_id_vec,
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

  std::shared_ptr<BufferPool> GetBufferPool(const ObjectID &qid);
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_WRITER_H
