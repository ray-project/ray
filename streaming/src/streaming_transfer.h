#ifndef RAY_STREAMING_TRANSFER_H
#define RAY_STREAMING_TRANSFER_H
#include "config.h"
#include "queue_interface.h"
#include "streaming.h"
#include "streaming_channel.h"

namespace ray {
namespace streaming {

class ProducerTransfer {
 public:
  explicit ProducerTransfer(std::shared_ptr<Config> &transfer_config);
  virtual ~ProducerTransfer() = default;
  virtual StreamingStatus CreateTransferChannel(ProducerChannelInfo &channel_info) = 0;
  virtual StreamingStatus DestroyTransferChannel(ProducerChannelInfo &channel_info) = 0;
  virtual StreamingStatus ClearTransferCheckpoint(ProducerChannelInfo &channel_info,
                                                  uint64_t checkpoint_id,
                                                  uint64_t checkpoint_offset) = 0;
  virtual StreamingStatus ProduceItemToChannel(ProducerChannelInfo &channel_info,
                                               uint8_t *data, uint32_t data_size) = 0;
  virtual StreamingStatus WaitChannelsReady(std::vector<ObjectID> &channels,
                                            uint32_t timeout,
                                            std::vector<ObjectID> &abnormal_channels) = 0;
  virtual StreamingStatus NotifyChannelConsumed(ProducerChannelInfo &channel_info,
                                                uint64_t channel_offset) = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
};

class ConsumerTransfer {
 public:
  explicit ConsumerTransfer(std::shared_ptr<Config> &transfer_config);
  virtual ~ConsumerTransfer() = default;
  virtual StreamingStatus CreateTransferChannel(ConsumerChannelInfo &channel_info) = 0;
  virtual StreamingStatus DestroyTransferChannel(ConsumerChannelInfo &channel_info) = 0;
  virtual StreamingStatus ClearTransferCheckpoint(ConsumerChannelInfo &channel_info,
                                                  uint64_t checkpoint_id,
                                                  uint64_t checkpoint_offset) = 0;
  virtual StreamingStatus ConsumeItemFromChannel(ConsumerChannelInfo &channel_info,
                                                 uint64_t &offset_id, uint8_t *&data,
                                                 uint32_t &data_size,
                                                 uint32_t timeout) = 0;
  virtual StreamingStatus NotifyChannelConsumed(ConsumerChannelInfo &channel_info,
                                                uint64_t offset_id) = 0;
  virtual StreamingStatus WaitChannelsReady(std::vector<ObjectID> &channels,
                                            uint32_t timeout,
                                            std::vector<ObjectID> &abnormal_channels) = 0;

 protected:
  std::shared_ptr<Config> transfer_config_;
};

class MockProducer : public ProducerTransfer {
 public:
  explicit MockProducer(std::shared_ptr<Config> &transfer_config)
      : ProducerTransfer(transfer_config){};
  StreamingStatus CreateTransferChannel(ProducerChannelInfo &channel_info) override;

  StreamingStatus DestroyTransferChannel(ProducerChannelInfo &channel_info) override;

  StreamingStatus ClearTransferCheckpoint(ProducerChannelInfo &channel_info,
                                          uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }

  StreamingStatus ProduceItemToChannel(ProducerChannelInfo &channel_info, uint8_t *data,
                                       uint32_t data_size) override;

  StreamingStatus WaitChannelsReady(std::vector<ObjectID> &channels, uint32_t timeout,
                                    std::vector<ObjectID> &abnormal_channels) override {
    return StreamingStatus::OK;
  }
  StreamingStatus NotifyChannelConsumed(ProducerChannelInfo &channel_info,
                                        uint64_t channel_offset) override {
    return StreamingStatus::OK;
  }
};

class MockConsumer : public ConsumerTransfer {
 public:
  explicit MockConsumer(std::shared_ptr<Config> &transfer_config)
      : ConsumerTransfer(transfer_config){};
  StreamingStatus CreateTransferChannel(ConsumerChannelInfo &channel_info) override {
    return StreamingStatus::OK;
  }
  StreamingStatus DestroyTransferChannel(ConsumerChannelInfo &channel_info) override {
    return StreamingStatus::OK;
  }
  StreamingStatus ClearTransferCheckpoint(ConsumerChannelInfo &channel_info,
                                          uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }
  StreamingStatus ConsumeItemFromChannel(ConsumerChannelInfo &channel_info,
                                         uint64_t &offset_id, uint8_t *&data,
                                         uint32_t &data_size, uint32_t timeout) override;
  StreamingStatus NotifyChannelConsumed(ConsumerChannelInfo &channel_info,
                                        uint64_t offset_id) override;

  StreamingStatus WaitChannelsReady(std::vector<ObjectID> &channels, uint32_t timeout,
                                    std::vector<ObjectID> &abnormal_channels) override {
    return StreamingStatus::OK;
  }
};

class StreamingQueueProducer : public ProducerTransfer {
 public:
  explicit StreamingQueueProducer(std::shared_ptr<Config> &transfer_config);
  ~StreamingQueueProducer() override;
  StreamingStatus CreateTransferChannel(ProducerChannelInfo &channel_info) override;
  StreamingStatus DestroyTransferChannel(ProducerChannelInfo &channel_info) override;
  StreamingStatus ClearTransferCheckpoint(ProducerChannelInfo &channel_info,
                                          uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override;
  StreamingStatus ProduceItemToChannel(ProducerChannelInfo &channel_info, uint8_t *data,
                                       uint32_t data_size) override;
  StreamingStatus WaitChannelsReady(std::vector<ObjectID> &channels, uint32_t timeout,
                                    std::vector<ObjectID> &abnormal_channels) override;
  StreamingStatus NotifyChannelConsumed(ProducerChannelInfo &channel_info,
                                        uint64_t offset_id) override;

 private:
  StreamingStatus CreateQueue(ProducerChannelInfo &channel_info);
  /*!
   * @brief While resuming from FO, it's better way to reuse its original items since old
   * queue object
   * may have been created in ray actor in some worker. Then getting last message id from
   * existing queue
   * item and set this last message id as new offset id value before this queue is
   * subscribed by upstream.
   * @param q_id : queue obejct id
   * @return last message id in queue
   */
  uint64_t FetchLastMessageIdFromQueue(const ObjectID &queue_id,
                                       uint64_t &last_queue_seq_id);

 private:
  std::shared_ptr<QueueWriterInterface> queue_writer_;
};

class StreamingQueueConsumer : public ConsumerTransfer {
 public:
  explicit StreamingQueueConsumer(std::shared_ptr<Config> &transfer_config);
  ~StreamingQueueConsumer() override;
  StreamingStatus CreateTransferChannel(ConsumerChannelInfo &channel_info) override;
  StreamingStatus DestroyTransferChannel(ConsumerChannelInfo &channel_info) override;
  StreamingStatus ClearTransferCheckpoint(ConsumerChannelInfo &channel_info,
                                          uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override;
  StreamingStatus ConsumeItemFromChannel(ConsumerChannelInfo &channel_info,
                                         uint64_t &offset_id, uint8_t *&data,
                                         uint32_t &data_size, uint32_t timeout) override;
  StreamingStatus NotifyChannelConsumed(ConsumerChannelInfo &channel_info,
                                        uint64_t offset_id) override;
  StreamingStatus WaitChannelsReady(std::vector<ObjectID> &channels, uint32_t timeout,
                                    std::vector<ObjectID> &abnormal_channels) override;

 private:
  std::shared_ptr<QueueReaderInterface> queue_reader_;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_TRANSFER_H
