#ifndef RAY_STREAMING_MOCK_TRANSFER_H
#define RAY_STREAMING_MOCK_TRANSFER_H

#include "channel.h"

namespace ray {
namespace streaming {

class MockProducer : public ProducerChannel {
 public:
  explicit MockProducer(std::shared_ptr<Config> &transfer_config,
                        ProducerChannelInfo &channel_info)
      : ProducerChannel(transfer_config, channel_info){};
  StreamingStatus CreateTransferChannel() override;

  StreamingStatus DestroyTransferChannel() override;

  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }

  StreamingStatus ProduceItemToChannel(uint8_t *data, uint32_t data_size) override;

  StreamingStatus NotifyChannelConsumed(uint64_t channel_offset) override {
    return StreamingStatus::OK;
  }
};

class MockConsumer : public ConsumerChannel {
 public:
  explicit MockConsumer(std::shared_ptr<Config> &transfer_config,
                        ConsumerChannelInfo &c_channel_info)
      : ConsumerChannel(transfer_config, c_channel_info){};
  StreamingStatus CreateTransferChannel() override { return StreamingStatus::OK; }
  StreamingStatus DestroyTransferChannel() override { return StreamingStatus::OK; }
  StreamingStatus ClearTransferCheckpoint(uint64_t checkpoint_id,
                                          uint64_t checkpoint_offset) override {
    return StreamingStatus::OK;
  }
  StreamingStatus ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                         uint32_t &data_size, uint32_t timeout) override;
  StreamingStatus NotifyChannelConsumed(uint64_t offset_id) override;
};

}  // namespace streaming
}  // namespace ray
#endif
