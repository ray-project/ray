#include "gtest/gtest.h"
#include "streaming_transfer.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingMockTransfer, mock_produce_consume) {
  std::shared_ptr<Config> transfer_config;
  MockProducer producer(transfer_config);
  MockConsumer consumer(transfer_config);
  ObjectID channel_id = ObjectID::FromRandom();
  ProducerChannelInfo producer_channel_info;
  producer_channel_info.channel_id = channel_id;
  producer_channel_info.current_seq_id = 0;
  ConsumerChannelInfo consumer_channel_info;
  consumer_channel_info.channel_id = channel_id;
  producer.CreateTransferChannel(producer_channel_info);
  uint8_t data[3] = {1, 2, 3};
  producer.ProduceItemToChannel(producer_channel_info, data, 3);
  uint8_t *data_consumed;
  uint32_t data_size_consumed;
  uint64_t data_seq_id;
  consumer.ConsumeItemFromChannel(consumer_channel_info, data_seq_id, data_consumed,
                                  data_size_consumed, -1);
  EXPECT_EQ(data_size_consumed, 3);
  EXPECT_EQ(data_seq_id, 1);
  EXPECT_EQ(std::memcmp(data_consumed, data, 3), 0);
  consumer.NotifyChannelConsumed(consumer_channel_info, 1);

  auto status = consumer.ConsumeItemFromChannel(consumer_channel_info, data_seq_id,
                                                data_consumed, data_size_consumed, -1);
  EXPECT_EQ(status, StreamingStatus::NoSuchItem);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
