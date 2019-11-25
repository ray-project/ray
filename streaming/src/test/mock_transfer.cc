#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"
#include "transfer.h"

#include "test/mock_transfer.h"

using namespace ray;
using namespace ray::streaming;

namespace ray {
namespace streaming {
// For mock queue transfer
struct MockQueueItem {
  uint64_t seq_id;
  uint32_t data_size;
  std::shared_ptr<uint8_t> data;
};

struct MockQueue {
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBufferImpl<MockQueueItem>>>
      message_buffer_;
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBufferImpl<MockQueueItem>>>
      consumed_buffer_;
};
static MockQueue mock_queue;

StreamingStatus MockProducer::CreateTransferChannel() {
  mock_queue.message_buffer_[channel_info.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(500);
  mock_queue.consumed_buffer_[channel_info.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(500);
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::DestroyTransferChannel() {
  mock_queue.message_buffer_.erase(channel_info.channel_id);
  mock_queue.consumed_buffer_.erase(channel_info.channel_id);
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::ProduceItemToChannel(uint8_t *data, uint32_t data_size) {
  auto &ring_buffer = mock_queue.message_buffer_[channel_info.channel_id];
  if (ring_buffer->Full()) {
    return StreamingStatus::OutOfMemory;
  }
  MockQueueItem item;
  item.seq_id = channel_info.current_seq_id + 1;
  item.data.reset(new uint8_t[data_size]);
  item.data_size = data_size;
  std::memcpy(item.data.get(), data, data_size);
  ring_buffer->Push(item);
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                                     uint32_t &data_size,
                                                     uint32_t timeout) {
  auto &channel_id = channel_info.channel_id;
  if (mock_queue.message_buffer_.find(channel_id) == mock_queue.message_buffer_.end()) {
    return StreamingStatus::NoSuchItem;
  }

  if (mock_queue.message_buffer_[channel_id]->Empty()) {
    return StreamingStatus::NoSuchItem;
  }
  MockQueueItem item = mock_queue.message_buffer_[channel_id]->Front();
  mock_queue.message_buffer_[channel_id]->Pop();
  mock_queue.consumed_buffer_[channel_id]->Push(item);
  offset_id = item.seq_id;
  data = item.data.get();
  data_size = item.data_size;
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::NotifyChannelConsumed(uint64_t offset_id) {
  auto &channel_id = channel_info.channel_id;
  auto &ring_buffer = mock_queue.consumed_buffer_[channel_id];
  while (!ring_buffer->Empty() && ring_buffer->Front().seq_id <= offset_id) {
    ring_buffer->Pop();
  }
  return StreamingStatus::OK;
}
}  // namespace streaming
}  // namespace ray

TEST(StreamingMockTransfer, mock_produce_consume) {
  std::shared_ptr<Config> transfer_config;
  ObjectID channel_id = ObjectID::FromRandom();
  ProducerChannelInfo producer_channel_info;
  producer_channel_info.channel_id = channel_id;
  producer_channel_info.current_seq_id = 0;
  MockProducer producer(transfer_config, producer_channel_info);

  ConsumerChannelInfo consumer_channel_info;
  consumer_channel_info.channel_id = channel_id;
  MockConsumer consumer(transfer_config, consumer_channel_info);

  producer.CreateTransferChannel();
  uint8_t data[3] = {1, 2, 3};
  producer.ProduceItemToChannel(data, 3);
  uint8_t *data_consumed;
  uint32_t data_size_consumed;
  uint64_t data_seq_id;
  consumer.ConsumeItemFromChannel(data_seq_id, data_consumed, data_size_consumed, -1);
  EXPECT_EQ(data_size_consumed, 3);
  EXPECT_EQ(data_seq_id, 1);
  EXPECT_EQ(std::memcmp(data_consumed, data, 3), 0);
  consumer.NotifyChannelConsumed(1);

  auto status =
      consumer.ConsumeItemFromChannel(data_seq_id, data_consumed, data_size_consumed, -1);
  EXPECT_EQ(status, StreamingStatus::NoSuchItem);
}

class StreamingTransferTest : public ::testing::Test {
 public:
  StreamingTransferTest() {
    std::shared_ptr<RuntimeContext> runtime_context(new RuntimeContext());
    runtime_context->MarkMockTest();
    writer = std::make_shared<StreamingWriter>(runtime_context);
    reader = std::make_shared<StreamingReader>(runtime_context);
  }
  virtual ~StreamingTransferTest() = default;
  void InitTransfer(int channel_num = 1) {
    for (int i = 0; i < channel_num; ++i) {
      queue_vec.push_back(ObjectID::FromRandom());
    }
    std::vector<uint64_t> channel_id_vec(queue_vec.size(), 0);
    std::vector<uint64_t> queue_size_vec(queue_vec.size(), 10000);
    writer->Init(queue_vec, channel_id_vec, queue_size_vec);
    reader->Init(queue_vec, channel_id_vec, queue_size_vec, -1);
  }
  void DestroyTransfer() {
    writer.reset();
    reader.reset();
  }

 protected:
  std::shared_ptr<StreamingWriter> writer;
  std::shared_ptr<StreamingReader> reader;
  std::vector<ObjectID> queue_vec;
};

TEST_F(StreamingTransferTest, exchange_single_channel_test) {
  InitTransfer();
  writer->Run();
  uint8_t data[4] = {1, 2, 3, 0xff};
  uint32_t data_size = 4;
  writer->WriteMessageToBufferRing(queue_vec[0], data, data_size);
  std::shared_ptr<StreamingReaderBundle> msg;
  reader->GetBundle(5000, msg);
  StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
  auto &message_list = bundle_ptr->GetMessageList();
  auto &message = message_list.front();
  EXPECT_EQ(std::memcmp(message->RawData(), data, data_size), 0);
}

TEST_F(StreamingTransferTest, exchange_multichannel_test) {
  int channel_num = 4;
  InitTransfer(4);
  writer->Run();
  for (int i = 0; i < channel_num; ++i) {
    uint8_t data[4] = {1, 2, 3, (uint8_t)i};
    uint32_t data_size = 4;
    writer->WriteMessageToBufferRing(queue_vec[i], data, data_size);
    std::shared_ptr<StreamingReaderBundle> msg;
    reader->GetBundle(5000, msg);
    EXPECT_EQ(msg->from, queue_vec[i]);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
    auto &message_list = bundle_ptr->GetMessageList();
    auto &message = message_list.front();
    EXPECT_EQ(std::memcmp(message->RawData(), data, data_size), 0);
  }
}

TEST_F(StreamingTransferTest, exchange_consumed_test) {
  InitTransfer();
  writer->Run();
  uint32_t data_size = 8196;
  std::shared_ptr<uint8_t> data(new uint8_t[data_size]);
  auto func = [data, data_size](int index) { std::fill_n(data.get(), data_size, index); };

  int num = 10000;
  std::thread write_thread([this, data, data_size, &func, num]() {
    for (uint32_t i = 0; i < num; ++i) {
      func(i);
      writer->WriteMessageToBufferRing(queue_vec[0], data.get(), data_size);
    }
  });

  std::list<StreamingMessagePtr> read_message_list;
  while (read_message_list.size() < num) {
    std::shared_ptr<StreamingReaderBundle> msg;
    reader->GetBundle(5000, msg);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
    auto &message_list = bundle_ptr->GetMessageList();
    std::copy(message_list.begin(), message_list.end(),
              std::back_inserter(read_message_list));
  }
  int index = 0;
  for (auto &message : read_message_list) {
    func(index++);
    EXPECT_EQ(std::memcmp(message->RawData(), data.get(), data_size), 0);
  }
  write_thread.join();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
