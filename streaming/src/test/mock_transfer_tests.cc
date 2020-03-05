#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"

using namespace ray;
using namespace ray::streaming;

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
    runtime_context = std::make_shared<RuntimeContext>();
    runtime_context->MarkMockTest();
    writer = std::make_shared<DataWriter>(runtime_context);
    reader = std::make_shared<DataReader>(runtime_context);
  }
  virtual ~StreamingTransferTest() = default;
  void InitTransfer(int channel_num = 1) {
    for (int i = 0; i < channel_num; ++i) {
      queue_vec.push_back(ObjectID::FromRandom());
    }
    std::vector<uint64_t> channel_id_vec(queue_vec.size(), 0);
    std::vector<uint64_t> queue_size_vec(queue_vec.size(), 10000);
    // actor ids are not used in this test, so we can just use Nil.
    std::vector<ActorID> actor_id_vec(queue_vec.size(),
                                      ActorID::NilFromJob(JobID::FromInt(0)));
    writer->Init(queue_vec, actor_id_vec, channel_id_vec, queue_size_vec);
    reader->Init(queue_vec, actor_id_vec, channel_id_vec, queue_size_vec, -1);
  }
  void DestroyTransfer() {
    writer.reset();
    reader.reset();
  }

 protected:
  std::shared_ptr<DataWriter> writer;
  std::shared_ptr<DataReader> reader;
  std::vector<ObjectID> queue_vec;
  std::shared_ptr<RuntimeContext> runtime_context;
};

TEST_F(StreamingTransferTest, exchange_single_channel_test) {
  InitTransfer();
  writer->Run();
  uint8_t data[4] = {1, 2, 3, 0xff};
  uint32_t data_size = 4;
  writer->WriteMessageToBufferRing(queue_vec[0], data, data_size);
  std::shared_ptr<DataBundle> msg;
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
    std::shared_ptr<DataBundle> msg;
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

  size_t num = 10000;
  std::thread write_thread([this, data, data_size, &func, num]() {
    for (size_t i = 0; i < num; ++i) {
      func(i);
      writer->WriteMessageToBufferRing(queue_vec[0], data.get(), data_size);
    }
  });

  std::list<StreamingMessagePtr> read_message_list;
  while (read_message_list.size() < num) {
    std::shared_ptr<DataBundle> msg;
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

TEST_F(StreamingTransferTest, flow_control_test) {
  InitTransfer();
  writer->Run();
  uint32_t data_size = 8196;
  std::shared_ptr<uint8_t> data(new uint8_t[data_size]);
  auto func = [data, data_size](int index) { std::fill_n(data.get(), data_size, index); };

  size_t num = 10000;
  std::thread write_thread([this, data, data_size, &func, num]() {
    for (size_t i = 0; i < num; ++i) {
      func(i);
      writer->WriteMessageToBufferRing(queue_vec[0], data.get(), data_size);
    }
  });
  std::unordered_map<ObjectID, ProducerChannelInfo> *writer_offset_info = nullptr;
  std::unordered_map<ObjectID, ConsumerChannelInfo> *reader_offset_info = nullptr;
  writer->GetOffsetInfo(writer_offset_info);
  reader->GetOffsetInfo(reader_offset_info);
  uint32_t writer_step = runtime_context->GetConfig().GetWriterConsumedStep();
  uint32_t reader_step = runtime_context->GetConfig().GetReaderConsumedStep();
  uint64_t &writer_current_seq_id = (*writer_offset_info)[queue_vec[0]].current_seq_id;
  uint64_t &writer_current_message_id =
      (*writer_offset_info)[queue_vec[0]].current_message_id;
  uint64_t &reader_target_seq_id =
      (*reader_offset_info)[queue_vec[0]].queue_info.target_seq_id;
  while (writer_current_seq_id < writer_step) {
    STREAMING_LOG(INFO) << "Writer currrent seq id " << writer_current_seq_id
                        << " message " << writer_current_message_id << " consumer step "
                        << writer_step;
    std::this_thread::sleep_for(
        std::chrono::milliseconds(StreamingConfig::TIME_WAIT_UINT));
  }

  std::list<StreamingMessagePtr> read_message_list;
  while (read_message_list.size() < num) {
    std::shared_ptr<DataBundle> msg;
    reader->GetBundle(5000, msg);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
    auto &message_list = bundle_ptr->GetMessageList();
    std::copy(message_list.begin(), message_list.end(),
              std::back_inserter(read_message_list));
    ASSERT_GE(writer_step, writer_current_seq_id - msg->seq_id);
    ASSERT_GE(msg->seq_id + reader_step, reader_target_seq_id);
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
