#include "data_writer.h"
#include "gtest/gtest.h"

namespace ray {
namespace streaming {
void GenRandomChannelIdVector(std::vector<ObjectID> &input_ids, int n) {
  for (int i = 0; i < n; ++i) {
    input_ids.push_back(ObjectID::FromRandom());
  }
}

class MockWriter : public DataWriter {
 public:
  friend class MockWriterTest;
  MockWriter(std::shared_ptr<RuntimeContext> runtime_context)
      : DataWriter(runtime_context) {}
  void Init(const std::vector<ObjectID> &input_channel_vec) {
    output_queue_ids_ = input_channel_vec;
    for (size_t i = 0; i < input_channel_vec.size(); ++i) {
      const ChannelCreationParameter param;
      InitChannel(input_channel_vec[i], param, 0, 0xfff);
    }
    reliability_helper_ = ReliabilityHelperFactory::CreateReliabilityHelper(
        runtime_context_->GetConfig(), barrier_helper_, this, nullptr);
    event_service_ = std::make_shared<EventService>();
    runtime_context_->SetRuntimeStatus(RuntimeStatus::Running);
    event_service_->Run();
  }

  void Destroy() {
    event_service_->Stop();
    event_service_.reset();
  }

  bool IsMessageAvailableInBuffer(const ObjectID &id) {
    return DataWriter::IsMessageAvailableInBuffer(channel_info_map_[id]);
  }

  std::unordered_map<ObjectID, ProducerChannelInfo> &GetChannelInfoMap() {
    return channel_info_map_;
  };

  bool CollectFromRingBuffer(const ObjectID &id, uint64_t &buffer_remain) {
    return DataWriter::CollectFromRingBuffer(channel_info_map_[id], buffer_remain);
  }

  StreamingStatus WriteBufferToChannel(const ObjectID &id, uint64_t &buffer_remain) {
    return DataWriter::WriteBufferToChannel(channel_info_map_[id], buffer_remain);
  }

  void BroadcastBarrier(uint64_t barrier_id) {
    static const uint8_t barrier_data[] = {1, 2, 3, 4};
    DataWriter::BroadcastBarrier(barrier_id, barrier_data, 4);
  }

  uint64_t WriteMessageToBufferRing(const ObjectID &channel_id, uint8_t *data,
                                    uint32_t data_size) {
    return DataWriter::WriteMessageToBufferRing(channel_id, data, data_size);
  }
};

class MockWriterTest : public ::testing::Test {
 protected:
  virtual void SetUp() override {
    runtime_context.reset(new RuntimeContext());
    runtime_context->SetConfig(config);
    runtime_context->MarkMockTest();
    mock_writer.reset(new MockWriter(runtime_context));
  }
  virtual void TearDown() override { mock_writer->Destroy(); }

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
  StreamingConfig config;
  std::shared_ptr<MockWriter> mock_writer;
  std::vector<ObjectID> input_ids;
};

TEST_F(MockWriterTest, test_message_avaliablie_in_buffer) {
  int channel_num = 5;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  for (const auto &id : input_ids) {
    EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(id));
  }
  mock_writer->BroadcastBarrier(0);
  for (const auto &id : input_ids) {
    EXPECT_TRUE(mock_writer->IsMessageAvailableInBuffer(id));
  }
}

uint8_t data[] = {0x01, 0x02, 0x0f, 0xe, 0x00};
uint32_t data_size = 5;

TEST_F(MockWriterTest, test_write_message_to_buffer_ring) {
  int channel_num = 2;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  for (const auto &id : input_ids) {
    EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(id));
  }
  mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  EXPECT_TRUE(mock_writer->IsMessageAvailableInBuffer(input_ids[0]));
  EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(input_ids[1]));
}

TEST_F(MockWriterTest, test_collecting_buffer) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  uint64_t buffer_remain;
  mock_writer->CollectFromRingBuffer(input_ids[0], buffer_remain);
  EXPECT_TRUE(buffer_remain == 0);
  EXPECT_TRUE(mock_writer->IsMessageAvailableInBuffer(input_ids[0]));
  EXPECT_TRUE(mock_writer->GetChannelInfoMap()[input_ids[0]]
                  .writer_ring_buffer->IsTransientAvaliable());
}

TEST_F(MockWriterTest, test_write_to_transfer) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  uint64_t buffer_remain;
  EXPECT_EQ(mock_writer->WriteBufferToChannel(input_ids[0], buffer_remain),
            StreamingStatus::OK);
  EXPECT_TRUE(buffer_remain == 0);
  EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(input_ids[0]));
}

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
